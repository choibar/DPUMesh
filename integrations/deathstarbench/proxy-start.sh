#!/usr/bin/env bash
# Isolated sharded proxy for the HotelReservation integration.
# Usage: proxy-start.sh <1|2|4|8|12|16> <output-directory>
set -euo pipefail
if [[ $# != 2 || ! $1 =~ ^(1|2|4|8|12|16)$ ]]; then
  echo "usage: $0 <1|2|4|8|12|16> <output-directory>" >&2
  exit 2
fi
INTEGRATION_DIR="$(cd "$(dirname "$0")" && pwd)"
export DSB_INTEGRATION_DIR="$INTEGRATION_DIR"
export DSB_PROXY_ROOT="${DSB_PROXY_ROOT:-$(cd "$INTEGRATION_DIR/../../linkerd2-proxy" && pwd)}"
cd "$DSB_PROXY_ROOT"
source scripts/dev-proxy-env.sh >/dev/null
# Do not inherit experimental datapath modes from a previous benchmark shell.
for dmesh_key in "${!DMESH_@}"; do unset "$dmesh_key"; done
export LINKERD2_PROXY_LOG="${DSB_PROXY_LOG:-warn}"
export LINKERD2_PROXY_DOCA_DEV_PCI_ADDR="${DSB_DPU_PCI:-03:00.1}"
export LINKERD2_PROXY_DOCA_REP_PCI_ADDR="${DSB_HOST_PCI:-0b:00.1}"
export LINKERD2_PROXY_DOCA_SERVER_NAME=DPUMesh0
if [[ -n "${DSB_ROUTES_FILE:-}" ]]; then
  unset MOCK_POLICY_ECHO_TARGET MOCK_OUTBOUND_OPAQUE MOCK_DEST_OPAQUE
  export DMESH_ROUTES="$(realpath "$DSB_ROUTES_FILE")"
  export DMESH_SHARD_ROUTES="${DSB_SHARD_ROUTES:-1}"
else
  export MOCK_POLICY_ECHO_TARGET=1
fi
export DMESH_SHARDED=1
export DMESH_NUM_WORKERS="$1"
export DMESH_FLOW_PLACEMENT="${DSB_FLOW_PLACEMENT:-least-flows}"
case "${DSB_PROXY_POLL_MODE:-busy}" in
  busy) export DMESH_BUSY_POLL=1 ;;
  event) export DMESH_BUSY_POLL=0 ;;
  *) echo 'DSB_PROXY_POLL_MODE must be busy or event' >&2; exit 2 ;;
esac
export LINKERD2_PROXY_CORES=1
unset LINKERD2_PROXY_CORES_MIN LINKERD2_PROXY_CORES_MAX LINKERD2_PROXY_CORES_MAX_RATIO
export LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991
export LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143
unset DMESH_NO_TEARDOWN DMESH_SELECTIVE_H2
exec python3 - "$1" "$2" <<'PY'
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

cores = int(sys.argv[1])
out = Path(sys.argv[2]).resolve()
root = Path(os.environ['DSB_PROXY_ROOT'])
bindir = root / 'target/release'
roles = ('mock-identity', 'mock-destination', 'mock-policy', 'proxy')
executables = {role: str(bindir / ('linkerd2-proxy' if role == 'proxy' else role)) for role in roles}
arm_cores = cores
cpu_mask = os.environ.get('DSB_PROXY_CPU_MASK') or ('15' if arm_cores == 1 else f'{16-arm_cores}-15')
stop_script = str(Path(os.environ['DSB_INTEGRATION_DIR']) / 'proxy-stop.py')

def write_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)

def process_record(role, pid):
    stat = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
    return {'role': role, 'pid': pid, 'start_ticks': int(stat[19]), 'exe': executables[role]}

# The root child writes its real PID before exec; Popen's PID is sudo's PID.
proxy_child = r'''
import json, os, pathlib, sys
out, exe, mask = sys.argv[1:]
out = pathlib.Path(out)
pid = os.getpid()
stat = pathlib.Path('/proc/self/stat').read_text().split(') ', 1)[1].split()
(out / 'proxy.pid').write_text(str(pid) + '\n')
temporary = out / 'proxy.process.json.tmp'
temporary.write_text(json.dumps({'role': 'proxy', 'pid': pid, 'start_ticks': int(stat[19]), 'exe': exe}, indent=2) + '\n')
temporary.replace(out / 'proxy.process.json')
os.execvp('taskset', ['taskset', '-c', mask, exe])
'''

def spawn(role):
    controller = os.fork()
    if controller:
        (out / f'{role}.controller.pid').write_text(str(controller) + '\n')
        return
    os.setsid()
    null = os.open('/dev/null', os.O_RDWR)
    for fd in (0, 1, 2):
        os.dup2(null, fd)
    if null > 2:
        os.close(null)
    rc = 127
    try:
        with (out / f'{role}.log').open('w') as log:
            if role == 'proxy':
                command = ['sudo', '-n', '-E', 'python3', '-c', proxy_child,
                           str(out), executables[role], cpu_mask]
            else:
                command = ['taskset', '-c', '0-3', executables[role]]
            child = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT)
            if role != 'proxy':
                (out / f'{role}.pid').write_text(str(child.pid) + '\n')
                write_json(out / f'{role}.process.json', process_record(role, child.pid))
            rc = child.wait()
    except BaseException as error:
        (out / f'{role}.controller-error.txt').write_text(repr(error) + '\n')
    (out / f'{role}.exit').write_text(str(rc) + '\n')
    os._exit(0)

def check_alive(expected_roles):
    for role in expected_roles:
        exit_file = out / f'{role}.exit'
        if exit_file.exists():
            raise RuntimeError(f'{role} exited during startup: rc={exit_file.read_text().strip()}; see {role}.log')

def port_ready(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.2):
            return True
    except OSError:
        return False

spawned = False
try:
    if os.environ.get('DSB_ROUTES_FILE'):
        routes = json.loads(Path(os.environ['DSB_ROUTES_FILE']).read_text())
        actual_mode = 'busy' if os.environ['DMESH_BUSY_POLL'] == '1' else 'event'
        if routes.get('poll_mode', actual_mode) != actual_mode:
            raise RuntimeError('topology poll_mode differs from DSB_PROXY_POLL_MODE')
    for exe in executables.values():
        if not os.access(exe, os.X_OK):
            raise RuntimeError(f'missing executable: {exe}')
    subprocess.run(['sudo', '-n', 'true'], check=True)
    # Refuse existing relevant processes; never stop someone else's run.
    processes = subprocess.check_output(['ps', '-eo', 'pid=,comm='], text=True)
    conflicts = []
    relevant_names = {name[:15] for name in ('linkerd2-proxy', 'mock-identity', 'mock-destination', 'mock-policy', 'dpumesh_dpu')}
    for line in processes.splitlines():
        fields = line.split(None, 1)
        if len(fields) == 2 and fields[1] in relevant_names:
            conflicts.append(line.strip())
    if conflicts:
        raise RuntimeError('existing DPUMesh/control-plane processes: ' + '; '.join(conflicts))
    # A different executable could already own one of the required sockets.
    probes = []
    try:
        for port in (8087, 8088, 8089, 4140, 4991, 5143):
            probe = socket.socket()
            probes.append(probe)
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(('127.0.0.1', port))
    finally:
        for probe in probes:
            probe.close()
    out.mkdir(parents=True, exist_ok=True)
    if any((out / f'{role}.{suffix}').exists() for role in roles
           for suffix in ('pid', 'process.json', 'exit', 'log', 'controller.pid')):
        raise RuntimeError('output directory already contains lifecycle files; use a fresh directory')
    config = {'cores': cores, 'workers': cores, 'proxy_cpu_mask': cpu_mask,
              'sharded': True, 'busy_poll': os.environ['DMESH_BUSY_POLL'],
              'poll_mode': 'busy' if os.environ['DMESH_BUSY_POLL'] == '1' else 'event',
              'mock_cpu_mask': '0-3', 'expected_preallocated_dpa_threads': 32 * cores,
              'dpa_threads_per_worker': 32, 'flow_slots_per_worker': 32,
              'main_runtime_cores': 1, 'proxy_root': str(root), 'native_teardown': True, 'reverse_mode': 'dpu-dma'}
    write_json(out / 'launch.json', config)
    print(json.dumps({'event': 'launching', **config}), flush=True)
    for role in roles[:-1]:
        spawn(role)
        spawned = True
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        check_alive(roles[:-1])
        if all(port_ready(port) for port in (8087, 8088, 8089)):
            break
        time.sleep(0.1)
    else:
        raise RuntimeError('mock control-plane readiness timeout')
    spawn('proxy')
    deadline = time.monotonic() + max(60, cores * 15)
    failure = re.compile(r'DOCA comch initialization failure|Failed to (?:create|start|set) DPA|\[DOCA\]\[ERR\]', re.I)
    while time.monotonic() < deadline:
        check_alive(roles)
        path = out / 'proxy.log'
        log = path.read_text(errors='replace') if path.exists() else ''
        if failure.search(log):
            raise RuntimeError(f'DOCA/DPA initialization error; see {path}; requested {32 * cores} preallocated DPA threads')
        servers = set(re.findall(r'Started DOCA comch server (DPUMesh\d+) ', log))
        pools = len(re.findall(r'Created DPA thread pool with 32 threads', log))
        if servers == {f'DPUMesh{i}' for i in range(cores)} and pools == cores and port_ready(4991):
            # Worker names/affinity are observable even with Rust WARN logging.
            pid = int((out / 'proxy.pid').read_text())
            threads = {}
            for task in Path(f'/proc/{pid}/task').iterdir():
                try:
                    status = task.joinpath('status').read_text()
                except FileNotFoundError:
                    continue
                name = re.search(r'^Name:\s*(.+)$', status, re.M).group(1)
                allowed = re.search(r'^Cpus_allowed_list:\s*(.+)$', status, re.M).group(1)
                threads[int(task.name)] = {'name': name, 'cpu_mask': allowed}
            # SDK helper threads inherit their creating shard's comm name.
            names = {item['name'] for item in threads.values()}
            if not {f'dmesh-shard-{i}' for i in range(cores)} <= names:
                time.sleep(0.2)
                continue
            write_json(out / 'affinity.json', {'proxy_pid': pid, 'threads': threads})
            print(json.dumps({'event': 'ready', 'proxy_pid': pid, 'servers': sorted(servers),
                              'dpa_pools': pools, 'outdir': str(out)}), flush=True)
            sys.exit(0)
        time.sleep(0.2)
    raise RuntimeError(f'proxy readiness timeout; requested {cores} workers / {32 * cores} DPA threads; inspect proxy.log')
except Exception as error:
    print(f'LAUNCH_FAILED: {error}', file=sys.stderr, flush=True)
    if spawned:
        # The scoped cleanup validates PID birth time and executable first.
        cleanup = subprocess.run(['python3', stop_script, str(out), '--timeout', '30'])
        if cleanup.returncode:
            print('Scoped cleanup did not complete; no SIGKILL was sent. Inspect stop-result.json.', file=sys.stderr)
    sys.exit(1)
PY
