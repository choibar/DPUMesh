#!/usr/bin/env python3
"""Stop only processes recorded by dmesh-grpc-scale-launch.sh; never SIGKILL."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('outdir')
parser.add_argument('--timeout', type=float, default=45.0)
args = parser.parse_args()
if args.timeout <= 0 or args.timeout > 120:
    parser.error('--timeout must be in (0,120] seconds')
out = Path(args.outdir).resolve()
if not out.is_dir():
    parser.error('outdir does not exist')

# Root can inspect the root proxy's executable and signal it without races
# between separate privileged readlink/kill subprocesses.
if os.geteuid() != 0:
    sys.exit(subprocess.call(['sudo', '-n', 'python3', str(Path(__file__).resolve()),
                             str(out), '--timeout', str(args.timeout)]))

bindir = Path(json.loads((out / 'launch.json').read_text())['proxy_root']) / 'target/release'
roles = ('proxy', 'mock-policy', 'mock-destination', 'mock-identity')
results = []

def identity(record):
    pid = record['pid']
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
    except (FileNotFoundError, ProcessLookupError):
        return False
    if int(fields[19]) != record['start_ticks']:
        raise RuntimeError(f'PID {pid} was reused; refusing to signal it')
    if fields[0] == 'Z':
        return False
    try:
        actual = os.readlink(f'/proc/{pid}/exe')
    except (FileNotFoundError, ProcessLookupError):
        return False
    if actual != record['exe']:
        raise RuntimeError(f'PID {pid} executable mismatch: {actual}; expected {record["exe"]}')
    return True

def stop_one(role):
    path = out / f'{role}.process.json'
    if not path.exists():
        # A just-forked supervisor may still be writing the child record.
        if (out / f'{role}.controller.pid').exists():
            for _ in range(50):
                if path.exists() or (out / f'{role}.exit').exists():
                    break
                time.sleep(0.1)
        if not path.exists():
            exit_path = out / f'{role}.exit'
            if exit_path.exists():
                return {'role': role, 'stopped': True, 'exit': int(exit_path.read_text()),
                        'ok': False, 'error': 'launch failed before recording executable identity'}
            if (out / f'{role}.controller.pid').exists():
                raise RuntimeError(f'{role}: missing child identity; no signal sent')
            return {'role': role, 'stopped': True, 'ok': True, 'status': 'not launched'}
    record = json.loads(path.read_text())
    expected = str(bindir / ('linkerd2-proxy' if role == 'proxy' else role))
    if record.get('role') != role or record.get('exe') != expected or not isinstance(record.get('pid'), int) or record['pid'] <= 1:
        raise RuntimeError(f'invalid process record: {path}')
    deadline = time.monotonic() + args.timeout
    signaled = identity(record)
    if signaled:
        # Open pidfd after identity validation and revalidate before signalling;
        # the descriptor then protects against signalling a reused PID.
        try:
            pidfd = os.pidfd_open(record['pid'])
            try:
                if identity(record):
                    signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                else:
                    signaled = False
            finally:
                os.close(pidfd)
        except ProcessLookupError:
            signaled = False  # Normal exit raced the requested shutdown.
    exit_path = out / f'{role}.exit'
    while time.monotonic() < deadline:
        alive = identity(record)
        if not alive and exit_path.exists():
            rc = int(exit_path.read_text())
            expected_exit = rc == 0 or (role != 'proxy' and rc == -signal.SIGTERM)
            return {'role': role, 'pid': record['pid'], 'sigterm_sent': signaled,
                    'stopped': True, 'exit': rc, 'ok': expected_exit}
        time.sleep(0.1)
    return {'role': role, 'pid': record['pid'], 'sigterm_sent': signaled,
            'stopped': not identity(record), 'ok': False,
            'error': 'bounded shutdown or supervisor exit-record timeout; no SIGKILL sent'}

for role in roles:
    try:
        item = stop_one(role)
    except Exception as error:
        item = {'role': role, 'ok': False, 'stopped': False, 'error': str(error)}
    results.append(item)
    print(json.dumps(item), flush=True)
    if role == 'proxy' and not item.get('stopped'):
        # Keep control-plane support available while a stuck proxy is diagnosed.
        break
result = {'ok': all(item['ok'] for item in results), 'processes': results}
(out / 'stop-result.json').write_text(json.dumps(result, indent=2) + '\n')
sys.exit(0 if result['ok'] else 1)
