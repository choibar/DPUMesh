#!/usr/bin/env python3
"""Host CPU sampling with idle or external wrk2 load; optional separate perf diagnostic."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('run', type=Path)
p.add_argument('--tag', required=True)
p.add_argument('--rate', type=int, default=0, help='zero measures idle')
p.add_argument('--duration', type=int, default=30)
p.add_argument('--loadgen', default='r4')
p.add_argument('--target', required=True)
p.add_argument('--frontend-port', type=int, help='one shared frontend/NodePort endpoint; launch exactly one wrk process')
p.add_argument('--remote-root', required=True)
p.add_argument('--wrk', required=True)
p.add_argument('--cpus', default='0-3')
p.add_argument('--connections', type=int, default=64)
p.add_argument('--threads', type=int, default=1, help='wrk threads per frontend')
p.add_argument('--distribution', choices=['fixed', 'exp', 'norm', 'zipf'],
               help='requires the DSB wrk2 fork; omit for stock wrk2')
p.add_argument('--infra', required=True, type=Path)
p.add_argument('--project', required=True)
p.add_argument('--profile', action='store_true')
a = p.parse_args()
if Path(a.tag).name != a.tag or a.tag in ('.', '..'):
    p.error('tag must be a filename')
if a.duration < 20 or a.rate < 0 or a.connections < 4 or not 1 <= a.threads <= a.connections:
    p.error('duration >=20, rate >=0, connections >=4, 1<=threads<=connections required')
if a.frontend_port is not None and not 1 <= a.frontend_port <= 65535:
    p.error('frontend-port must be between 1 and 65535')
run = a.run.resolve()
out = run / a.tag
out.mkdir(exist_ok=False)
meta = json.loads((run / 'started.json').read_text())
topology = json.loads((run / 'topology.json').read_text())
records = {f.name.removesuffix('.process.json'): json.loads(f.read_text()) for f in run.glob('*.process.json')}
app_pids = ','.join(str(r['pid']) for r in records.values())
compose = ['docker', 'compose', '-p', a.project, '-f', str(a.infra / 'compose.json')]
ids = subprocess.check_output(compose + ['ps', '-q'], text=True).split()
for obj in json.loads(subprocess.check_output(['docker', 'inspect'] + ids)):
    assert obj['Config']['Labels']['com.docker.compose.project'] == a.project
    pid = obj['State']['Pid']
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
    records['infra:' + obj['Name'].lstrip('/')] = {'pid': pid, 'start_ticks': int(fields[19])}
hz = os.sysconf('SC_CLK_TCK')
def snapshot():
    processes = {}
    for name, r in records.items():
        s = Path(f'/proc/{r["pid"]}/stat').read_text().rsplit(') ', 1)[1].split()
        if int(s[19]) != r['start_ticks'] or s[0] == 'Z':
            raise RuntimeError('service exited or PID reused: ' + name)
        processes[name] = {'user': int(s[11]), 'system': int(s[12]), 'rss_pages': int(s[21])}
    cpus = {s.split()[0]: list(map(int, s.split()[1:])) for s in Path('/proc/stat').read_text().splitlines() if s.startswith('cpu')}
    network = {s.split(':', 1)[0].strip(): list(map(int, s.split(':', 1)[1].split()))
               for s in Path('/proc/net/dev').read_text().splitlines() if ':' in s}
    return dict(time=time.time(), processes=processes, cpus=cpus, network=network)
def rpc_snapshot():
    return {f.stem: json.loads(f.read_text()) for f in (run / 'stats').glob('*.json')}

rpc_before = rpc_snapshot()
load = None
profiles = []
samples = []
scheduled = time.time() + (2 if a.rate else 0)
profile_start = scheduled + 3
try:
    if a.rate:
        fes = [x for x in topology['processes'] if x['service'] == 'frontend']
        if a.frontend_port is not None:
            clients = [dict(name='nodeport', url=f'http://{a.target}:{a.frontend_port}', rate=a.rate)]
        else:
            if a.rate < len(fes):
                raise ValueError('rate must reach every frontend')
            clients = [dict(name=f['name'], url='http://' + a.target + ':' + f['env']['DSB_PORT'],
                            rate=a.rate // len(fes) + (i < a.rate % len(fes))) for i, f in enumerate(fes)]
        job = dict(output=a.remote_root + '/' + run.name + '/' + a.tag,
                   wrk=a.wrk, cpus=a.cpus, duration=a.duration, connections=a.connections,
                   threads=a.threads, distribution=a.distribution,
                   start_at=scheduled,
                   script=(Path(meta['source']) / 'wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua').read_text(),
                   clients=clients)
        (out / 'remote-job.json').write_text(json.dumps(job, indent=2))
        stream = (out / 'remote-result.json').open('w')
        stderr = (out / 'remote-stderr.log').open('w')
        command = ['ssh', '-o', 'BatchMode=yes', a.loadgen,
                   shlex.join(['python3', a.remote_root + '/remote-load.py'])]
        load = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=stream, stderr=stderr, text=True)
        load.stdin.write(json.dumps(job))
        load.stdin.close()
    deadline = scheduled + a.duration + (25 if load else 0)
    profiled = False
    while True:
        now = time.time()
        if now >= deadline:
            if load and load.poll() is None:
                raise TimeoutError('remote measurement deadline')
            break
        samples.append(snapshot())
        if a.profile and not profiled and now >= profile_start:
            profiled = True
            seconds = min(20, a.duration - 5)
            cmds = [
                ['sudo', '-n', 'perf', 'record', '-e', 'cpu-clock', '-F', '99', '--call-graph', 'fp', '-p', app_pids,
                 '-o', str(out / 'perf.data'), '--', 'sleep', str(seconds)],
                ['sudo', '-n', 'perf', 'stat', '-x', ';', '-e', 'task-clock,cycles,instructions,context-switches,cpu-migrations',
                 '-p', app_pids, '-o', str(out / 'perf-stat.csv'), '--', 'sleep', str(seconds)],
            ]
            (out / 'perf-commands.json').write_text(json.dumps(cmds, indent=2))
            for i, cmd in enumerate(cmds):
                f = (out / f'perf-{i}.log').open('w')
                profiles.append((subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT), f))
        if load and load.poll() is not None:
            break
        time.sleep(.5)
finally:
    if load:
        # The remote runner has its own duration/deadline and child cleanup.
        if load.poll() is None:
            load.wait(timeout=a.duration + 30)
        stream.close()
        stderr.close()
    for proc, f in profiles:
        proc.wait(timeout=30)
        f.close()
    if profiles:
        files = [str(out / name) for name in ('perf.data', 'perf-stat.csv') if (out / name).exists()]
        if files:
            subprocess.run(['sudo', '-n', 'chown', f'{os.getuid()}:{os.getgid()}'] + files, check=True)
samples.append(snapshot())
(out / 'samples.json').write_text(json.dumps(samples))
time.sleep(1)
(out / 'rpc-before.json').write_text(json.dumps(rpc_before))
(out / 'rpc-after.json').write_text(json.dumps(rpc_snapshot()))
if load:
    if load.returncode:
        raise RuntimeError('remote runner failed; inspect remote-stderr.log')
    result = json.loads((out / 'remote-result.json').read_text())
    for c in result['clients']:
        (out / (c['name'] + '.log')).write_text(c.pop('raw'))
    if any(c['exit'] != 0 or c['rps'] is None for c in result['clients']):
        raise RuntimeError('wrk failed to start or finish; inspect frontend logs and remote-result.json')
    start, end = result['start'], result['end']
else:
    start, end = scheduled, scheduled + a.duration
    result = dict(ok=True, rps=0, clients=[], start=start, end=end)
window = [s for s in samples if start <= s['time'] <= end]
if len(window) < 2:
    raise RuntimeError('insufficient samples or loadgen/host clocks disagree')
first, last = window[0], window[-1]
dt = last['time'] - first['time']
cpu = {name: {kind: 100 * (v[kind] - first['processes'][name][kind]) / hz / dt
             for kind in ('user', 'system')} for name, v in last['processes'].items()}
result.update(offered_rps=a.rate, duration=a.duration, profile=a.profile,
              profile_exits=[proc.returncode for proc, _ in profiles],
              cpu_window_seconds=dt, process_cpu=cpu,
              host_service_cpu=sum(sum(v.values()) for k, v in cpu.items() if not k.startswith('infra:')),
              infra_cpu=sum(sum(v.values()) for k, v in cpu.items() if k.startswith('infra:')),
              max_frontend_p99_ms=max((c['p99_ms'] for c in result['clients'] if c['p99_ms'] is not None), default=None))
result['host_service_cpu_us_per_http'] = result['host_service_cpu'] / 100 * 1e6 / result['rps'] if result['rps'] else None
(out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: v for k, v in result.items() if k not in ('clients', 'process_cpu', 'cpus_before', 'cpus_after')}))
if any(proc.returncode for proc, _ in profiles):
    raise SystemExit('perf diagnostic failed')
