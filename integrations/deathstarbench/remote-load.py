#!/usr/bin/env python3
"""Run a bounded wrk2 batch on the load generator; read the job as JSON on stdin."""
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import time

job = json.load(sys.stdin)
out = Path(job['output'])
out.mkdir(parents=True, exist_ok=False)
(out / 'job.json').write_text(json.dumps(job, indent=2))
children = []
start = None
usage0 = resource.getrusage(resource.RUSAGE_CHILDREN)
def cpu_snapshot():
    return {s.split()[0]: list(map(int, s.split()[1:]))
            for s in Path('/proc/stat').read_text().splitlines() if s.startswith('cpu')}
try:
    for item in job['clients']:
        lua = out / (item['name'] + '.lua')
        lua.write_text(job['script'].replace('http://localhost:5000', item['url']))
    time.sleep(max(0, job['start_at'] - time.time()))
    start = time.time()
    cpu_before = cpu_snapshot()
    for item in job['clients']:
        cmd = ['taskset', '-c', job['cpus'], job['wrk'], '-t' + str(job.get('threads', 1)),
               '-c' + str(job['connections']), '-d' + str(job['duration']) + 's',
               '-R', str(item['rate']), '-L', '-s',
               str(out / (item['name'] + '.lua')), item['url']]
        if job.get('distribution'):
            cmd[4:4] = ['-D', job['distribution']]
        log = out / (item['name'] + '.log')
        stream = log.open('w')
        proc = subprocess.Popen(cmd, stdout=stream, stderr=subprocess.STDOUT)
        birth = int(Path(f'/proc/{proc.pid}/stat').read_text().rsplit(') ', 1)[1].split()[19])
        (out / (item['name'] + '.process.json')).write_text(json.dumps(
            {'pid': proc.pid, 'start_ticks': birth, 'command': cmd}))
        children.append((item, proc, stream, log, cmd))
    deadline = time.monotonic() + job['duration'] + 20
    while any(p.poll() is None for _, p, _, _, _ in children):
        if time.monotonic() >= deadline:
            raise TimeoutError('remote wrk deadline')
        time.sleep(.1)
finally:
    for _, proc, stream, _, _ in children:
        if proc.poll() is None:
            proc.terminate()
        proc.wait(timeout=10)
        stream.close()
end = time.time()
cpu_after = cpu_snapshot()
usage1 = resource.getrusage(resource.RUSAGE_CHILDREN)
clients = []
for item, proc, _, log, cmd in children:
    raw = log.read_text()
    def value(pattern):
        m = re.search(pattern, raw, re.M)
        return m.group(1) if m else None
    rps = value(r'Requests/sec:\s+([\d.]+)')
    counts = {k: int(v) for k, v in re.findall(
        r'(connect|read|write|timeout) (\d+)', value(r'Socket errors:\s+([^\n]+)') or '')}
    c = dict(name=item['name'], command=cmd, offered_rps=item['rate'],
             rps=float(rps) if rps else None, exit=proc.returncode,
             non2xx=int(value(r'Non-2xx or 3xx responses:\s+(\d+)') or 0),
             socket_errors=counts, raw=raw)
    for q in ('50', '99'):
        v = value(r'^\s*' + q + r'\.000%\s+(\S+)')
        m = re.fullmatch(r'([\d.]+)(us|ms|s)', v or '')
        c['p' + q + '_ms'] = float(m[1]) * {'us': .001, 'ms': 1, 's': 1000}[m[2]] if m else None
    c['ok'] = proc.returncode == 0 and c['rps'] is not None and not c['non2xx'] and not any(counts.values())
    clients.append(c)
result = dict(start=start, end=end, clients=clients, hostname=os.uname().nodename,
              cpus_before=cpu_before, cpus_after=cpu_after,
              cpu_seconds=usage1.ru_utime + usage1.ru_stime - usage0.ru_utime - usage0.ru_stime,
              rps=sum(c['rps'] or 0 for c in clients), ok=all(c['ok'] for c in clients))
(out / 'result.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result))
