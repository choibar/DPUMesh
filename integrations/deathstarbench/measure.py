#!/usr/bin/env python3
"""Run reproducible wrk2 sweeps against an already validated stack."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('run',type=Path);p.add_argument('--wrk',required=True,type=Path)
p.add_argument('--rates',default='500,2000,5000');p.add_argument('--duration',type=int,default=20)
p.add_argument('--repeats',type=int,default=3);p.add_argument('--cpus',default='12-15')
a=p.parse_args();run=a.run.resolve();meta=json.loads((run/'started.json').read_text());source=Path(meta['source'])
if a.duration<15: p.error('wrk2 calibration requires a duration of at least 15 seconds')
t=json.loads((run/'topology.json').read_text())
frontends=[int(x['env']['DSB_PORT']) for x in t['processes'] if x['service']=='frontend']
if len(frontends)!=1: p.error('this sweep requires one frontend; use independent load generators for replicated frontends')
script=(source/'wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua').read_text().replace('http://localhost:5000',f'http://127.0.0.1:{frontends[0]}')
lua=run/'mixed.lua';lua.write_text(script)
records={f.stem.split('.')[0]:json.loads(f.read_text()) for f in run.glob('*.process.json')}
hz=os.sysconf('SC_CLK_TCK')
def snapshot():
 out={}
 for name,r in records.items():
  fields=Path(f'/proc/{r["pid"]}/stat').read_text().rsplit(') ',1)[1].split()
  if int(fields[19])!=r['start_ticks']:raise RuntimeError('service PID reused')
  out[name]=int(fields[11])+int(fields[12])
 return out
results=[]
for rate in map(int,a.rates.split(',')):
 for repeat in range(1,a.repeats+1):
  tag=f'load-{rate}-r{repeat}'
  log=run/(tag+'.log')
  if log.exists():raise RuntimeError('refusing to overwrite measurement')
  cmd=['taskset','-c',a.cpus,str(a.wrk.resolve()),'-t4','-c64',f'-d{a.duration}s','-R',str(rate),'--latency','-s',str(lua),f'http://127.0.0.1:{frontends[0]}']
  before=snapshot();usage_before=resource.getrusage(resource.RUSAGE_CHILDREN);start=time.time()
  with log.open('w') as stream:
   proc=subprocess.Popen(cmd,cwd=source,stdout=stream,stderr=subprocess.STDOUT)
   rc=proc.wait(timeout=a.duration+30)
  finish=time.time();usage_after=resource.getrusage(resource.RUSAGE_CHILDREN);after=snapshot();raw=log.read_text()
  def value(pattern):
   m=re.search(pattern,raw);return m.group(1) if m else None
  rps=value(r'Requests/sec:\s+([\d.]+)')
  errors=value(r'Non-2xx or 3xx responses:\s+(\d+)')
  row=dict(rate=rate,repeat=repeat,start=start,end=finish,exit=rc,rps=float(rps) if rps else None,
    p50=value(r'^\s*50\.000%\s+(\S+)'),p95=value(r'^\s*95\.000%\s+(\S+)'),p99=value(r'^\s*99\.000%\s+(\S+)'),
    non2xx=int(errors or 0),socket_errors=value(r'Socket errors:\s+([^\n]+)'),
    loadgen_cpu=100*((usage_after.ru_utime+usage_after.ru_stime)-(usage_before.ru_utime+usage_before.ru_stime))/(finish-start),
    host_cpu={n:100*(after[n]-v)/hz/(finish-start) for n,v in before.items()},command=cmd)
  # Percentile rows are multiline; retain raw logs as source of truth.
  for q in ('50','95','99'):
   m=re.search(r'^\s*'+q+r'\.000%\s+(\S+)',raw,re.M)
   row['p'+q]=m.group(1) if m else None
  m=re.search(r'^\s*([\d.]+)\s+0\.950000\s+',raw,re.M)
  row['p95']=(m.group(1)+'ms') if m else row['p95']
  row['ok']=rc==0 and row['rps'] is not None and not row['non2xx'] and not row['socket_errors']
  results.append(row);(run/'measurements.json').write_text(json.dumps(results,indent=2)+'\n')
  print(json.dumps(row),flush=True)
  if not row['ok']:raise SystemExit('invalid measurement; inspect log')
