#!/usr/bin/env python3
"""Simultaneous wrk2 clients for all frontends; sum RPS, retain per-client latency."""
import argparse
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('run',type=Path);p.add_argument('--wrk',required=True,type=Path)
p.add_argument('--rate',type=int,required=True,help='total offered RPS, divided across frontends')
p.add_argument('--duration',type=int,default=40);p.add_argument('--tag',required=True)
p.add_argument('--cpus',default='12-15');p.add_argument('--connections',type=int,default=64,help='per frontend')
p.add_argument('--infra',type=Path);p.add_argument('--project',default='dpumesh-scale')
a=p.parse_args()
if a.duration<20 or a.rate<1 or a.connections<4:p.error('duration >=20, rate >0, connections >=4 required')
run=a.run.resolve();meta=json.loads((run/'started.json').read_text());source=Path(meta['source'])
t=json.loads((run/'topology.json').read_text());fes=[x for x in t['processes'] if x['service']=='frontend']
if a.rate<len(fes):p.error('rate must reach every frontend')
out=run/a.tag;out.mkdir(exist_ok=False)
records={f.stem.split('.')[0]:json.loads(f.read_text()) for f in run.glob('*.process.json')}
if a.infra:
 compose=['docker','compose','-p',a.project,'-f',str(a.infra.resolve()/'compose.json')]
 ids=subprocess.check_output(compose+['ps','-q'],text=True).split()
 for obj in json.loads(subprocess.check_output(['docker','inspect']+ids)):
  if obj['Config']['Labels'].get('com.docker.compose.project')!=a.project:raise RuntimeError('infra owner mismatch')
  pid=obj['State']['Pid'];st=Path(f'/proc/{pid}/stat').read_text().rsplit(') ',1)[1].split()
  records['infra:'+obj['Name'].lstrip('/')]={'pid':pid,'start_ticks':int(st[19])}
hz=os.sysconf('SC_CLK_TCK')
def snapshot():
 out={}
 for name,r in records.items():
  st=Path(f'/proc/{r["pid"]}/stat').read_text().rsplit(') ',1)[1].split()
  if int(st[19])!=r['start_ticks'] or st[0]=='Z':raise RuntimeError(f'service exited/PID reused: {name}')
  out[name]={'ticks':int(st[11])+int(st[12]),'rss_pages':int(st[21])}
 cpus={v.split()[0]:list(map(int,v.split()[1:])) for v in Path('/proc/stat').read_text().splitlines() if v.startswith('cpu')}
 return {'time':time.time(),'processes':out,'cpus':cpus}
def millis(value):
 m=re.fullmatch(r'([\d.]+)(us|ms|s)',value or '')
 return float(m[1])*{'us':.001,'ms':1,'s':1000}[m[2]] if m else None
script=(source/'wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua').read_text()
children=[];samples=[];before=snapshot();usage0=resource.getrusage(resource.RUSAGE_CHILDREN)
try:
 for i,fe in enumerate(fes):
  port=fe['env']['DSB_PORT'];lua=out/(fe['name']+'.lua')
  lua.write_text(script.replace('http://localhost:5000','http://127.0.0.1:'+port))
  rate=a.rate//len(fes)+(i<a.rate%len(fes));threads=max(1,4//len(fes))
  cmd=['taskset','-c',a.cpus,str(a.wrk.resolve()),f'-t{threads}',f'-c{a.connections}',f'-d{a.duration}s','-R',str(rate),'--latency','-s',str(lua),'http://127.0.0.1:'+port]
  log=out/(fe['name']+'.log');stream=log.open('w')
  proc=subprocess.Popen(cmd,cwd=source,stdout=stream,stderr=subprocess.STDOUT)
  children.append(dict(name=fe['name'],proc=proc,stream=stream,log=log,command=cmd,rate=rate,start=time.time()))
 deadline=time.monotonic()+a.duration+30
 while any(x['proc'].poll() is None for x in children):
  if time.monotonic()>deadline:raise TimeoutError('load generator deadline')
  samples.append(snapshot());time.sleep(1)
finally:
 for x in children:
  if x['proc'].poll() is None:x['proc'].terminate()
  x['proc'].wait(timeout=10);x['stream'].close()
after=snapshot();usage1=resource.getrusage(resource.RUSAGE_CHILDREN);dt=after['time']-before['time'];clients=[]
for x in children:
 raw=x['log'].read_text()
 def value(pattern):
  m=re.search(pattern,raw,re.M);return m.group(1) if m else None
 rps=value(r'Requests/sec:\s+([\d.]+)');errors=value(r'Socket errors:\s+([^\n]+)')
 counts={k:int(v) for k,v in re.findall(r'(connect|read|write|timeout) (\d+)',errors or '')}
 client={'name':x['name'],'offered_rps':x['rate'],'rps':float(rps) if rps else None,'exit':x['proc'].returncode,
  'non2xx':int(value(r'Non-2xx or 3xx responses:\s+(\d+)') or 0),'socket_errors':counts,'command':x['command'],'start':x['start']}
 for q in ('50','99'):client['p'+q+'_ms']=millis(value(r'^\s*'+q+r'\.000%\s+(\S+)'))
 v=value(r'^\s*([\d.]+)\s+0\.950000\s+');client['p95_ms']=float(v) if v else None
 client['ok']=client['exit']==0 and client['rps'] is not None and not client['non2xx'] and not any(counts.values())
 clients.append(client)
row={'offered_rps':a.rate,'duration':a.duration,'start':before['time'],'end':after['time'],'frontends':len(fes),'connections_per_frontend':a.connections,
 'rps':sum(x['rps'] or 0 for x in clients),'ok':all(x['ok'] for x in clients),'clients':clients,
 'loadgen_cpu':100*(usage1.ru_utime+usage1.ru_stime-usage0.ru_utime-usage0.ru_stime)/dt,
 'process_cpu':{n:100*(after['processes'][n]['ticks']-v['ticks'])/hz/dt for n,v in before['processes'].items()}}
row['rate_fraction']=row['rps']/a.rate
row['max_frontend_p99_ms']=max((x['p99_ms'] for x in clients if x['p99_ms'] is not None),default=None)
row['host_service_cpu']=sum(v for k,v in row['process_cpu'].items() if not k.startswith('infra:'))
row['infra_cpu']=sum(v for k,v in row['process_cpu'].items() if k.startswith('infra:'))
row['meets_offered_rate']=row['ok'] and row['rate_fraction']>=.98
# Same-window RPS needs no averaging of percentiles. Preserve each raw HDR table.
(out/'samples.json').write_text(json.dumps([before]+samples+[after]))
(out/'result.json').write_text(json.dumps(row,indent=2)+'\n')
print(json.dumps({k:v for k,v in row.items() if k not in ('clients','process_cpu')}),flush=True)
