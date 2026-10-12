#!/usr/bin/env python3
"""One gRPC connection, optionally many RPCs, with per-replica counter evidence."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('run',type=Path)
p.add_argument('--service',default='srv-user')
p.add_argument('--endpoint',default='')
p.add_argument('--calls',type=int,default=1)
p.add_argument('--parallel',type=int,default=1)
p.add_argument('--worker',type=int,default=0)
p.add_argument('--tag',required=True)
p.add_argument('--require-spread',action='store_true')
a=p.parse_args(); run=a.run.resolve()
if Path(a.tag).name!=a.tag or a.tag in ('.','..'):p.error('tag must be a filename')
t=json.loads((run/'topology.json').read_text()); start=json.loads((run/'started.json').read_text())
if a.service not in t['services'] or not 0<=a.worker<t['workers']:p.error('unknown service/worker')
if a.require_spread and (a.service!='srv-user' or a.endpoint):p.error('spread requires the user VIP business probe')
out=run/(a.tag+'.json')
if out.exists():p.error('use a new tag')
source=Path(start['source']); native=start['native']
env={k:v for k,v in os.environ.items() if not k.startswith(('DSB_','DPUMESH_','DMESH_'))}
env.update(DSB_TRANSPORT=start['mode'],DSB_TOPOLOGY=str(run/'topology.json'),DPUMESH_CONFIG=str(run/'registry'),
 DPUMESH_SERVER=f'DPUMesh{a.worker}',DPUMESH_PCI_ADDR=t['processes'][0]['env']['DPUMESH_PCI_ADDR'],
 DPUMESH_POD_IP='10.81.250.1',DPUMESH_WORKLOAD='routing-probe',DPUMESH_REVERSE='dpu-dma',
 GOMAXPROCS='2',TLS='false',LD_LIBRARY_PATH=native+':/opt/mellanox/doca/lib/x86_64-linux-gnu:/opt/mellanox/flexio/lib')
def snapshot():
 return {f.stem:json.loads(f.read_text()) for f in (run/'stats').glob('*.json')}
time.sleep(.6);before=snapshot()
cmd=[str(source/'bin/dmesh-probe'),'-service',a.service,'-calls',str(a.calls),'-parallel',str(a.parallel)]
if a.endpoint:cmd+=['-endpoint',a.endpoint]
if a.service=='srv-user':cmd+=['-login']
r=subprocess.run(cmd,cwd=source,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=45)
(run/(a.tag+'.log')).write_text(r.stdout)
time.sleep(.6);after=snapshot();counts={}
for e in t['services'][a.service]['endpoints']:
 key='hotel-'+e['id'];n=0
 for method,m in after.get(key,{}).get('methods',{}).items():
  if method.startswith('/grpc.health.'):continue
  n+=m['calls']-before.get(key,{}).get('methods',{}).get(method,{}).get('calls',0)
 counts[e['id']]=n
spread=all(counts[e['id']]>0 for e in t['services'][a.service]['endpoints'] if e['enabled'])
result=dict(ok=r.returncode==0 and (not a.require_spread or spread),exit=r.returncode,
 command=cmd,worker=a.worker,calls_by_replica=counts,before=before,after=after)
out.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ('before','after')}))
raise SystemExit(not result['ok'])
