#!/usr/bin/env python3
"""Check each gRPC service, then real HTTP multi-hop paths. Saves JSON evidence."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('run',type=Path);p.add_argument('--output',default='validation.json');p.add_argument('--baseline',type=Path)
p.add_argument('--probe-placement',choices=['worker0','spread'],default='worker0',help='worker used by each sequential readiness client')
a=p.parse_args();a.run=a.run.resolve()
t=json.loads((a.run/'topology.json').read_text());started=json.loads((a.run/'started.json').read_text())
source=Path(started['source']);result={'mode':started['mode'],'probe_placement':a.probe_placement,'rpc':[],'http':[]}
env={k:v for k,v in os.environ.items() if not k.startswith(('DSB_','DPUMESH_','DMESH_'))}
env.update(DSB_TRANSPORT=started['mode'],DSB_TOPOLOGY=str(a.run/'topology.json'),
 DPUMESH_CONFIG=str(a.run/'registry'),DPUMESH_SERVER='DPUMesh0',DPUMESH_PCI_ADDR=t['processes'][0]['env']['DPUMESH_PCI_ADDR'],
 DPUMESH_POD_IP='10.81.250.1',DPUMESH_WORKLOAD='hotel-probe',DPUMESH_REVERSE='dpu-dma',
 GOMAXPROCS='2',TLS='false',LD_LIBRARY_PATH=started['native']+':/opt/mellanox/doca/lib/x86_64-linux-gnu:/opt/mellanox/flexio/lib')
try:
 for service_index,s in enumerate(t['services']):
  env['DPUMESH_SERVER']='DPUMesh'+str(service_index%t['workers'] if a.probe_placement=='spread' else 0)
  cmd=[str(source/'bin/dmesh-probe'),'-service',s]
  if s=='srv-user':cmd+=['-login']
  r=subprocess.run(cmd,cwd=source,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=35)
  (a.run/(a.output+'.'+s+'.log')).write_text(r.stdout)
  result['rpc'].append({'service':s,'exit':r.returncode,'worker':env['DPUMESH_SERVER']})
  if r.returncode:raise RuntimeError(f'RPC probe failed for {s}')
 paths=[
 '/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=37.7749&lon=-122.4194',
 '/recommendations?require=dis&lat=37.7749&lon=-122.4194',
 '/user?username=Cornell_30&password=0000000000',
 '/review?hotelId=1&username=Cornell_30&password=0000000000',
 '/restaurants?hotelId=1&username=Cornell_30&password=0000000000',
 '/museums?hotelId=1&username=Cornell_30&password=0000000000',
 '/cinema?hotelId=1&username=Cornell_30&password=0000000000',
 '/reservation?inDate=2015-04-25&outDate=2015-04-26&hotelId=1&customerName=DPUMeshProbe&username=Cornell_30&password=0000000000&number=1',
 ]
 port=next(x['env']['DSB_PORT'] for x in t['processes'] if x['service']=='frontend')
 def request(path):
  with urllib.request.urlopen('http://127.0.0.1:'+port+path,timeout=20) as r:
   return {'path':path,'status':r.status,'body':json.loads(r.read())}
 for path in paths:result['http'].append(request(path))
 messages={x['path'].split('?')[0]:x['body'] for x in result['http']}
 if messages['/user'].get('message')!='Login successfully!' or messages['/reservation'].get('message')!='Reserve successfully!':raise RuntimeError('login/reservation business result failed')
 if a.baseline:
  baseline=json.loads(a.baseline.read_text())
  def canonical(x):
   if isinstance(x,list):return sorted((canonical(v) for v in x),key=lambda v:json.dumps(v,sort_keys=True))
   if isinstance(x,dict):return {k:canonical(v) for k,v in x.items()}
   return x
  result['matches_tcp']=canonical(result['http'])==canonical(baseline['http'])
  if not result['matches_tcp']:raise RuntimeError('HTTP results differ from TCP baseline')
 with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
  rows=list(pool.map(request,[paths[0]]*64))
 result['concurrent_search_ok']=all(x['status']==200 and x['body']['features'] for x in rows)
 if not result['concurrent_search_ok']:raise RuntimeError('concurrent search failed')
 result['additional_frontends']=[]
 for frontend in [x for x in t['processes'] if x['service']=='frontend'][1:]:
  port=frontend['env']['DSB_PORT']
  replies=[request(path) for path in paths]
  if not replies[0]['body']['features']:raise RuntimeError('replicated frontend search failed')
  result['additional_frontends'].append({'port':port,'http':replies})
 result['ok']=True
except Exception as e:
 result.update(ok=False,error=str(e))
finally:
 (a.run/a.output).write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps({'ok':result['ok'],'mode':result['mode'],'error':result.get('error')}))
raise SystemExit(not result['ok'])
