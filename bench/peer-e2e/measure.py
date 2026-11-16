#!/usr/bin/env python3
"""Measure an already-deployed native RDMA E2E fixture, without redeploying it.

CPU and HTTP response throughput use the same central 20 seconds of a 30s
run. Client P50/P99 cover its full measurement interval and are labelled so.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import threading
import time

SNAPSHOT = r'''
import os,json,pathlib,time,urllib.request
root=pathlib.Path('/proc/1')
def stat(p):
 s=p.read_text(); f=s[s.rfind(')')+2:].split()
 return {'ticks':int(f[11])+int(f[12]),'start':int(f[19])}
tasks={}
for p in (root/'task').iterdir():
 try: tasks[p.name]=dict(stat(p/'stat'),name=(p/'comm').read_text().strip())
 except FileNotFoundError: pass
with urllib.request.urlopen('http://127.0.0.1:17191/metrics',timeout=5) as r: metrics=r.read().decode()
print(json.dumps({'mono':time.monotonic(),'wall':time.time(),'hz':os.sysconf('SC_CLK_TCK'),
 'process':stat(root/'stat'),'tasks':tasks,'metrics':metrics,
 'workers':int(os.environ['DMESH_NUM_WORKERS']),'sharded':os.environ.get('DMESH_SHARDED')}))
'''


def summarize(a, b, workers):
    if a['process']['start'] != b['process']['start']:
        raise RuntimeError('DPU proxy restarted during sampling')
    if a['workers'] != workers or b['workers'] != workers or a['sharded'] != '1':
        raise RuntimeError('unexpected SHARDED/worker configuration')
    elapsed = b['mono'] - a['mono']
    rows = {}
    for tid, first in a['tasks'].items():
        if not re.fullmatch(r'dmesh-shard-\d+', first['name']): continue
        last = b['tasks'].get(tid)
        if not last or last['start'] != first['start']:
            raise RuntimeError('ARM worker restarted')
        rows[first['name']] = (last['ticks'] - first['ticks']) / a['hz'] / elapsed
    if len(rows) != workers: raise RuntimeError('unexpected number of ARM workers')
    def responses(text):
        values = {}
        for line in text.splitlines():
            m = re.fullmatch(r'(response_total\{(.*)\}) ([0-9.eE+\-]+)', line)
            if m and 'direction="outbound"' in m[2].split(','):
                values[m[1]] = float(m[3])
        return values
    before, after = responses(a['metrics']), responses(b['metrics'])
    count = 0
    for key in before.keys() | after.keys():
        delta = after.get(key, 0) - before.get(key, 0)
        if delta < 0: raise RuntimeError('HTTP metric reset')
        count += delta
    if count <= 0: raise RuntimeError('no outbound HTTP response counter; L7 E2E not verified')
    return {'seconds':elapsed,'worker_cores':sum(rows.values()),'workers':rows,
            'process_cores':(b['process']['ticks']-a['process']['ticks'])/a['hz']/elapsed,
            'http_responses':count,'http_rps':count/elapsed}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--namespace',default='dpumesh-native-peer')
    p.add_argument('--dpu-namespace',default='dpumesh-system')
    p.add_argument('--source-dpu',required=True)
    p.add_argument('--destination-dpu',required=True)
    p.add_argument('--workers',required=True,type=int,choices=[1,2,4,8,16])
    p.add_argument('--concurrency-per-worker',type=int,default=256,
                   help='closed-loop in-flight RPCs per worker (default: 256)')
    p.add_argument('--duration',type=int,default=30)
    p.add_argument('--warmup',type=int,default=3)
    p.add_argument('--trim',type=int,default=5)
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--kubectl',default='kubectl',help='command prefix, parsed with shlex')
    a=p.parse_args()
    if a.concurrency_per_worker <= 0: p.error('concurrency per worker must be positive')
    if a.duration <= 2*a.trim: p.error('duration must exceed twice the trim')
    a.out.mkdir(parents=True,exist_ok=False)
    kube=shlex.split(a.kubectl)
    def call(*args, input=None):
        return subprocess.run(kube+list(args),input=input,text=True,capture_output=True,
                              check=True,timeout=45).stdout
    def snapshot(pod):
        return json.loads(call('exec','-n',a.dpu_namespace,pod,'-c','runtime','--','python3','-c',SNAPSHOT))
    def pair_snapshot():
        with ThreadPoolExecutor(2) as pool:
            futures=[pool.submit(snapshot,pod) for pod in [a.source_dpu,a.destination_dpu]]
            return [f.result() for f in futures]
    server=json.loads(call('get','pod','-n',a.namespace,'native-peer-server','-o','json'))
    endpoint=server['status']['podIP']
    start_file='/tmp/native-peer-start-'+str(os.getpid())
    command=['exec','-n',a.namespace,'native-peer-client','-c','app','--','bash','-ec',
        'exec env DPUMESH_SERVICE_IP="$1" DPUMESH_SERVICE_PORT=50051 '
        'taskset -c "$HOST_CPUS" /opt/native/bin/channel-bench -mode client '
        '-connections "$2" -concurrency "$3" -warmup "$4" -duration "$5" '
        '-rpc-timeout 30s -timeout 180s -start-file "$6"',
        'native-peer',endpoint,str(a.workers),str(a.workers*a.concurrency_per_worker),str(a.warmup)+'s',str(a.duration)+'s',start_file]
    proc=subprocess.Popen(kube+command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    preflight=threading.Event(); lines=[]
    def collect():
        with (a.out/'client.log').open('w') as log:
            for line in proc.stdout:
                log.write(line); log.flush(); lines.append(line)
                if 'PREFLIGHT_OK' in line: preflight.set()
    reader=threading.Thread(target=collect,daemon=True); reader.start()
    limit=time.monotonic()+90
    while not preflight.wait(.2):
        if proc.poll() is not None or time.monotonic()>limit:
            raise RuntimeError('preflight failed/timed out; see client.log')
    start=time.time()+3
    stamp=datetime.fromtimestamp(start,timezone.utc).isoformat().replace('+00:00','Z')
    call('exec','-i','-n',a.namespace,'native-peer-client','-c','app','--',
         'bash','-ec','cat > "$1.tmp"\nmv "$1.tmp" "$1"','native-peer',start_file,input=stamp+'\n')
    def until(t):
        while time.time()<t: time.sleep(max(0,min(.2,t-time.time())))
    until(start+a.warmup+a.trim); before=pair_snapshot()
    until(start+a.warmup+a.duration-a.trim); after=pair_snapshot()
    (a.out/'snapshots.json').write_text(json.dumps({'before':before,'after':after},indent=2))
    code=proc.wait(timeout=70); reader.join()
    results=[]
    for line in lines:
        try: value=json.loads(line)
        except ValueError: continue
        if value.get('event')=='result': results.append(value)
    if code or len(results)!=1 or not results[0].get('ok'):
        raise RuntimeError('client invalid; see client.log')
    result={'client':results[0], 'central_window': [a.trim,a.duration-a.trim],
            'source':summarize(before[0],after[0],a.workers),
            'destination':summarize(before[1],after[1],a.workers)}
    (a.out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))


if __name__=='__main__': main()
