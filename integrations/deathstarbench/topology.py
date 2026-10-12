#!/usr/bin/env python3
"""Generate host process configuration and conservative native flow budgets."""
import argparse
import json
from pathlib import Path

PORTS = dict(geo=8083, rate=8084, search=8082, profile=8081, recommendation=8085,
             user=8086, reservation=8087, review=8088, attractions=8089)
EDGES = {'frontend': ['search','profile','recommendation','user','reservation','review','attractions'],
         'search': ['geo','rate']}

def generate(workers, replicas, frontends, pci, cpus, gomax, placement='round-robin', frontend_workers=None, poll_mode='busy'):
    if poll_mode not in ('busy', 'event'): raise ValueError('invalid proxy poll mode')
    if placement not in ('round-robin','staggered'): raise ValueError('invalid placement')
    if workers not in (1,2,4,8,12,16) or frontends < 1 or not 1 <= gomax:
        raise ValueError('invalid workers/frontend/GOMAXPROCS')
    if set(replicas) - set(PORTS) or any(not 1 <= n <= 16 for n in replicas.values()):
        raise ValueError('invalid replica specification')
    if frontend_workers is not None and (len(frontend_workers) != frontends or
            any(w < 0 or w >= workers for w in frontend_workers)):
        raise ValueError('frontend workers must name one valid worker per frontend')
    services = {f'srv-{name}': {'vip': f'10.80.0.{i+1}', 'port': port,
        'discovery': f'srv-{name}.dmesh:{port}', 'endpoints': [],
        'tcp': [f'127.0.0.1:{port+10000+r*1000}' for r in range(replicas.get(name,1))]}
        for i,(name,port) in enumerate(PORTS.items())}
    processes=[]; budgets=[0]*workers; ports=set()
    for service_index,name in enumerate(list(PORTS)+['frontend']):
        for r in range(frontends if name=='frontend' else replicas.get(name,1)):
            i=len(processes)
            # Rotate each service block so its last-started replica does not
            # always share one worker when replica count equals worker count.
            worker=((service_index+r) if placement=='staggered' else i)%workers
            if name=='frontend' and frontend_workers is not None:
                worker=frontend_workers[r]
            # One demand-created backend per DPU worker; no spare flows.
            maximum=workers if name!='frontend' else 0
            out=len(EDGES.get(name,[])); budget=maximum+out
            port=(15000+r if name=='frontend' else PORTS[name]+10000+1000*r)
            if port > 65535 or port in ports: raise ValueError('TCP port collision')
            ports.add(port)
            env={'DSB_PORT':str(port), 'DPUMESH_PCI_ADDR':pci, 'DPUMESH_SERVER':f'DPUMesh{worker}',
                 'DPUMESH_POD_IP':f'10.81.{service_index+1}.{r+1}',
                 'DPUMESH_WORKLOAD':f'hotel-{name}-{r}', 'DPUMESH_REVERSE':'dpu-dma',
                 'GOMAXPROCS':str(gomax), 'LOG_LEVEL':'warn', 'JAEGER_SAMPLE_RATIO':'0', 'TLS':'false'}
            if name!='frontend':
                s=services[f'srv-{name}']
                endpoint=f'{env["DPUMESH_POD_IP"]}:{s["port"]}'
                s['endpoints'].append(dict(id=f'{name}-{r}', dma=endpoint, worker=worker,
                    tcp=f'127.0.0.1:{port}', enabled=True))
                env.update(DSB_REPLICA=f'{name}-{r}', DPUMESH_SERVICE=endpoint, DPUMESH_BACKEND_MAX=str(maximum))
            if budget>32: raise ValueError(f'{name} exceeds channel capacity')
            budgets[worker]+=budget
            processes.append(dict(name=f'{name}-{r}', service=name, replica=r, worker=worker,
                                  cpu_mask=cpus, env=env, steady_flow_budget=budget))
    # Comch aliases do not pin data flows. Model every worker touching every
    # replica, with evenly placed client flows; this is a sizing estimate.
    backend_count=sum(len(s['endpoints']) for s in services.values())
    client_count=sum(len(EDGES.get(p['service'],[])) for p in processes)
    budgets=[backend_count+(client_count+workers-1)//workers]*workers
    if max(budgets)>30: raise ValueError(f'worker budget {budgets} leaves fewer than 2 reconnect slots (limit 32)')
    return dict(schema_version=2, generation=1, workers=workers, placement=placement,
                frontend_workers=[x['worker'] for x in processes if x['service']=='frontend'],
                sharded=True, busy_poll=poll_mode=='busy', poll_mode=poll_mode, reverse='dpu-dma', services=services,
                processes=processes, worker_flow_budget=budgets,
                note='Worker-local lazy backends: all replicas per worker plus evenly placed outgoing flows. Control alias is not data placement. Runtime admission enforces capacity; skew and reconnect need headroom.')

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('output',type=Path);p.add_argument('--workers',type=int,default=4)
    p.add_argument('--replicas',default='');p.add_argument('--frontends',type=int,default=1)
    p.add_argument('--pci',required=True);p.add_argument('--cpus',default='0-11');p.add_argument('--gomaxprocs',type=int,default=4)
    p.add_argument('--placement',choices=['round-robin','staggered'],default='round-robin')
    p.add_argument('--frontend-workers',help='comma-separated DMA worker per frontend; avoid co-locating busy ingress and search senders')
    p.add_argument('--poll-mode',choices=['busy','event'],default='busy',help='record proxy mode; use matching DSB_PROXY_POLL_MODE at launch')
    p.add_argument('--audit-rpcs',action='store_true',help='write optional per-replica RPC counters in output/stats')
    a=p.parse_args()
    specs={k:int(v) for k,v in (item.split(':') for item in a.replicas.split(',') if item)}
    frontend_workers=None if a.frontend_workers is None else [int(w) for w in a.frontend_workers.split(',')]
    t=generate(a.workers,specs,a.frontends,a.pci,a.cpus,a.gomaxprocs,a.placement,frontend_workers,a.poll_mode)
    if a.audit_rpcs:
        for item in t['processes']:item['env']['DSB_AUDIT_DIR']=str(a.output.resolve()/'stats')
    a.output.mkdir(parents=True,exist_ok=False)
    (a.output/'topology.json').write_text(json.dumps(t,indent=2)+'\n')
    (a.output/'registry').write_text(''.join(f'{s["vip"]}:{s["port"]} {name} {i+2}\n' for i,(name,s) in enumerate(t['services'].items())))
    print(json.dumps({'workers':a.workers,'processes':len(t['processes']),'flow_budget':t['worker_flow_budget']}))
