#!/usr/bin/env python3
"""Aggregate worker_backend_bench results into CSV and a Markdown table."""
import argparse
import collections
import csv
import json
from pathlib import Path
import statistics

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('results', nargs='+', type=Path)
p.add_argument('--out', required=True, type=Path)
a=p.parse_args()
rows=[]
for path in a.results: rows.extend(json.loads(path.read_text()))
groups=collections.defaultdict(list)
for r in rows: groups[r['workers'],r['replicas'],r['flows']].append(r)
summary=[]
for (w,reps,f),rs in sorted(groups.items()):
    cpus=[]; dispatcher=[]; client=[]; server=[]
    for r in rs:
        perworker=collections.defaultdict(float)
        dispatch=0
        for t in r['dpu_cpu']['tasks'].values():
            if t['name'].startswith('dmesh-shard-'): perworker[t['name']]+=t['pct']
            if t['name'] == 'dmesh-control': dispatch+=t['pct']
        cpus.append([perworker[f'dmesh-shard-{i}'] for i in range(w)])
        dispatcher.append(dispatch)
        hc=r['host_cpu']['process_pct']
        client.append(hc[str(r['client_pid'])])
        server.append(sum(hc.get(str(pid),0) for pid in r['server_pids']))
    summary.append(dict(workers=w, replicas=reps, flows=f, flows_per_worker=f/w, repeats=len(rs),
        rps_median=statistics.median(r['rps'] for r in rs),
        rps_min=min(r['rps'] for r in rs),rps_max=max(r['rps'] for r in rs),
        p50_us_median=statistics.median(r['p50_us'] for r in rs),
        p99_us_median=statistics.median(r['p99_us'] for r in rs),
        errors=sum(r['errors'] for r in rs),
        worker_cpu_pct=[statistics.median(c[i] for c in cpus) for i in range(w)],
        dispatcher_cpu_pct=statistics.median(dispatcher),
        host_client_cpu_pct=statistics.median(client),host_server_cpu_pct=statistics.median(server),
        audited_requests=sum(v['requests'] for r in rs for v in r['flow_audit'].values()),
        cross_worker=max(v['cross_worker'] for r in rs for v in r['flow_audit'].values()),
        cross_thread=max(v['cross_thread'] for r in rs for v in r['flow_audit'].values())))
a.out.parent.mkdir(parents=True,exist_ok=True)
a.out.with_suffix('.json').write_text(json.dumps({'summary':summary,'runs':rows},indent=2)+'\n')
with a.out.with_suffix('.csv').open('w') as out:
    writer=csv.DictWriter(out,fieldnames=list(summary[0]));writer.writeheader();writer.writerows(summary)
print('| Workers | Replicas | Flows | RPC/s median | P99 ms | Worker CPU % | Host client/server CPU % |')
print('|---:|---:|---:|---:|---:|---|---|')
for r in summary:
    c=r['worker_cpu_pct']
    print(f"| {r['workers']} | {r['replicas']} | {r['flows']} | {r['rps_median']:,.0f} | {r['p99_us_median']/1000:.2f} | {min(c):.1f}–{max(c):.1f} | {r['host_client_cpu_pct']:.0f}/{r['host_server_cpu_pct']:.0f} |")
