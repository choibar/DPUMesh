#!/usr/bin/env python3
"""Plot a summarize_worker_backend.py JSON artifact."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('summary',type=Path);p.add_argument('--out',type=Path,required=True)
a=p.parse_args();rows=json.loads(a.summary.read_text())['summary']
fig,axes=plt.subplots(1,2,figsize=(10,4.2),sharey=True,layout='constrained')
for ax,scaled in zip(axes,(False,True)):
 for k,color in zip((1,2,4),('#2962A3','#E17828','#379268')):
  rs=sorted((r for r in rows if r['flows_per_worker']==k and r['replicas']==(r['workers'] if scaled else 1)),key=lambda r:r['workers'])
  xs=[r['workers'] for r in rs];ys=[r['rps_median']/1000 for r in rs]
  ax.errorbar(xs,ys,yerr=[[r['rps_median']/1000-r['rps_min']/1000 for r in rs],[r['rps_max']/1000-r['rps_median']/1000 for r in rs]],color=color,marker='o',capsize=3,label=f'{k} client flows / worker')
 ax.set_title('Server replicas = workers' if scaled else 'One server replica')
 ax.set_xlabel('DPU Arm workers');ax.set_xticks([1,2,4,8]);ax.set_ylim(bottom=0);ax.grid(alpha=.25)
axes[0].set_ylabel('Throughput (K RPC/s)');axes[1].legend(frameon=False,fontsize=9)
fig.suptitle('gRPC-go 64B echo · 1 client process · 64 RPCs/flow\nSharded busy-poll · DPU-DMA · worker-local backends',fontsize=12)
fig.savefig(a.out,dpi=180)
