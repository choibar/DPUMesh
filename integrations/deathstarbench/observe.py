#!/usr/bin/env python3
"""Low-overhead /proc samples for one recorded proxy or host run."""
import argparse
import json
import os
from pathlib import Path
import time
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('directory',type=Path);p.add_argument('--seconds',type=int,default=600)
a=p.parse_args();records=[json.loads(f.read_text()) for f in a.directory.glob('*.process.json')]
with (a.directory/'cpu.jsonl').open('x') as out:
 for _ in range(a.seconds):
  row={'time':time.time(),'hz':os.sysconf('SC_CLK_TCK'),'processes':{},'cpus':{}}
  for r in records:
   pid=r['pid']
   try:
    proc=Path(f'/proc/{pid}')
    stat=proc.joinpath('stat').read_text().rsplit(') ',1)[1].split()
    if int(stat[19])!=r['start_ticks']:continue
    tasks={}
    for task in proc.joinpath('task').iterdir():
     try:
      st=task.joinpath('stat').read_text().rsplit(') ',1)[1].split()
      tasks[task.name]={'name':task.joinpath('comm').read_text().strip(),'ticks':int(st[11])+int(st[12]),'cpu':int(st[36])}
     except (FileNotFoundError, ProcessLookupError): pass
    row['processes'][str(pid)]={'ticks':int(stat[11])+int(stat[12]),'tasks':tasks}
   except (FileNotFoundError, ProcessLookupError):pass
  row['cpus']={v.split()[0]:list(map(int,v.split()[1:])) for v in Path('/proc/stat').read_text().splitlines() if v.startswith('cpu')}
  out.write(json.dumps(row)+'\n');out.flush()
  if not row['processes']:break
  time.sleep(1)
