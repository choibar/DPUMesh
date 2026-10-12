#!/usr/bin/env python3
"""Reset only this integration's isolated reservation data/cache, after host stop."""
import argparse
import json
from pathlib import Path
import socket
import subprocess
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('infra',type=Path);p.add_argument('--project',required=True);p.add_argument('--stopped-run',required=True,type=Path)
p.add_argument('--allow-failed-stop',action='store_true',help='permit a recorded cleanup error only if all recorded processes have exited; preserves the failed stop result')
a=p.parse_args()
if not json.loads((a.stopped_run/'stop.json').read_text())['ok']:
 if not a.allow_failed_stop:p.error('run must have stopped cleanly (or explicitly allow a failed stop after verifying proxy cleanup)')
 print('Recorded cleanup failure retained; checking every process exited before resetting isolated data.')
# Existing records must still identify exited processes.
for f in a.stopped_run.glob('*.process.json'):
 r=json.loads(f.read_text());st=Path(f'/proc/{r["pid"]}/stat')
 if st.exists():
  fields=st.read_text().rsplit(') ',1)[1].split()
  if fields[0]!='Z' and int(fields[19])==r['start_ticks']:p.error('recorded process is still alive')
compose=['docker','compose','-p',a.project,'-f',str((a.infra/'compose.json').resolve())]
cid=subprocess.check_output(compose+['ps','-q','mongodb-reservation'],text=True).strip()
obj=json.loads(subprocess.check_output(['docker','inspect',cid]))[0]
if obj['Config']['Labels'].get('com.docker.compose.project')!=a.project:p.error('container project mismatch')
subprocess.run(['docker','exec',cid,'mongo','--quiet','--eval',
 'db.getSiblingDB("reservation-db").reservation.deleteMany({})'],check=True)
cfg=json.loads((a.infra/'config.json').read_text())
for key,value in cfg.items():
 if key.endswith('MemcAddress'):
  host,port=value.rsplit(':',1)
  with socket.create_connection((host,int(port)),timeout=5) as s:
   s.sendall(b'flush_all\r\n')
   if s.recv(128)!=b'OK\r\n':raise RuntimeError('cache reset failed')
print('Isolated reservation rows/cache reset; service startup restores the seed reservation.')
