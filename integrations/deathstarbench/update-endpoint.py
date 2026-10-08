#!/usr/bin/env python3
"""Enable/disable an existing replica and wait for all three route consumers."""
import argparse
import json
import os
from pathlib import Path
import tempfile
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('manifest',type=Path)
p.add_argument('--proxy-run',required=True,type=Path)
p.add_argument('--endpoint',required=True)
p.add_argument('--enabled',required=True,choices=['true','false'])
a=p.parse_args(); manifest=a.manifest.resolve()
t=json.loads(manifest.read_text())
matches=[e for s in t['services'].values() for e in s['endpoints'] if e['id']==a.endpoint]
if len(matches)!=1:p.error('endpoint must identify exactly one replica')
matches[0]['enabled']=a.enabled=='true';t['generation']+=1
fd,tmp=tempfile.mkstemp(prefix='.routes-',dir=manifest.parent)
try:
 with os.fdopen(fd,'w') as f:
  json.dump(t,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
 os.replace(tmp,manifest)
finally:
 if os.path.exists(tmp):os.unlink(tmp)
marker=f"dmesh routes applied generation={t['generation']}"
deadline=time.monotonic()+5
while time.monotonic()<deadline:
 if all(marker in (a.proxy_run/(role+'.log')).read_text() for role in ['proxy','mock-policy','mock-destination']):
  print(json.dumps(dict(generation=t['generation'],endpoint=a.endpoint,enabled=matches[0]['enabled'],applied=True)))
  break
 time.sleep(.05)
else:raise SystemExit('route update written but not acknowledged by every consumer; inspect logs')
