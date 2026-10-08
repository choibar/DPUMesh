#!/usr/bin/env python3
"""Create isolated, unpublished DSB infrastructure; write its host config.json."""
import argparse
import json
from pathlib import Path
import subprocess

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('directory',type=Path);p.add_argument('--project',required=True)
p.add_argument('action',choices=['up','down'])
a=p.parse_args();a.directory.mkdir(parents=True,exist_ok=True)
f=a.directory/'compose.json'
services={'consul':{'image':'hashicorp/consul:latest','command':['agent','-dev','-client=0.0.0.0']},
          'jaeger':{'image':'jaegertracing/all-in-one:latest'}}
for s in ('geo','profile','rate','review','attractions','recommendation','reservation','user'):
    services['mongodb-'+s]={'image':'mongo:5.0'}
for s in ('rate','profile','review','reserve'):
    services['memcached-'+s]={'image':'memcached:latest'}
command=['docker','compose','-p',a.project,'-f',str(f)]
if a.action=='down':
    subprocess.run(command+['down'],check=True)  # Preserve volumes/data; no -v.
else:
    if f.exists(): raise SystemExit('infra directory already initialized')
    f.write_text(json.dumps({'services':services},indent=2))
    subprocess.run(command+['up','-d','--pull','never','--wait','--wait-timeout','60'],check=True)
    def address(s,port):
        cid=subprocess.check_output(command+['ps','-q',s],text=True).strip()
        obj=json.loads(subprocess.check_output(['docker','inspect',cid]))[0]
        nets=obj['NetworkSettings']['Networks']
        return next(iter(nets.values()))['IPAddress']+':'+str(port)
    cfg={'consulAddress':address('consul',8500),'jaegerAddress':address('jaeger',6831),'KnativeDomainName':''}
    names={'Geo':('geo',8083),'Profile':('profile',8081),'Rate':('rate',8084),
           'Review':('review',8088),'Attractions':('attractions',8089),'Recommend':('recommendation',8085),
           'Reserve':('reservation',8087),'User':('user',8086)}
    for key,(s,port) in names.items():
        cfg[key+'Port']=str(port);cfg[key+'MongoAddress']=address('mongodb-'+s,27017)
    for key,s in [('Rate','rate'),('Profile','profile'),('Review','review'),('Reserve','reserve')]:
        cfg[key+'MemcAddress']=address('memcached-'+s,11211)
    cfg.update(SearchPort='8082',FrontendPort='15000')
    (a.directory/'config.json').write_text(json.dumps(cfg,indent=2)+'\n')
