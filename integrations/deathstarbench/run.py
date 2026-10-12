#!/usr/bin/env python3
"""Start/stop only this HotelReservation run's processes, with PID birth checks."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('action',choices=['start','stop','status'])
p.add_argument('--run',required=True,type=Path)
p.add_argument('--source',type=Path)
p.add_argument('--native-lib',type=Path)
p.add_argument('--mode',choices=['tcp','dmesh'],default='dmesh')
a=p.parse_args();a.run=a.run.resolve()
t=json.loads((a.run/'topology.json').read_text())

def identity(record):
    try:
        fields=Path(f'/proc/{record["pid"]}/stat').read_text().rsplit(') ',1)[1].split()
        if int(fields[19])!=record['start_ticks']: raise RuntimeError('PID reused; refusing to signal')
        if fields[0]=='Z': return False
        exe=os.readlink(f'/proc/{record["pid"]}/exe')
        if exe!=record['exe']: raise RuntimeError(f'executable mismatch: {exe}')
        return True
    except FileNotFoundError: return False

def records():
    return [(item,json.loads((a.run/(item['name']+'.process.json')).read_text()))
        for item in t['processes'] if (a.run/(item['name']+'.process.json')).exists()]

def wait_for_launch():
    deadline=time.monotonic()+120
    while not (a.run/'launch-complete.json').exists():
        if time.monotonic()>deadline: raise RuntimeError('supervisor launch deadline exceeded; inspect recorded processes')
        time.sleep(.1)

if a.action=='start':
    if not a.source or not a.native_lib: p.error('--source and --native-lib required')
    source=a.source.resolve();native=a.native_lib.resolve()
    if list(a.run.glob('*.process.json')) or (a.run/'started.json').exists(): raise SystemExit('use a fresh run directory')
    if not (source/'config.json').is_file(): raise SystemExit('missing infrastructure config.json')
    for item in t['processes']:
        from_mask=set()
        for part in item['cpu_mask'].split(','):
            lo,_,hi=part.partition('-');from_mask.update(range(int(lo),int(hi or lo)+1))
        if not from_mask <= os.sched_getaffinity(0): raise SystemExit('CPU mask exceeds allowed host CPUs')
        if not (source/'bin'/item['service']).is_file(): raise SystemExit('missing executable')
    reservations={}
    for item in t['processes']:
        if a.mode=='tcp' or item['service']=='frontend':
            # Keep later services' ports out of the ephemeral allocator while
            # earlier children open DB/client connections. Release each port
            # immediately before launching its owner; no global sysctl change.
            sock=socket.socket()
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('0.0.0.0',int(item['env']['DSB_PORT'])))
            reservations[item['name']]=sock
    (a.run/'started.json').write_text(json.dumps(dict(mode=a.mode,source=str(source),native=str(native),launch_barrier=True)))
    pid=os.fork()
    if pid:
        for reservation in reservations.values():reservation.close()
        wait_for_launch()
        print(json.dumps({'supervisor':pid,'launch_complete':True}))
        raise SystemExit(int((a.run/'supervisor-error.txt').exists()))
    os.setsid()
    fd=os.open('/dev/null',os.O_RDWR)
    for n in (0,1,2):os.dup2(fd,n)
    def reap(name,proc,log):
        (a.run/(name+'.exit')).write_text(str(proc.wait()))
        log.close()
    threads=[]
    try:
        for item in t['processes']:
            env={k:v for k,v in os.environ.items() if not k.startswith(('DPUMESH_','DMESH_','DSB_'))}
            env.update(item['env'],DSB_TRANSPORT=a.mode,DSB_TOPOLOGY=str(a.run/'topology.json'),
                       DPUMESH_CONFIG=str(a.run/'registry'),LD_LIBRARY_PATH=str(native)+':/opt/mellanox/doca/lib/x86_64-linux-gnu:/opt/mellanox/flexio/lib')
            exe=str(source/'bin'/item['service']); log=(a.run/(item['name']+'.log')).open('w')
            reservation=reservations.pop(item['name'],None)
            if reservation is not None:reservation.close()
            proc=subprocess.Popen(['taskset','-c',item['cpu_mask'],exe],cwd=source,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT)
            fields=Path(f'/proc/{proc.pid}/stat').read_text().rsplit(') ',1)[1].split()
            (a.run/(item['name']+'.process.json')).write_text(json.dumps(dict(pid=proc.pid,start_ticks=int(fields[19]),exe=exe)))
            thread=threading.Thread(target=reap,args=(item['name'],proc,log));thread.start();threads.append(thread)
            time.sleep(.4)
    except BaseException as e:
        (a.run/'supervisor-error.txt').write_text(repr(e))
    finally:
        for reservation in reservations.values():reservation.close()
    (a.run/'launch-complete.json').write_text(json.dumps({'processes':len(threads)}))
    for thread in threads:thread.join()
    os._exit(0)
elif a.action=='stop':
    if (a.run/'started.json').exists() and json.loads((a.run/'started.json').read_text()).get('launch_barrier'):
        wait_for_launch()  # No new children may appear after the stop snapshot.
    errors=[]
    # Drain ingress first, then search, then leaf services.
    for group in (['frontend'],['search'],[s for s in {x['service'] for x in t['processes']} if s not in ('frontend','search')]):
        selected=[(x,r) for x,r in records() if x['service'] in group]
        for item,r in selected:
            if identity(r):
                try:
                    fd=os.pidfd_open(r['pid'])
                    try:
                        if identity(r): signal.pidfd_send_signal(fd,signal.SIGTERM)
                    finally: os.close(fd)
                except ProcessLookupError: pass
        deadline=time.monotonic()+20
        while time.monotonic()<deadline and any(identity(r) or not (a.run/(x['name']+'.exit')).exists() for x,r in selected):time.sleep(.1)
        for item,r in selected:
            exitfile=a.run/(item['name']+'.exit')
            if identity(r) or not exitfile.exists() or exitfile.read_text().strip()!='0': errors.append(item['name'])
    (a.run/'stop.json').write_text(json.dumps({'ok':not errors,'failed':errors},indent=2))
    print(json.dumps({'ok':not errors,'failed':errors}))
    raise SystemExit(bool(errors))
else:
    print(json.dumps([{**x,'alive':identity(r),'pid':r['pid']} for x,r in records()],indent=2))
