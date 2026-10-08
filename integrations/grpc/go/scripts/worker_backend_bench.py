#!/usr/bin/env python3
"""Isolated worker-local gRPC benchmark. Run on DPU; --host-role is internal.

Uses the existing scoped proxy launcher/stopper. Does not change CPUs, devices,
network configuration, or installed host libraries. Requires an isolated host
build of bench-client/server at --host-root. All run artifacts go to --out.
"""
import argparse
import collections
import json
import os
from pathlib import Path
import re
import signal
import shlex
import subprocess
import sys
import threading
import time

REPO = next((p for p in Path(__file__).resolve().parents if (p/'integrations/deathstarbench').is_dir()), None)
HZ = os.sysconf('SC_CLK_TCK')


def dump(path, obj):
    path.write_text(json.dumps(obj, indent=2) + '\n')


def record(pid, exe):
    st = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
    return dict(pid=pid, start_ticks=int(st[19]), exe=str(exe))


def stop_record(r):
    try:
        current = record(r['pid'], r['exe'])
        if current != r or os.readlink(f"/proc/{r['pid']}/exe") != r['exe']:
            raise RuntimeError('process identity changed; refusing signal')
        fd = os.pidfd_open(r['pid'])
        try:
            if record(r['pid'], r['exe']) != r:
                raise RuntimeError('process identity changed')
            signal.pidfd_send_signal(fd, signal.SIGTERM)
        finally:
            os.close(fd)
    except ProcessLookupError:
        pass
    except FileNotFoundError:
        pass


def sample(pids):
    row = {'ns': time.time_ns(), 'processes': {}}
    for pid in pids:
        try:
            proc = Path(f'/proc/{pid}')
            st = proc.joinpath('stat').read_text().rsplit(') ', 1)[1].split()
            tasks = {}
            for task in proc.joinpath('task').iterdir():
                try:
                    ts = task.joinpath('stat').read_text().rsplit(') ', 1)[1].split()
                    tasks[task.name] = {'name': task.joinpath('comm').read_text().strip(),
                                        'ticks': int(ts[11])+int(ts[12]), 'cpu': int(ts[36])}
                except (FileNotFoundError, ProcessLookupError):
                    pass
            row['processes'][str(pid)] = {'ticks': int(st[11])+int(st[12]), 'tasks': tasks}
        except (FileNotFoundError, ProcessLookupError):
            pass
    return row


class Sampler:
    def __init__(self, path, pids):
        self.stop = threading.Event()
        self.path, self.pids = path, pids
        self.thread = threading.Thread(target=self.run)
    def run(self):
        with self.path.open('w') as f:
            while not self.stop.is_set():
                f.write(json.dumps(sample(self.pids())) + '\n'); f.flush()
                self.stop.wait(1)
    def __enter__(self):
        self.thread.start(); return self
    def __exit__(self, *args):
        self.stop.set(); self.thread.join()


def host_env(root):
    env = os.environ.copy()
    env.update(LD_LIBRARY_PATH=str(root/'build/lib')+':/opt/mellanox/doca/lib/x86_64-linux-gnu:/opt/mellanox/flexio/lib',
               DPUMESH_PCI_ADDR='0b:00.1', DPUMESH_SERVER='DPUMesh0', DPUMESH_REVERSE='dpu-dma')
    return env


def host_action(a):
    out = a.out; out.mkdir(parents=True, exist_ok=True)
    root = a.host_root
    if a.host_role == 'servers':
        for i in range(a.replicas):
            env = host_env(root)
            env.update(GOMAXPROCS='4', DPUMESH_POD_IP=f'10.81.1.{i+1}',
                       DPUMESH_SERVICE=f'10.81.1.{i+1}:8080', DPUMESH_BACKEND_MAX=str(a.workers))
            exe = root/'integrations/grpc/go/bin/bench-server'
            # Child records its identity after exec via parent readiness polling.
            with (out/f'server-{i}.log').open('w') as log:
                child = subprocess.Popen(['taskset', '-c', '8-15', str(exe)], env=env,
                                         stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                         start_new_session=True)
            for _ in range(100):
                if child.poll() is not None: raise RuntimeError('server exited during launch')
                if os.readlink(f'/proc/{child.pid}/exe') == str(exe): break
                time.sleep(.02)
            dump(out/f'server-{i}.process.json', record(child.pid, exe))
            deadline = time.monotonic()+30
            while time.monotonic() < deadline:
                if 'bench-server: serving' in (out/f'server-{i}.log').read_text(): break
                if child.poll() is not None: raise RuntimeError('server failed')
                time.sleep(.1)
            else: raise RuntimeError('server startup timeout')
        print('SERVERS_READY', flush=True)
    elif a.host_role == 'stop':
        records = [json.loads(p.read_text()) for p in out.glob('server-*.process.json')]
        for r in records: stop_record(r)
        deadline = time.monotonic()+20
        while time.monotonic() < deadline:
            alive = []
            for r in records:
                try:
                    st = Path(f"/proc/{r['pid']}/stat").read_text().rsplit(') ',1)[1].split()
                    if st[0] != 'Z' and int(st[19]) == r['start_ticks']: alive.append(r)
                except FileNotFoundError: pass
            if not alive: print('SERVERS_STOPPED'); return
            time.sleep(.2)
        raise RuntimeError('server shutdown timeout; no SIGKILL sent')
    else:
        env = host_env(root)
        env.update(GOMAXPROCS='8', DPUMESH_POD_IP='10.81.2.1', DPUMESH_SERVICE_IP='10.80.0.1',
                   DPUMESH_SERVICE_PORT='8080', BENCH_P=str(a.flows), BENCH_M='64',
                   BENCH_PAYLOAD='64', BENCH_WARM=str(a.warm), BENCH_DUR=str(a.duration))
        exe = root/'integrations/grpc/go/bin/bench-client'
        pids = [json.loads(p.read_text())['pid'] for p in out.parent.glob('server-*.process.json')]
        with (out/'client.log').open('w') as log:
            child = subprocess.Popen(['taskset', '-c', '0-7', str(exe)], env=env,
                                     stdout=log, stderr=log)
            with Sampler(out/'host-cpu.jsonl', lambda: pids+[child.pid]):
                try: rc = child.wait(timeout=a.warm+a.duration+90)
                except subprocess.TimeoutExpired:
                    child.terminate()
                    child.wait(timeout=20)
                    raise
        raw = (out/'client.log').read_text()
        results = re.findall(r'^RESULT_JSON (.*)$', raw, re.M)
        if not results: raise RuntimeError('missing benchmark result: '+raw[-3000:])
        result = json.loads(results[-1]); result.update(client_pid=child.pid, server_pids=pids, exit=rc)
        dump(out/'result.json', result)
        print(json.dumps(result), flush=True)
        if rc: raise RuntimeError('client failed')


def remote(a, role, out, **kwargs):
    cmd = ['python3', str(a.host_root/'worker_backend_bench.py'), '--host-role', role,
           '--host-root', str(a.host_root), '--out', str(out), '--workers', str(kwargs.get('workers',1)),
           '--replicas', str(kwargs.get('replicas',1)), '--flows', str(kwargs.get('flows',1)),
           '--warm', str(a.warm), '--duration', str(a.duration)]
    return subprocess.run(['ssh', a.host, shlex.join(cmd)], text=True, capture_output=True, timeout=a.warm+a.duration+150)


def summarize_cpu(path, start, end, offset=0):
    rows = [json.loads(x) for x in path.read_text().splitlines()]
    rows = [r for r in rows if start <= r['ns']+offset <= end]
    if len(rows) < 2: raise RuntimeError('insufficient CPU samples')
    totals, spans, tasks = collections.Counter(), collections.Counter(), {}
    for a,b in zip(rows, rows[1:]):
        dt = (b['ns']-a['ns'])/1e9
        for pid,v in b['processes'].items():
            if pid not in a['processes']: continue
            prev = a['processes'][pid]
            totals[pid] += (v['ticks']-prev['ticks'])/HZ
            spans[pid] += dt
            for tid,t in v['tasks'].items():
                if tid not in prev['tasks']: continue
                x = tasks.setdefault(tid, {'name': t['name'], 'seconds':0, 'span':0, 'cpus':set()})
                x['seconds'] += (t['ticks']-prev['tasks'][tid]['ticks'])/HZ
                x['span'] += dt; x['cpus'].add(t['cpu'])
    return {'process_pct': {p:100*t/spans[p] for p,t in totals.items()},
            'tasks': {tid:{'name': t['name'], 'pct':100*t['seconds']/t['span'], 'cpus':sorted(t['cpus'])} for tid,t in tasks.items()}}


def run(a):
    a.out.mkdir(parents=True, exist_ok=True)
    if any(a.out.iterdir()):
        raise RuntimeError('use a fresh, empty output directory to preserve prior results')
    subprocess.run(['scp', str(Path(__file__)), a.host+':'+str(a.host_root/'worker_backend_bench.py')], check=True)
    # Timestamp alignment: select the least delayed SSH sample.
    clocks=[]
    for _ in range(3):
        t0=time.time_ns(); r=subprocess.check_output(['ssh',a.host,'date +%s%N'],text=True); t1=time.time_ns()
        clocks.append((t1-t0, int(r)-(t0+t1)//2))
    jitter, offset=min(clocks)
    dump(a.out/'config.json',dict(warm=a.warm,duration=a.duration,repeats=a.repeats,host=a.host,
         host_root=str(a.host_root), host_clock_offset_ns=offset, clock_rtt_ns=jitter,
         flow_placement='round-robin',host_client_cpus='0-7',host_server_cpus='8-15',client_gomaxprocs=8,server_gomaxprocs=4,
         online_cpus=Path('/sys/devices/system/cpu/online').read_text().strip()))
    (a.out/'dpu-system.txt').write_text(subprocess.check_output(['lscpu'],text=True))
    (a.out/'host-system.txt').write_text(subprocess.check_output(['ssh',a.host,'lscpu'],text=True))
    (a.out/'binary-sha256.txt').write_text(subprocess.check_output(['sha256sum',str(REPO/'linkerd2-proxy/target/release/linkerd2-proxy')],text=True)+subprocess.check_output(['ssh',a.host,'sha256sum',str(a.host_root/'integrations/grpc/go/bin/bench-client'),str(a.host_root/'integrations/grpc/go/bin/bench-server'),str(a.host_root/'build/lib/libdpumesh.so')],text=True))
    results=[]
    specs = [tuple(map(int,s.split(':'))) for s in a.cases.split(',')] if a.cases else [
        (w,r,w*k) for w in (1,2,4,8) for r in sorted({1,w}) for k in (1,2,4)]
    for workers, replicas, flows in specs:
        if workers not in (1,2,4,8) or not 1 <= replicas <= 8 or not workers <= flows <= min(32,4*workers) or flows % workers or flows//workers not in (1,2,4):
            raise ValueError('cases require 1/2/4/8 workers, 1..8 replicas, and 1/2/4 flows per worker within 32 slots')
        name=f'w{workers}-r{replicas}-f{flows}'
        case=a.out/name;case.mkdir()
        hostcase=a.host_root/'worker-local-runs'/a.out.name/name
        endpoints=[{'id':f'echo-{i}', 'dma':f'10.81.1.{i+1}:8080','worker':i%workers,'enabled':True} for i in range(replicas)]
        routes={'schema_version':2,'generation':1,'workers':workers,'services':{'echo':{
            'vip':'10.80.0.1','port':8080,'discovery':'echo.dmesh:8080','endpoints':endpoints}}}
        dump(case/'routes.json',routes)
        env=os.environ.copy();env.update(DSB_ROUTES_FILE=str(case/'routes.json'),DSB_PROXY_POLL_MODE='busy',
            DSB_PROXY_CPU_MASK=','.join(str(x) for x in range(12-workers,12)),
            DSB_FLOW_PLACEMENT='round-robin',
            DSB_PROXY_LOG='warn,dmesh_doca::backend=info,linkerd_app_outbound::http::dmesh_pool=info')
        proxy=case/'proxy'
        print('START',name,flush=True)
        try:
            with (case/'launch.log').open('w') as log:
                subprocess.run(['bash',str(REPO/'integrations/deathstarbench/proxy-start.sh'),str(workers),str(proxy)],
                               env=env,stdout=log,stderr=log,check=True,timeout=max(100,workers*20))
            pid=int((proxy/'proxy.pid').read_text())
            # Validate actual affinity, not just launch intent. Exclude SDK helpers
            # by requiring each logical worker to have a pinned shard thread.
            affinity=json.loads((proxy/'affinity.json').read_text())['threads']
            for w in range(workers):
                expected=str(11-w)
                if not any(t['name']==f'dmesh-shard-{w}' and t['cpu_mask']==expected for t in affinity.values()):
                    raise RuntimeError('worker affinity mismatch')
            r=remote(a,'servers',hostcase,workers=workers,replicas=replicas)
            (case/'host-start.log').write_text(r.stdout+r.stderr)
            if r.returncode: raise RuntimeError('host servers failed')
            time.sleep(1)
            before=(proxy/'proxy.log').read_text()
            if '[dispatcher] session=' in before: raise RuntimeError('backend created before first client request')
            prior_audits = {}
            for rep in range(a.repeats):
                trial=case/f'run-{rep+1}';trial.mkdir()
                startbyte=(proxy/'proxy.log').stat().st_size
                with Sampler(trial/'dpu-cpu.jsonl',lambda:[pid]):
                    r=remote(a,'client',hostcase/f'run-{rep+1}',workers=workers,replicas=replicas,flows=flows)
                    (trial/'remote.log').write_text(r.stdout+r.stderr)
                    time.sleep(2)
                subprocess.run(['scp','-q',a.host+':'+str(hostcase/f'run-{rep+1}')+'/*',str(trial)+'/'],check=True)
                with (proxy/'proxy.log').open('rb') as f: f.seek(startbyte); segment=f.read().decode(errors='replace')
                (trial/'proxy.log').write_text(segment)
                if r.returncode: raise RuntimeError('client failed: '+r.stderr[-1000:])
                result=json.loads((trial/'result.json').read_text())
                result.update(workers=workers,replicas=replicas,repeat=rep+1,case=name)
                result['host_cpu']=summarize_cpu(trial/'host-cpu.jsonl',result['start_ns'],result['end_ns'])
                result['dpu_cpu']=summarize_cpu(trial/'dpu-cpu.jsonl',result['start_ns'],result['end_ns'],offset)
                assignments=re.findall(r'\[dispatcher\] session=(\d+/\d+) flow=(\d+/\d+) owner=(\d+) slot=(\d+) epoch=1 backend=(\d)',segment)
                result['client_flows_by_worker']=dict(collections.Counter(int(x[2]) for x in assignments if x[4]=='0'))
                result['backend_flows_by_worker']=dict(collections.Counter(int(x[2]) for x in assignments if x[4]=='1'))
                audits={}
                for x in re.finditer(r'dmesh flow audit address=(\S+) route=Route \{ worker: (\d+), owner: (\d+), slot: (\d+), epoch: (\d+) \} requests=(\d+) cross_worker=(\d+) cross_thread=(\d+)',segment):
                    ep,w,owner,slot,epoch,n,cw,ct=x.groups();audits[f'{ep}/{w}/{owner}/{slot}/{epoch}']={'worker':int(w),'requests':int(n),'cross_worker':int(cw),'cross_thread':int(ct)}
                result['flow_audit']={key:{**v, 'requests':v['requests']-prior_audits.get(key,{}).get('requests',0)} for key,v in audits.items()}
                prior_audits.update(audits)
                if not audits or any(x['cross_worker'] or x['cross_thread'] for x in audits.values()):
                    raise RuntimeError('missing or nonlocal flow audit')
                if sum(result['client_flows_by_worker'].values()) != flows: raise RuntimeError('client flow count mismatch')
                if any(result['client_flows_by_worker'].get(w,0) != flows//workers for w in range(workers)):
                    raise RuntimeError('client flow distribution mismatch')
                dump(trial/'summary.json',result)
                results.append(result);dump(a.out/'results.json',results)
                print('RESULT',name,rep+1,round(result['rps']), 'p99_us',round(result['p99_us']), 'errors',result['errors'],flush=True)
        finally:
            r=remote(a,'stop',hostcase)
            (case/'host-stop.log').write_text(r.stdout+r.stderr)
            if proxy.joinpath('launch.json').exists():
                stopped=subprocess.run(['python3',str(REPO/'integrations/deathstarbench/proxy-stop.py'),str(proxy),'--timeout','30'],capture_output=True,text=True)
                (case/'stop.log').write_text(stopped.stdout+stopped.stderr)
                if stopped.returncode: raise RuntimeError('proxy scoped cleanup failed')
            if r.returncode: raise RuntimeError('host scoped cleanup failed')
        time.sleep(1)
    print('ALL_DONE',len(results),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--host',default='youngmin@192.168.100.1')
    p.add_argument('--host-root',type=Path,default=Path('/tmp/dmesh-worker-backend-host'))
    p.add_argument('--host-role',choices=['servers','client','stop'])
    p.add_argument('--workers',type=int,default=1);p.add_argument('--replicas',type=int,default=1)
    p.add_argument('--flows',type=int,default=1)
    p.add_argument('--warm',type=int,default=10);p.add_argument('--duration',type=int,default=30)
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--cases',help='workers:replicas:flows comma-separated; default full 21-case matrix')
    a=p.parse_args();a.out=a.out.resolve()
    if min(a.warm, a.duration, a.repeats) < 1: p.error('durations and repetitions must be positive')
    if a.host_role: host_action(a)
    else: run(a)

if __name__=='__main__':main()
