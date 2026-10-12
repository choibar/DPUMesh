#!/usr/bin/env python3
"""Host half of a live server-restart test; requires a running two-worker proxy.

Usage: python3 worker_backend_lifecycle_host.py HOST_ROOT OUTPUT_DIRECTORY
Uses worker_backend_bench.py alongside this file for scoped server lifecycle.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import worker_backend_bench as bench

root, out = map(lambda x: Path(x).resolve(), sys.argv[1:])
out.mkdir(parents=True, exist_ok=False)
a = argparse.Namespace(out=out, host_root=root, workers=2, replicas=1, host_role='servers')
child = None
try:
    bench.host_action(a)
    env = bench.host_env(root)
    env.update(GOMAXPROCS='8', DPUMESH_POD_IP='10.81.2.1',
               DPUMESH_SERVICE_IP='10.80.0.1', DPUMESH_SERVICE_PORT='8080')
    with (out/'client.log').open('w') as log:
        child = subprocess.Popen(['taskset','-c','0-7',str(root/'integrations/grpc/go/bin/worker-smoke')],
                                 env=env, stdin=subprocess.PIPE, stdout=log, stderr=log, text=True)
        deadline = time.monotonic()+60
        while time.monotonic()<deadline:
            if 'RESTART_READY' in (out/'client.log').read_text(): break
            if child.poll() is not None: raise RuntimeError('client exited before restart')
            time.sleep(.1)
        else: raise RuntimeError('client first-use/partial-close timeout')
        a.host_role='stop'; bench.host_action(a)
        (out/'server-0.log').rename(out/'before-restart-server.log')
        (out/'server-0.process.json').rename(out/'before-restart-server.process.json')
        time.sleep(1)
        a.host_role='servers'; bench.host_action(a)
        child.stdin.write('restarted\n'); child.stdin.flush()
        rc=child.wait(timeout=75)
    text=(out/'client.log').read_text()
    print(text,flush=True)
    if rc or 'WORKER_SMOKE_OK' not in text: raise RuntimeError('worker smoke failed')
    bench.dump(out/'validation.json',dict(concurrent_first_rpc=True,partial_close=True,
                                         server_restart_with_client_flows_alive=True,
                                         sizes=[1,8064,8065,8192,8193,65537,1048577]))
finally:
    if child is not None and child.poll() is None:
        child.terminate(); child.wait(timeout=20)
    a.host_role='stop'; bench.host_action(a)
