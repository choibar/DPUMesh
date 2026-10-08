#!/usr/bin/env python3
"""One channel-bench run on the host: a fresh DPU proxy (DPUMesh only), the
server, then the client; CPU of the server, the DPU proxy and the pinned host
cores over the client's measurement window; DPU proxy errors. Writes
<out>/<tag>.json and the raw logs next to it. Exits nonzero unless the run
is valid.

The server and client are any channel-bench implementation: run.py appends
`-mode server|client`, the load flags and, for TCP, `-tcp`.

  run.py --tag go-c1 --server 'channel-bench' --client 'channel-bench' \\
         --connections 1 --concurrency 64 --dpu-dir DPUMesh-online-boutique
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import time

HZ = os.sysconf("SC_CLK_TCK")
SERVICE_IP, SERVICE_PORT = "10.99.1.60", 9000
SERVER_POD, CLIENT_POD = "10.99.0.60", "10.99.0.61"
TCP_ADDR = "127.0.0.1:19100"
DPU_STOP = "bash stop.sh proxy mock-identity mock-policy mock-destination"


def cpus(spec):
    out = []
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        out += range(int(lo), int(hi or lo) + 1)
    return out


def proc_ticks(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def thread_ticks(pid):
    """{tid: (name, ticks)} for every thread of the process."""
    out = {}
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return out
    for tid in tids:
        try:
            with open(f"/proc/{pid}/task/{tid}/stat") as f:
                stat = f.read()
        except OSError:
            continue
        name = stat[stat.index("(") + 1:stat.rindex(")")]
        fields = stat.rsplit(")", 1)[1].split()
        out[tid] = (name, int(fields[11]) + int(fields[12]))
    return out


def top_threads(a, b, secs, n=6):
    """The busiest threads over the window: [name, CPU %], 100% = one core."""
    rows = [(name, 100 * (ticks - a[tid][1]) / HZ / secs)
            for tid, (name, ticks) in b.items() if tid in a]
    return [[name, round(pct, 1)] for name, pct in sorted(rows, key=lambda r: -r[1])[:n]]


def core_ticks(cores):
    """(busy, total) clock ticks summed over the given CPUs."""
    busy = total = 0
    with open("/proc/stat") as f:
        for line in f:
            name, *vals = line.split()
            if not name.startswith("cpu") or name == "cpu" or int(name[3:]) not in cores:
                continue
            v = [int(x) for x in vals[:8]]
            idle = v[3] + v[4]
            busy += sum(v) - idle
            total += sum(v)
    return busy, total


class Dpu:
    def __init__(self, host, root, env):
        self.host, self.dir, self.env = host, f"{root}/bench/grpc/dpu", env

    def sh(self, cmd, check=True, timeout=120):
        exports = " ".join(f"{k}={shlex.quote(v)}" for k, v in self.env.items())
        full = f"cd {self.dir} && {'export ' + exports + ' && ' if exports else ''}{cmd}"
        r = subprocess.run(["ssh", "-n", self.host, full], capture_output=True, text=True, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError(f"dpu: {cmd}: {r.stdout}{r.stderr}")
        return r.stdout

    def proxy_ticks(self):
        """(proxy CPU ticks, DPU uptime seconds), both read on the DPU."""
        out = self.sh("cat /proc/$(cat run/proxy.pid)/stat /proc/uptime").splitlines()
        fields = out[0].rsplit(")", 1)[1].split()
        return int(fields[11]) + int(fields[12]), float(out[1].split()[0])


def snapshot(server, client, cores_c, cores_s, dpu):
    s = {"t": time.monotonic(), "server": proc_ticks(server.pid),
         "server_threads": thread_ticks(server.pid), "client_threads": thread_ticks(client.pid),
         "client_cores": core_ticks(cores_c), "server_cores": core_ticks(cores_s)}
    if dpu:
        s["proxy"] = dpu.proxy_ticks()
    return s


def window(a, b):
    secs = b["t"] - a["t"]
    out = {"host_window_seconds": secs,
           "server_cpu_pct": 100 * (b["server"] - a["server"]) / HZ / secs,
           "server_threads": top_threads(a["server_threads"], b["server_threads"], secs),
           "client_threads": top_threads(a["client_threads"], b["client_threads"], secs)}
    for k in ("client_cores", "server_cores"):
        busy, total = (y - x for x, y in zip(a[k], b[k]))
        out[k + "_busy_pct"] = 100 * busy / total if total else None
    if "proxy" in a:
        ticks, up = (y - x for x, y in zip(a["proxy"], b["proxy"]))
        out["proxy_cpu_pct"] = 100 * ticks / HZ / up
    return out


def wait_for(path, text, proc, seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if proc.poll() is not None:
            return False
        try:
            if text in open(path).read():
                return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
    ap.add_argument("--label", default="", help="build under test, recorded verbatim")
    ap.add_argument("--server", required=True, help="server command")
    ap.add_argument("--client", required=True, help="client command")
    ap.add_argument("--transport", choices=("dpumesh", "tcp"), default="dpumesh")
    ap.add_argument("--connections", type=int, default=1)
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--warmup", default="3s")
    ap.add_argument("--duration", default="10s")
    ap.add_argument("--lib", default="", help="LD_LIBRARY_PATH for both processes")
    ap.add_argument("--env", action="append", default=[], help="KEY=VALUE for both processes")
    ap.add_argument("--client-cpus", default="18-25")
    ap.add_argument("--server-cpus", default="26-33")
    ap.add_argument("--pool", type=int, help="server backend pool (default: connections)")
    ap.add_argument("--pci", default="94:00.0", help="host DPUMesh PCI function")
    ap.add_argument("--comch", default="DPUMeshBench0", help="Comch server name")
    ap.add_argument("--dpu", default="192.168.100.2")
    ap.add_argument("--dpu-dir", default="DPUMesh-online-boutique", help="DPUMesh checkout on the DPU")
    ap.add_argument("--dpu-env", action="append", default=[], help="KEY=VALUE for the DPU scripts")
    ap.add_argument("--l7", action="store_true", help="the proxy terminates HTTP/2")
    a = ap.parse_args()

    os.makedirs(a.out, exist_ok=True)
    base = os.path.join(a.out, a.tag)
    native = a.transport == "dpumesh"
    pool = str(a.pool or a.connections)
    env = dict(os.environ)
    if a.lib:
        env["LD_LIBRARY_PATH"] = a.lib
    env.update(kv.split("=", 1) for kv in a.env)
    if native:
        env.update(DPUMESH_PCI_ADDR=a.pci, DPUMESH_SERVER=a.comch, DPUMESH_REVERSE="dpu-dma")
    server_env = dict(env, DPUMESH_POD_IP=SERVER_POD, DPUMESH_SERVICE=f"{SERVICE_IP}:{SERVICE_PORT}",
                      DPUMESH_BACKEND_POOL=pool, DPUMESH_BACKEND_MAX=pool)
    client_env = dict(env, DPUMESH_POD_IP=CLIENT_POD, DPUMESH_SERVICE_IP=SERVICE_IP,
                      DPUMESH_SERVICE_PORT=str(SERVICE_PORT))
    tcp = ["-tcp", TCP_ADDR] if not native else []
    server_cmd = ["taskset", "-c", a.server_cpus] + shlex.split(a.server) + ["-mode", "server"] + tcp
    client_cmd = (["taskset", "-c", a.client_cpus] + shlex.split(a.client) +
                  ["-mode", "client", "-connections", str(a.connections), "-concurrency", str(a.concurrency),
                   "-warmup", a.warmup, "-duration", a.duration] + tcp)
    record = {"tag": a.tag, "label": a.label, "transport": a.transport, "l7": a.l7 if native else None,
              "connections": a.connections, "concurrency": a.concurrency, "pool": int(pool) if native else None,
              "server_cmd": server_cmd, "client_cmd": client_cmd, "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
              "problems": []}

    dpu = None
    if native:
        dpu_env = dict(kv.split("=", 1) for kv in a.dpu_env)
        dpu_env.update(BENCH_L7="1" if a.l7 else "0", DPUMESH_SERVER=a.comch)
        dpu = Dpu(a.dpu, a.dpu_dir, dpu_env)
        record["dpu_env"] = dpu_env
        dpu.sh(DPU_STOP, check=False)
        dpu.sh(f"bash start.sh mocks && sleep 2 && bash start.sh proxy {shlex.quote(a.tag)}")
        time.sleep(1)

    server = client = None
    try:
        with open(base + "-server.log", "w") as slog:
            server = subprocess.Popen(server_cmd, env=server_env, stdout=slog, stderr=subprocess.STDOUT)
        if not wait_for(base + "-server.log", "CHANNEL_BENCH_SERVER_READY", server, 60):
            raise RuntimeError("server not ready")
        cores_c, cores_s = cpus(a.client_cpus), cpus(a.server_cpus)
        snaps = {}
        result = None
        with open(base + "-client.log", "w") as clog, open(base + "-client.jsonl", "w") as cout:
            client = subprocess.Popen(client_cmd, env=client_env, stdout=subprocess.PIPE, stderr=clog, text=True)
            for line in client.stdout:
                cout.write(line)
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                if ev.get("event") in ("measure_start", "measure_end"):
                    snaps[ev["event"]] = snapshot(server, client, cores_c, cores_s, dpu)
                elif ev.get("event") == "result":
                    result = ev
            record["client_exit"] = client.wait()
        record["client"] = result
        if "measure_start" in snaps and "measure_end" in snaps:
            record.update(window(snaps["measure_start"], snaps["measure_end"]))
        if result is None or not result.get("ok"):
            record["problems"].append("client: " + (result or {}).get("error", "no result"))
    except Exception as e:  # noqa: BLE001 - recorded, then the run is invalid
        record["problems"].append(str(e))
    finally:
        for p in (client,):
            if p and p.poll() is None:
                p.kill()
        if server and server.poll() is None:
            server.terminate()
            try:
                record["server_exit"] = server.wait(30)
            except subprocess.TimeoutExpired:
                server.kill()
                record["server_exit"] = "killed"
        elif server:
            record["server_exit"] = server.returncode
        if record.get("server_exit") != 0:
            record["problems"].append(f"server exit {record.get('server_exit')}")
        if dpu:
            time.sleep(2)
            log = f"run/proxy-{shlex.quote(a.tag)}.log"
            counts = dpu.sh(f"for p in '\\]\\[ERR\\]' flexio_crash_data 'device-side error' panicked; "
                            f"do grep -c \"$p\" {log}; done", check=False).split()
            record["dpu"] = dict(zip(("err", "flexio_crash", "device_error", "panics"),
                                     (int(x) for x in counts)))
            record["dpu_stop"] = dpu.sh(DPU_STOP, check=False).strip()
            subprocess.run(["scp", "-q", f"{a.dpu}:{dpu.dir}/{log}", base + "-proxy.log"], check=False)
            if any(record["dpu"].values()):
                record["problems"].append(f"dpu: {record['dpu']}")
            if "KILL" in record["dpu_stop"]:
                record["problems"].append("dpu: proxy needed SIGKILL")
    record["ok"] = not record["problems"]
    with open(base + ".json", "w") as f:
        json.dump(record, f, indent=1)
    c = record.get("client") or {}
    print(f"{a.tag}: ok={record['ok']} qps={c.get('qps', 0):.0f} p50={c.get('latency_p50_us', 0):.0f}us "
          f"p99={c.get('latency_p99_us', 0):.0f}us server={record.get('server_cpu_pct', 0):.0f}% "
          f"proxy={record.get('proxy_cpu_pct') or 0:.0f}% {'; '.join(record['problems'])}")
    sys.exit(0 if record["ok"] else 1)


if __name__ == "__main__":
    main()
