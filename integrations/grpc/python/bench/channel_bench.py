#!/usr/bin/env python3
"""channel-bench for grpcio: the peer of integrations/grpc/go/cmd/channel-bench.

It speaks the same wire (/dmesh.ChannelBench/Echo, a 64-byte raw request
echoed unchanged, bytes 0-7 the little-endian sequence number and 8-15 the
worker ID), takes the same flags and prints the same JSON lines, over DPUMesh
(the patched grpcio and dpumesh_grpc) or, with -tcp host:port, kernel TCP.
The server is a synchronous grpc.server with a 10-thread pool, as Online
Boutique's Python services run; the client keeps each RPC loop going from
future callbacks. -start-file is not supported. native_dials counts each
channel's transitions to READY: grpcio has no dial hook.
"""
import concurrent.futures
import datetime
import json
import math
import os
import signal
import struct
import sys
import threading
import time

METHOD = "/dmesh.ChannelBench/Echo"
PAYLOAD_BYTES = 64


def parse_flags(argv):
    """-name value, -name=value and the -- forms, as Go's flag package does."""
    flags = {}
    i = 0
    while i < len(argv):
        arg = argv[i]
        if not arg.startswith("-"):
            raise ValueError("unexpected argument " + arg)
        body = arg.lstrip("-")
        if "=" in body:
            name, value = body.split("=", 1)
        elif i + 1 < len(argv):
            name, value = body, argv[i + 1]
            i += 1
        else:
            raise ValueError("flag -%s needs a value" % body)
        flags[name] = value
        i += 1
    return flags


def duration_ns(text):
    """Go time.ParseDuration for one number and one unit of ns, us, ms, s, m or h."""
    for unit, scale in (("ns", 1), ("us", 1e3), ("ms", 1e6), ("s", 1e9), ("m", 60e9), ("h", 3600e9)):
        if text.endswith(unit) and text[: -len(unit)].replace(".", "", 1).isdigit():
            return int(float(text[: -len(unit)]) * scale)
    raise ValueError("invalid duration " + text)


def wall(mono_ns):
    """RFC 3339 wall time of a time.monotonic_ns() instant."""
    now = datetime.datetime.now(datetime.timezone.utc)
    at = now + datetime.timedelta(microseconds=(mono_ns - time.monotonic_ns()) / 1e3)
    return at.isoformat().replace("+00:00", "Z")


def emit(record):
    sys.stdout.write(json.dumps(record) + "\n")
    sys.stdout.flush()


def marker(event, at):
    emit({"event": event, "timestamp": wall(at), "emitted_at": wall(time.monotonic_ns())})


def run_server(tcp):
    import grpc

    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        "dmesh.ChannelBench",
        {"Echo": grpc.unary_unary_rpc_method_handler(lambda request, context: request)}),))
    stop = threading.Event()
    # Handled here, so the DPUMesh runtime closes at exit (atexit) after the
    # server stops.
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    if tcp:
        server.add_insecure_port(tcp)
        server.start()
    else:
        os.environ["DPUMESH_ENABLE"] = "1"
        import dpumesh_grpc

        dpumesh_grpc.serve(server, "[::]:0")
    sys.stderr.write("CHANNEL_BENCH_SERVER_READY service=%s\n" % (tcp or os.environ.get("DPUMESH_SERVICE", "")))
    sys.stderr.flush()
    while not stop.wait(1):
        pass
    server.stop(5).wait()
    sys.stderr.write("CHANNEL_BENCH_SERVER_CLOSED\n")
    sys.stderr.flush()
    return 0


class Worker:
    def __init__(self, load, worker_id, call, dials):
        self.load = load
        self.id = worker_id
        self.call = call
        self.dials = dials
        self.payload = bytearray([(worker_id + 1) & 0xFF] * PAYLOAD_BYTES)
        struct.pack_into("<Q", self.payload, 8, worker_id)
        self.sequence = 0
        self.request = b""
        self.started = 0
        self.completed = 0
        self.errors = 0
        self.error = None
        self.latencies = []

    def issue(self):
        load = self.load
        if load.cancel or time.monotonic_ns() >= load.end:
            load.finish()
            return
        self.sequence += 1
        struct.pack_into("<Q", self.payload, 0, self.sequence)
        self.request = bytes(self.payload)
        self.started = time.monotonic_ns()
        self.call.future(self.request, timeout=load.rpc_timeout).add_done_callback(self.done)

    def done(self, future):
        completed = time.monotonic_ns()
        try:
            reply = future.result()
            if reply != self.request:
                raise RuntimeError("payload mismatch: %d bytes sent, %d received" % (PAYLOAD_BYTES, len(reply)))
            if self.dials[0] != 1:
                raise RuntimeError("unexpected reconnect: %d connections" % self.dials[0])
        except Exception as e:  # noqa: BLE001 - one failure ends the run
            self.errors += 1
            self.error = "worker %d request %d: %s" % (self.id, self.sequence, e)
            self.load.cancel = True
            self.load.finish()
            return
        self.completed += 1
        if self.load.start <= completed < self.load.end:
            self.latencies.append(completed - self.started)
        self.issue()


class Load:
    def __init__(self, rpc_timeout, workers):
        self.rpc_timeout = rpc_timeout
        self.cancel = False
        self.start = self.end = 0
        self._running = workers
        self._lock = threading.Lock()
        self.done = threading.Event()

    def finish(self):
        with self._lock:
            self._running -= 1
            if self._running == 0:
                self.done.set()


def run_client(flags, tcp):
    import grpc

    connections = int(flags.get("connections", "1"))
    concurrency = int(flags.get("concurrency", "64"))
    warmup = duration_ns(flags.get("warmup", "3s"))
    duration = duration_ns(flags.get("duration", "10s"))
    rpc_timeout = duration_ns(flags.get("rpc-timeout", "5s")) / 1e9
    ip, port = os.environ.get("DPUMESH_SERVICE_IP"), os.environ.get("DPUMESH_SERVICE_PORT")
    if (not tcp and not (ip and port)) or not 1 <= connections <= 4 or concurrency < connections or duration <= 0:
        sys.stderr.write("set DPUMESH_SERVICE_IP/PORT for the client, connections 1..4, "
                         "concurrency >= connections, and valid durations\n")
        return 2
    if not tcp:
        os.environ["DPUMESH_ENABLE"] = "1"
        import dpumesh_grpc
    per_conn = [concurrency // connections + (1 if i < concurrency % connections else 0) for i in range(connections)]
    # A local subchannel pool keeps each channel on its own connection.
    options = [("grpc.use_local_subchannel_pool", 1), ("grpc.enable_retries", 0)]
    channels, calls, dials, errors = [], [], [], []
    rpc_errors = 0
    for i in range(connections):
        channel = grpc.insecure_channel(tcp, options) if tcp else \
            dpumesh_grpc.insecure_channel("%s:%s" % (ip, port), options)
        ready = [0]

        def watch(state, ready=ready, last=[None]):
            if state == grpc.ChannelConnectivity.READY and last[0] != state:
                ready[0] += 1
            last[0] = state

        channel.subscribe(watch, try_to_connect=False)
        call = channel.unary_unary(METHOD)
        channels.append(channel)
        calls.append(call)
        dials.append(ready)
        payload = bytes([(i + 1) & 0xFF] * PAYLOAD_BYTES)
        try:
            if call(payload, timeout=rpc_timeout) != payload:
                raise RuntimeError("payload mismatch")
        except Exception as e:  # noqa: BLE001
            rpc_errors += 1
            errors.append("connection %d preflight: %s" % (i + 1, e))
            break
        # Connectivity callbacks run on their own thread; let READY land.
        settle = time.monotonic() + 1
        while ready[0] == 0 and time.monotonic() < settle:
            time.sleep(0.001)
    workers, cpu_pct, cpu_seconds, start, end = [], 0.0, 0.0, 0, 0
    if not errors:
        sys.stderr.write("PREFLIGHT_OK connections=%d concurrency=%s payload=%dB\n"
                         % (connections, per_conn, PAYLOAD_BYTES))
        load = Load(rpc_timeout, concurrency)
        worker_id = 0
        for conn in range(connections):
            for _ in range(per_conn[conn]):
                workers.append(Worker(load, worker_id, calls[conn], dials[conn]))
                worker_id += 1
        load_start = time.monotonic_ns()
        load.start = start = load_start + warmup
        load.end = end = start + duration
        for w in workers:
            w.issue()
        time.sleep(max(0, start - time.monotonic_ns()) / 1e9)
        cpu_start, cpu_start_at = time.process_time(), time.monotonic_ns()
        marker("measure_start", start)
        time.sleep(max(0, end - time.monotonic_ns()) / 1e9)
        cpu_seconds = (time.monotonic_ns() - cpu_start_at) / 1e9
        cpu_pct = 100 * (time.process_time() - cpu_start) / cpu_seconds
        marker("measure_end", end)
        if not load.done.wait(rpc_timeout + 30):
            errors.append("workers still running after the measurement")
    latencies = sorted(l for w in workers for l in w.latencies)
    for w in workers:
        rpc_errors += w.errors
        if w.error:
            errors.append(w.error)
    for channel in channels:
        channel.close()
    dial_counts = [d[0] for d in dials]
    errors += ["expected one connection per channel, got %d" % d for d in dial_counts if d != 1]
    elapsed = (end - start) / 1e9
    if not errors and not latencies:
        errors.append("no successful RPC completions in measurement window")

    def pct(p):
        return latencies[math.ceil(p * len(latencies)) - 1] / 1e3 if latencies else 0

    result = {
        "event": "result", "ok": not errors, "connections": connections, "concurrency": concurrency,
        "concurrency_per_conn": per_conn, "payload_bytes": PAYLOAD_BYTES,
        "warmup_seconds": warmup / 1e9, "duration_seconds": duration / 1e9, "elapsed_secs": elapsed,
        "client_process_cpu_pct": cpu_pct, "client_cpu_sample_seconds": cpu_seconds,
        "measurement_start": wall(start), "measurement_end": wall(end),
        "completed": len(latencies), "total_completed_including_warmup_and_drain": sum(w.completed for w in workers),
        "rpc_errors": rpc_errors, "native_dials": dial_counts, "reconnects": sum(max(0, d - 1) for d in dial_counts),
        "qps": len(latencies) / elapsed if elapsed > 0 else 0,
        "latency_mean_us": sum(latencies) / len(latencies) / 1e3 if latencies else 0,
        "latency_p50_us": pct(0.5), "latency_p99_us": pct(0.99),
    }
    if errors:
        result["error"] = "\n".join(errors)
    emit(result)
    if errors:
        sys.stderr.write("\n".join(errors) + "\n")
    return 0 if not errors else 1


def main():
    try:
        flags = parse_flags(sys.argv[1:])
    except ValueError as e:
        sys.stderr.write("%s\n" % e)
        return 2
    tcp = flags.get("tcp", "")
    mode = flags.get("mode", "client")
    if mode == "server":
        return run_server(tcp)
    if mode == "client":
        return run_client(flags, tcp)
    sys.stderr.write("unknown mode %s\n" % mode)
    return 2


if __name__ == "__main__":
    sys.exit(main())
