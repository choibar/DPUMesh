# Go gRPC transport

`dmeshgo` implements `net.Conn` and `net.Listener` over the native API. One
process owns one channel, one shared host–DPU Comch control connection, one EQ
and one poller goroutine. Each Go connection owns a native QP; DMA rings,
buffers and DPA resources remain per flow.

- The poller is the EQ's only consumer. It polls the EQ without blocking and,
  while it is empty, parks on Go's netpoller with a duplicate of the EQ fd
  (`eq_wait_linux.go`); no OS thread waits in cgo. The library's timers (naps,
  retained-tail deadlines, the backstop) and doorbells raise that fd, so the
  wait needs no Go timer. Commands interrupt it through the read deadline. It
  hands receive leases to their connections and runs every QP destruction;
  the native API requires destruction to be serialized with polling.
- Read, Write and Dial run on the caller's goroutine. Each connection has its
  own lock, so no connection waits for another's I/O. A Dial waits for the
  DPU's answer without blocking other traffic; a `DialContext` whose context
  ends first returns at once, and the late stream is aborted. Bytes or a FIN
  the peer sends before the dial returns (an HTTP/2 server's SETTINGS) are
  held for the new connection, not dropped.
- Reads keep native receive leases until the bytes are consumed. A write that
  finds no transmit capacity waits for the EQ's TX_READY. A writer blocked on
  a departed peer fails with `EPIPE`, because that peer returns no credit.
- Concurrent Read/Write, deadlines and Close follow the Go networking
  contract. Close wakes blocked calls, returns queued leases and reports the
  native close result once.
- A channel that serves `DPUMESH_SERVICE` is polled for the transport's whole
  life. Streams that arrive before the first `Listen` are held for it. Once a
  listener has closed, new streams are aborted at once instead of left
  hanging.

Both reverse paths wait the same way. The EQ fd is registered with Go's
runtime netpoller using an owned duplicate, so waiting parks the goroutine
without blocking an OS thread in cgo; native code still owns and drains the EQ
and its doorbells. The library arms that fd for what has no doorbell: its nap
and linger timers, retained TX deadlines and the backstop, and on DPU-DMA the
DOORBELL the DPU sends after an ARM (`design/HOST.md`, idle wake). The
knobs are `DPUMESH_NAP_US`, `DPUMESH_NAP_CAP_US`, `DPUMESH_LINGER_US` and
`DPUMESH_BACKSTOP_MS`; the poller adds no Go timer of its own.

Build the native library from the repository root, then compile the module:

```sh
make lib
(cd integrations/grpc/go && go test -race ./...)
(cd integrations/grpc/go && go build -a -o bin/echo-client ./cmd/echo-client)
(cd integrations/grpc/go && go build -a -o bin/echo-server ./cmd/echo-server)
```

Use Go 1.26 or newer. Rebuild the native library for the host architecture and
rebuild the Go binaries after transport changes; the cgo build links
`build/lib/libdpumesh.so` from this checkout. The DPU transport and proxy must
also be rebuilt: the session control protocol is incompatible with the old
per-flow Comch implementation, although the public C ABI remains version 5.
The unit tests open no DOCA device. They run the adapter over an in-memory
native with the same lease, credit, TX_READY and FIN contract, including real
gRPC-go clients and servers: message sizes around the fragment and post
limits, concurrent calls, deadline and cancellation resets, GracefulStop,
client close and keepalive pings. The fake poisons released buffers and
checks for double releases and leaked leases.

Configure `DPUMESH_POD_IP` and `DPUMESH_SERVER` before opening a connection,
and either `DPUMESH_PCI_ADDR` (the process opens the DOCA device itself) or a
running `dpumesh_broker` that owns the device for it (`DPUMESH_BROKER`, default
`/run/dpumesh/broker.sock`). A server additionally sets `DPUMESH_SERVICE` to its
`<host>:<port>` [service target](../../../design/API.md#naming).
[Root configuration](../../../README.md#configuration) defines these values. The older `Dial`/`Listen` signatures accept only labels
that agree with this process configuration; they do not create separate
physical registrations.

The `dmeshgo/dmeshgrpc` package switches a program by configuration alone:
with `DPUMESH_ENABLE=1`, `dmeshgrpc.Listen(tcpAddr)` serves `DPUMESH_SERVICE`
and `dmeshgrpc.DialOptions()` routes `"<ip>:<port>"` targets over DPUMesh;
otherwise they return a TCP listener and no options. `cmd/health-bench` loads
any server's standard gRPC health `Check` and reports calls/s and latency per
target, over DPUMesh with `DPUMESH_ENABLE=1` and over TCP otherwise.

Use `DialContext(ctx, serviceIP, port)` in `grpc.WithContextDialer` and
`ListenService()` with `grpc.Server.Serve`. A service address is a Service
ClusterIP and port; the DPU chooses its native backend. `ListenService` serves
`DPUMESH_SERVICE`, which the native library resolves when it opens the channel,
so a server names no ClusterIP; its `Addr` is the Service port, with the IP only
when the target is an IPv4 literal. `ListenAddress(serviceIP, port)` is
deprecated: it also checks that the target resolves to that address. The
examples in `cmd/echo-client` and `cmd/echo-server` run the standard gRPC
health RPC; the client dials `DPUMESH_SERVICE_IP:DPUMESH_SERVICE_PORT`.
The service channel registers a listener without opening spare backend flows.
On first use, the DPU requests a flow pinned to the client flow's worker; H2
clients on that worker share it per replica. The Go server accepts one connection
per active worker/replica pair. `DPUMESH_BACKEND_POOL` is ignored;
`DPUMESH_BACKEND_MAX` caps backends within the channel's shared 32-flow capacity.
Host and DPU must both use session protocol v3 (public ABI5 is unchanged).

Close all connections and listeners before calling `CloseTransport`. It returns
`EBUSY` without invalidating active objects. If native channel teardown fails,
the Go wrapper retains the channel and `CloseTransport` can be retried; new
connections remain disabled until cleanup succeeds. Native QP destruction has
a different ABI5 contract: `Conn.Close` consumes its QP even if it returns an
error, so the QP itself must not be retried. A failed per-flow teardown keeps
its native resources owned by the channel for subsequent channel cleanup.

## Real hardware lifecycle test

Build `go build -a -o bin/channel-smoke ./cmd/channel-smoke`. With separate
configured server/client processes, run `./bin/channel-smoke -mode server` and
`timeout 120s ./bin/channel-smoke -mode client -rounds 40 -timeout 90s -rpc-timeout 5s`.
It verifies payload bytes, keeps a sibling connection active during repeated
close/reopen, checks native close errors, and recreates the process channel.
The server handles SIGTERM by stopping gRPC and closing its native transport.

Earlier versions of both `dpu-dma` and `host-dpa` passed on the jet1/BF-3 testbed. See the
[hardware validation report](../../../docs/2026-09-25_channel-comch-grpc-validation.md)
for topology, exact environment, build commands and results. That report predates
the netpoller wait, the idle wake and the payload larger than 1 MiB added above.
The waiter has unit coverage for nested readiness, fd ownership and interrupts.
`dpu-dma` with this waiter passed the hardware regression
(`bench-results/2026-09-30_host-idle-wake.md`); `host-dpa` was not available on
that testbed.

### Worker-local backend benchmark

`scripts/worker_backend_bench.py` runs an isolated Host/DPU matrix for the lazy,
per-worker backend implementation. It uses one Host client process, 64-byte echo,
64 in-flight RPCs per client flow, sharded busy-poll DPU workers, and DPU-DMA.
The default matrix uses 1/2/4/8 workers, 1 or W server replicas, and W/2W/4W
client flows (at most the current 32-flow channel limit). Client flow placement
is round-robin to hold each worker's client flow count constant; backend flows
remain pinned to the requesting worker.

Build protocol-v4 Host libraries and `bench-client`/`bench-server` under the
isolated `--host-root` first, and build the DPU release proxy and mock services.
On the current 12-online-core DPU and 16-core Host, run:

```sh
taskset -c 0-3 python3 integrations/grpc/go/scripts/worker_backend_bench.py \
  --out /tmp/dmesh-worker-local-bench/full \
  --host youngmin@192.168.100.1 \
  --host-root /tmp/dmesh-worker-backend-host
python3 integrations/grpc/go/scripts/summarize_worker_backend.py \
  /tmp/dmesh-worker-local-bench/full/results.json \
  --out /tmp/dmesh-worker-local-bench/summary
```

The harness expects DPU CPUs 4–11 and Host CPUs 0–15 to be online. It fixes Host
client affinity to 0–7 (`GOMAXPROCS=8`) and all server replicas to 8–15
(`GOMAXPROCS=4` each). Workers use descending DPU CPUs starting at 11. It does
not online/offline CPUs or change device/network settings. Scoped cleanup checks
process identity before SIGTERM and never uses SIGKILL. Existing DPU proxies
cause startup to fail rather than being stopped.

Each case uses 10 seconds of warm-up, 30 seconds of measurement, and three
repetitions. Results include measurement-window CPU samples, per-worker native
flow assignments, and cumulative H2 request locality counters (delta per run,
including warm-up). Thread-locality counters apply to sharded mode. The H2
request queue still exists within each worker; zero cross-thread counts verify
that its producer and consumer ran on the same OS thread. CPU use near 100% in
busy-poll mode alone does not establish a compute bottleneck.

Lazy backend admission allows up to 30 seconds for a cold worker/replica
connection; an earlier caller cancellation still aborts the request immediately.
This covers simultaneous setup of 64 backends in the 8-worker/8-replica case.
H2 service readiness retains its separate three-second bound. A regression test
publishes the backend after six seconds to exercise the previous five-second
failure boundary.

`cmd/worker-smoke` and `scripts/worker_backend_lifecycle_host.py` additionally
check concurrent first RPCs, closing one client connection while others continue,
and replacing the server process while client flows remain alive. Payloads carry
unique sequence patterns, including boundary sizes up to 1,048,577 bytes.
