# Go gRPC transport

`dmeshgo` implements `net.Conn` and `net.Listener` over the native API. One
process owns one channel, one shared host–DPU Comch control connection and one
EQ poller. Each Go connection owns a native QP; DMA rings, buffers and DPA
resources remain per flow. Reads retain native RX leases until consumed, and
writes resume on EQ readiness. Concurrent read/write, deadlines and connection
close follow the Go networking contract. The poller keeps progressing while any
QP exists, including when no goroutine is blocked in Read or Write, so shared
control events and buffered TX deadlines continue to run.

DPU-DMA has no data completion doorbell. Its poller uses a Go timer with
2–128µs empty-poll backoff, reset by received events and local writes/waiters;
actual wake latency also depends on the Go scheduler. It does not enable native
fd notifications solely to poll memory again. Retained TX deadlines shorten
the next wait, and writes wake the poller even without a parked reader.

For host-DPA the EQ fd is registered with Go's runtime netpoller using an owned
duplicate. Waiting parks the goroutine without blocking an OS thread in cgo;
native code still owns and drains the EQ and its doorbells. The maximum wait
is 1ms even without a completion. `DPUMESH_SPIN_US` and `DPUMESH_TICK_US` retain
their native fd behavior; they do not control the DPU-DMA Go timer path.

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
The unit tests open no DOCA device.

Configure `DPUMESH_PCI_ADDR`, `DPUMESH_POD_IP` and `DPUMESH_SERVER` before
opening a connection. A server additionally sets `DPUMESH_SERVICE` to its
`<host>:<port>` [service target](../../../design/API.md#naming).
[Root configuration](../../../README.md#configuration) defines these values. The older `Dial`/`Listen` signatures accept only labels
that agree with this process configuration; they do not create separate
physical registrations.

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
Host and DPU must both use session protocol v2 (public ABI5 is unchanged).

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
the timer/netpoll changes and the payload larger than 1 MiB added above. The new
host-DPA waiter has unit coverage for nested readiness and fd ownership; the
historical report does not establish hardware validation of that waiter.

### Worker-local backend benchmark

`scripts/worker_backend_bench.py` runs an isolated Host/DPU matrix for the lazy,
per-worker backend implementation. It uses one Host client process, 64-byte echo,
64 in-flight RPCs per client flow, sharded busy-poll DPU workers, and DPU-DMA.
The default matrix uses 1/2/4/8 workers, 1 or W server replicas, and W/2W/4W
client flows (at most the current 32-flow channel limit). Client flow placement
is round-robin to hold each worker's client flow count constant; backend flows
remain pinned to the requesting worker.

Build protocol-v2 Host libraries and `bench-client`/`bench-server` under the
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
