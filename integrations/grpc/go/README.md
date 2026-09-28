# Go gRPC transport

`dmeshgo` implements `net.Conn` and `net.Listener` over the native API. One
process owns one channel, one shared host–DPU Comch control connection and one
EQ poller. Each Go connection owns a native QP; DMA rings, buffers and DPA
resources remain per flow. Reads retain native RX leases until consumed, and
writes resume on EQ readiness. Concurrent read/write, deadlines and connection
close follow the Go networking contract. The poller keeps progressing while any
QP exists, including when no goroutine is blocked in Read or Write, so shared
control events and buffered TX deadlines continue to run.

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

Both `dpu-dma` and `host-dpa` passed on the jet1/BF-3 testbed. See the
[hardware validation report](../../../docs/2026-09-25_channel-comch-grpc-validation.md)
for topology, exact environment, build commands and results.
