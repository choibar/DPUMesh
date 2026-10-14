# Native gRPC channel benchmark

This fixture sends unary gRPC echo calls with exactly 64 application payload
bytes. The raw codec avoids protobuf serialization; gRPC and HTTP/2 framing
remain present. Client and server use the native DPUMesh transport.

With the normal DPUMesh service environment set in each process:

```sh
channel-bench -mode server
channel-bench -mode client -connections 3 -concurrency 64 -warmup 3s -duration 10s
```

`-tcp host:port` runs the same fixture over kernel TCP for a baseline: the
server listens there and the client dials it, with no DPUMesh environment.
The wire protocol is fixed — method `/dmesh.ChannelBench/Echo`, 64-byte raw
request echoed unchanged, bytes 0–7 the little-endian sequence number and
8–15 the worker ID — so a fixture in another language interoperates with
this one in either role.

`-connections` accepts 1 through 4. `-concurrency` is the total number of RPC
loops, distributed as evenly as possible across the connections. At 64 total
loops, 3 connections receive 22, 21, and 21 loops. Each connection completes a
verified preflight RPC before warmup begins. Every request carries a worker ID
and sequence number, and every response is compared to the full request.

For multiple client processes, pass the same optional `-start-file /path/start`
to each client. Each process emits a JSON `load_ready` event after all its
preflight RPCs and RPC workers are ready. After receiving every readiness event,
the controller publishes a common future load-start time as an RFC3339Nano
timestamp in that file. Write a temporary file and rename it to the target path
atomically; use a new path for each run. Clients poll every 5 ms, then wait until
the specified time before starting warmup. Measurement begins at the common
start time plus `-warmup` and ends after `-duration`. Timestamps that are invalid
or already past when read fail the run. The overall `-timeout` bounds both waits;
cancellation also releases waiting workers and closes native connections.
Without `-start-file`, startup behavior is unchanged. Connection limits remain
1 through 4 per process, and concurrency is still a per-process total.

The measurement counts successful RPC completions in the fixed monotonic
interval `[measurement_start, measurement_end)`. Calls crossing the warmup
boundary are included; calls completing during the final drain are excluded.
Each measured completion contributes a latency sample. Percentiles use nearest
rank over all measured samples; there is no first-N sample limit. QPS uses the
fixed measurement duration, excluding startup and teardown.

Standard output contains JSON lines with `event` values `measure_start`,
`measure_end`, and `result`. Markers report both the scheduled `timestamp` and
actual `emitted_at`. The result includes QPS, completed requests, latency in
microseconds, per-connection native dial counts, RPC errors, reconnect count,
and process CPU usage. `client_process_cpu_pct` is user plus system CPU across
all client threads; 100% corresponds to one fully occupied CPU. Its sampling
interval is reported separately as `client_cpu_sample_seconds`.

Every RPC has a deadline (`-rpc-timeout`, default 5 seconds). Any RPC failure,
payload mismatch, reconnect, or native close failure invalidates the run and
causes a nonzero exit. The client closes every connection and the shared native
channel before emitting the final result. SIGTERM/SIGINT gracefully stops the
server, observes native connection close errors, and releases its channel.

Measured single-ARM-core results for both reverse modes and 1–4 connections:
[2026-09-25 benchmark report](../../../../../bench-results/2026-09-25_grpc-go-64b-1core.md).
