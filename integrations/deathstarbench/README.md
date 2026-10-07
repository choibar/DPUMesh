# HotelReservation over the current DPUMesh Go API

This integration targets DeathStarBench `hotelres-bench-patches` commit
`312855ac450bcd35550a482517fd47e093a26ac4`. It patches the shared gRPC dialer,
nine service listeners, and ten command lifecycles. It uses
`integrations/grpc/go` / `libdpumesh.so.5`; **do not link `apps/dmeshgo` or
`libdmesh_hostlib`**. Host and DPU must run matching session-protocol builds.

The host runs native Go processes. Frontend HTTP, databases, cache, Consul and
Jaeger retain their original transports. DPU gRPC is plaintext with the proxy
performing L7 processing; TLS configuration is explicitly rejected in DMA mode.
Tracing and existing gRPC call options are preserved. The client's keepalive
interval is explicitly five minutes to match the server's default minimum
(the old timeout-only configuration resulted in `too_many_pings` GOAWAYs).

Each process opens one channel/Comch/EQ. `DPUMesh<i>` selects a control
connection alias; the dispatcher assigns each outgoing flow independently.
Topology schema 2 separates client service VIPs from replica DMA endpoints
(stable Pod IP + service port). The DPU resolves the VIP through manifest-backed
policy/discovery and retains HTTP P2C/Peak-EWMA selection per gRPC stream.

Listeners register their endpoint without preallocating backend flows. The first
request to a replica creates a backend on the requesting worker. Compatible
client flows on that worker share one H2 connection per replica; another worker
creates its own connection. Pools and connectors retain the logical worker ID
also in unsharded mode. Backend setup uses the control dispatcher, while request
and response processing stays on the owning worker in sharded mode.
Source policy/authorization/metrics remain outside the shared transport layer.
`DPUMESH_BACKEND_POOL` is no longer used. `DPUMESH_BACKEND_MAX` limits each Host
process's demand-created backends; generated topologies allow one per worker.
The Host's 32 flow slots also include outgoing connections. Topology budgets
model all worker/replica pairs and balanced clients, and need additional review
for skew and reconnects. No live flow migration or idle backend eviction is added.

Session protocol v2 requires matching rebuilt Host and DPU binaries; the public
Host ABI remains 5. First-use setup has a bounded wait and request admission is
bounded; connection creation is deduplicated per worker/replica. Listener
registration currently follows service-channel initialization, and the Host
must continue EQ progress while accepting connections.

The proxy supports both busy-poll and event-driven operation. Busy-poll remains
the launcher's default. For event mode, generate the topology with
`--poll-mode event`, then launch with `DSB_PROXY_POLL_MODE=event` alongside
`DSB_ROUTES_FILE`. The launcher rejects an explicit topology/mode mismatch and
records the selected mode in `launch.json`. Use `--poll-mode busy` and
`DSB_PROXY_POLL_MODE=busy` for a controlled comparison with identical routing.

Event mode waits on the control/data PE notification fds, local TX/FIN and RX
credit work, and new IO registrations. Both shared PEs use DOCA `PROGRESS_ALL`:
on Linux re-arming clears previous notifications, so the driver does not call
the explicit clear operation that failed after consumer teardown. A bounded
data drain and cooperative yields keep H2 tasks running under backpressure.
A 1ms safety timer still advances setup/private reverse-DMA work without a
shared notification fd; event mode is not a guarantee of zero periodic polls.
The Host Go DPU-DMA timer path is unchanged by this DPU mode switch.

The geo service serializes `KNearest` calls across server instances: the upstream
`go-geoindex` dependency lazily mutates a package-global longitude-distance map
even during searches. Without this guard, concurrent RPCs can terminate geo
with `concurrent map read and map write`. Both comparison modes use the guard.

The [replica audit](../../bench-results/2026-09-29_deathstarbench-replica-audit.md)
records the **previous** single-VIP backend registration defect. A schema 2 DSB
run must provide `DSB_ROUTES_FILE`; launching without it selects the legacy echo
Forward mode, which is retained for isolated echo benchmarks.

`hotelreservation.patch` contains the existing-source changes; `overlay/`
contains the new adapter, shutdown manager, tests and actual gRPC probe.
`prepare.py` copies a clean source to a fresh directory and leaves the input
untouched. Avoid using an independently modified registry: replica registration
IDs must remain unique. For example, create a clean worktree of the baseline:

```sh
git -C /path/to/DeathStarBench worktree add --detach /path/to/dsb-clean 312855ac
python3 /path/to/DPUMesh/integrations/deathstarbench/prepare.py \
  /path/to/dsb-clean/hotelReservation /path/to/hotel-current \
  --dpumesh /path/to/DPUMesh
bash /path/to/DPUMesh/integrations/deathstarbench/build-host.sh \
  /path/to/DPUMesh /path/to/hotel-current
```

Use Go 1.26+. `prepare.py` resolves Go dependencies without the old vendor tree;
its generated local replace is specific to the supplied checkout, not checked
into the upstream application. Record the resulting go.mod/go.sum and binary
hashes with the experiment.

On the host, create a separate infrastructure project with no published ports:

```sh
python3 infra.py /path/to/run-infra --project dsb-current up
cp /path/to/run-infra/config.json /path/to/hotel-current/config.json
python3 topology.py /path/to/run --workers 4 --pci 0b:00.1 \
  --cpus 0-11 --gomaxprocs 4
```

`infra.py` uses locally available Mongo 5.0, Memcached, Consul and Jaeger images
(`--pull never`), separate containers and data. Record their image IDs. Cleanup
uses `down` without deleting volumes. Configuration assumes host access to the
Docker bridge. The default TCP ports are 18081–18089; frontend is 15000.
For replicas, use e.g. `--replicas search:2,profile:2 --frontends 2`.
`--placement staggered` rotates the worker assignment by service as well as by
replica. This avoids placing every service's last-started replica on the same
worker when replica count equals worker count. It changes placement only;
the proxy's backend selection and connection reuse semantics remain the same.

The same manifest drives host endpoint registration and DPU service membership.
The `registry` text file remains a compatibility artifact; current native IPv4
address resolution does not depend on it. The bare-metal mock controllers read
`DMESH_ROUTES` and expose the normal policy/discovery APIs. This is not a
production Consul/Kubernetes watcher. DMA readiness is checked with gRPC probes,
not TCP connects to the endpoint keys.

On the DPU, build transport before proxy, then launch:

```sh
ninja -C /path/to/DPUMesh/src/transport/build
# From linkerd2-proxy; flags match the validated testbed release build.
RUSTFLAGS='--cfg tokio_unstable -C target-cpu=native' cargo rustc \
  -p linkerd2-proxy --release --offline --bin linkerd2-proxy -- \
  -C lto=thin -C codegen-units=16 \
  -C link-arg=-L/opt/mellanox/doca/lib/aarch64-linux-gnu \
  -C link-arg=-L/opt/mellanox/flexio/lib \
  -C link-arg=-ldoca_common -C link-arg=-ldoca_dpa -C link-arg=-lflexio
# Rebuild the controllers too: policy/destination now consume the manifest.
for bin in mock-policy mock-destination mock-identity; do
  RUSTFLAGS='--cfg tokio_unstable -C target-cpu=native' cargo rustc \
    -p linkerd-app-integration --release --offline --bin "$bin" -- \
    -C lto=thin -C codegen-units=16
done
# Generate the host topology first and copy the same file to the DPU.
DSB_ROUTES_FILE=/path/to/copied-topology.json \
  bash /path/to/DPUMesh/integrations/deathstarbench/proxy-start.sh 4 /path/to/dpu-run
```

Override `DSB_DPU_PCI`, `DSB_HOST_PCI`, `DSB_PROXY_ROOT` when needed. The launcher
requires the existing mock binaries, sets sharded + busy-poll, checks conflicts,
and records PID birth times and shard affinity. It never changes NIC/SF/EU
configuration. It keeps the current global DPA context and per-flow threads.

Back on the host:

```sh
python3 run.py start --run /path/to/run --source /path/to/hotel-current \
  --native-lib /path/to/DPUMesh/build/lib --mode dmesh
# start waits until every child is launched; validate checks actual readiness.
python3 validate.py /path/to/run
python3 measure.py /path/to/run --wrk /path/to/wrk2/wrk --cpus 12-15
python3 run.py stop --run /path/to/run
```

Between configurations, stop the host stack and use
`reset-data.py /path/to/run-infra --project dsb-current --stopped-run /path/to/run`
to reset the isolated reservation collection and caches. It verifies that the
recorded processes exited and only addresses the named Compose project.
If a previous run reported a cleanup error, preserve that result and verify
proxy cleanup separately. `--allow-failed-stop` permits resetting isolated data
only after every recorded host process has exited; it does not mark the old
run as successfully stopped.

Run `observe.py /path/to/dpu-run` alongside measurements to collect per-shard
CPU samples. Use a fresh run directory with `--mode tcp` for the direct-TCP
reference, with identical affinity, replicas, GOMAXPROCS and infrastructure.
Direct TCP is a transport reference, not an equivalent mesh-proxy baseline.
`validate.py` checks all nine health RPCs, a seeded user login, eight frontend
paths including reservation, and 64 concurrent searches. It writes a reservation
in the isolated test database; use separate/reset data for strict comparisons.
`measure.py` keeps original mixed-workload logic and only updates its frontend
URL. It currently supports one frontend and writes offered/achieved RPS,
latencies, errors, commands and host CPU. For multiple frontends,
`measure-replicas.py` launches one wrk2 process per frontend simultaneously:

```sh
python3 measure-replicas.py /path/to/run --wrk /path/to/wrk2/wrk \
  --rate 8000 --duration 60 --tag rate-8000-r1 \
  --infra /path/to/run-infra --project dsb-current
```

`--rate` is the aggregate offered RPS, divided across all frontends; the default
is 64 HTTP connections per frontend. Each fresh tag retains its commands, raw
HDR output, per-process CPU samples, infra CPU and result JSON. RPS is summed;
percentiles stay per frontend (the reported maximum frontend p99 is **not** a
merged/global p99). Overload results are retained even when errors occur.
`meets_offered_rate` means no HTTP/socket/process errors and achieved/offered
RPS >= 98%; it does not impose a latency SLO. Use warmup, a load sweep and
repeated longer confirmation trials to bracket capacity.

Native slots and reconnect headroom are estimated by `topology.py`; confirm
actual per-worker backend counts and EU usage.

Stop DPU only after host processes have drained:

```sh
python3 proxy-stop.py /path/to/dpu-run
python3 infra.py /path/to/run-infra --project dsb-current down
```

Shutdown closes gRPC/HTTP servers with a five-second drain deadline, closes
outbound ClientConns/listeners, then calls `CloseTransport`. Cleanup failures
produce nonzero exit status. Run cleanup checks executable and PID birth time
and sends only scoped SIGTERM; it reports stuck processes instead of silently
killing them. Preserve those logs separately from traffic results.

The native descriptor-ring kernel now checkpoints its consumer cursor and
cumulative submitted-copy count, then immediately retriggers after a bounded
activation. Idle and receive-credit waits are bounded too. This avoids keeping
a polling kernel scheduled indefinitely past the DPA watchdog limit; it does
not change the per-flow DPA thread allocation model. See
`tests/dpa_poll_test.py` for CPU-side SDK-substitute tests of the actual loop,
and the hardware validation report for long-lived channel tests.

Validated hardware results, resource counts, limitations and raw-log locations:
[2026-09-29 integration report](../../bench-results/2026-09-29_deathstarbench-current-api.md).
This includes direct TCP, sharded DMA with 1/2/4 workers, a 185-second idle
interval, and two 13-process replica runs using the same live DPU proxy.

## Replica routing checks

Generate the topology with `--audit-rpcs` to enable optional `DSB_AUDIT_DIR`
server counters, then use the single-connection probe:

```sh
python3 routing-probe.py /path/to/run --calls 2048 --parallel 64 \
  --tag single-connection --require-spread
python3 routing-probe.py /path/to/run --service srv-search \
  --endpoint search-0 --tag direct-search-0
```

`--require-spread` requires the diagnostic server method counters in `run/stats`;
`dmesh.NewServer` attaches a stats handler only when this directory is set.
The probe checks real user
business RPCs, not just health calls. Per-endpoint probes also work without the
counters. Keep readiness/probe traffic outside measured windows.

On the DPU, update an existing replica and wait for all route consumers:

```sh
python3 update-endpoint.py /path/to/copied-topology.json --proxy-run /path/to/dpu-run \
  --endpoint user-0 --enabled false
```

The update uses an atomic file replacement and increasing generation. The command
acknowledges manifest consumption by the three processes; discovery delivery to
every cached balancer is asynchronous. The hardware lifecycle test allowed one
second for propagation before checking replica counters. Invalid or
stale snapshots retain the last valid routes. Service identity and worker-count
changes require restarting the stack. Endpoint worker metadata selects the
control alias; a backend flow is always created on the requesting worker. Disabled endpoints remain known DMA destinations;
closed/unpublished DMA backends never fall through to TCP. Request retries are
not added, including for reservation RPCs. Ordinary endpoint readiness is distinct
from membership; absence of a backend flow triggers lazy creation, not exclusion
from routing.


Implementation and hardware evidence:
[replica routing report](../../bench-results/2026-09-29_deathstarbench-replica-routing.md).
The endpoint owner uses a bounded 128-request queue, including same-worker calls;
client readiness inside that owner has a three-second limit. This does not add
an HTTP/2 hop. Removal stops new dispatch and releases the cached client;
already dispatched calls follow their existing cancellation/deadline lifecycle.
A separate forced streaming-drain timer and live worker migration are not provided.


## External wrk2 and host CPU comparison

`measure-remote.py` runs on the service Host and starts the bounded
`remote-load.py` runner over SSH on a separate load generator. Copy the runner
and a compatible wrk2 binary into a fresh remote directory first. HTTP goes
directly to `--target`, not through SSH. Ensure the frontend ports are reachable.
For example, with the load generator on r4:

```sh
ssh r4 'mkdir -p /tmp/dsb-load'
scp remote-load.py /path/to/wrk2/wrk r4:/tmp/dsb-load/
python3 measure-remote.py /path/to/run --tag rate4000-r1 --rate 4000 \
  --duration 30 --loadgen r4 --target HOST_ETHERNET_IP \
  --remote-root /tmp/dsb-load --wrk /tmp/dsb-load/wrk --cpus 0-3 \
  --connections 64 --infra /path/to/run-infra --project dsb-current
```

Use `--rate 0` for idle CPU. `--profile` adds separate 20-second, 99Hz
frame-pointer perf and counter diagnostics; it requires noninteractive sudo
for perf and ownership of its two output files. Keep profiled trials out of
throughput summaries. Both machines' clocks should agree (CPU windows use the
remote load interval). Record clock-check bounds and preserve the per-host
samples if comparing small differences.

The output separates application user/system CPU, infrastructure CPU, r4
load-generator CPU, whole-host CPU/IRQ samples, interface bytes, HTTP errors,
per-frontend latency, and optional server RPC counters. 100% application CPU
means one core; CPU microseconds per HTTP request include application/runtime
work, not just transport. Compare identical replica counts and achieved loads.
The launcher temporarily reserves future listener ports during process startup
to reduce collisions when the Host's ephemeral range overlaps service ports;
it does not change system port allocation settings.

To compare with the September 3 HotelReservation workload, use the DSB wrk2
fork with `--threads 4 --distribution exp` (build it on the load generator if
the Host binary requires a newer glibc). Keep the original replica specification:
`reservation:4,rate:4,search:4,profile:2,geo:2,recommendation:2`, four frontends,
and 64 HTTP connections per frontend. This is 25 Host processes, not 40.

Process count alone does not balance DPU routing work. Each process channel's
outgoing connections share its worker, so placing a frontend and a search
sender together concentrates both layers of fan-out on one core. The topology
generator accepts `--frontend-workers` to place ingress independently while
preserving backend endpoints and checking each worker's flow budget. Record
the full topology with each measurement; a good mapping depends on workload
and replica counts. `--rate` is offered HTTP load, not a measured maximum.

For the 25-process, 16-worker comparison above, the final measured placement
uses `--frontend-workers 14,15,12,13`; search senders retain workers 6–9.
See the [historical regression comparison and fix](../../bench-results/2026-09-29_dsb-hotelres-performance-fix.md)
for the exact topology, polling changes, repeated results, and CPU definitions.


For one wrk process targeting a shared frontend NodePort, pass
`--frontend-port NODEPORT` to `measure-remote.py`. This overrides the default
one-client-per-frontend behavior: `--threads`, `--connections`, and `--rate`
then apply to the single process. For example, use `--target 10.8.8.2
--frontend-port NODEPORT --threads 10 --connections 1000 --duration 20
--cpus 0-9 --distribution exp`. The runner records the exact `-L` command.

The NodePort must point to the **current DPUMesh frontend endpoints**. An existing
HotelReservation Service backed by other Pods measures that separate deployment.
For native Host processes, a selectorless experimental Service can use explicit
EndpointSlices (one slice per distinct frontend port). Service traffic remains
HTTP/TCP into the frontend; backend gRPC calls use DPUMesh. Kubernetes distributes
TCP connections, so HTTP keep-alive requests on a connection stay on its selected
frontend. Keep the experiment Service isolated and remove only its owned resources
after measuring. The 20-second runs include wrk2 startup/calibration; do not treat
them as interchangeable with the previous 30-second, multiple-process runs.
