# Native Host–Host RDMA fixture

This fixture keeps DPUMesh's native Comch session/flow dispatcher and Linkerd
`DMESH_SHARDED=1` execution path. It adds the jukebox peer transport beneath
Linkerd's connector. Each worker keeps its own current-thread Tokio runtime;
the App, tracing subscriber and metrics registry are still shared.
`DMESH_SHARDED=1` does not create a private tracing subscriber per worker.

The path is:

```
Host gRPC -> native DMA -> source Linkerd L7 -> peer RDMA
          -> destination Linkerd L7 -> native DMA -> Host gRPC server
```

The private peer DPA ABI is separate from the native Host DMA ABI. The native
and peer threads share the node DPA runtime; SDK object creation/destruction
uses its SDK mutex, while each worker owns its progress engines and QPs.

The current Pod-pair data codec is `dmesh4/1`: a four-byte type/namespace/slot/
length header with no stream generation. Control protocol v3 and the matched
ARM/DPA ABI are required on both peers; legacy formats are rejected. A handle
is reused only after reader/custody/NIC fences and bilateral READY/RELEASE/ACK.
See [the wire/lifecycle contract](../../design/DMESH4.md). Node-pool sharing
remains a separate proposal.

## Build and run

Build on a DOCA 3.5 ARM DPU with `bash bench/peer-e2e/build-dpu.sh`. The script
builds the native transport, DPA app, Linkerd executable and development L7
control-plane services. Use an isolated checkout; it does not deploy anything.

`run-dpu.sh` runs the DPU side in the foreground. Required inputs include:

| Setting | Meaning |
| --- | --- |
| `DPUMESH_NODE_NAME` | Paired Host's Kubernetes node name |
| `DPUMESH_PEER_RDMA_DEVICE`, `DPUMESH_PEER_GID_INDEX` | Fabric verbs device/GID |
| `DMESH_NUM_WORKERS`, `PROXY_CPUS` | Worker count and distinct allowed ARM CPUs |
| `DPUMESH_TOPOLOGY_FILE`, `DPUMESH_CONTROLLER_KEY_DIR` | Signed Kubernetes topology and verification keys |
| `DPUMESH_NODE_KEY` | Node's private authentication key |
| `DPUMESH_PEER_CRYPTO` | Existing inline-IPsec owner socket (`unix:...`) |
| `DPUMESH_LOCAL_*` | Paired-Host mutual TLS control address and certificates |

The peer data path is always WRITE_WITH_IMM+DPA. Setting
`DPUMESH_PEER_RDMA_DEVICE` enables it; without a fabric device setting, the
runtime remains local-only. The former transport, offload and Sync-wait
selectors have been removed. SEND, WRITE+Sync and ARM-only IMM sources and
their tests are archived in `src/transport/peer/legacy`, outside the build.
Native Host DMA remains independent. Rebuild ARM, DPA and the proxy together
when updating the private peer ABI. Bootstrap still uses one protected 64-byte
SEND per lane; steady-state DATA and credits use WRITE_WITH_IMM.

The D2D resource budget is defined in
`src/transport/peer/peer_config.h`: `PEER_RDMA_CLUSTER_NODES=4`
(including this node), `PEER_RDMA_MAX_WORKERS=16`, and
`PEER_RDMA_QPS_PER_PEER=1`. ARM and DPA lane tables, pending-work lane lists,
QPN hashes and CQ requests derive from these values. Change the shared header
and rebuild both ARM and DPA, then relink the proxy; these are compile-time
budgets, not environment variables. H2D DMA capacity is independent.

For 16 workers, run with `DMESH_NUM_WORKERS=16` and 16
distinct CPUs in `PROXY_CPUS`. Each worker creates one RDMA processing DPA
thread, with a separate SDK yield helper on the same EU. The default
`DMESH_PEER_FIRST_EU=32` maps processing workers 0..15 to EUs 32..47. Each
worker reserves three QP slots, for at most 48 local QPs across three peers;
QPs are created on demand. The requested per-worker DPA CQ size is 2,048 for WRITE_WITH_IMM.
The SDK may round allocations further.

**This is a resource/admission limit on the current Pod-pair transport.**
It allows only one Pod-pair lane per peer node per worker, counting inbound,
outbound, retiring and quarantined slots. A second Pod pair to the same peer
is rejected until the old slot is fully retired; it is not multiplexed onto
the existing QP. Multiple streams of an admitted Pod pair still reuse that
pair's channel. Cross-Pod sharing and node-scoped IPsec remain the separate
proposal in `design/RDMA_NODE_POOL.md`.

The script enables `DMESH_PEER_REGISTRATION=1` and `DMESH_SHARDED=1`. It keeps
shared tracing and uses `warn,linkerd=info,hickory=error` unless explicitly
overridden. Host partial-tail batching is the native 500us default.
Do not start a second crypto owner on a NIC already owned by one.

The fixture uses the target repository's **development** policy, identity and
destination services. These are not a production Linkerd control plane.
Kubernetes identity verification, topology signatures and peer IPsec remain
real. Production routing supplies resolved remote Pod endpoints through the
normal Linkerd discovery path. The fixture dials the server Pod IP directly;
it does not treat a ClusterIP as a remote Pod or replace Service discovery.

## Dynamic Kubernetes registration

Run the migrated `node.dpumeshd` with `--native-channels`. Its device plugin
exposes `dpumesh.io/channel`, mounts a per-allocation registration socket and
sets `DPUMESH_REGISTRATION_SOCKET` in the application container. A Pod must
request and limit exactly one channel, disable its ServiceAccount token mount,
and belong to its declared `DPUMESH_SERVICE_NAME` Service.

The native Host library asks the agent to seal the DPU's fresh challenge. The
agent checks kernel PID/cgroup evidence, the Kubernetes Pod/container/Service
view and kubelet allocation before sealing it over the paired-Host TLS session.
The agent briefly waits for kubelet to publish a new container ID; the DPU
keeps a valid assertion pending until the signed Pod update arrives, within
the original 15-second HELLO deadline. Neither wait grants access early.
Payload continues over native DMA, not through this socket. The library holds
the socket for the session lifetime.

HTTP/2 transport pools retain the verified source registration even if the
endpoint stack is built before its signed topology arrives. A later RDMA
reconnect therefore uses the same source Pod identity. Pool keys include the
worker, endpoint and registration generation; two Pods cannot reuse each
other's authenticated connection. Known or withdrawn remote endpoints never
fall back to TCP. Unclassified endpoints retain the ordinary connector's TCP
behavior until topology identifies them; external TCP routing is preserved.

Pod deletion, a changed UID/container, loss of authoritative Kubernetes watch,
or control-session loss revokes registration. DPU dispatch and peer connections
reject further work. A slot is reusable only after native flows and peer
QP/DMA references have retired; an elapsed timeout does not grant reuse.

`pods.py` prints a client/server Pod and Service fixture. It requires the native
Host library, `channel-bench` and agent staged on both Hosts. It uses privileged
Pods to expose the DOCA device in this development testbed.

## Measurements

`measure.py` measures an already-deployed fixture. It requires successful gRPC
preflight, pins Host work to `HOST_CPUS`, and defaults total concurrency to
`256 * workers`. Set `--concurrency-per-worker 512` for `512 * workers`
concurrent closed-loop RPCs, distributed evenly over `workers` connections.
For a 30-second run, worker CPU and Linkerd HTTP response
throughput use the same central 5–25-second interval. Client P50/P99 cover the
full 30 seconds and are reported separately. The script rejects changed worker
counts, restarts, missing L7 counters and client errors.

### WRITE batch fence occupancy

Rebuild ARM and DPA together, then set `DPUMESH_DPA_FENCE_STATS_MS=1000` in
both DPU runtime environments. The default is `0` (disabled). This applies to the WRITE_WITH_IMM+DPA path.
The ARM worker requests a serialized, immutable DPA snapshot once per interval
per active lane and prints `WRITE_IMM_FENCE_STATS`; REMOVE prints the last
partial interval. Snapshot/reset completes before its control acknowledgement.
This opt-in measurement adds control traffic and ARM log output; use a separate
run with diagnostics disabled for uninstrumented throughput numbers.

`avg` is the number of unretired batch fences immediately after each batch
commit, averaged over batch submissions. It includes the newly committed
batch and is **not a time-weighted mean**. A value near 1 means almost no older
batch is waiting at submission. `max` is the interval high-water mark;
`current` is the depth at snapshot time. `full_stalls` counts admission visits
blocked by all 32 fences being occupied, not requests dropped or milliseconds
stalled. The histogram covers depths 1, 2–4, 5–8, 9–16 and 17–32. Sampling is
independent of the number of DATA requests merged into one physical WRITE.

Run the existing fixture with one connection and concurrency 512:

```sh
python3 bench/peer-e2e/measure.py \
  --source-dpu SOURCE_POD --destination-dpu DESTINATION_POD \
  --workers 1 --concurrency-per-worker 512 --out RUN_DIR
kubectl logs -n dpumesh-system SOURCE_POD -c runtime > RUN_DIR/source-runtime.log
kubectl logs -n dpumesh-system DESTINATION_POD -c runtime > RUN_DIR/destination-runtime.log
python3 bench/peer-e2e/fence_stats.py RUN_DIR/source-runtime.log RUN_DIR/destination-runtime.log
```

The summary weights interval means by their sample counts and reports
P50/P95/P99 **bucket ranges**, rather than inventing exact percentiles. Optional
`--start-unix-ns` / `--end-unix-ns` filters keep only complete snapshot intervals
inside the measurement window. The DPA statistics are stored in each `wim_lane`, including its 256B fence
ring. The live lane layout excludes Sync Event fields. A frozen shared
snapshot adds 64B per transport context, and ARM keeps a report timestamp per QP.

The local test coverage is described in `Makefile`, `tests/peer_authority_test.c`
and `tests/peer_connection_test.c`. Hardware results must come from the native
fixture; prior jukebox benchmark numbers are not native migration results.

Native peer TX now writes H2 bytes directly into a worker-owned arena and lends
each sealed chunk to the existing borrowed-payload carrier. There is no separate
2 MiB TX staging allocation per peer connection or staging-to-carrier DATA copy.
The arena is allocated lazily (1024 x 64 KiB per worker); chunks reserve 64 bytes
of headroom and expose 65,472 body bytes. The peer driver prefetches at most eight
writable chunks per connection. Writable reservations and unacknowledged bytes share
the 2 MiB connection admission budget, and NIC holds retain chunks independently of
ACKs and connection teardown. The arena outlives the transport's MR registrations.
RX still uses its copied adapter; QP TX rings remain for control/copied frames.
The historical hardware measurements below predate this TX change.

Hardware migration validation and the first 1/2/4/8-worker sweep are recorded
in `bench-results/native-rdma-migration-20261030/report.txt` (local artifacts).
The native release build retains `target-cpu=native`; absolute RPS and atomic
helper sample shares are not directly interchangeable with prior jukebox runs.

With `DMESH_SHARDED=1`, `DPUMESH_L7_TRACE_SCOPE=worker` (the default) creates a
fresh tracing registry, EnvFilter and reload layer per worker. Use
`DPUMESH_L7_TRACE_SCOPE=shared` to select the original global subscriber for a
same-binary A/B. Restart both DPU runtimes when changing scope. Non-sharded
mode defaults to `shared`; explicitly selecting `worker` requires sharded mode.
The outer dmesh span is constructed under its worker's dispatcher before the
thread starts, and the thread keeps that dispatcher through runtime teardown.

The single admin log-level endpoint updates each independent worker filter;
its handle list is used only during startup/admin changes, not span callbacks.
The global subscriber remains the fallback for main/admin/control threads.
Tokio-console diagnostics use shared scope. Optional admin log streaming across
private worker subscribers is not covered by this benchmark; use shared scope
when relying on the global log stream.

Native tracing ownership A/B artifacts are in
`bench-results/native-rdma-tracing-ab-20261105/`.
The same-binary closed-loop rerun with concurrency 512 per worker is in
`bench-results/native-rdma-tracing-ab-c512-20261105/`, including the c256
comparison and the crypto owner's SA-ID exhaustion/recovery record.
