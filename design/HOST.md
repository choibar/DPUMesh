# Host library over the DPUMesh transport

The host library (`libdpumesh.so.5`) is the DPUmesh native API and core; the
DPU runs the DPUMesh transport and its embedded Linkerd proxy. The two
meet at DPUMesh's host wire: one Comch control session per channel, a forward
descriptor ring per connection and, back to the host, either the DPA-free push
channel (`DPUMESH_REVERSE=dpu-dma`, the default) or the host-dpa reverse path
(`DPUMESH_REVERSE=host-dpa`): a host-owned DPA thread per connection that mirrors the
forward path, polling the DPU's descriptor ring and copying its tx_staging
into the connection's window (flow modes CLIENT / BACKEND_PULL, so the DPU
exports rcv_ring + tx_staging instead of pushing).

## Layout

- `include/dpumesh`: public API and descriptor layout (ABI 5).
- `src/core/dmesh_core.c`: channels, EQs, QPs, TX reservation and credits,
  custody ACK reclamation, RX delivery and the accept queue, FIN and teardown,
  the EQ readiness fd and its idle-wake policy (naps, linger, doorbells).
- `src/core/native_transport.h`: the private carrier contract
  (open, connect, submit, poll, release, wait, resolve, disconnect, close).
- `src/core/carrier.c`: the carrier over the channel layer.
- `src/transport/host/channel.[ch]`: the channel layer, the only host-library
  files that include DOCA headers; they call the transport's host sources
  (`src/transport/{common,host}/*.c`) for device open, Comch client and
  session setup, ring setup and flow-tagged export messages. `src/core` includes only
  `channel.h`, which exposes no DOCA types.
- `src/transport/host/host_stubs.c`: server-only symbols the shared
  transport sources reference but a host never executes.
- `src/facade`: the native API and the POSIX preload shim.

## Mapping onto the channel layer

| API | Carrier |
|---|---|
| `dmesh_create_channel` | Opens the PCI device and one Comch client/PE, completes HELLO, registers one TX pool and one RX region (32 windows of 1 MiB). A server channel resolves its `DPUMESH_SERVICE` target and registers a listener without data flows. The DPU requests a BACKEND flow on first use of each `(worker, replica)` pair; the Host opens it with the requested worker/token. Client flows on that worker share its H2 connection. `DPUMESH_BACKEND_MAX` limits total backend flows within the shared 32-slot channel. |
| `dmesh_create_qp` | Sends flow-tagged OPEN on the channel session and waits for READY. Opens an `INGRESS_PUSH` flow: source `DPUMESH_POD_IP` and the QP port, destination the address DNS gives for the `<host>:<port>` target ([naming](API.md#naming)), `DPUMESH_WORKLOAD` as identity label. |
| inbound stream | The first push batch on a BACKEND flow enters the core's accept queue under that flow's upstream port. After the stream closes, the flow reopens under a new port. |
| `dmesh_post_send` | The descriptor's TX-pool range is posted to the flow's forward ring as one or two DPUMesh descriptors (a multiple of 128 bytes plus a remainder of at most 128 bytes, each at most 8064 bytes). |
| custody ACK | The DPA's `consumer_head` passing a descriptor's ticket. |
| `dmesh_poll_eq` receive | One push batch `{seq, pos, len}` becomes one receive event pointing into the flow's data ring. No copy. |
| `dmesh_release_rx_buffer` | Marks the batch released; the consumption cursor advances over the released prefix and the DPU pulls it for flow control. A long-held batch blocks only its own QP. |
| `dmesh_destroy_qp`, `dmesh_abort_qp` | Retain the current FIN behavior. Transport cleanup sends flow CLOSE after local reverse DMA stops; matching CLOSED permits resource release. A flow close leaves the channel's Comch session and siblings alive. |

### DPU-side guarantees

Submission never frees a DMA source. The forward ring's `consumer_head` (the
custody ACK) and the proxy's reverse `completed_bytes` advance only on the
copy's CQE, and on push flows `pushed_bytes` advances only after both the data
DMA and the descriptor DMA have completed. The proxy writer gets its staging
back through that completion cursor (`dmesh_doca_conn_tx_completed`), not
through the bytes `send_staged` accepted. A failed descriptor submission fails
the flow instead of silently losing accepted data.

Every activation of the DPU forward poller is bounded and ends with a
retrigger (`device/dpa_kernel.c`); an infinite polling activation violates the
SDK's scheduled kernel time limit. Threads are not pinned by default: the DPA
scheduler places them.

Setting any `DPUMESH_DPA_EU_*` variable pins each worker's pool to a fixed EU
range. `DPUMESH_DPA_EU_BASE` selects the first EU (default 0). Choose a free
range when another DPA process shares the device; fixed affinity does not
reserve EUs. The Boutique bench profile uses 64 on the test node. A dispatcher
worker k (the proxy's `DMESH_NUM_WORKERS`) takes pool k; a worker that serves
its own Comch server `DPUMesh<k>` takes pool k; anything else takes creation
order. Every stream holds one pool thread, and DPA threads are not preempted:
two busy streams on one EU starve each other, from a few hundred milliseconds
to as long as the other stays busy. With only the base set every pool spans
the whole range from it, so different workers' streams share EUs. Three
variables give each pool its own range; every thread of a pool stays inside
that range:

- `DPUMESH_DPA_EU_STRIDE`: pool k uses the `stride` EUs from `k * stride`
  after the base.
- `DPUMESH_DPA_EU_END`: first EU no pool may use (default: every EU the device
  reports). Set it when the device takes DPA threads only below some EU (190
  on the BF-3 test node, although it reports 254).
- `DPUMESH_DPA_EU_OFFSETS`: `o0,o1,...`, increasing, the start of pool k from
  the base; pool k ends where pool k+1 starts, the last listed pool at the
  end. Pools past the list use the stride. Gives a worker with many
  connections a wider range than the rest.

A pool whose streams outnumber its EUs reuses its own EUs and logs `more
streams than its N EUs` once; size the ranges so each covers its worker's
peak stream count. The log line `Assigned DPA pool thread i (EU e)` shows
where each stream runs. A value out of range fails the pool with
`DOCA_ERROR_INVALID_VALUE`. The Online Boutique benchmark (microservices-demo
`mesh-bench`) ran 14 workers with `END=190` and a stride of 13, before flows
were placed by the dispatcher.

### Readiness: no background thread

The library creates no thread of its own. The EQ thread that calls
`dmesh_poll_eq` drains every stripe in line (`dpumesh_eq_drain`) and
publishes its QPs' retained transmit tails, and `dmesh_eq_fd` hands out an
epoll set that wakes it: its eventfd (deliveries from other EQ threads,
accepts, a send while it sleeps), a one-shot timerfd programmed to the
earliest retained-tail deadline, a one-shot nap/backstop timerfd, the
doorbells of the stripes it owns, and the spare set: the doorbells of the
spare backend flows and the channel's wake fd. A stripe's doorbell is the
carrier's per-slot epoll of the private reverse MsgQ notification fd in
host-dpa mode; the core moves it from the spare set to the owning EQ at
connect/accept and back at free. A drain snapshots active stripes, including
closed flows awaiting custody/FIN retirement, and skips unused stripes. It
progresses the shared control PE once per pass under a channel mutex; each
flow then observes the retained session error and advances its private
reverse engine. The control PE's notification fd is the channel's wake fd.
No control thread is added.

Each empty poll chooses the next wake (`dpumesh_eq_arm`). After work, the EQ
re-polls on the nap timer, doubling from `DPUMESH_NAP_US` (10) to
`DPUMESH_NAP_CAP_US` (100); then it keeps polling every nap cap until
`DPUMESH_LINGER_US` (1000) has passed since its last work, where a send counts
as work because its reply is expected. Then it sleeps: it sets its asleep
flag, arms the stripe doorbells and asks the channel to arm (`idle_arm`),
leaving only the `DPUMESH_BACKSTOP_MS` (200) timer. With more live EQs than
allowed CPUs it skips the naps and linger. A backstop expiry that finds work
means a wake was missed and is counted (`DPUMESH_WAIT_STATS` writes the
counters to `<dir>/dpumesh-wait.<pid>` once a second).

Three things have no completion doorbell, and each gets a wake:
- **Push batches** (the DPU's DMA engine writes the window). `idle_arm` sends
  the session message `ARM` listing, per push flow, the next descriptor
  sequence the host has not read. The DPU sends `DOORBELL` at once if a listed
  flow already published that sequence, otherwise after its next descriptor
  completion. On a DPU whose Comch sessions belong to the dispatcher (the
  proxy), the dispatcher hands each listed flow's sequence to the flow's
  worker, which rings through its control reply queue; the dispatcher sends
  one DOORBELL per ARM. At most one ARM is outstanding per channel; a DOORBELL
  releases it and restarts the naps, since its descriptor may become visible
  after the message. Plan and race argument: `docs/2026-09-30_host-wait-doorbell-plan.md`.
- **Control messages** (DOORBELL, CLOSED, ERROR) raise the control PE's
  notification fd once `idle_arm` requested it; the drain pass progresses it.
  The control PE runs in `DOCA_PE_EVENT_MODE_PROGRESS_ALL`: in the selective
  default an armed PE delivers no new event until the raised notification is
  cleared, which stalled a synchronous flow open behind an idle wake.
- **Custody ACKs** (the DPU's DPA writes `consumer_head` into host memory).
  An EQ with custody outstanding keeps polling instead of sleeping. A send
  publishes custody under the slot lock before it reads the asleep flag, and
  the arming EQ sets the flag before it reads custody under that lock, so a
  send either finds the EQ awake or wakes it.

### Host-dpa reverse path

`DPUMESH_HOST_DPA_PCI` names the host function whose DPA runs the reverse threads
(default `0b:00.0`); it must differ from the Comch function (flexio allows one
process per function) and its vhca needs a DPA EU partition on the DPU
(`dpaeumgmt partition create --vhca_list 0 --range_eus 0-63`, or
`scripts/assign_dpa_eu.sh` for SF groups). Each connection imports the DPU's
exports on that device, gets one DPA thread + msgq (its own progress engine)
and the same `poll_desc_ring` kernel the DPU runs forward; completions arrive
as msgq messages and the application's releases feed the kernel's staging gate
(`rx_consumed_pos`, published every 64 KiB). The dpacc host stub is compiled with
`-fPIC` so `dpa_kernel.a` links into the shared library (root `Makefile`).
A host SF cannot create the DPA process itself (refused by the firmware), but
`DPUMESH_HOST_DPA_DEV=<ibdev of the SF>` runs the official extended-context flow:
the process is created on the PF (`DPUMESH_HOST_DPA_PCI`), `doca_dpa_device_extend`
extends it to the SF, every DPA object (thread, completions, msgqs, mmap
handles, buf_arr) is created on the SF, and the kernel switches to it with
`doca_dpa_dev_device_set` (the thread argument's `dpa_dev`, also passed to the
init RPC; without the switch nothing activates). Verified 2026-09-24 at the
PF's speed (1 flow 10.7 Gbps, 4 flows 32 Gbps, 64 B RTT 25 µs); the SF needs
no EU partition of its own, only the PF's vhca does. Two firmware-level
observations on this node: several host processes can each hold a DPA
process on the same PF (and share one SF), but once a DPA thread is
*started* on one extended SF, a comch consumer-completion CQ cannot be
created on any other SF (devx syndrome 0x5ecb3), in the same process or
another, whatever the partition layout; the second SF's extension, thread,
doca_dpa_completion and msgqs still succeed, and a thread merely created
(not started) on the first SF does not block (`scripts/probe/sf_ext_probe`,
2026-09-24). One SF per pod with its own DPA objects waits on that firmware
limit; the deployment model below is what runs today.

### Deployment model: SF for Comch, DPA on the PF

Each pod owns one SF of the node's host PF and uses it for Comch only
(`DPUMESH_PCI_ADDR` = the SF; the DPU serves the same server name on every
SF representor). Every pod creates its own DPA process on the host PF
(`DPUMESH_HOST_DPA_PCI`, `DPUMESH_REVERSE=host-dpa`, no `DPUMESH_HOST_DPA_DEV`): several
processes on one PF are verified side by side, and the SF-extended path
brings no benefit on this firmware. Node prep is one EU partition for the PF
vhca (`dpaeumgmt partition create --vhca_list 0 --range_eus 0-63`); SF vhcas
need no partition and an extended thread cannot use one anyway. What a pod
needs: its SF's uverbs device, the PF's uverbs device (the DPA process, the
descriptor-ring window and the reverse completions live there), `IPC_LOCK`,
and `DPUMESH_POD_IP`. All pods share the PF vhca and its EUs, so this model
trades isolation for the host DPA; a pod that may not see the PF runs
`DPUMESH_REVERSE=dpu-dma` instead (no host DPA, 14.4 vs 20.4 Gbit/s at two flows).
Once the firmware allows a running thread per SF, `DPUMESH_HOST_DPA_DEV=<own SF>`
moves the DPA objects onto the pod's SF without a code change.

## Limits

A DPU worker serves 64 flows (one DPA thread each), shared by client QPs and
the backend pool. The channel establishes Comch once; opening a QP still waits
synchronously for its own DMA setup and READY, bounded by a five-second control
timeout. `dmesh_msg_max` is 8192; a descriptor larger than 8064 bytes is
split by the channel layer and reassembled by the stream.

## Channel session and shutdown

The private Comch envelope carries a version, message type/length, flow ID,
generation and status. HELLO/HELLO_ACK bind the session; OPEN/READY and
REVERSE_EXPORT prepare individual flows; CLOSE/CLOSED release them; ERROR
reports flow or session failure. Stale generations cannot mutate a reused slot.
The host serializes control PE progress and callbacks with the channel mutex.
The DPU keeps separate session and flow tables; its DPA pool keys ownership by
logical flow, so sharing Comch never shares a DPA thread accidentally.

A successful close requires more than kernel loop exit. The kernel publishes
its issued-copy count before `stopped`, and the CPU checks that every copy has
produced its DMA-completed message while continuing to progress the local
MsgQ. These messages follow DMA completion ([DOCA Comch documentation](https://docs.nvidia.com/doca/sdk/doca-comch.pdf)).
Pending CPU DMA tasks are drained or cancelled before task buffers, contexts,
mappings or staging memory are released. Each cleanup step checks SDK errors
and preserves remaining ownership for retry. Host reverse cleanup precedes
CLOSE; DPU cleanup precedes CLOSED with status zero. The proxy also retires
its Rust IO staging pointers under their mutex and acknowledges this to C before
C may release any staging mappings; backend IO can live on another runtime.
DPA threads, descriptor rings and local CPU/DPA MsgQs remain per flow.

Native copies now carry FLUSH on each submission, retaining optimized completion
reports. This avoids a shutdown deadlock in which the last flush needs a receive
credit held by an unflushed operation. Doorbell batching performance must be
remeasured on hardware; the separate microbenchmark still controls its own
batching.

`DMESH_NO_TEARDOWN` retains resource-owning flows and reports an unsuccessful
close instead of bypassing its existing hardware workaround. On abnormal
session loss, exported sources that may still have a host reverse reader are
quarantined, consuming bounded flow/session capacity until process/device
recovery. The proxy's void driver destructor retains a live device domain rather
than freeing the owner underneath unfinished cleanup. A lost session is not proof that the remote DMA reader stopped.

The public API and SONAME remain ABI 5; explicit listen and public async
connect/close are later steps. The private Comch protocol and DPA argument
layout changed: rebuild the host library, DPU transport/proxy and DPA kernel
together. Mixed old/new peers are unsupported. Unit/fault tests and builds do
not replace hardware validation of both reverse modes and close/reconnect
under load.

## DPU worker mode

The DPUMesh proxy's event-driven worker (`DMESH_BUSY_POLL` unset) stops
serving a flow after an idle gap of about a second on this build; the
measured configuration runs the proxy with `DMESH_BUSY_POLL=1`. The cause is
in the proxy's notification handling, not in this library.

## Configuration

All values come from the environment; `.env.example` lists them with
placeholders. `DPUMESH_PCI_ADDR`, `DPUMESH_SERVER` (default `DPUMesh0`),
`DPUMESH_POD_IP`, `DPUMESH_WORKLOAD`, `DPUMESH_POD_ID` (default 0),
`DPUMESH_SERVICE` (the `<host>:<port>` target a server serves),
`DPUMESH_TARGETS` (the targets the preload shim carries),
`DPUMESH_BACKEND_MAX`
(demand-created flows, default 32; outgoing flows share the same capacity).
`DPUMESH_NAP_US` (10), `DPUMESH_NAP_CAP_US` (100),
`DPUMESH_LINGER_US` (1000) and `DPUMESH_BACKSTOP_MS` (200) set the idle wake;
`DPUMESH_SPIN_US` and `DPUMESH_TICK_US` are ignored with a warning.
`DPUMESH_WAIT_STATS` names a directory for the idle-wake counters. `DPUMESH_CARRIER_TRACE` and `DPUMESH_CORE_TRACE` print flow,
descriptor and event traces to stderr.

## Build

```sh
make lib            # host library and preload shim
make test           # host-only checks and ABI contract
make examples       # native and preload examples; see examples/README.md
cd src/transport && meson setup build && meson compile -C build   # transport, on the DPU
```
