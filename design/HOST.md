# Host library over the DPUMesh transport

The host library (`libdpumesh.so.6`) is the DPUmesh native API and core; the
DPU runs the DPUMesh transport and its embedded Linkerd proxy. The two
meet at DPUMesh's host wire: one Comch control session per channel, a forward
descriptor ring per connection and, back to the host, either the DPA-free push
channel (`DPUMESH_REVERSE=dpu-dma`, the default) or the host-dpa reverse path
(`DPUMESH_REVERSE=host-dpa`): a host-owned DPA thread per connection that mirrors the
forward path, polling the DPU's descriptor ring and copying its tx_staging
into the connection's window (flow modes CLIENT / BACKEND_PULL, so the DPU
exports rcv_ring + tx_staging instead of pushing).

## Layout

- `include/dpumesh`: public API and descriptor layout (ABI 6).
- `src/core/dmesh_core.c`: channels, QPs, TX reservation and credits,
  custody ACK reclamation, RX delivery and the accept queue, FIN and teardown,
  the channel's and each QP's readiness fd and their idle-wake policy
  (doorbells, re-polls, the backstop).
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
| `dmesh_create_channel` | Opens the PCI device and one Comch client/PE, completes HELLO, registers one TX pool and one RX region (32 windows of 1 MiB). A server channel resolves its target (`<pod ip>:DPUMESH_PORT`, or `DPUMESH_SERVICE`) and registers a listener without data flows. The DPU requests a BACKEND flow on first use of each `(worker, replica)` pair; the Host opens it with the requested worker/token. Client flows on that worker share its H2 connection. `DPUMESH_BACKEND_MAX` limits total backend flows within the shared 32-slot channel. |
| `dmesh_create_qp` | Sends flow-tagged OPEN on the channel session and waits for READY. Opens an `INGRESS_PUSH` flow: source the pod IP (`DPUMESH_POD_IP`, or in a Pod the route source toward `KUBERNETES_SERVICE_HOST`) and the QP port, destination the address DNS gives for the `<host>:<port>` target ([naming](API.md#naming)), `DPUMESH_WORKLOAD` as identity label. |
| inbound stream | The first push batch on a BACKEND flow enters the core's accept queue under that flow's upstream port. After the stream closes, the flow reopens under a new port. |
| `dmesh_post_send` | The descriptor's TX-pool range is posted to the flow's forward ring as one or two DPUMesh descriptors (a multiple of 128 bytes plus a remainder of at most 128 bytes, each at most 8064 bytes). |
| custody ACK | The DPA's `consumer_head` passing a descriptor's ticket. |
| `dmesh_poll_qp` receive | One push batch `{seq, pos, len}` becomes one receive event pointing into the flow's data ring. No copy. |
| `dmesh_release_rx_buffer` | Marks the batch released; the consumption cursor advances over the released prefix and the DPU pulls it for flow control. A long-held batch blocks only its own QP. |
| `dmesh_destroy_qp`, `dmesh_abort_qp` | Retain the current FIN behavior. Transport cleanup sends flow CLOSE after local reverse DMA stops; matching CLOSED permits resource release. A flow close leaves the channel's Comch session and siblings alive. The flow's window is reused only after the application releases every batch it still holds. |
| `PREPARE`, `SCALE` events | The DPU sends them on the client flow they concern. The session's progress records the request on the flow, whose control news wakes the QP owning it; its `dmesh_poll_qp` takes the request. `dmesh_spare_ready` answers with SPARE_READY on the spare's flow, `dmesh_scale_ack` with SCALE_ACK on the flow the SCALE came on. |
| preload `shutdown(SHUT_WR)` | Sends SHUTDOWN with the count of descriptors posted on the flow; the proxy reads end of stream after all of them, and the flow keeps receiving until the QP closes. |

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
SDK's scheduled kernel time limit. DPA threads are not pinned: the DPA
scheduler places them. The `DPUMESH_DPA_EU_*` variables of earlier builds are
ignored.

### Readiness: no background thread

The library creates no thread of its own. Two kinds of fd wake the threads
that call it, each an epoll set:

- **The channel's** (`dmesh_channel_fd`): the control PE's notification fd
  (the channel's wake fd), the spare set (the doorbells of the stripes no QP
  owns: a backend flow awaiting its first stream, a closed flow still
  retiring) and a one-shot timer. `dmesh_accept` acknowledges them,
  progresses the control PE once under a channel mutex, interprets the active
  stripes (with flow wake, only those no QP owns) and returns the next inbound
  stream. Its stripe moves from the spare set to the new QP's EQ, whose
  first poll returns the held first bytes and what queued behind them.
- **A QP's** (`dmesh_qp_fd`): its EQ, private to the QP: an eventfd
  (deliveries by the channel, its flow's control news, a loss, a sender
  waiting for space while it sleeps), a timerfd (the backstop, periodic from
  the fd's hand-out, and re-polls ahead of it) and its stripe's doorbell.
  `dmesh_poll_qp` drains that stripe (`dpumesh_eq_drain`), taking a batch of
  the carrier's events per hold of the stripe, and then the QP's inbox; there
  is no ready list, since the QP is the EQ's only connection. A
  stripe's doorbell is the carrier's per-slot epoll of the private reverse
  MsgQ notification fd in host-dpa mode, or its flow's own consumer with flow
  wake; it moves back to the spare set when the QP is freed.

A drain snapshots active stripes, including closed flows awaiting custody/FIN
retirement, and skips unused stripes; each flow observes the retained session
error and advances its private reverse engine. No control thread is added.

Each empty QP poll arms the QP's next wake (`dpumesh_eq_arm`): it sets its
asleep flag and arms its stripe's doorbell (one request per wake: the stripe
rearms only after its doorbell rang), then drains once more. Only what has no
doorbell and must not wait keeps it polling, on a 100 µs re-poll ahead of the
backstop (below). The backstop fires every 200 ms; it only bounds a missed
wake. An empty `dmesh_accept` arms the channel: the unowned stripes and
`idle_arm`, re-polling after 100 µs when something is outstanding and
otherwise after 200 ms, which also paces the session's keepalive. A backstop
expiry that finds work means a wake was missed and is counted. Each EQ keeps
its counters itself; with `DPUMESH_WAIT_STATS` the channel's poller writes
their sum to `<dir>/dpumesh-wait.<pid>` once a second.

Three things have no completion doorbell, and each gets a wake:
- **Push batches** (the DPU's DMA engine writes the window). Without the Comch
  fast path, `idle_arm` sends the session message `ARM` listing, per push
  flow, the next descriptor sequence the host has not read. The DPU sends
  `DOORBELL` at once if a listed flow already published that sequence,
  otherwise after its next descriptor completion. On a DPU whose Comch
  sessions belong to the dispatcher (the proxy), the dispatcher hands each
  listed flow's sequence to the flow's worker, which rings through its control
  reply queue; the dispatcher sends one DOORBELL per ARM. At most one ARM is
  outstanding per channel; a DOORBELL releases it, and the channel re-polls
  after 100 µs, since its descriptor may become visible after the message.
  Plan and race argument: `docs/2026-09-30_host-wait-doorbell-plan.md`.
- **Control messages** (DOORBELL, CLOSED, ERROR) raise the control PE's
  notification fd once `idle_arm` requested it; the channel's poller
  progresses it. The control PE runs in `DOCA_PE_EVENT_MODE_PROGRESS_ALL`: in
  the selective default an armed PE delivers no new event until the raised
  notification is cleared, which stalled a synchronous flow open behind an
  idle wake.
- **Custody ACKs** (the DPU's DPA writes `consumer_head` into host memory).
  A QP sleeps on open custody, reclaimed on its next wake, unless a sender
  waits for the space it frees (or its closed flow still retires): then it
  re-polls. A sender that arms such a wait counts itself before it reads the
  asleep flag, and the arming QP sets the flag before it reads the waiter
  count, so the sender either finds the QP awake or wakes it.

### Flow wake

With the Comch fast path (`DPUMESH_COMCH_WAKE`, on by default on DPU-DMA), the
DPU sends each push descriptor as a Comch message. Each of the session's 32
flow slots has a consumer of its own, on its own PE in `PROGRESS_ALL` mode,
created with the session before any flow opens (all or none; a failure fails
the session). They start one at a time: each start and stop sends
the DPU a message from the Comch connection's small pool of internal tasks
(about 16), which the control PE frees. A push flow's OPEN names its slot's
consumer, and the DPU sends that flow's descriptors there. The slot's PE fd
is the stripe's doorbell, nested directly in its QP's epoll set, and the QP
alone advances and drains it: a completion wakes only its QP's poller, with
no hop through the channel's. Control messages still arrive on the control
PE; a flow's CLOSED or ERROR marks its stripe, and the channel's poller wakes
the QP through its eventfd.

A broker's client has no PE, so its session has neither the fast path nor flow
wake: the broker opens the session without the consumers, the channel reports
flow wake off, and push batches wake the client through `ARM` and `DOORBELL`
(the broker relays both: the arm page and kick eventfd one way, the wake
eventfd the other).

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
timeout. One receive carries at most 8192 bytes; a descriptor larger than 8064 bytes is
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

The public API and SONAME are ABI 6: the channel is the listening socket
and each QP has its own fd; public async connect/close is a later step. The private Comch protocol and DPA argument
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
placeholders. On the dpu-dma reverse path a host broker can own the DOCA device
(`dpumesh_broker --listen <socket> --pci <addr>`, [plan](../docs/2026-09-29_host-broker-plan.md)):
the channel is its client when `DPUMESH_BROKER` names the socket, or when it is
unset and `/run/dpumesh/broker.sock` exists, and maps the memory the broker
registered; its idle wake is `ARM`/`DOORBELL` through the broker (see Flow
wake). Otherwise the application opens the device itself at
`DPUMESH_PCI_ADDR` (the direct path); `DPUMESH_BROKER=off` forces that, and the
host-dpa path always takes it. The broker trusts its clients' addresses and
workload unless `--verify addr` (LISTEN and source addresses must be the
client's own network namespace's) or `--verify pod` (also: the client runs in a
Pod, and its flows carry the Pod's mesh identity from the kubelet's service
account token). A client registers at most `--client-mem-mib` (256) of pinned
memory, and `--max-clients` (64) are served at once. A lost Comch session or
broker reaches the application as a transport loss (`design/API.md`).
`DPUMESH_SERVER` (default `DPUMesh0`),
`DPUMESH_POD_IP` (when unset: the address a verifying broker established, else the Pod's route source), `DPUMESH_WORKLOAD`, `DPUMESH_POD_ID` (default 0),
`DPUMESH_PORT` (the port a server serves on its pod IP), `DPUMESH_SERVICE`
(a `<host>:<port>` target served instead),
`DPUMESH_TARGETS` (the targets the preload shim carries),
`DPUMESH_BACKEND_MAX`
(demand-created flows, default 32; outgoing flows share the same capacity).
The idle wake has no knobs. `DPUMESH_WAIT_STATS` names a directory for the
idle-wake counters. `DPUMESH_CARRIER_TRACE` and `DPUMESH_CORE_TRACE` print flow,
descriptor and event traces to stderr.

## Build

```sh
make lib            # host library and preload shim
make broker         # build/bin/dpumesh_broker, the process that owns the DOCA device
make test           # host-only checks and ABI contract
make examples       # native and preload examples; see examples/README.md
cd src/transport && meson setup build && meson compile -C build   # transport, on the DPU
```
