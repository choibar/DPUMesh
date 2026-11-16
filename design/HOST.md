# Host library over the DPUMesh transport

The host library (`libdpumesh.so.6`) is the DPUmesh native API and core; the
DPU runs the DPUMesh transport and its embedded Linkerd proxy. The two
meet at DPUMesh's host wire: one Comch control session per channel, a forward
descriptor ring per connection that the DPU's DPA copies from, and, back to
the host, the DPU's reverse DPA thread: one per DPU worker, it copies a
flow's batches into the connection's window on the flow's own queue,
publishes their descriptors and wakes the flow through its slot's sync event.
A host process that dies with copies to it in flight holds back only its own
flows. The host runs no DPA.

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
| `dmesh_create_channel` | Opens the PCI device and one Comch client/PE, completes HELLO, registers one TX pool and one RX region (32 windows of 1 MiB). A server channel resolves its `DPUMESH_SERVICE` target and registers a listener without data flows. The DPU requests a BACKEND flow on first use of each `(worker, replica)` pair; the Host opens it with the requested worker/token. Client flows on that worker share its H2 connection. `DPUMESH_BACKEND_MAX` limits total backend flows within the shared 32-slot channel. |
| `dmesh_create_qp` | Sends flow-tagged OPEN on the channel session and waits for READY. Opens a CLIENT flow: source `DPUMESH_POD_IP` and the QP port, destination the address DNS gives for the `<host>:<port>` target ([naming](API.md#naming)), `DPUMESH_WORKLOAD` as identity label. |
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
custody ACK) advances only on the copy's CQE, and the reverse `pushed_bytes`
only after the reverse DPA thread's copy completed and its descriptor was
stored into the host's slot ring. The proxy writer gets its staging back
through that completion cursor (`dmesh_doca_conn_tx_completed`), not through
the bytes `send_staged` accepted.

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
  stripe's doorbell is its flow slot's wake fd (flow wake, below); it moves
  back to the spare set when the QP is freed.

A drain snapshots active stripes, including closed flows awaiting custody/FIN
retirement, and skips unused stripes; each flow observes the retained session
error. No control thread is added.

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

A flow's receives raise its slot's fd (flow wake, below). Two other things
need a wake:
- **Control messages** (CLOSED, ERROR, the DPU's requests) raise the control
  PE's notification fd once `idle_arm` requested it; the channel's poller
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

Each of the session's 32 flow slots has a sync event of its own, on its own
PE in `PROGRESS_ALL` mode, created with the session before any flow opens
(all or none; a failure fails the session). Its 64-bit word is the slot's
line of the session's tail table page (`DMESH_EC_SE_OFF`), which the host
exports with TAILS. After storing a batch's descriptor, the DPU's reverse DPA
thread stores a value the word never held into it; a wait-not-equal task on
the word's value at arming completes on that store and raises the slot's PE
fd. The fd is the stripe's doorbell, nested directly in its QP's epoll set,
and the QP alone advances and drains it: a receive wakes only its QP's
poller, with no hop through the channel's and no CPU on the DPU. Control
messages still arrive on the control PE; a flow's CLOSED, ERROR or DPU
request marks its stripe, and the channel's poller wakes the QP through its
eventfd.

### Deployment model

A pod needs its Comch device (`DPUMESH_PCI_ADDR`, the host PF or an SF of
it, whose representor the DPU serves) with that device's uverbs device,
`IPC_LOCK` and `DPUMESH_POD_IP`. The host side runs no DPA, so it needs no
DPA EU partition and does not count against the chip's DPA process limit.

## Limits

A DPU worker serves 64 flows (one DPA thread each), shared by client QPs and
the backend pool. The channel establishes Comch once; opening a QP still waits
synchronously for its own DMA setup and READY, bounded by a five-second control
timeout. One receive carries at most 8192 bytes; a descriptor larger than 8064 bytes is
split by the channel layer and reassembled by the stream.

## Channel session and shutdown

The private Comch envelope carries a version, message type/length, flow ID,
generation and status. HELLO/HELLO_ACK bind the session; TAILS exports its
tail table page; OPEN/READY prepare individual flows; CLOSE/CLOSED release
them; ERROR reports flow or session failure. Stale generations cannot mutate a reused slot.
The host serializes control PE progress and callbacks with the channel mutex.
The DPU keeps separate session and flow tables; its DPA pool keys ownership by
logical flow, so sharing Comch never shares a DPA thread accidentally.

A successful close requires more than kernel loop exit. The kernel publishes
its issued-copy count before `stopped`, and the CPU checks that every copy has
produced its DMA-completed message while continuing to progress the local
MsgQ. These messages follow DMA completion ([DOCA Comch documentation](https://docs.nvidia.com/doca/sdk/doca-comch.pdf)).
Pending CPU DMA tasks are drained or cancelled before task buffers, contexts,
mappings or staging memory are released. Each cleanup step checks SDK errors
and preserves remaining ownership for retry. DPU cleanup, which first waits
for the reverse DPA thread to finish the flow's batches, precedes CLOSED with
status zero. The proxy also retires
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
session loss a flow tears down like a closed one: no host reads DPU memory,
so the DPU's own DPA stopping is the fence. A flow whose cleanup fails is
quarantined, consuming bounded flow/session capacity until process/device
recovery. The proxy's void driver destructor retains a live device domain rather
than freeing the owner underneath unfinished cleanup.

The public API and SONAME are ABI 6: the channel is the listening socket
and each QP has its own fd; public async connect/close is a later step. The private Comch protocol and DPA argument
layout changed: rebuild the host library, DPU transport/proxy and DPA kernel
together. Mixed old/new peers are unsupported. Unit/fault tests and builds do
not replace hardware validation of close/reconnect under load.

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
The idle wake has no knobs. `DPUMESH_WAIT_STATS` names a directory for the
idle-wake counters. `DPUMESH_CARRIER_TRACE` and `DPUMESH_CORE_TRACE` print flow,
descriptor and event traces to stderr.

## Build

```sh
make lib            # host library and preload shim
make test           # host-only checks and ABI contract
make examples       # native and preload examples; see examples/README.md
cd src/transport && meson setup build && meson compile -C build   # transport, on the DPU
```
