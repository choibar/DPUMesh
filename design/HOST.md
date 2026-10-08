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
  the EQ readiness fd (doorbells, fallback tick, spin window).
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

### Readiness: no background thread

The library creates no thread of its own. The EQ thread that calls
`dmesh_poll_eq` drains every stripe in line (`dpumesh_eq_drain`) and
publishes its QPs' retained transmit tails, and `dmesh_eq_fd` hands out an
epoll set that wakes it: its eventfd (deliveries from other EQ threads,
accepts), a one-shot timerfd programmed to the earliest retained-tail
deadline, the doorbells of the stripes it owns, the doorbells of the spare
backend flows, and a fallback tick. A stripe's doorbell is the
carrier's per-slot epoll of the private reverse MsgQ notification fd in host-dpa
mode; the core moves it from the spare set to the owning EQ at connect/accept
and back at free. A drain snapshots active stripes, including closed flows
awaiting custody/FIN retirement, and skips unused stripes. It progresses the
shared control PE once per pass under a channel mutex; each flow then observes
the retained session error and advances its private reverse engine.
Its fd is not registered in competing EQs; the existing fallback tick runs
while a flow is open, including idle host-dpa flows. No control thread is added.

Two things have no doorbell: custody ACKs (the DPU's DPA writes
`consumer_head` into host memory) and push-wire batches (the DPU's DMA engine
writes the window). While a sleeping EQ has either outstanding, a periodic
timerfd (`DPUMESH_TICK_US`, default 50) polls for it. Open flows also need this
tick for shared control progress. Historical performance figures below predate
the per-channel Comch change and require remeasurement.

An armed completion queue raises a hardware event per completion, so an EQ
that runs empty first spins for `DPUMESH_SPIN_US` (default 1000): it signals
its own eventfd and leaves it unread, so the caller's sleep returns at once and
it polls again; only an EQ empty for the whole window acknowledges its
doorbells and arms them before the real sleep. With the window at 50 us the
2-flow 8 KiB echo lost 8% to wake latency; at 1 ms it gains on the old drain
threads (20.4 vs 18.9 Gbps, 64 B RTT 26.4 vs 27.0 us) with fewer threads and
less host CPU (4 flows: 197% vs 255%).

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

A DPU worker serves 32 flows (one DPA thread each), shared by client QPs and
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
(demand-created flows, default 32; outgoing flows share the same capacity). `DPUMESH_SPIN_US` is the empty-poll window before an EQ arms
its doorbells and `DPUMESH_TICK_US` the fallback poll period while doorbell-less
traffic is outstanding. `DPUMESH_CARRIER_TRACE` and `DPUMESH_CORE_TRACE` print flow,
descriptor and event traces to stderr.

## Build

```sh
make lib            # host library and preload shim
make test           # host-only checks and ABI contract
make examples       # native and preload examples; see examples/README.md
cd src/transport && meson setup build && meson compile -C build   # transport, on the DPU
```
