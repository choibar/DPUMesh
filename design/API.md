# Native API

`include/dpumesh/dmesh.h` declares nineteen calls with ABI major 5. They
describe reliable full-duplex byte streams through channels, event queues
(EQs), queue pairs (QPs) and registered buffers, and expose no remote-memory
operations. The façade and the channel/QP core are linked into
`libdpumesh.so.5`; the carrier maps them onto the DPUMesh transport
([host library](HOST.md)).

## Objects and ownership

A channel is the registration and memory domain. Each polling thread owns an
EQ; each QP belongs to one EQ. Construct channel, EQ, then QP and destroy them
in reverse order; destroying a parent with live children returns `EBUSY`. A
QP's transmit calls form a serial stream; the caller serializes them against
QP destruction. `user_data` belongs to the application.

| Surface | Calls |
|---|---|
| Channel | `dmesh_create_channel`, `dmesh_destroy_channel`, `dmesh_pod_id`, `dmesh_msg_max`, `dmesh_post_max` |
| EQ | `dmesh_create_eq`, `dmesh_destroy_eq`, `dmesh_eq_fd`, `dmesh_eq_next_deadline_ns` |
| QP | `dmesh_create_qp`, `dmesh_destroy_qp`, `dmesh_abort_qp` |
| TX | `dmesh_alloc`, `dmesh_post_send`, `dmesh_flush`, `dmesh_tx_inflight` |
| Events | `dmesh_poll_eq`, `dmesh_release_rx_buffer` |
| Statistics | `dmesh_get_tx_stats` |

## Naming

A service target is `"<host>:<port>"`: host is a Service DNS name (`"name"` in
the calling Pod's namespace, `"name.namespace"`, or a full name) or an IPv4
literal, and port is the Service port. The cluster DNS answers every name; the
library stores no address. `dmesh_create_qp(eq, target)` connects to a
target, and a server's `DPUMESH_SERVICE` is the target it serves.

- `dmesh_create_qp` fails with `EINVAL` for a malformed target, `ENOENT` for a
  name without an IPv4 address, `EAGAIN` while DNS gives no answer, and
  `ENOSPC` once the process has used 1024 distinct addresses.
- Answers are cached per target for five seconds, the default record TTL of
  CoreDNS. A QP keeps the address it opened with.
- A server resolves `DPUMESH_SERVICE` once, when its channel is created; every
  backend flow it offers the DPU carries that address.
- The preload shim carries a `connect()` over DPUmesh when its destination is
  the current address of a target in `DPUMESH_TARGETS` (separated by commas or
  spaces); other destinations use kernel TCP, logged. A listed target on the
  dialed port without a DNS answer, or a malformed list, fails the connect with
  `EHOSTUNREACH`.

## Transmit and receive

`dmesh_alloc` acquires the QP's transmit gate and reserves contiguous memory;
`dmesh_post_send` commits the reservation and releases the gate. Allocation
pressure returns `EAGAIN` and a later `TX_READY` event permits the retry. The
library emits complete transfer units immediately and retains a short tail
until `dmesh_flush`, pressure or a bounded deadline. `dmesh_tx_inflight`
reports whether submitted units remain in DPU custody. An event may carry part
of a write, and several writes can share a unit.

`dmesh_poll_eq` returns `CONN_REQ` (an accepted QP, followed by its first
payload), `RECV`, `RECV_FIN`, `TX_READY` and `TX_ERROR` events. A `RECV` event
holds its buffer until `dmesh_release_rx_buffer`; `RECV_FIN` and readiness
events hold nothing. Process a returned batch before destroying a QP it names.

## Close

`dmesh_destroy_qp` flushes outgoing data and sends FIN; the handle is unusable
afterwards while the transport retires in-flight units. `dmesh_abort_qp`
cancels the stream; retained receive buffers must still be released. A failed
device quiescence retains registered resources so DMA cannot target freed
memory.

## Event loop

A server sets `DPUMESH_SERVICE` before creating its channel. An event loop
drains `dmesh_poll_eq` until empty, then waits on `dmesh_eq_fd` and drains the
eventfd on wake, bounding the wait by `dmesh_eq_next_deadline_ns` (`-1` when
no tail needs service). Poll one EQ from one thread.

## Transport semantics

- `dmesh_create_channel` establishes one shared Comch session. `dmesh_create_qp`
  opens a logical flow on it and synchronously waits for READY; it fails with
  `ENOSPC` when the DPU worker's 32 flows are in use. Closing one flow keeps
  sibling flows and the channel's Comch session alive.
- `dmesh_destroy_qp` and `dmesh_abort_qp` end that connection; the peer's
  close arrives as `RECV_FIN`.
- A QP's receive buffers are consumed in arrival order on the wire: a buffer
  held for long stalls only that QP.
- `dmesh_msg_max` is 8192. `dmesh_pod_id` returns `DPUMESH_POD_ID`.

The [examples](../examples/README.md) show the lifecycle for each API; the
[tests](../tests/README.md) cover the core and façades without a device.
