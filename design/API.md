# Native API

`include/dpumesh/dmesh.h` declares fourteen calls with ABI major 6. They
describe reliable full-duplex byte streams as sockets: a channel per process,
which is also its listening socket, and queue pairs (QPs), each with its own
readiness fd, over registered buffers. They expose no remote-memory
operations. The façade and the channel/QP core are linked into
`libdpumesh.so.6`; the carrier maps them onto the DPUMesh transport
([host library](HOST.md)).

## Objects and ownership

A channel is the registration and memory domain and the process's listening
socket. A QP is one connected stream, dialed (`dmesh_create_qp`) or accepted
(`dmesh_accept`), with its own fd. Destroy the QPs before their channel;
destroying a channel with a live QP returns `EBUSY`. One thread at a time
polls a QP, and one polls the channel. A QP's transmit calls form a serial
stream; the caller serializes them against QP destruction. `user_data`
belongs to the application.

| Surface | Calls |
|---|---|
| Channel | `dmesh_create_channel`, `dmesh_destroy_channel`, `dmesh_post_max`, `dmesh_channel_fd`, `dmesh_accept` |
| QP | `dmesh_create_qp`, `dmesh_qp_fd`, `dmesh_destroy_qp`, `dmesh_abort_qp` |
| TX | `dmesh_alloc`, `dmesh_post_send`, `dmesh_flush` |
| Events | `dmesh_poll_qp`, `dmesh_release_rx_buffer` |

## Naming

A service target is `"<host>:<port>"`: host is a Service DNS name (`"name"` in
the calling Pod's namespace, `"name.namespace"`, or a full name) or an IPv4
literal, and port is the Service port. The cluster DNS answers every name; the
library stores no address. `dmesh_create_qp(ch, target)` connects to a
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
until `dmesh_flush`, pressure or a bounded deadline, at which the QP's fd
wakes its poller to publish it. An event may carry part of a write, and
several writes can share a unit.

`dmesh_poll_qp` returns the QP's `RECV`, `RECV_FIN`, `TX_READY` and
`TX_ERROR` events; an accepted QP's first poll returns its first bytes. A
`RECV` event holds its buffer until `dmesh_release_rx_buffer`; `RECV_FIN` and
readiness events hold nothing. Process a returned batch before destroying
the QP.

## Close

`dmesh_destroy_qp` flushes outgoing data and sends FIN; the handle is unusable
afterwards while the transport retires in-flight units. `dmesh_abort_qp`
cancels the stream; retained receive buffers must still be released. A failed
device quiescence retains registered resources so DMA cannot target freed
memory.

## Event loop

A server sets `DPUMESH_SERVICE` before creating its channel. An event loop
waits on the channel's fd and on each QP's fd, as on a listening socket and
its connections, in whatever poller it already runs (epoll, a netpoller,
libuv).

- The channel's fd is readable when its own work waits: the session's
  control messages, keepalive and loss, the flows no QP owns, and inbound
  streams. `dmesh_accept` does that work and returns the next inbound stream,
  until `EAGAIN`, which arms the fd. Every process polls it, a client too.
- A QP's fd is readable when that QP has events or its retained tail comes
  due. `dmesh_poll_qp` until it returns 0 settles the fd and arms the QP's
  doorbell.

With the Comch fast path a QP's own completions raise its fd
([flow wake](HOST.md#flow-wake)); without it the channel's work hands each
QP its receives and raises its fd.

## Transport semantics

- `dmesh_create_channel` establishes one shared Comch session. `dmesh_create_qp`
  opens a logical flow on it and synchronously waits for READY; it fails with
  `ENOSPC` when the DPU worker's 64 flows are in use. Closing one flow keeps
  sibling flows and the channel's Comch session alive.
- `dmesh_destroy_qp` and `dmesh_abort_qp` end that connection; the peer's
  close arrives as `RECV_FIN`.
- A QP's receive buffers are consumed in arrival order on the wire: a buffer
  held for long stalls only that QP.
- One `RECV` carries at most 8192 bytes.
- A lost Comch session (the DPU proxy restarted or the device went away)
  never recovers. The channel's poller, or a dial, that first sees it ends
  the process with exit status 75, so its supervisor restarts it onto a live
  session. With `DPUMESH_EXIT_ON_LOSS=0` the process lives: the channel's and
  every QP's fd stay readable, `dmesh_accept` and `dmesh_poll_qp` return the
  session's errno (`ECONNRESET` for a dropped peer), the poll after the events
  that landed before the loss, and `dmesh_create_qp` fails with it.

The [examples](../examples/README.md) show the lifecycle for each API; the
[tests](../tests/README.md) cover the core and façades without a device.
