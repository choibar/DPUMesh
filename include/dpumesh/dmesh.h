/* DPUmesh native C API.
 *
 * Sockets over the DPU: a channel per process, which is also its listening
 * socket, and full-duplex QPs, each with its own readiness fd, registered TX
 * reservations, zero-copy RX events and one-shot TX-ready notifications. See
 * design/API.md for the complete contract. */
#ifndef DMESH_H
#define DMESH_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "dmesh_common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Internal transport context. */
struct dpumesh_ctx;

/* ===== Handles ===== */

/* One transport channel per process. */
typedef struct dmesh_channel {
    struct dpumesh_ctx *ctx;
    int            block_size;  /* max contiguous alloc/post reservation */
} dmesh_channel_t;

/* A QP's private wait set (internal). */
struct dmesh_eq;

/* One persistent full-duplex service byte stream. */
typedef struct dmesh_qp {
    dmesh_channel_t *ep;
    struct dmesh_eq *eq;       /* its readiness fd and event state (internal) */
    void     *user_data;       /* application-owned context */
    int       role;            /* DMESH_ROLE_CLIENT (connect) | DMESH_ROLE_SERVER (accept) */

    /* addressing */
    uint16_t  local_port;      /* my port (this conn's id) */
    int16_t   dst_service;     /* peer service (the service connected to / caller's) */
    int16_t   remote_pod;      /* SERVER: the DPU-facing peer pod (learned at accept),
                                * or DMESH_POD_REMOTE when the peer is on another
                                * node — a pod id is a node-local transport
                                * identifier, so a remote peer has none, and the
                                * reply routes from the DPU's conntrack instead.
                                * CLIENT: always DMESH_POD_BLANK — the DPU selects the
                                * backend, so the host never names one. */
    uint16_t  remote_port;     /* SERVER: the peer uP (learned at accept); CLIENT: 0. */
    /* Independent inbound and outbound close state. */
    uint8_t   peer_closed;     /* INBOUND: the peer's FIN landed -> EOF (sticky) */
    uint8_t   fin_sent;        /* OUTBOUND: our FIN is on the wire (sticky) */
    uint16_t  seq;             /* per-QP outbound descriptor counter */

    /* inbound view (rx_slot>=0 => one fragment is held on the conn; the socket
     * facade's partial-read cursor lives here too) */
    int            rx_slot;    /* landing byte-offset in host RX buffer; -1 = none */
    const uint8_t *rx_buf;
    uint32_t       rx_len;
    uint32_t       rx_pos;

    /* Outbound bytes live in the per-conn TX byte-ring (keyed by local_port);
     * the conn holds no TX buffer state of its own. */
} dmesh_qp_t;

/* ===== Events ===== */

typedef enum {
    DMESH_EVENT_RECV     = 1, /* one inbound byte-stream fragment; holds an RX credit */
    DMESH_EVENT_RECV_FIN = 2, /* peer closed the conn (EOF); no credit held */
    DMESH_EVENT_TX_READY = 3, /* an EAGAIN-blocked QP should retry dmesh_alloc */
    DMESH_EVENT_TX_ERROR = 4, /* deferred buffered-tail submission failed */
    DMESH_EVENT_PREPARE  = 5, /* migration: the DPU moves this client QP's stream;
                               * open a spare stream to its target, the "ip:port"
                               * in buf/len, then dmesh_spare_ready the spare */
    DMESH_EVENT_SCALE    = 6, /* flow scaling: buf points to a dmesh_scale_t for
                               * this client QP's target; answer with
                               * dmesh_scale_ack on this QP */
} dmesh_event_type_t;

/* A SCALE request: each client of target should keep count streams to it. The
 * DPU places one svc-to-svc pair's streams on distinct workers, so count
 * streams spread the pair's traffic over count DPU cores. */
typedef struct dmesh_scale {
    uint32_t token;      /* echoed by dmesh_scale_ack */
    uint32_t count;      /* >= 1 */
    char     target[24]; /* "ip:port", NUL-terminated */
} dmesh_scale_t;

typedef struct dmesh_event {
    dmesh_qp_t         *qp;       /* the QP polled (use qp->user_data for your
                                   * per-conn context) */
    dmesh_event_type_t  type;
    const uint8_t      *buf;      /* RECV: points INTO the RX mmap (zero-copy); valid
                                   * until dmesh_release_rx_buffer. PREPARE / SCALE:
                                   * the request, valid until the QP's next poll.
                                   * Else NULL. */
    uint32_t            len;      /* RECV: byte-stream fragment length; PREPARE /
                                   * SCALE: the request's size. Else 0. */
    int32_t             _rx_token; /* internal release token; -1 = nothing held */
} dmesh_event_t;

/* ===== Channel: the process's listening socket (ibv_open_device + PD) ===== */

/* Create a channel serving $DPUMESH_SERVICE, or "<pod ip>:$DPUMESH_PORT" when
 * only the port is set (a client with neither). NULL indicates failure. */
dmesh_channel_t *dmesh_create_channel(void);

/* Destroy an idle channel. Returns EBUSY while a QP remains, changing nothing,
 * or while a receive buffer is still held, having stopped the channel: release
 * it and destroy again. Safe on NULL. */
int dmesh_destroy_channel(dmesh_channel_t *s);

int dmesh_post_max(dmesh_channel_t *s);    /* max length of one alloc/post (block size) */

/* Readiness fd (an epoll set, level-triggered) of the channel's own work: the
 * session's control messages, keepalive and loss, and the streams the DPU
 * routes to $DPUMESH_SERVICE. Every process polls it and calls dmesh_accept()
 * whenever it is readable, a client too. It is readable from creation until
 * the first dmesh_accept(). */
int dmesh_channel_fd(dmesh_channel_t *ch);

/* Do the channel's work, then return the next inbound stream: a QP whose
 * first bytes its own poll returns as RECV. NULL with EAGAIN when none waits
 * (the fd is then armed for the next), or the session's errno after a
 * transport loss (see dmesh_poll_qp). On a client it does the work alone. One
 * thread at a time. */
dmesh_qp_t *dmesh_accept(dmesh_channel_t *ch);

/* ===== QP: a connected socket (rdma_cm) ===== */

/* Connect a QP to a "<host>:<port>" service target: host is a Service DNS name
 * ("name" in this Pod's namespace, "name.namespace", or a full name) or an
 * IPv4 literal, port the Service port. Returns EINVAL (malformed target),
 * ENOENT (no such name), EAGAIN (DNS temporarily unavailable — retry), ENOSPC,
 * ENOMEM, EMFILE, or the session's errno after a transport loss. */
dmesh_qp_t *dmesh_create_qp(dmesh_channel_t *ch, const char *target);

/* Readiness fd (an epoll set, level-triggered): poll it, then call
 * dmesh_poll_qp() until it returns 0, which settles the fd and arms the
 * transport's doorbell for the next sleep. -1 when unavailable. */
int dmesh_qp_fd(dmesh_qp_t *qp);

/* Flush, send FIN, release RX credit, and destroy the QP and its fd. The
 * pointer is invalid on return. Defer destruction until the current poll
 * batch is fully dispatched. */
int dmesh_destroy_qp(dmesh_qp_t *c);

/* Discard unsent bytes, reset the connected stream, and destroy the QP. */
int dmesh_abort_qp(dmesh_qp_t *c);

/* ===== Send (ibv_post_send) ===== */

/* Reserve `len` contiguous bytes in registered TX memory. A successful
 * reservation opens a transmit call that dmesh_post_send closes; the QP admits
 * no other transmit until then. Returns NULL with:
 *   EAGAIN   QP window or shared block pool exhausted. DMESH_EVENT_TX_READY is armed.
 *   EINVAL   invalid length or unestablished connection.
 *   EDEADLK  a transmit call is already open on this QP. */
void *dmesh_alloc(dmesh_qp_t *c, uint32_t len);

/* Commit bytes from the current dmesh_alloc() reservation, close the transmit
 * call and submit them at once, a partial transport batch included. `buf` must
 * match the reservation and `len` must fit it. Returns EINVAL for invalid input
 * or no open transmit call, or EBADMSG for a submission fault, which leaves the
 * bytes committed for the next post or dmesh_flush. Ownership transfers to the
 * transport on success. A flow fault the transport reports later arrives once
 * as DMESH_EVENT_TX_ERROR and is sticky on the QP. */
int dmesh_post_send(dmesh_qp_t *c, const void *buf, uint32_t len);

/* Submit committed bytes a failed post left behind; a post submits its own,
 * so otherwise a no-op. A descriptor fault returns EBADMSG, an open transmit
 * call EDEADLK. */
int dmesh_flush(dmesh_qp_t *c);

/* ===== Event polling ===== */

/* Nonblocking single-consumer poll of one QP. Returns up to max_events events,
 * 0 when empty, or EINVAL. Order is preserved across partial batches. A
 * TX_ERROR event has no RX buffer and makes later TX calls fail with the
 * latched transport error. A lost transport session ends the process (exit
 * status 75); with DPUMESH_EXIT_ON_LOSS=0 this returns -1 with the session's
 * errno instead, once the events that landed before the loss are drained. */
int dmesh_poll_qp(dmesh_qp_t *qp, dmesh_event_t *events, int max_events);

/* Tell the DPU that this client QP, opened for a PREPARE, is a ready spare
 * (its server's first bytes have arrived); the DPU then drains the stream
 * the spare replaces. */
int dmesh_spare_ready(dmesh_qp_t *c);

/* Answer a SCALE event on the QP it came on: applied is how many of the
 * application's clients of the target now keep the requested count (0: none
 * can, e.g. no client uses a scaling-aware resolver). */
int dmesh_scale_ack(dmesh_qp_t *c, uint32_t token, uint32_t applied);

/* Release the zero-copy RX buffer held by a RECV event and invalidate event->buf.
 * Idempotent, valid after QP closure, and a no-op for other event types. */
void dmesh_release_rx_buffer(dmesh_channel_t *s, dmesh_event_t *event);

#ifdef __cplusplus
}
#endif

#endif /* DMESH_H */
