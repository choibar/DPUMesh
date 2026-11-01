/* Internal DPUmesh transport core shared by the native and preload APIs. */

#ifndef DMESH_CORE_H
#define DMESH_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>

#include <dpumesh/dmesh.h>        /* dmesh_channel_t / dmesh_qp_t + the public protos
                                   * implemented in dmesh_core.c (lifecycle) */
#include <dpumesh/dmesh_common.h>

/* ====== Default constants ====== */
#define DPUMESH_SLOT_SIZE_DEFAULT       8192            /* 8KB */
/* Slot pool size (host TX byte-ring + host RX). num_slots × slot_size is at
 * most DPU_BUFFER_SIZE so in-flight bytes inside the DPU staging stay bounded
 * (TX byte-ring occupancy; RX slot admission). The default is normalized down
 * to a whole number of K-ring receive-credit quanta. */
#define DPUMESH_NUM_SLOTS_DEFAULT       8192
/* The host→DPU descriptor ring depth is not configurable: it is the channel layer-ABI
 * constant DMA_RING_SIZE (dpumesh/dmesh_common.h), which the host and the DPA
 * kernel must agree on at build time. */

/* ====== Configuration ====== */
typedef struct {
    int num_slots;        /* slots per pool (0 = default) */
    int slot_size;        /* bytes per slot (0 = default) */
} dpumesh_config_t;

#define DPUMESH_CONFIG_DEFAULT { 0, 0 }

/* ====== SwDescriptor (host-internal RX/TX descriptor, packed) ====== */
/* Host-internal descriptor (not a wire layout): the façade builds it for
 * dpumesh_enqueue (translated to dma_desc) and dpumesh_dequeue fills it from a
 * delivered RX event. Carries the oriented endpoint tuple — see design/API.md §5/§6. */
typedef struct {
    int32_t  body_buf_slot;         /* TX slot (send) | RX landing byte-offset (recv) */
    uint32_t body_len;
    /* ---- oriented endpoint tuple ---- */
    int16_t  src_pod;               /* sender pod (always concrete) */
    uint16_t src_port;              /* sender port */
    int16_t  src_service;           /* sender's own service (= ep service_id); SVC_NONE if none */
    int16_t  dst_service;           /* peer service (routing input when dst_pod==BLANK) */
    int16_t  dst_pod;               /* dest pod; DMESH_POD_BLANK(-1) -> DPU resolves dst_service */
    uint16_t dst_port;              /* dest port; DMESH_PORT_BLANK(0) -> accept queue */
    uint16_t seq;                   /* per-conn sequence (match key with port) */
    uint16_t flags;                 /* private carrier: explicit object adapter */
    int8_t   valid;
} sw_descriptor_t;

/* ====== Opaque context ====== */
typedef struct dpumesh_ctx dpumesh_ctx_t;

/* Connection table index space: each port in [1,65535] is a connection; 0 = BLANK. */
#define DMESH_PORT_SPACE  65536
/* The port table covers every representable port, so a uint16_t port needs no
 * upper bound check at the API surface. */
_Static_assert(DMESH_PORT_SPACE == (UINT16_MAX + 1),
               "the port table must cover every uint16_t port");
/* Max EQs per channel: one per QP. The carrier has at most 32 flows, so this
 * leaves room for an accept or a dial in progress. */
#define DMESH_MAX_EQ      64

/* Events other threads raise for a QP, in dmesh_eq.events. */
enum { DMESH_EQ_TX_READY = 1, DMESH_EQ_TX_ERROR = 2 };

/* Idle-wake counters of one EQ (DPUMESH_WAIT_STATS). Its poller alone writes
 * them, with plain stores; the channel's dump reads them. */
struct dmesh_wait_stats {
    atomic_uint_fast64_t busy, arms, backstop_wakes, backstop_work, custody_sleeps;
    atomic_uint_fast64_t fired[4];       /* EQ_FIRED_* bit i, per empty poll */
};

/* A QP's private wait set and event state (dmesh_qp_fd, dmesh_poll_qp), with
 * one consumer at a time: the QP's poller. The QP is its only connection. */
typedef struct dmesh_eq dmesh_eq_t;
struct dmesh_eq {
    dmesh_channel_t   *ch;
    atomic_uint_fast32_t stripes;   /* stripes bound here (stripe_bind) */
    struct dmesh_qp *accept_spare; /* preallocated before consuming an accept descriptor;
                                    * prevents OOM from stranding SERVER_PENDING */
    int                epfd;        /* the QP's readiness fd (dmesh_qp_fd): an epoll set
                                     * nesting notify_efd, tick_fd and the doorbell of
                                     * its stripe */
    int                notify_efd;  /* eventfd other threads write for this QP; writes
                                     * begin when dmesh_qp_fd hands out epfd (wants_notify). */
    int                tick_fd;     /* timerfd: the periodic backstop, armed with the fd,
                                     * and one-shot re-polls ahead of it (dpumesh_eq_arm) */
    /* Idle wake state, owned by this EQ's poller. */
    int                repoll;      /* tick_fd holds a re-poll ahead of the backstop */
    int                events_since_arm; /* its poller got events since it last armed */
    atomic_int         asleep;      /* doorbells armed: a sender waiting for space wakes it */
    /* Set when dmesh_qp_fd exposes epfd. Poll-only EQs skip eventfd writes and
     * never arm doorbells. */
    atomic_int         wants_notify;
    int                reg_idx;     /* slot in ctx->eqs[], for destroy */
    atomic_uint        events;      /* DMESH_EQ_* raised by any thread, taken by the poller */
    struct dmesh_wait_stats st;
};

/* ====== Lifecycle ====== */
/* Internal channel constructor contract. service_name identifies the advertised
 * service (NULL/"" for a pure client); config = NULL selects defaults. The
 * direct transport owns privileged DOCA registration and mapped memory. */
int  dpumesh_init(dpumesh_ctx_t **ctx, const char *service_name,
                  const dpumesh_config_t *config);
void dpumesh_destroy(dpumesh_ctx_t *ctx);

/* Service lookup contract shared by native and preload facades. resolve_name
 * takes a "<host>:<port>" target and answers through DNS; resolve_addr answers
 * whether an address belongs to a target in $DPUMESH_TARGETS. Both return a
 * process-local service id, ENOENT for a destination outside the mesh, or
 * EAGAIN when resolution is temporarily unavailable. invalidate drops the
 * cached answers for an address after a connection error. */
int dmesh_config_listen_port(void);               /* $DPUMESH_PORT, -1 = not a server */
int dmesh_resolve_name_via(dpumesh_ctx_t *ctx, const char *name);
int dmesh_resolve_addr_via(dpumesh_ctx_t *ctx, uint32_t ip_net, uint16_t port_host);
void dmesh_resolve_invalidate(uint32_t ip_net, uint16_t port_host);

/* Integer entry point for the CLIENT QP, shared by the shim and the public
 * name-taking wrapper. The public dmesh_create_qp(ch, name) lives in
 * dmesh_core.c and calls this after resolve_name. */
dmesh_qp_t *dmesh_qp_open(dmesh_channel_t *ch, int dst_service_id);

/* ====== Query configured values ====== */
/* Max contiguous message = the per-conn TX block size (the reserve/alloc length cap). */
int dpumesh_get_block_size(dpumesh_ctx_t *ctx);

/* ====== Raw Buffer API ====== */

/* Pop one new-connection descriptor off the accept ring. Nonblocking: 0 + *desc,
 * or -1 when empty. The channel's poller (dmesh_accept) is its consumer. */
int dpumesh_dequeue(dpumesh_ctx_t *ctx, sw_descriptor_t *desc);

/* Get pointer to RX buffer data for a slot (zero-copy read). */
uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot);

/* Free an RX buffer slot after reading. */
void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot);

/* Per-connection TX byte-ring lifecycle: reserve, fill, commit, select, track,
 * and enqueue. Reserve is nonblocking and arms TX_READY on EAGAIN. Selection
 * returns the next committed bytes, at most one unit. Tracking precedes ring
 * publication. */
uint8_t *dpumesh_tx_reserve(dpumesh_ctx_t *ctx, uint16_t port, uint32_t len);
int      dpumesh_tx_commit(dpumesh_ctx_t *ctx, uint16_t port,
                           const void *buf, uint32_t len);
void     dpumesh_tx_discard_unsent(dpumesh_ctx_t *ctx, uint16_t port);
int      dpumesh_tx_next_send(dpumesh_ctx_t *ctx, uint16_t port,
                              size_t *out_moff, uint32_t *out_len);
int      dpumesh_tx_track(dpumesh_ctx_t *ctx, uint16_t port, uint16_t seq, uint32_t len);

/* Enqueue a descriptor to TX SQ. Returns 0 on success, -1 on failure. */
int dpumesh_enqueue(dpumesh_ctx_t *ctx, const sw_descriptor_t *desc);

/* ====== Connection API (connection-oriented, full-duplex — no RPC matching) ======
 *
 * A port is a connection, like a socket fd: it owns one peer and one inbound
 * message queue. Inbound is routed by dst_port into that queue; there is no
 * request-to-response matching. */

/* DMESH_ROLE_* live in <dpumesh/dmesh_common.h> (shared with the DPU side). */

/* Allocate a host-unique conn port (>=1) as CLIENT or SERVER (allocates its inbound
 * ring); 0 on exhaustion. `user` is the app's conn handle and `eq` the QP's own
 * event queue, which deliveries from other threads wake. Both are stored before
 * the port goes live. Release with dpumesh_free_port (reclaims undelivered
 * inbound credits). */
uint16_t dpumesh_alloc_port(dpumesh_ctx_t *ctx, int role, void *user, struct dmesh_eq *eq);
/* Promote a drain-created DMESH_ROLE_SERVER_PENDING slot to a live SERVER conn: attach
 * the app's conn handle `user` and bind it to the accepting `eq`. Returns `port` on
 * success, 0 if the slot is not pending (already accepted / freed / race). */
uint16_t dpumesh_accept_port(dpumesh_ctx_t *ctx, uint16_t port, void *user, struct dmesh_eq *eq);
void     dpumesh_free_port(dpumesh_ctx_t *ctx, uint16_t port);

/* Pop the next inbound message descriptor for a conn (CLIENT or SERVER — one path).
 * Returns 1 + fills *out (body at *out->body_buf_slot in the shared RX mmap; free
 * via dpumesh_rx_free after reading), or 0 if the conn inbox is empty. */
int dpumesh_conn_recv(dpumesh_ctx_t *ctx, uint16_t port, sw_descriptor_t *out);

/* Take one raised DMESH_EQ_* event of a QP: 1 if it was pending. TX_READY is
 * taken only while the QP's armed dmesh_alloc(EAGAIN) is still waiting;
 * TX_ERROR leaves the failure sticky. Single-consumer (the QP's poller). */
int dpumesh_qp_take(dmesh_qp_t *c, unsigned event);

/* The reverse path has no background thread. A QP's poller drains its stripe:
 * dpumesh_eq_drain interprets the stripe's published reverse entries under its
 * lock; dpumesh_eq_arm, called when dmesh_poll_qp runs empty, arms the
 * stripe's doorbell, so the QP's fd wakes its poller. The session and the
 * unowned stripes are the channel's (dmesh_accept). */
int  dpumesh_eq_drain(struct dmesh_eq *eq);
void dpumesh_eq_arm(struct dmesh_eq *eq);
/* The drain does not write its own EQ's eventfd: before an empty
 * dmesh_poll_qp returns, dpumesh_qp_pending says whether anything queued for
 * the QP is still unconsumed, and dpumesh_qp_kick_pending raises the eventfd
 * if so (a caller that stops polling before the QP is empty). */
int  dpumesh_qp_pending(dmesh_qp_t *c);
void dpumesh_qp_kick_pending(dmesh_qp_t *c);
/* dmesh_poll_qp returned events: an arm that only the backstop prompted
 * then reports a missed wake. */
static inline void dpumesh_eq_note_work(struct dmesh_eq *eq) { eq->events_since_arm = 1; }
/* Entering dmesh_poll_qp: the EQ is no longer asleep. */
static inline void dpumesh_eq_awake(struct dmesh_eq *eq)
{
    if (atomic_load_explicit(&eq->asleep, memory_order_relaxed))
        atomic_store_explicit(&eq->asleep, 0, memory_order_relaxed);
}
/* The errno of a lost transport session, or 0. Without DPUMESH_EXIT_ON_LOSS=0
 * the process exits with DMESH_EXIT_LOST when the loss is first seen. */
#define DMESH_EXIT_LOST 75   /* EX_TEMPFAIL */
int  dpumesh_lost(dpumesh_ctx_t *ctx);

/* ====== Connection lifecycle — internal, shared by both surfaces ======
 *
 * Transport calls, with nothing socket- or verbs-specific, used by both
 * src/facade/dmesh_api.c and src/facade/dmesh_preload.c. The public half of the
 * lifecycle
 * (dmesh_create_channel / dmesh_create_qp / dmesh_destroy_qp) is declared in
 * <dpumesh/dmesh.h>. */

/* Pop the next inbound connection off the channel-wide accept queue and bind it to
 * `eq`, the new QP's own: allocate a SERVER conn that learns its peer (pod,port)
 * and holds the first fragment (c->rx_slot). NULL+EAGAIN if none pending;
 * NULL+ENOMEM on alloc failure, which drops the message and reclaims its RX
 * credit. dmesh_accept(), the channel's poller, is its only caller. */
dmesh_qp_t *dpumesh_accept(dmesh_eq_t *eq);

/* dmesh_tx_qp_valid() validates the handle, reporting EDEADLK when a transmit
 * call is already open; dmesh_tx_call_active() reports whether one is open.
 * dmesh_tx_publish() submits every committed byte at once. */
int  dmesh_tx_qp_valid(dmesh_qp_t *c);
int  dmesh_tx_call_active(dmesh_qp_t *c);
int  dmesh_tx_publish(dmesh_qp_t *c);

/* Send an ordered zero-length FIN on the connection's forward ring. The call
 * first waits (bounded) for submitted data's DMA copies to complete. A drain
 * or ring timeout returns EBADMSG without latching fin_sent; an open transmit
 * call returns EDEADLK. The FIN remains in flight until the DPU retires the
 * old connection key, which quarantines the local port across close. */
int dmesh_send_fin(dmesh_qp_t *c);

#ifdef __cplusplus
}
#endif

#endif /* DMESH_CORE_H */
