#ifndef DMESH_PEER_WIRE_H
#define DMESH_PEER_WIRE_H

#include <stddef.h>
#include <stdint.h>
#include "peer_rdma_limits.h"

/* The byte carrier underneath the peer channel's authenticated session.
 *
 * It moves whole messages and knows nothing else: not that a message holds
 * ciphertext, not what the channel's own frames look like, not that a
 * handshake precedes them. That is the whole point — TCP and an RDMA queue
 * pair both fit behind this, so CI exercises the entire stack above it on a
 * machine with no NIC, and bring-up can fall back to sockets without changing
 * a line of the layers that matter.
 *
 * A message is a chunk of TLS ciphertext. Its bound is the largest peer frame
 * plus the per-record overhead TLS adds when it splits that frame across
 * records. The generic TLS carrier needs 72KiB including record overhead.
 * Native DPA slots and pair-scoped arena frames are separately bounded to
 * 64KiB, including their four-byte dmesh4 header. */
#define PEER_WIRE_MSG_MAX 73728u

/* Worker-confined synchronous leases. Never access a pointer or call an op
 * after wire close. Each direction permits one outstanding lease. */
enum {
    PEER_WIRE_TX = 1,
    PEER_WIRE_RX = 2,
    PEER_WIRE_FRAME_TX = 3
};

struct peer_wire_token {
    uint64_t conn_epoch;
    uint64_t lease_generation;
    uint32_t slot;
    uint32_t kind;
};

struct peer_wire_tx_lease {
    uint8_t *data;
    size_t cap;
    struct peer_wire_token token;
};

struct peer_wire_rx_lease {
    const uint8_t *data;
    size_t len;
    struct peer_wire_token token;
};

/* Pair-frame TX is one contiguous registered header + body. Control/copied
 * frames use SEND-slot scratch; arena batches reserve header headroom.
 * An arena payload may replace the scratch: the carrier retains it and releases
 * it only after SEND CQ success or a successful QP destruction fence. Errors
 * and STREAM_ACK alone are not permission to reuse NIC-readable memory.
 * All callbacks run on the owning worker (or after workers join at teardown).
 * region names the stable DPU arena, alive until the carrier is destroyed;
 * this API does not accept Host-exported or arbitrary transient mappings. */
#include "peer_data_wire.h"
#define PEER_WIRE_HEADER_BYTES PEER_DATA_HEADER_BYTES
#define PEER_WIRE_FRAME_BYTES (64u * 1024u)
_Static_assert(PEER_WIRE_FRAME_BYTES == PEER_RDMA_SLOT_BYTES, "RX slot/frame capacity");
#define PEER_WIRE_TX_BODY_BYTES (PEER_WIRE_FRAME_BYTES - PEER_WIRE_HEADER_BYTES)

struct peer_wire_payload {
    const uint8_t *data;
    /* TX only: writable exclusive headroom immediately preceding data.
     * Encoder writes once before commit; header AND body stay immutable until
     * SEND CQ/QP fence. RX payload views leave this NULL. */
    uint8_t *tx_header;
    size_t len;
    void *region;
    size_t region_len;
    void *owner, *cookie;
    void (*retain)(void *owner, void *cookie);
    void (*release)(void *owner, void *cookie);
};

struct peer_wire_frame_lease {
    uint8_t *header, *body;
    size_t body_cap;
    struct peer_wire_token token;
};

/* Contiguous RC receive: one 64KiB arena frame, header followed by body.
 * Pool payload.data/len describe the FULL writable frame, including header.
 * frame_acquire exposes header=base and a payload view at base+4.
 * The pool returns one owned reference. Consumers retain the payload before
 * releasing the synchronous frame view. Only the carrier owner reposts RQ
 * slots, after exclusive() observes all asynchronous readers have retired.
 * A replacement chunk may be posted while the old chunk is still consumed.
 * Pool memory outlives every carrier registration and all DMA readers. */
#define PEER_WIRE_BODY_BYTES PEER_WIRE_TX_BODY_BYTES

struct peer_wire_rx_pool {
    void *ctx;
    int (*acquire)(void *, const char *local_uid, struct peer_wire_payload *);
    int (*exclusive)(const struct peer_wire_payload *);
};

struct peer_wire_rx_frame {
    const uint8_t *header;
    struct peer_wire_payload payload;
    size_t body_len;
    struct peer_wire_token token;
};

static inline int peer_wire_payload_valid(const struct peer_wire_payload *p, size_t len)
{
    if (!p || !p->data || !p->region || !len || p->len != len || !p->retain || !p->release ||
        !p->region_len)
        return 0;

    uintptr_t base = (uintptr_t)p->region, addr = (uintptr_t)p->data;
    return addr >= base && addr - base <= p->region_len && len <= p->region_len - (addr - base) &&
           p->region_len <= UINTPTR_MAX - base;
}

static inline int peer_wire_frame_payload_valid(const struct peer_wire_payload *p, size_t len)
{
    if (!peer_wire_payload_valid(p, len) || !p->tx_header || len > PEER_WIRE_TX_BODY_BYTES)
        return 0;

    uintptr_t base = (uintptr_t)p->region, addr = (uintptr_t)p->data;
    return addr - base >= PEER_WIRE_HEADER_BYTES &&
           (uintptr_t)p->tx_header == addr - PEER_WIRE_HEADER_BYTES;
}

/* Called only for an unposted/consumed RQ slot. A consumer has no way to
 * acquire a new reference after the frame view was released, so an acquire
 * observation of exclusivity permits the NIC to overwrite the chunk. */
static inline int peer_wire_rx_refill(const struct peer_wire_rx_pool *pool,
                                      const char *uid,
                                      struct peer_wire_payload *p)
{
    if (p->data && pool->exclusive(p))
        return 1;

    struct peer_wire_payload next = {0};
    int rc = pool->acquire(pool->ctx, uid, &next);
    if (rc != 1)
        return rc;
    if (!peer_wire_payload_valid(&next, PEER_WIRE_FRAME_BYTES)) {
        if (next.release)
            next.release(next.owner, next.cookie);
        return -1;
    }

    if (p->release)
        p->release(p->owner, p->cookie);
    *p = next;

    return 1;
}

struct peer_wire_ops {
    /* Start a connection. Returns 0 with *wc set even when the connect has not
     * completed, which is the common case: `established` reports when it has. */
    int (*connect)(void *wctx, uint32_t ip_be, uint16_t port, void **wc);
    /* Absorb readiness: finish outbound connects, flush what a full send queue
     * held back, and hand out inbound connections that finished arriving.
     * Returns whether anything actually moved, never whether work is pending —
     * a caller that spins on the second one never sleeps. */
    int (*progress)(void *wctx, void **accepted, int max, int *n_accepted);
    /* All of `len` is taken (1), none of it is (0), or the connection is dead
     * (-1). A message is never partially accepted: the layer above encrypts in
     * sequence, so a half-sent message has no recoverable state. */
    int (*send_msg)(void *wc, const void *buf, size_t len);
    /* One whole message, 0 when none has arrived yet, negative on fault. */
    long (*recv_msg)(void *wc, void *buf, size_t cap);
    int (*established)(void *wc);
    int (*faulted)(void *wc);
    void (*close)(void *wc);
    /* The one descriptor a worker's event loop waits on for every connection
     * this carrier holds, or -1 when it has none. */
    int (*epfd)(void *wctx);
    void (*ctx_free)(void *wctx);
    /* Optional complete groups: TX three ops, RX two ops. Reserve/acquire:
     * 1 grants ownership, 0 unavailable, -1 fault; outputs cleared on 0/-1.
     * Commit: 1 posts the whole nonempty prefix, -1 terminal, never 0.
     * Valid commit/cancel/release consume and clear the lease, even on fault.
     * Invalid tokens return -1 without touching another owner's slot.
     * Cancel/release: 0 success, -1 fault. SEND CQ returns posted TX memory;
     * RX is reposted only on release, never while borrowed. */
    int (*tx_reserve)(void *wc, struct peer_wire_tx_lease *out);
    int (*tx_commit)(void *wc, struct peer_wire_tx_lease *lease, size_t len);
    int (*tx_cancel)(void *wc, struct peer_wire_tx_lease *lease);
    int (*rx_acquire)(void *wc, struct peer_wire_rx_lease *out);
    int (*rx_release)(void *wc, struct peer_wire_rx_lease *lease);
    /* Optional complete group. Reserve: 1/0/-1 as above. Commit always
     * consumes a valid lease, returning 1 or -1, never would-block. The full
     * frame starts at payload->tx_header and spans 4 + body_len bytes when
     * payload is non-NULL; otherwise contiguous lease.header + lease.body. The caller encodes the
     * header into the chosen frame before commit. Cancel sends nothing. */
    int (*frame_reserve)(void *wc, struct peer_wire_frame_lease *out);
    int (*frame_commit)(void *wc,
                        struct peer_wire_frame_lease *,
                        size_t body_len,
                        const struct peer_wire_payload *payload);
    int (*frame_cancel)(void *wc, struct peer_wire_frame_lease *);
    /* Optional complete contiguous arena RX group. Configure before preparing any QP.
     * acquire: 1 frame, 0 pending, -1 failure, -2 legacy connection.
     * release ends the view; retained arena references remain valid even
     * after QP destruction. No payload bytes are copied by these operations. */
    int (*rx_pool_set)(void *ctx, const struct peer_wire_rx_pool *);
    int (*frame_acquire)(void *wc, struct peer_wire_rx_frame *);
    int (*frame_release)(void *wc, struct peer_wire_rx_frame *);
};

static inline int peer_wire_ops_valid(const struct peer_wire_ops *ops)
{
    if (!ops)
        return 0;

    int tx = !!ops->tx_reserve + !!ops->tx_commit + !!ops->tx_cancel;
    int rx = !!ops->rx_acquire + !!ops->rx_release;
    int frame = !!ops->frame_reserve + !!ops->frame_commit + !!ops->frame_cancel;
    int arena_rx = !!ops->rx_pool_set + !!ops->frame_acquire + !!ops->frame_release;
    return (tx == 0 || tx == 3) && (rx == 0 || rx == 2) && (frame == 0 || frame == 3) &&
           (arena_rx == 0 || arena_rx == 3);
}

/* The TCP carrier: what CI runs and what bring-up falls back to. It binds and
 * listens before returning, so a port that cannot be taken is reported here
 * rather than surfacing later as connections that never arrive. */
int peer_wire_tcp_new(uint32_t bind_ip_be,
                      uint16_t port,
                      const struct peer_wire_ops **ops,
                      void **wctx,
                      char *error,
                      size_t error_len);
/* The port actually bound, which is what the caller asked for unless it asked
 * for 0. */
uint16_t peer_wire_tcp_port(void *wctx);

/* The RDMA carrier: what the mesh runs on between nodes. The bind address must
 * be the local address of an rdma device, because the protection domain and
 * completion queue every connection shares are taken from the device that
 * address resolves to. Like the TCP carrier it binds and listens before
 * returning, so a fabric that cannot carry this is reported here. */
int peer_wire_rdma_new(uint32_t bind_ip_be,
                       uint16_t port,
                       const struct peer_wire_ops **ops,
                       void **wctx,
                       char *error,
                       size_t error_len);
uint16_t peer_wire_rdma_port(void *wctx);

#endif /* DMESH_PEER_WIRE_H */
