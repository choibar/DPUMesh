#ifndef DMESH_PEER_STREAM_H
#define DMESH_PEER_STREAM_H

#include "peer_authority.h"
#include "peer_flow_origin.h"
#include "peer_pair_transport.h"

#define PEER_STREAM_CAP 64u
/* Per-stream TX admission budget, not a separately allocated staging ring. */
#define PEER_STREAM_TX_BYTES (2u * 1024u * 1024u)
/* The channel may retain this many unacknowledged bytes. ACK follows the
 * local reader's consumption, so a stalled reader cannot grow storage. */
#define PEER_STREAM_RX_BYTES DMESH_PEER_STAGING_MAX

struct peer_stream_worker;
struct peer_stream_info {
    struct peer_flow_origin origin;
    uint32_t token, src_ip, dst_ip;
    uint16_t dst_port;
    uint8_t incoming, state; /* 1 pending, 2 open, 3 closed */
};

/* Transport lifetime stays with the node. All functions are worker-owned.
 * listener must check a live, registered native Host LISTEN, not a Pod claim. */
int peer_stream_worker_new(struct peer_authority *, const char *node, const uint8_t key[32],
                            struct peer_pair_transport *, int (*listener)(uint32_t, uint16_t),
                            struct peer_stream_worker **);
int peer_stream_worker_progress(struct peer_stream_worker *);
void peer_stream_worker_stop(struct peer_stream_worker *);
/* Quiesce first; destroy the transport before freeing registered arena memory. */
int peer_stream_worker_free(struct peer_stream_worker **);
/* Stop using the table but keep the arena backing alive until the transport
 * has destroyed its registrations. The node frees the worker afterwards. */
int peer_stream_worker_quiesce(struct peer_stream_worker *);

/* Network-order IPv4; lookup is against the signed, held topology. */
int peer_stream_open(struct peer_stream_worker *, const struct peer_flow_origin *,
                      uint32_t ip, uint16_t port, uint32_t *token);
int peer_stream_next(struct peer_stream_worker *, unsigned *cursor, struct peer_stream_info *);
int peer_stream_info(struct peer_stream_worker *, uint32_t token, struct peer_stream_info *);
/* RX staging only; the legacy TX outputs are NULL/0. */
int peer_stream_buffers(struct peer_stream_worker *, uint32_t token,
                         void **tx, size_t *tx_bytes, void **rx, size_t *rx_bytes);
/* TX has no separate staging ring: acquire writable arena chunks, then submit
 * each chunk once with peer_stream_send(chunk, bytes). 1 acquired, 0 pressure,
 * negative errno. The returned body has exclusive wire-header headroom. */
int peer_stream_tx_acquire(struct peer_stream_worker *, uint32_t token,
                           uint32_t *chunk, void **data, size_t *capacity);
int peer_stream_receive(struct peer_stream_worker *, uint32_t token, uint32_t *pos, uint32_t *bytes);
int peer_stream_consumed(struct peer_stream_worker *, uint32_t token, uint32_t position);
int peer_stream_send(struct peer_stream_worker *, uint32_t token, uint32_t chunk, uint32_t bytes);
int peer_stream_completed(struct peer_stream_worker *, uint32_t token, uint64_t *bytes);
int peer_stream_fin(struct peer_stream_worker *, uint32_t token);
/* Call only AFTER every Rust reader/writer has cleared its staging pointer.
 * A token is not recycled until transport custody has also ended.
 * Returns 1 if removed, 0 while DMA retirement is pending, or -errno. */
int peer_stream_detach(struct peer_stream_worker *, uint32_t token);

#endif
