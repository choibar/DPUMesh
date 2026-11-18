#ifndef DMESH_PEER_CONNECTION_IO_H
#define DMESH_PEER_CONNECTION_IO_H

/* Worker-local byte I/O adapter between Linkerd and the peer channel protocol. */
#include "peer_config.h"
#include "peer_authority.h"
#include "peer_identity.h"
#include "peer_pair_transport.h"

struct peer_connection_worker;
struct peer_connection_info {
    struct peer_flow_origin origin;
    uint32_t token, src_ip, dst_ip;
    uint16_t dst_port;
    uint8_t incoming, state; /* 1 pending, 2 open, 3 closed */
};

/* Transport lifetime stays with the node. All functions are worker-owned.
 * listener must check a live, registered native Host LISTEN, not a Pod claim. */
int peer_connection_worker_new(struct peer_authority *, const char *node, const uint8_t key[32],
                            struct peer_pair_transport *, int (*listener)(uint32_t, uint16_t),
                            struct peer_connection_worker **);
int peer_connection_worker_progress(struct peer_connection_worker *);
void peer_connection_worker_stop(struct peer_connection_worker *);
/* Quiesce first; destroy the transport before freeing registered arena memory. */
int peer_connection_worker_free(struct peer_connection_worker **);
/* Stop using the table but keep the arena backing alive until the transport
 * has destroyed its registrations. The node frees the worker afterwards. */
int peer_connection_worker_quiesce(struct peer_connection_worker *);

/* Network-order IPv4; lookup is against the signed, held topology. */
int peer_connection_open(struct peer_connection_worker *, const struct peer_flow_origin *,
                      uint32_t ip, uint16_t port, uint32_t *token);
int peer_connection_next(struct peer_connection_worker *, unsigned *cursor, struct peer_connection_info *);
int peer_connection_info(struct peer_connection_worker *, uint32_t token, struct peer_connection_info *);
/* RX staging only; the legacy TX outputs are NULL/0. */
int peer_connection_buffers(struct peer_connection_worker *, uint32_t token,
                         void **tx, size_t *tx_bytes, void **rx, size_t *rx_bytes);
/* TX has no separate staging ring: acquire writable arena chunks, then submit
 * each chunk once with peer_connection_send(chunk, bytes). 1 acquired, 0 pressure,
 * negative errno. The returned body has exclusive wire-header headroom. */
int peer_connection_tx_acquire(struct peer_connection_worker *, uint32_t token,
                           uint32_t *chunk, void **data, size_t *capacity);
int peer_connection_receive(struct peer_connection_worker *, uint32_t token, uint32_t *pos, uint32_t *bytes);
int peer_connection_consumed(struct peer_connection_worker *, uint32_t token, uint32_t position);
int peer_connection_send(struct peer_connection_worker *, uint32_t token, uint32_t chunk, uint32_t bytes);
int peer_connection_completed(struct peer_connection_worker *, uint32_t token, uint64_t *bytes);
int peer_connection_fin(struct peer_connection_worker *, uint32_t token);
/* Call only AFTER every Rust reader/writer has cleared its staging pointer.
 * A token is not recycled until transport custody has also ended.
 * Returns 1 if removed, 0 while DMA retirement is pending, or -errno. */
int peer_connection_detach(struct peer_connection_worker *, uint32_t token);

#endif
