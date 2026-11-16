#ifndef DMESH_PEER_WIRE_WRITE_SYNC_H
#define DMESH_PEER_WIRE_WRITE_SYNC_H
#include "peer_pair_transport.h"

#define PEER_WRITE_SYNC_RING_DEFAULT (2u * 1024u * 1024u)
#define PEER_WRITE_SYNC_RING_MAX (16u * 1024u * 1024u)

/* ARM-owned RC WRITE + real external-buffer DOCA Sync Events. Uses a
 * DOCA-device PD for all registrations and QPs; no DPA work. Pair/IPsec
 * barriers remain mandatory. Prototype RX adapter copies before credit. */
int peer_wire_write_sync_new(struct ibv_context *,
                             uint8_t port,
                             uint32_t gid,
                             uint16_t mtu,
                             uint32_t ring_bytes,
                             const struct peer_wire_ops **,
                             void **,
                             char *error,
                             size_t error_len);
int peer_write_sync_prepare(void *, void **, struct peer_verbs_endpoint *);
int peer_write_sync_prepare_pair(void *,
                                 const struct dmesh_peer_pair *,
                                 void **,
                                 struct peer_verbs_endpoint *);
int peer_write_sync_activate(void *, const struct peer_verbs_endpoint *, int protection_ready);
int peer_write_sync_send_drained(void *);
int peer_write_sync_close(void *);
int peer_pair_write_sync_new(const struct peer_pair_transport_config *,
                             struct ibv_context *,
                             uint8_t port,
                             uint32_t gid,
                             uint16_t mtu,
                             uint32_t ring_bytes,
                             struct peer_pair_transport **,
                             char *error,
                             size_t error_len);
#endif
