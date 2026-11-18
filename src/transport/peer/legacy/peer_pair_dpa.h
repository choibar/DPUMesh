#ifndef DMESH_PEER_PAIR_DPA_H
#define DMESH_PEER_PAIR_DPA_H
#include "peer_dpa_owner.h"

int peer_pair_dpa_new(const struct peer_pair_transport_config *,
                      struct ibv_context *,
                      uint8_t port,
                      uint32_t gid,
                      uint16_t mtu,
                      const struct peer_dpa_runtime *,
                      struct peer_pair_transport **,
                      char *,
                      size_t);
/* Same driver used by runtime and hardware tests. ctx_free detaches its DPA
 * state before freeing mappings; attachment failure must not leave a running
 * thread referencing the state. Base DPA/dev remain caller-owned. */
int peer_pair_dpa_driver_new(struct ibv_context *,
                             uint8_t port,
                             uint32_t gid,
                             uint16_t mtu,
                             unsigned eu,
                             const struct peer_dpa_runtime *,
                             struct peer_pair_driver *);
#endif
