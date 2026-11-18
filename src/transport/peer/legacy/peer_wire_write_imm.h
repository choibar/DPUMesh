#ifndef DMESH_PEER_WIRE_WRITE_IMM_H
#define DMESH_PEER_WIRE_WRITE_IMM_H
#include "peer_pair_transport.h"
#include "peer_write_protocol.h"

#define PEER_WRITE_IMM_RING_DEFAULT PEER_WRITE_RING_DEFAULT

/* Experimental, ARM-owned, manual RC WRITE_WITH_IMM carrier. One QP per pair
 * lane; existing manager/IPsec barrier is mandatory. Device context borrowed.
 * RX copies out of its own byte ring; no proxy arena pointer is fabricated.
 * No DPA, DOCA Sync Event, CM or data-path SEND (apart from protected bootstrap).
 * epfd is an epoll of the nonblocking verbs completion channel and a budget
 * continuation eventfd. progress arms before polling; idle workers may sleep.
 */
int peer_wire_write_imm_new(struct ibv_context *,
                            uint8_t port,
                            uint32_t gid,
                            uint16_t mtu,
                            uint32_t ring_bytes,
                            const struct peer_wire_ops **,
                            void **,
                            char *error,
                            size_t error_len);
int peer_write_imm_prepare(void *, void **, struct peer_verbs_endpoint *);
int peer_write_imm_prepare_pair(void *,
                                const struct dmesh_peer_pair *,
                                void **,
                                struct peer_verbs_endpoint *);
int peer_write_imm_activate(void *, const struct peer_verbs_endpoint *, int protection_ready);
int peer_write_imm_send_drained(void *);
int peer_write_imm_close(void *);
int peer_pair_write_imm_new(const struct peer_pair_transport_config *,
                            struct ibv_context *,
                            uint8_t port,
                            uint32_t gid,
                            uint16_t mtu,
                            uint32_t ring_bytes,
                            struct peer_pair_transport **,
                            char *error,
                            size_t error_len);
#endif
