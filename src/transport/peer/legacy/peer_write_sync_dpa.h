#ifndef DMESH_WRITE_SYNC_DPA_H
#define DMESH_WRITE_SYNC_DPA_H
#include <infiniband/verbs.h>
#include "peer_pair_dpa.h"
struct peer_sync_engine;
struct peer_sync_events;
struct peer_wsd;
struct peer_wsd_qp;
int peer_wsd_new(struct peer_sync_engine *,
                 const struct peer_dpa_runtime *,
                 unsigned eu,
                 struct peer_wsd **);
int peer_wsd_new_imm(struct peer_sync_engine *,
                     const struct peer_dpa_runtime *,
                     unsigned eu,
                     struct peer_wsd **);
int peer_wsd_free(struct peer_wsd *);
int peer_wsd_qp_new(struct peer_wsd *,
                    unsigned lane,
                    uint32_t generation,
                    uint32_t capacity,
                    uint32_t gid,
                    uint16_t mtu,
                    struct peer_verbs_endpoint *,
                    struct peer_wsd_qp **);
struct dpa_rdma_counter_msg;
void peer_wsd_counter_message(const struct dpa_rdma_counter_msg *);
/* Data-owner wait hooks; PE notification must be armed before arm_worker,
 * followed by a final transport scan. Cancel on every wait/scan exit. */
int peer_wsd_arm_worker(unsigned worker);
void peer_wsd_disarm_worker(unsigned worker);
int peer_wsd_register(struct peer_wsd *, void *, size_t, uint32_t);
int peer_wsd_unregister(struct peer_wsd *, uint32_t);
int peer_wsd_activate(struct peer_wsd_qp *, const struct peer_verbs_endpoint *, int);
int peer_wsd_event_export(struct peer_wsd_qp *, void *, size_t);
unsigned peer_wsd_hello_size(const struct peer_wsd_qp *);
int peer_wsd_configure_native(
    struct peer_wsd_qp *, uint64_t, uint32_t, uint32_t, const void *, size_t);
uint32_t peer_wsd_qpn(struct peer_wsd_qp *);
int peer_wsd_modify(struct peer_wsd_qp *, struct ibv_qp_attr *, int);
int peer_wsd_destroy(struct peer_wsd_qp *);
/* Entries includes any descriptor reserved by a currently held frame lease. */
int peer_wsd_admit(struct peer_wsd_qp *, unsigned entries);
int peer_wsd_data(struct peer_wsd_qp *, uint64_t, uint64_t, uint32_t, uint32_t);
int peer_wsd_head(struct peer_wsd_qp *, uint64_t, uint64_t);
int peer_wsd_recv(struct peer_wsd_qp *, const struct ibv_recv_wr *);
int peer_wsd_send(struct peer_wsd_qp *, const struct ibv_send_wr *);
int peer_wsd_poll(struct peer_wsd *, int max, struct ibv_wc *);
void peer_wsd_debug(struct peer_wsd_qp *);
/* RX tail, returned remote head, local completed byte end, consumed requests. */
int peer_wsd_counters(struct peer_wsd_qp *, uint64_t *, uint64_t *, uint64_t *, uint64_t *);
int peer_pair_write_sync_dpa_new(const struct peer_pair_transport_config *,
                                 struct ibv_context *,
                                 uint8_t,
                                 uint32_t,
                                 uint16_t,
                                 uint32_t,
                                 const struct peer_dpa_runtime *,
                                 struct peer_pair_transport **,
                                 char *,
                                 size_t);
int peer_pair_write_imm_dpa_new(const struct peer_pair_transport_config *,
                                struct ibv_context *,
                                uint8_t,
                                uint32_t,
                                uint16_t,
                                uint32_t,
                                const struct peer_dpa_runtime *,
                                struct peer_pair_transport **,
                                char *,
                                size_t);
#endif
