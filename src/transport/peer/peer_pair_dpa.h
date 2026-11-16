#ifndef DMESH_PEER_PAIR_DPA_H
#define DMESH_PEER_PAIR_DPA_H
#include "peer_pair_transport.h"
struct doca_dpa;
struct doca_dev;

struct peer_dpa_runtime {
    struct doca_dpa *dpa;
    struct doca_dev *dev;
    /* Runs on the initialization owner, before ARM workers start. Attach the
     * private state to the selected logical DPA EU (not an ARM worker ID). */
    int (*attach)(void *, unsigned eu, uint64_t state);
    /* Optional staged-start cancellation: 1 detached before run, 0 needs STOP.
     * Same initialization owner as attach; never called concurrently with run. */
    int (*cancel_attach)(void *, unsigned eu, uint64_t state);
    void *attach_ctx;
    /* Production: sparse worker-local RDMA EU contexts (shared fallback when
     * EUs < workers). Select a COMPACT child index, not a global pool index,
     * from a revalidated local Pod pair before QP creation.
     * Zero/default fields retain the standalone single-thread probe API. */
    unsigned workers, rdma_first, rdma_threads, owner_worker;
    int (*select_group)(void *, const struct dmesh_peer_pair *);
    void *select_ctx;
};

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
