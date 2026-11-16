#ifndef DMESH_PEER_DPA_DEVICE_H
#define DMESH_PEER_DPA_DEVICE_H
#include "peer_dpa_abi.h"
/* Bounded ring + CQ service; returns consumed descriptors/CQEs, not inflight. */
unsigned peer_dpa_drain(struct peer_dpa_thread_arg *arg, unsigned budget);

static inline int peer_dpa_attached(const struct peer_dpa_thread_arg *arg)
{
    if (arg->rdma_state)
        return 1;

    for (unsigned i = 0; i < arg->rdma_count && i < DPA_RDMA_CONTEXTS; i++)
        if (arg->rdma_states[i])
            return 1;
    return 0;
}
#endif
