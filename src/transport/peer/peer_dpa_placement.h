#ifndef DMESH_PEER_DPA_PLACEMENT_H
#define DMESH_PEER_DPA_PLACEMENT_H
#include <limits.h>

/* The existing MsgQ PE owner is physical EU % workers. A worker creates
 * contexts only on its local RDMA EUs. If RDMA EUs are fewer than workers,
 * workers without a local EU share a bounded, deterministic fallback EU.
 * Child indices are compact worker-local indices, not global pool indices. */
static inline unsigned peer_dpa_local_first(unsigned dma, unsigned workers, unsigned worker)
{
    unsigned offset = dma % workers; /* callers validate workers/worker first */
    return worker >= offset ? worker - offset : workers - (offset - worker);
}

static inline unsigned
peer_dpa_child_count(unsigned dma, unsigned rdma, unsigned workers, unsigned worker)
{
    if (!rdma || !workers || worker >= workers || rdma > UINT_MAX - dma)
        return 0;
    if (rdma < workers)
        return 1;

    unsigned first = peer_dpa_local_first(dma, workers, worker);
    return 1 + (rdma - 1 - first) / workers;
}

static inline unsigned
peer_dpa_child_eu(unsigned dma, unsigned rdma, unsigned workers, unsigned worker, unsigned child)
{
    unsigned count = peer_dpa_child_count(dma, rdma, workers, worker);
    if (!count || child >= count || rdma > UINT_MAX - dma)
        return UINT_MAX;

    unsigned first = peer_dpa_local_first(dma, workers, worker);
    unsigned group = rdma < workers ? first % rdma : first + child * workers;
    return dma + group;
}

static inline int
peer_dpa_worker_owns_eu(unsigned dma, unsigned rdma, unsigned workers, unsigned worker, unsigned eu)
{
    if (!peer_dpa_child_count(dma, rdma, workers, worker) || eu < dma || eu - dma >= rdma)
        return 0;
    if (rdma < workers)
        return eu == peer_dpa_child_eu(dma, rdma, workers, worker, 0);

    return eu % workers == worker;
}
#endif
