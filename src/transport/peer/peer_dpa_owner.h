#ifndef DMESH_PEER_DPA_OWNER_H
#define DMESH_PEER_DPA_OWNER_H
#include <limits.h>
#include "peer_pair_transport.h"
struct doca_dev;
struct doca_dpa;

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

/* dma is the first RDMA EU; rdma is the number of consecutive RDMA EUs.
 * The existing MsgQ PE owner is physical EU % workers. A worker creates
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

struct peer_dpa_owner;
/* Initialization is serialized before worker tasks start. The returned owner
 * borrows dev/dpa, which must outlive it. One context and MsgQ per ARM worker. */
int peer_dpa_owner_new(struct doca_dev *, struct doca_dpa *, unsigned worker,
                       unsigned workers, unsigned physical_eu, struct peer_dpa_owner **);
const struct peer_dpa_runtime *peer_dpa_owner_runtime(struct peer_dpa_owner *);
int peer_dpa_owner_progress(struct peer_dpa_owner *);
int peer_dpa_owner_fd(struct peer_dpa_owner *);
int peer_dpa_owner_arm(struct peer_dpa_owner *);
int peer_dpa_owner_disarm(struct peer_dpa_owner *);
/* Destroy transport first: REMOVE/QP destruction/STOP must have detached all
 * device state. Failure retains the object and its mappings for retry. */
int peer_dpa_owner_free(struct peer_dpa_owner **);
#endif
