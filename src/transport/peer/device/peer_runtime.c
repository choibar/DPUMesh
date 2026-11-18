#include <doca_dpa_dev.h>
#include <doca_dpa_dev_comch_msgq.h>
#include "peer_write_imm_dpa.h"

static int peer_dpa_attached(const struct peer_dpa_thread_arg *arg)
{
    if (arg->rdma_state)
        return 1;
    unsigned count = arg->rdma_count < DPA_RDMA_CONTEXTS ? arg->rdma_count : DPA_RDMA_CONTEXTS;
    for (unsigned i = 0; i < count; i++)
        if (arg->rdma_states[i])
            return 1;
    return 0;
}

static unsigned drain_context(struct peer_dpa_thread_arg *arg, unsigned context, unsigned budget)
{
    uint64_t state = context == DPA_RDMA_CONTEXTS ? arg->rdma_state : arg->rdma_states[context];
    return state && budget ? peer_write_imm_dpa_drain(arg, context, state, budget) : 0;
}

static unsigned peer_dpa_drain(struct peer_dpa_thread_arg *arg, unsigned budget)
{
    if (!budget)
        return 0;
    if (!arg->rdma_count)
        return drain_context(arg, DPA_RDMA_CONTEXTS, budget); /* standalone probe */

    unsigned count = arg->rdma_count;
    if (count > DPA_RDMA_CONTEXTS)
        return 0;

    unsigned first = arg->rdma_cursor % count, work = 0;
    for (unsigned visit = 0; visit < count && work < budget; visit++) {
        unsigned context = (first + visit) % count;
        unsigned left = budget - work;
        work += drain_context(arg, context, left < 64 ? left : 64);
        arg->rdma_cursor = (context + 1) % count;
    }

    return work;
}

/* Separate entry and argument ABI from the native Host DMA kernel. The two
 * threads share an exact EU affinity, as in the original jukebox scheduler. */
__dpa_global__ void dpumesh_peer_yield(uint64_t resume)
{
    doca_dpa_dev_thread_notify(resume);
    doca_dpa_dev_thread_reschedule();
}

__dpa_rpc__ uint64_t dpumesh_peer_start(uint64_t resume)
{
    doca_dpa_dev_thread_notify(resume);
    return 0;
}

__dpa_global__ void dpumesh_peer_run(uint64_t address)
{
    struct peer_dpa_thread_arg *arg = (void *)address;
    unsigned work = 0, idle = 0;

    for (;;) {
        doca_dpa_dev_completion_element_t element;
        unsigned completed = 0;
        while (arg->dpa_producer_comp &&
               doca_dpa_dev_get_completion(arg->dpa_producer_comp, &element))
            completed++;
        if (completed)
            doca_dpa_dev_completion_ack(arg->dpa_producer_comp, completed);

        unsigned n = peer_dpa_drain(arg, 256 - work);
        work += n;
        idle = n ? 0 : idle + 1;

        /* STOP detaches the last context before its final shared-memory ACK.
         * No autonomous wake remains once all contexts have detached. */
        if (!peer_dpa_attached(arg)) {
            doca_dpa_dev_thread_reschedule();
            return;
        }

        if (work >= 256 || idle >= 64) {
            doca_dpa_dev_thread_notify(arg->yield_notification);
            doca_dpa_dev_thread_reschedule();
            return;
        }
    }
}
