#include <doca_dpa_dev.h>
#include <doca_dpa_dev_comch_msgq.h>
#include "peer_dpa_rdma.h"

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
