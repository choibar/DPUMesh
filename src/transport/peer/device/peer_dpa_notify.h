#ifndef DMESH_PEER_DPA_NOTIFY_H
#define DMESH_PEER_DPA_NOTIFY_H
#include <doca_dpa_dev_comch_msgq.h>
#include "peer_dpa_ring.h"
#include "peer_dpa_profile.h"

/* Called on the base PF, AFTER external-window status/credit writeback.
 * The MsgQ is routed to ARM worker eu%A by init_comch_dpa_msgq; its callback
 * wakes the context's ARM owner if different. The hint carries no QP pointer.
 * Notifications are unconditional on ARM's parked state: arm -> drain ->
 * wait in the existing worker loop closes the publication/sleep race.
 * Coalesce each bounded RDMA pass, not each CQE. Do not wait for a full batch. */
static inline void peer_dpa_notify(struct peer_dpa_thread_arg *arg, struct peer_dpa_device_state *s)
{
    if (!s->notify_pending)
        return;

    peer_profile_count(&s->profile, PEER_CNT_NOTIFY_ATTEMPT);

    if (!arg->dpa_producer || !arg->dpa_producer_comp) {
        peer_profile_count(&s->profile, PEER_CNT_NOTIFY_UNAVAILABLE);
        return;
    }

    struct peer_dpa_profile_stamp stamp = peer_profile_begin(&s->profile, PEER_PROF_NOTIFY);

    /* One producer per RDMA EU, shared by its contexts. No other EU consumes
     * this credit. Never spin here and starve another QP or context. */
    if (doca_dpa_dev_comch_producer_is_consumer_empty(arg->dpa_producer, arg->dpu_consumer_id)) {
        peer_profile_count(&s->profile, PEER_CNT_NOTIFY_NO_CREDIT);
        peer_profile_end(&s->profile, PEER_PROF_NOTIFY, stamp);
        return;
    }

    struct dpa_rdma_notify_msg msg = {.type = DPA_MSG_RDMA_READY,
                                      .version = DPA_RDMA_NOTIFY_VERSION,
                                      .eu_index = (uint8_t)arg->eu_index,
                                      .worker = (uint8_t)s->owner_worker};
    doca_dpa_dev_comch_producer_post_send_imm_only(arg->dpa_producer,
                                                   arg->dpu_consumer_id,
                                                   (const uint8_t *)&msg,
                                                   sizeof(msg),
                                                   DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    /* Request a producer completion even for a single idle wake, like ring
     * ACKs. The existing kernel drains it and any older deferred DMA reports. */
    arg->producer_deferred = 0;
    s->notify_pending = 0;
    peer_profile_count(&s->profile, PEER_CNT_NOTIFY_SENT);
    peer_profile_end(&s->profile, PEER_PROF_NOTIFY, stamp);
}
#endif
