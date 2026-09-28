#include "comch_msgq.h"

#include "dpa.h"
#include "object.h"
#include "dpa_common.h"
#include "comch_common.h"
#include <doca_pe.h>
#include <doca_build_config.h>


static doca_error_t init_msgqs(struct doca_dev *dev, struct doca_pe *pe,
                               struct dmesh_doca_dpa_thread *thread,
                               struct dmesh_doca_dpa_comch *comch)
{
    struct dmesh_doca_dpa_msgq_create_attr attr = {
        .dev = dev, .dpa = thread->dpa, .max_num_msg = CC_DPA_MAX_MSG_NUM,
        .consumer_comp = comch->consumer_comp, .producer_comp = comch->producer_comp,
        .pe = pe, .ctx_state_changed_cb = dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb,
        .ctx_user_data = comch, .is_send = true,
    };
    doca_error_t result = dmesh_doca_dpa_msgq_create(&attr, &comch->send);
    if (result != DOCA_SUCCESS) return result;
    attr.is_send = false;
    return dmesh_doca_dpa_msgq_create(&attr, &comch->recv);
}

doca_error_t init_comch_dpa_msgq(struct dmesh_conn *conn, struct doca_pe *pe)
{
    doca_error_t result = dmesh_doca_dpa_comch_create(conn);
    if (result != DOCA_SUCCESS) return result;
    return init_msgqs(conn->objs->dev, pe, conn->dpa_thread, conn->dpa_comch);
}

static void endpoint_received(void *owner, uint32_t pos, uint32_t len, uint32_t count)
{
    (void)count;
    struct dmesh_dpa_endpoint *ep = owner;
    if (!ep->recv_segs) return;
    if (ep->recv_seg_cnt == DMESH_RECV_SEG_MAX) { ++ep->recv_seg_dropped; return; }
    int tail = (ep->recv_seg_head + ep->recv_seg_cnt) % DMESH_RECV_SEG_MAX;
    ep->recv_segs[tail] = (struct dmesh_recv_seg){pos, len};
    ++ep->recv_seg_cnt;
}

doca_error_t dmesh_dpa_endpoint_init_comch(struct dmesh_dpa_endpoint *ep, struct doca_dev *dev)
{
    doca_error_t result = dmesh_dpa_comch_create(ep->dpa_thread, &ep->dpa_comch);
    if (result != DOCA_SUCCESS) return result;
    ep->dpa_comch->owner = ep;
    ep->dpa_comch->received = endpoint_received;
    return init_msgqs(dev, ep->pe, ep->dpa_thread, ep->dpa_comch);
}
