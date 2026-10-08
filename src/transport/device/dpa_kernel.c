/* DPUMesh descriptor-ring DMA datapath and Comch initialization. */
#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

_Static_assert(sizeof(struct dpa_thread_ctx) == 96, "native DPA argument layout");

/* dpa_dev: extended DPA context handle whose device the consumer belongs to
 * (0 = base context); an RPC runs outside the thread, so it must switch too. */
__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer, uint32_t num_msg, uint64_t dpa_dev)
{
    DOCA_DPA_DEV_LOG_INFO("recv thread init RPC, num_msg: %u\n", num_msg);
    if (dpa_dev != 0)
        doca_dpa_dev_device_set((doca_dpa_dev_t)dpa_dev);
	doca_dpa_dev_comch_consumer_ack(consumer, num_msg);

	return 0;
}

/* Bound every activation, including idle and credit-starved polling. The SDK
 * watchdog forbids indefinitely scheduled kernels. Retrigger retains this
 * flow's thread/EU ownership model; it does not multiplex other rings. */
#define DMA_POLL_ACTIVATION_BUDGET 65536u

/* Every native copy is flushed when submitted, so stopping never needs an
 * extra receive credit merely to flush previously queued DMA operations. */
static void stop_desc_ring(struct dpa_thread_ctx *arg, uint64_t submitted)
{
    arg->dma_submitted = submitted;
    __dpa_thread_window_writeback();
    arg->stopped = 1;
    __dpa_thread_window_writeback();
    doca_dpa_dev_thread_finish();
}

static void poll_desc_ring(struct dpa_thread_ctx *a)
{
    const uint32_t ring_size = a->buf_arr_size;
    const uint32_t ring_mask = ring_size - 1;
    uint32_t budget = DMA_POLL_ACTIVATION_BUDGET;
    struct dma_ring_ctrl *ctrl = (void *)doca_dpa_dev_buf_get_external_ptr(
        doca_dpa_dev_buf_array_get_buf(a->dpa_buf_arr, 0));
    __dpa_thread_window_read_inv();
    uint64_t complete = ctrl->consumer_head;
    uint64_t bytes = ctrl->completed_bytes;
    uint64_t submit = a->submit_head;
    uint64_t submitted = a->dma_submitted;

    /* A completion must be requested for every copy. Re-arm on entry and
     * after consuming a CQE; retrigger alone does not arm the completion CQ. */
    doca_dpa_dev_completion_request_notification(a->dpa_producer_comp);
    while (budget-- != 0) {
        doca_dpa_dev_completion_element_t comp;
        __dpa_thread_window_read_inv();

        /* Always drain completions, including when RX credits/staging or the
         * submission window are exhausted and while shutting down. Same-flow
         * copies use one producer and one CQ in submission order. Descriptor
         * slots remain owned until this successful completion prefix advances. */
        if (doca_dpa_dev_get_completion(a->dpa_producer_comp, &comp)) {
            int ok = doca_dpa_dev_get_completion_type(comp) == DOCA_DPA_DEV_COMP_SEND;
            doca_dpa_dev_completion_ack(a->dpa_producer_comp, 1);
            doca_dpa_dev_completion_request_notification(a->dpa_producer_comp);
            if (!ok || complete == submit) {
                a->dma_error = 1;
                ctrl->error = 1;
                __dpa_thread_window_writeback();
                stop_desc_ring(a, submitted);
                return;
            }
            const struct dma_desc *d = (void *)doca_dpa_dev_buf_get_external_ptr(
                doca_dpa_dev_buf_array_get_buf(a->dpa_buf_arr, (complete & ring_mask) + 1));
            bytes += d->size;
            ++complete;
            ctrl->completed_bytes = bytes;
            ctrl->consumer_head = complete;
            __dpa_thread_window_writeback();
        }

        /* Stop admission immediately, but keep the kernel alive until every
         * submitted copy has produced its CQE. CPU teardown additionally
         * drains the matching immediate receive messages. */
        if (a->stop || ctrl->error) {
            if (complete == submit) {
                a->submit_head = submit;
                stop_desc_ring(a, submitted);
                return;
            }
            continue;
        }
        uint64_t tail = ctrl->producer_tail;
        if (submit == tail || submit - complete >= DMESH_DPA_MAX_INFLIGHT)
            continue;
        if (tail < submit || tail - complete > ring_size) {
            ctrl->error = 2;
            __dpa_thread_window_writeback();
            continue;
        }
        if (a->rd_fc) {
            uint32_t unread = (a->pos + a->buf_size - a->rd_pos) % a->buf_size;
            if (a->buf_size - unread < 3u * 8064u)
                continue;
        }
        const struct dma_desc *d = (void *)doca_dpa_dev_buf_get_external_ptr(
            doca_dpa_dev_buf_array_get_buf(a->dpa_buf_arr, (submit & ring_mask) + 1));
        if (d->size == 0 || d->size > 8064u || d->size > a->buf_size) {
            ctrl->error = 2;
            __dpa_thread_window_writeback();
            continue;
        }
        uint32_t len = (uint32_t)d->size;
        if (doca_dpa_dev_comch_producer_is_consumer_empty(a->dpa_producer, 1))
            continue;
        if (a->pos + len > a->buf_size)
            a->pos = 0;
        struct comch_dma_comp_msg msg = {
            .type = COMCH_MSG_TYPE_DMA_COMPLETED, .pos = a->pos,
            .length = len, .count = 1,
        };
        /* FLUSH submits work; omitting OPTIMIZE_REPORTS requests its CQE.
         * No CPU source reuse is allowed merely because this call returned. */
        doca_dpa_dev_comch_producer_dma_copy(a->dpa_producer, 1,
            a->dpu_mmap, a->src_addr + a->pos, a->host_mmap, d->addr, len,
            (uint8_t *)&msg, sizeof(msg), DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        ++submit;
        ++submitted;
        a->pos += len;
        if (a->pos == a->buf_size)
            a->pos = 0;
    }
    a->submit_head = submit;
    a->dma_submitted = submitted;
    __dpa_thread_window_writeback();
}

__dpa_global__ void run_dma_manager(void)
{
    struct dpa_thread_ctx *state = (void *)doca_dpa_dev_thread_get_local_storage();
    if (state == NULL) {
        DOCA_DPA_DEV_LOG_ERR("DMA thread has no local storage\n");
        doca_dpa_dev_thread_finish();
        return;
    }
    /* Extended context: select its device before accessing communication objects. */
    if (state->dpa_dev != 0)
        doca_dpa_dev_device_set((doca_dpa_dev_t)state->dpa_dev);

    /* An idle, host-driven thread may have no descriptor ring. */
    if (state->dpa_buf_arr == 0) {
        doca_dpa_dev_thread_reschedule();
        return;
    }

    poll_desc_ring(state);

    /* A descriptor-ring writer does not signal a completion, so ordinary
     * reschedule could sleep forever after an idle activation. Retrigger
     * requests immediate execution; TLS retains this thread's progress. */
    if (!state->stopped)
        doca_dpa_dev_thread_retrigger();
}
