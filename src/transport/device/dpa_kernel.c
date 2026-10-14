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
        const struct dma_desc *d = (void *)doca_dpa_dev_buf_get_external_ptr(
            doca_dpa_dev_buf_array_get_buf(a->dpa_buf_arr, (submit & ring_mask) + 1));
        if (d->size == 0 || d->size > 8064u || d->size > a->buf_size) {
            ctrl->error = 2;
            __dpa_thread_window_writeback();
            continue;
        }
        uint32_t len = (uint32_t)d->size;
        /* RX staging remains owned until the CPU reader consumes it, even
         * after DMA completion. A copy stays contiguous: wrapping reserves
         * both the unused tail and the copy itself. Leave at least one byte
         * free so equal producer/consumer offsets can only mean empty. */
        uint32_t unread = (a->pos + a->buf_size - a->rx_consumed_pos) % a->buf_size;
        uint32_t free_space = a->buf_size - unread;
        uint32_t remaining = a->buf_size - a->pos;
        uint32_t wrap_padding = len > remaining ? remaining : 0;
        if (free_space <= wrap_padding + len)
            continue;
        if (doca_dpa_dev_comch_producer_is_consumer_empty(a->dpa_producer, 1))
            continue;

        if (wrap_padding != 0)
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

/*
 * Flow-multiplexed forward DMA (dpa_common.h, DMESH_DPA_MUX). One thread
 * drains up to DMESH_MUX_RINGS connections' forward rings in turn through a
 * single producer. Every copy has its own CQE; the FIFO records the copies in
 * submission order, so a CQE is matched to its ring and a ring's custody ACK
 * still advances only after its copy landed. The ARM opens and closes a slot
 * through its cmd field; this thread answers in the slot's state. A ring that
 * cannot proceed (staging full, bad descriptor) is skipped; only the shared
 * ARM credit and the FIFO cut a whole pass short.
 */
#define MUX_ACTIVATION_US 20000u /* bounded activation, like the native kernel's budget */

static volatile struct dma_ring_ctrl *mux_ctrl(const struct dpa_mux_ring *g)
{
    /* Resolved on every access: a pointer kept across rings of other mmaps
     * costs about twice as much per read (docs/2026-10-13_dpa-mux-probe.md). */
    return (void *)doca_dpa_dev_buf_get_external_ptr(
        doca_dpa_dev_buf_array_get_buf((doca_dpa_dev_buf_arr_t)g->buf_arr, 0));
}

static const volatile struct dma_desc *mux_desc(const struct dpa_mux_ring *g, uint64_t ticket)
{
    return (void *)doca_dpa_dev_buf_get_external_ptr(
        doca_dpa_dev_buf_array_get_buf((doca_dpa_dev_buf_arr_t)g->buf_arr,
                                       (uint32_t)(ticket & (g->ring_size - 1)) + 1));
}

static void mux_fail(struct dpa_mux_ring *g, uint64_t error)
{
    volatile struct dma_ring_ctrl *ctrl = mux_ctrl(g);

    g->error = error;
    g->state = DMESH_MUX_ERROR;
    ctrl->error = (uint32_t)error;
    __dpa_thread_window_writeback();
}

/* Completions arrive in submission order: each pops the FIFO head. */
static void mux_drain(struct dpa_mux_ctx *m)
{
    doca_dpa_dev_completion_element_t comp;

    while (doca_dpa_dev_get_completion(m->producer_comp, &comp)) {
        int ok = doca_dpa_dev_get_completion_type(comp) == DOCA_DPA_DEV_COMP_SEND;
        struct dpa_mux_ring *g;
        uint64_t e;

        doca_dpa_dev_completion_ack(m->producer_comp, 1);
        doca_dpa_dev_completion_request_notification(m->producer_comp);
        if (m->fifo_head == m->fifo_tail) {
            m->bad_cqes++;
            continue;
        }
        e = m->fifo[m->fifo_head % DMESH_MUX_FIFO];
        m->fifo_head++;
        g = &m->ring[(uint32_t)(e >> 32) % DMESH_MUX_RINGS];
        g->inflight--;
        if (!ok) {
            m->err_cqes++;
            g->failed++;
            if (g->state == DMESH_MUX_ACTIVE)
                mux_fail(g, DMESH_MUX_ERR_COPY);
        } else if (g->state == DMESH_MUX_ACTIVE) {
            volatile struct dma_ring_ctrl *ctrl = mux_ctrl(g);

            g->complete++;
            g->completed_bytes += (uint32_t)e;
            ctrl->completed_bytes = g->completed_bytes;
            ctrl->consumer_head = g->complete;
            __dpa_thread_window_writeback();
        }
    }
}

/* Up to DMESH_MUX_QUANTUM copies from one ring. Returns -1 when the ARM has
 * no receive posted, which ends the pass for every ring. */
static int mux_service(struct dpa_mux_ctx *m, uint32_t r, uint64_t now)
{
    struct dpa_mux_ring *g = &m->ring[r];
    volatile struct dma_ring_ctrl *ctrl = mux_ctrl(g);
    uint64_t tail = ctrl->producer_tail;
    uint32_t n = 0;

    if (tail == g->submit)
        return 0;
    if (tail < g->submit || tail - g->complete > g->ring_size) {
        mux_fail(g, DMESH_MUX_ERR_TAIL);
        return 0;
    }
    while (n < DMESH_MUX_QUANTUM && g->submit != tail && m->fifo_tail - m->fifo_head < DMESH_MUX_FIFO) {
        const volatile struct dma_desc *d = mux_desc(g, g->submit);
        uint64_t addr = d->addr, size = d->size;
        uint32_t len, unread, free_space, remaining, wrap_padding;

        if (size == 0 || size > 8064u || size > g->buf_size || addr < g->host_base ||
            addr - g->host_base > g->host_len || size > g->host_len - (addr - g->host_base)) {
            mux_fail(g, DMESH_MUX_ERR_DESC);
            return 0;
        }
        len = (uint32_t)size;
        /* Staging stays owned until the reader releases it; keep one byte
         * free so equal positions only ever mean empty. */
        unread = (uint32_t)((g->pos + g->buf_size - g->rx_consumed_pos) % g->buf_size);
        free_space = (uint32_t)g->buf_size - unread;
        remaining = (uint32_t)(g->buf_size - g->pos);
        wrap_padding = len > remaining ? remaining : 0;
        if (free_space <= wrap_padding + len)
            return 0; /* this ring waits for its reader; the others go on */
        if (doca_dpa_dev_comch_producer_is_consumer_empty(m->producer, 1)) {
            m->credit_waits++;
            return -1;
        }
        if (wrap_padding != 0)
            g->pos = 0;
        struct dmesh_mux_msg msg = {
            .type = DMESH_MUX_MSG_DMA_DONE, .ring = (uint8_t)r, .slot = (uint16_t)g->slot,
            .generation = (uint32_t)g->generation, .pos = (uint32_t)g->pos, .length = len,
        };
        doca_dpa_dev_comch_producer_dma_copy(m->producer, 1,
            (doca_dpa_dev_mmap_t)g->dpu_mmap, g->staging_addr + g->pos,
            (doca_dpa_dev_mmap_t)g->host_mmap, addr, len,
            (uint8_t *)&msg, sizeof(msg), DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        m->fifo[m->fifo_tail % DMESH_MUX_FIFO] = (uint64_t)r << 32 | len;
        m->fifo_us[m->fifo_tail % DMESH_MUX_FIFO] = now;
        m->fifo_tail++;
        g->inflight++;
        g->submit++;
        g->pos += len;
        if (g->pos == g->buf_size)
            g->pos = 0;
        n++;
    }
    return (int)n;
}

__dpa_global__ void run_dma_mux(void)
{
    struct dpa_mux_ctx *m = (void *)doca_dpa_dev_thread_get_local_storage();
    uint64_t start, now;

    if (m == NULL) {
        DOCA_DPA_DEV_LOG_ERR("Mux thread has no local storage\n");
        doca_dpa_dev_thread_finish();
        return;
    }
    if (m->dpa_dev != 0)
        doca_dpa_dev_device_set((doca_dpa_dev_t)m->dpa_dev);

    doca_dpa_dev_completion_request_notification(m->producer_comp);
    start = __dpa_thread_time();
    for (;;) {
        int credit = 1;

        now = __dpa_thread_time();
        if (now - start > MUX_ACTIVATION_US)
            break;
        mux_drain(m);

        /* Stop: no new copies; finish once every copy has completed. */
        if (m->stop) {
            if (m->fifo_head == m->fifo_tail) {
                m->stopped = 1;
                __dpa_thread_window_writeback();
                doca_dpa_dev_thread_finish();
                return;
            }
            continue;
        }

        __dpa_thread_window_read_inv();
        for (uint32_t k = 0; k < DMESH_MUX_RINGS; k++) {
            uint32_t r = (uint32_t)((m->rr + k) % DMESH_MUX_RINGS);
            struct dpa_mux_ring *g = &m->ring[r];
            uint64_t cmd = g->cmd;

            if (cmd == DMESH_MUX_CMD_OPEN) {
                if (g->state == DMESH_MUX_IDLE)
                    g->state = DMESH_MUX_ACTIVE;
                if (credit && g->state == DMESH_MUX_ACTIVE && mux_service(m, r, now) < 0)
                    credit = 0; /* no more copies this pass; closes still answered */
            } else if (cmd == DMESH_MUX_CMD_CLOSE && g->state != DMESH_MUX_CLOSED && g->inflight == 0) {
                g->state = DMESH_MUX_CLOSED;
            }
        }
        m->rr = (m->rr + 1) % DMESH_MUX_RINGS;
        m->heartbeat++;
        m->oldest_age_us = m->fifo_head == m->fifo_tail ? 0 :
                           now - m->fifo_us[m->fifo_head % DMESH_MUX_FIFO];
    }
    __dpa_thread_window_writeback();
    /* Descriptor writers raise no completion, so poll again at once. */
    doca_dpa_dev_thread_retrigger();
}
