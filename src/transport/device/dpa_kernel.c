/* DPUMesh descriptor-ring DMA datapath and Comch initialization. */
#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

_Static_assert(sizeof(struct dpa_thread_ctx) == 112, "native DPA argument layout");

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

/* Starts a pod mux thread: it has no msgq from the ARM, so its first run is
 * this notification. */
__dpa_rpc__ uint64_t mux_kick_rpc(doca_dpa_dev_notification_completion_t notify)
{
    doca_dpa_dev_thread_notify(notify);
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
    if (arg->pooled)
        doca_dpa_dev_thread_reschedule();
    else
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

    /* A pooled thread keeps its comch across flows, and the next flow's first
     * request must wake it: ack what earlier flows left and re-arm. A per-flow
     * thread ends with its comch and has nothing to drain. */
    if (state->pooled && state->dpa_consumer_comp != 0) {
        doca_dpa_dev_comch_consumer_completion_element_t e;
        uint32_t n = 0;
        while (doca_dpa_dev_comch_consumer_get_completion(state->dpa_consumer_comp, &e)) {
            doca_dpa_dev_comch_consumer_completion_ack(state->dpa_consumer_comp, 1);
            ++n;
        }
        if (n != 0)
            doca_dpa_dev_comch_consumer_ack(state->dpa_consumer, n);
        doca_dpa_dev_comch_consumer_completion_request_notification(state->dpa_consumer_comp);
    }

    /* A stopped run (a pooled thread between flows) touches no mapping. */
    if (state->stopped) {
        if (state->pooled)
            doca_dpa_dev_thread_reschedule();
        else
            doca_dpa_dev_thread_finish();
        return;
    }

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
 * Pod-multiplexed forward DMA (dpa_common.h, DMESH_DPA_MUX). One thread
 * drains the forward rings of one host session through a single producer.
 * Every copy has its own CQE; the FIFO records the copies in submission
 * order, so a CQE is matched to its ring and a ring's custody ACK still
 * advances only after its copy landed. A pass reads the session's tail lines
 * once and visits only the rings whose tail moved. A ring that cannot
 * proceed (staging full, its worker's consumer out of credits, bad
 * descriptor) is skipped; only a full FIFO cuts the pass short.
 */
#define MUX_ACTIVATION_US 20000u /* bounded activation, like the native kernel's budget */

static volatile struct dma_ring_ctrl *mux_ctrl(const struct dpa_mux_ring_cfg *c)
{
    /* Resolved on every access: a pointer kept across rings of other mmaps
     * costs about twice as much per read (docs/2026-10-13_dpa-mux-probe.md). */
    return (void *)doca_dpa_dev_buf_get_external_ptr(
        doca_dpa_dev_buf_array_get_buf((doca_dpa_dev_buf_arr_t)c->buf_arr, 0));
}

static const volatile struct dma_desc *mux_desc(const struct dpa_mux_ring_cfg *c, uint64_t ticket)
{
    return (void *)doca_dpa_dev_buf_get_external_ptr(
        doca_dpa_dev_buf_array_get_buf((doca_dpa_dev_buf_arr_t)c->buf_arr,
                                       (uint32_t)(ticket & (c->ring_size - 1)) + 1));
}

static void mux_fail(struct dpa_mux_ctx *m, uint32_t r, uint64_t error)
{
    volatile struct dma_ring_ctrl *ctrl = mux_ctrl(&m->cfg[r]);

    m->st[r].error = error;
    m->st[r].state = DMESH_MUX_ERROR;
    m->open_mask &= ~(1ull << r);
    ctrl->error = (uint32_t)error;
    __dpa_thread_window_writeback();
}

/* Completions arrive in submission order: each pops the FIFO head. */
static void mux_drain(struct dpa_mux_ctx *m)
{
    doca_dpa_dev_completion_element_t comp;

    while (doca_dpa_dev_get_completion(m->producer_comp, &comp)) {
        int ok = doca_dpa_dev_get_completion_type(comp) == DOCA_DPA_DEV_COMP_SEND;
        struct dpa_mux_ring_state *s;
        uint32_t r;
        uint64_t e;

        doca_dpa_dev_completion_ack(m->producer_comp, 1);
        doca_dpa_dev_completion_request_notification(m->producer_comp);
        if (m->fifo_head == m->fifo_tail) {
            m->bad_cqes++;
            continue;
        }
        e = m->fifo[m->fifo_head % DMESH_MUX_FIFO];
        m->fifo_head++;
        r = (uint32_t)(e >> 32) % DMESH_MUX_RINGS;
        s = &m->st[r];
        s->inflight--;
        if (!ok) {
            m->err_cqes++;
            s->failed++;
            if (s->state == DMESH_MUX_ACTIVE)
                mux_fail(m, r, DMESH_MUX_ERR_COPY);
        } else if (s->state == DMESH_MUX_ACTIVE) {
            volatile struct dma_ring_ctrl *ctrl = mux_ctrl(&m->cfg[r]);

            s->complete++;
            s->completed_bytes += (uint32_t)e;
            ctrl->completed_bytes = s->completed_bytes;
            ctrl->consumer_head = s->complete;
            __dpa_thread_window_writeback();
        }
    }
}

/* The ARM changed a command: open, close or leave rings. */
static void mux_commands(struct dpa_mux_ctx *m)
{
    m->ctl_seen = m->ctl_seq;
    for (uint32_t r = 0; r < DMESH_MUX_RINGS; r++) {
        struct dpa_mux_ring_state *s = &m->st[r];
        uint64_t cmd = m->cfg[r].cmd, bit = 1ull << r;

        if (cmd == DMESH_MUX_CMD_OPEN && s->state == DMESH_MUX_IDLE) {
            s->state = DMESH_MUX_ACTIVE;
            m->open_mask |= bit;
        } else if (cmd == DMESH_MUX_CMD_CLOSE && s->state != DMESH_MUX_CLOSED) {
            m->open_mask &= ~bit;
            m->closing_mask |= bit;
        }
    }
}

/* Up to DMESH_MUX_QUANTUM copies from ring r up to tail. Returns -1 when the
 * FIFO is full, which ends the pass. */
static int mux_service(struct dpa_mux_ctx *m, uint32_t r, uint64_t tail, uint64_t now)
{
    const struct dpa_mux_ring_cfg *c = &m->cfg[r];
    struct dpa_mux_ring_state *s = &m->st[r];
    uint32_t n = 0;

    if (tail - s->complete > c->ring_size) {
        mux_fail(m, r, DMESH_MUX_ERR_TAIL);
        return 0;
    }
    while (n < DMESH_MUX_QUANTUM && s->submit != tail) {
        const volatile struct dma_desc *d;
        uint64_t addr, size;
        uint32_t len, unread, free_space, remaining, wrap_padding;

        if (m->fifo_tail - m->fifo_head >= DMESH_MUX_FIFO) {
            m->fifo_full++;
            return -1;
        }
        d = mux_desc(c, s->submit);
        addr = d->addr;
        size = d->size;
        if (size == 0 || size > 8064u || size > c->buf_size || addr < c->host_base ||
            addr - c->host_base > c->host_len || size > c->host_len - (addr - c->host_base)) {
            mux_fail(m, r, DMESH_MUX_ERR_DESC);
            return 0;
        }
        len = (uint32_t)size;
        /* Staging stays owned until the reader releases it; keep one byte
         * free so equal positions only ever mean empty. */
        unread = (uint32_t)((s->pos + c->buf_size - c->rx_consumed_pos) % c->buf_size);
        free_space = (uint32_t)c->buf_size - unread;
        remaining = (uint32_t)(c->buf_size - s->pos);
        wrap_padding = len > remaining ? remaining : 0;
        if (free_space <= wrap_padding + len)
            return 0; /* this ring waits for its reader; the others go on */
        if (doca_dpa_dev_comch_producer_is_consumer_empty(m->producer, (uint32_t)c->consumer)) {
            m->credit_waits++;
            return 0; /* its worker has no receive posted; other workers' rings go on */
        }
        if (wrap_padding != 0)
            s->pos = 0;
        struct dmesh_mux_msg msg = {
            .type = DMESH_MUX_MSG_DMA_DONE, .ring = (uint8_t)r, .slot = (uint16_t)c->slot,
            .generation = (uint32_t)c->generation, .pos = (uint32_t)s->pos, .length = len,
        };
        doca_dpa_dev_comch_producer_dma_copy(m->producer, (uint32_t)c->consumer,
            (doca_dpa_dev_mmap_t)c->dpu_mmap, c->staging_addr + s->pos,
            (doca_dpa_dev_mmap_t)c->host_mmap, addr, len,
            (uint8_t *)&msg, sizeof(msg), DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        m->fifo[m->fifo_tail % DMESH_MUX_FIFO] = (uint64_t)r << 32 | len;
        m->fifo_us[m->fifo_tail % DMESH_MUX_FIFO] = now;
        m->fifo_tail++;
        m->copies++;
        s->inflight++;
        s->submit++;
        s->pos += len;
        if (s->pos == c->buf_size)
            s->pos = 0;
        n++;
    }
    return (int)n;
}

/* One read per tail line holding an open ring, then a visit to each open ring
 * whose published tail differs from what it has submitted. */
static void mux_pass(struct dpa_mux_ctx *m, uint64_t now)
{
    uint32_t tails[DMESH_MUX_RINGS];

    __dpa_thread_window_read_inv();
    for (uint32_t l = 0; l < DMESH_TAILS_LINES; l++) {
        const volatile uint32_t *line;

        if (((m->open_mask >> (l * DMESH_TAILS_PER_LINE)) & ((1ull << DMESH_TAILS_PER_LINE) - 1)) == 0)
            continue;
        line = (void *)doca_dpa_dev_buf_get_external_ptr(
            doca_dpa_dev_buf_array_get_buf((doca_dpa_dev_buf_arr_t)m->tails, l));
        for (uint32_t i = 0; i < DMESH_TAILS_PER_LINE; i++)
            tails[l * DMESH_TAILS_PER_LINE + i] = line[i];
        m->tail_reads++;
    }
    /* The next pass starts after the last ring served, or at the ring the
     * full FIFO stopped, so every ring with work gets the first turn in
     * order, however many of the 32 are open. */
    uint32_t next = m->rr;
    for (uint32_t k = 0; k < DMESH_MUX_RINGS; k++) {
        uint32_t r = (uint32_t)((m->rr + k) % DMESH_MUX_RINGS);
        struct dpa_mux_ring_state *s = &m->st[r];
        uint32_t ahead;

        if (!(m->open_mask & (1ull << r)))
            continue;
        /* The table holds the tail's low 32 bits; a ring is never more than
         * ring_size ahead, so the difference is exact. */
        ahead = tails[r] - (uint32_t)s->submit;
        if (ahead == 0)
            continue;
        if (ahead > m->cfg[r].ring_size) {
            mux_fail(m, r, DMESH_MUX_ERR_TAIL);
            continue;
        }
        m->visits++;
        if (mux_service(m, r, s->submit + ahead, now) < 0) {
            next = r;
            break;
        }
        next = (r + 1) % DMESH_MUX_RINGS;
    }
    m->rr = next;
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

        if (m->ctl_seq != m->ctl_seen)
            mux_commands(m);
        if (m->closing_mask != 0)
            for (uint32_t r = 0; r < DMESH_MUX_RINGS; r++)
                if ((m->closing_mask & (1ull << r)) && m->st[r].inflight == 0) {
                    m->st[r].state = DMESH_MUX_CLOSED;
                    m->closing_mask &= ~(1ull << r);
                }
        if (m->open_mask != 0)
            mux_pass(m, now);
        m->heartbeat++;
        m->oldest_age_us = m->fifo_head == m->fifo_tail ? 0 :
                           now - m->fifo_us[m->fifo_head % DMESH_MUX_FIFO];
    }
    __dpa_thread_window_writeback();
    /* Descriptor writers raise no completion, so poll again at once. */
    doca_dpa_dev_thread_retrigger();
}
