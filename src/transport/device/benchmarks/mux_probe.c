/* Measurements for DPA flow multiplexing (common/dpa_probe.h); not part of the native loop. */
#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"
#include "dpa_probe.h"

static const uint32_t r_steps[MUX_PROBE_R_STEPS] = {1, 2, 4, 8, 16};

/* Ring layout as the native kernel reads it: element 0 is the control block,
 * elements 1..N the descriptors. */
static volatile uint64_t *ring_tail(uint64_t buf_arr)
{
    doca_dpa_dev_buf_t buf = doca_dpa_dev_buf_array_get_buf((doca_dpa_dev_buf_arr_t)buf_arr, 0);
    volatile struct dma_ring_ctrl *ctrl = (void *)doca_dpa_dev_buf_get_external_ptr(buf);
    return &ctrl->producer_tail;
}

static volatile uint64_t *ring_desc(uint64_t buf_arr, uint32_t i)
{
    doca_dpa_dev_buf_t buf = doca_dpa_dev_buf_array_get_buf((doca_dpa_dev_buf_arr_t)buf_arr, 1 + i);
    return (void *)doca_dpa_dev_buf_get_external_ptr(buf);
}

/* Cycles per microsecond, so the ARM side can report passes in time. */
static void calibrate(struct mux_probe_state *s)
{
    uint64_t t0 = __dpa_thread_time(), c0 = __dpa_thread_cycles(), t1;

    do {
        t1 = __dpa_thread_time();
    } while (t1 - t0 < 2000);
    s->cal_cycles = __dpa_thread_cycles() - c0;
    s->cal_us = t1 - t0;
}

/* Up to MUX_PROBE_CHUNK passes of the current (variant, R); configurations
 * that need more rings than were given stay at zero passes. */
static void poll_chunk(struct mux_probe_state *s)
{
    const uint64_t total = MUX_PROBE_VARIANTS * MUX_PROBE_R_STEPS;
    volatile uint64_t *ptr[MUX_PROBE_MAX_RINGS];
    struct mux_probe_stat *st;
    uint32_t variant = 0, R = 0, r, i, n;
    uint64_t sink = 0;

    while (s->cursor < total) {
        variant = (uint32_t)(s->cursor / MUX_PROBE_R_STEPS);
        R = r_steps[s->cursor % MUX_PROBE_R_STEPS];
        if (s->num_rings != 0 && (variant == MUX_PROBE_SAME_RING || R <= s->num_rings))
            break;
        s->cursor++;
    }
    if (s->cursor >= total)
        return;
    st = &s->stat[variant][s->cursor % MUX_PROBE_R_STEPS];

    for (r = 0; r < R; r++)
        ptr[r] = variant == MUX_PROBE_SAME_RING ? ring_desc(s->ring_buf_arr[0], r)
                                                : ring_tail(s->ring_buf_arr[r]);
    if (st->passes == 0)
        st->min = UINT64_MAX;
    n = MUX_PROBE_CHUNK;
    if (st->passes + n > s->passes)
        n = (uint32_t)(s->passes - st->passes);

    for (i = 0; i < n; i++) {
        uint64_t c0 = __dpa_thread_cycles(), d;

        if (variant == MUX_PROBE_LOOKUP) {
            __dpa_thread_window_read_inv();
            for (r = 0; r < R; r++)
                sink += *ring_tail(s->ring_buf_arr[r]);
        } else if (variant == MUX_PROBE_INV_EACH) {
            for (r = 0; r < R; r++) {
                __dpa_thread_window_read_inv();
                sink += *ptr[r];
            }
        } else {
            __dpa_thread_window_read_inv();
            for (r = 0; r < R; r++)
                sink += *ptr[r];
        }
        d = __dpa_thread_cycles() - c0;
        if (d < st->min)
            st->min = d;
        if (d > st->max)
            st->max = d;
        st->sum += d;
    }
    st->passes += n;
    s->sink += sink;
    if (st->passes >= s->passes)
        s->cursor++;
}

/* Up to MUX_PROBE_SLICE serialized copies, alternating between the two
 * sources over this thread's single producer. Each copy waits for its CQE, so
 * an error is attributed to the source that caused it, and the copies after
 * it show whether the shared producer still works. */
static void fault_slice(struct mux_probe_state *s)
{
    doca_dpa_dev_comch_producer_t producer = s->dpa_producer;
    doca_dpa_dev_completion_t comp_q = s->dpa_producer_comp;
    struct comch_dma_comp_msg msg = {
        .type = COMCH_MSG_TYPE_DMA_COMPLETED, .length = (uint32_t)s->copy_len, .count = 1,
    };
    doca_dpa_dev_completion_element_t comp;
    uint32_t k, i;
    uint64_t t0, type;

    doca_dpa_dev_completion_request_notification(comp_q);
    for (k = 0; k < MUX_PROBE_SLICE && !s->stop; k++) {
        const int post = s->stalled_src != 0;

        if (post && s->post_ok + s->post_err + s->post_timeouts >= MUX_PROBE_POST_STALL) {
            s->done = 1;
            return;
        }
        /* After a stall only the other source is copied. */
        i = post ? (uint32_t)(2 - s->stalled_src) : (uint32_t)(s->iter & 1);

        /* Each copy delivers one message to the ARM consumer; wait for a
         * posted receive, and hand the EU back if the ARM side is behind. */
        t0 = __dpa_thread_time();
        while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, 1)) {
            if (__dpa_thread_time() - t0 > s->timeout_us) {
                s->credit_waits++;
                return;
            }
        }

        msg.pos = (uint32_t)(i * s->copy_len);
        doca_dpa_dev_comch_producer_dma_copy(producer, 1,
            (doca_dpa_dev_mmap_t)s->dst_mmap, s->dst_addr + i * s->copy_len,
            (doca_dpa_dev_mmap_t)s->src_mmap[i], s->src_addr[i], (uint32_t)s->copy_len,
            (uint8_t *)&msg, sizeof(msg), DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

        t0 = __dpa_thread_time();
        while (!doca_dpa_dev_get_completion(comp_q, &comp)) {
            if (__dpa_thread_time() - t0 > s->timeout_us)
                break;
        }
        if (__dpa_thread_time() - t0 > s->timeout_us) {
            /* No CQE: keep going with the other source; the ARM side counts
             * which copies' messages ever arrive. */
            if (post) {
                s->post_timeouts++;
            } else {
                s->stalled_src = i + 1;
                s->stall_iter = s->iter;
            }
            s->iter++;
            continue;
        }
        type = (uint64_t)doca_dpa_dev_get_completion_type(comp);
        doca_dpa_dev_completion_ack(comp_q, 1);
        doca_dpa_dev_completion_request_notification(comp_q);
        if (post) {
            if (type == DOCA_DPA_DEV_COMP_SEND)
                s->post_ok++;
            else {
                s->post_err++;
                s->post_last_err_type = type;
            }
        } else if (type == DOCA_DPA_DEV_COMP_SEND) {
            s->ok[i]++;
        } else {
            if (s->err[i] == 0)
                s->first_err_iter[i] = s->iter;
            s->err[i]++;
            s->last_err_type[i] = type;
        }
        s->iter++;

        t0 = __dpa_thread_time();
        while (__dpa_thread_time() - t0 < s->gap_us) {
        }
    }
}

/* Bounded work per activation, then retrigger, like the native kernel. The
 * ARM side reads the state back with d2h_memcpy and ends a run with stop. */
__dpa_global__ void run_mux_probe(void)
{
    struct mux_probe_state *s = (void *)doca_dpa_dev_thread_get_local_storage();

    if (s == NULL) {
        DOCA_DPA_DEV_LOG_ERR("Probe thread has no local storage\n");
        doca_dpa_dev_thread_finish();
        return;
    }
    if (s->dpa_dev != 0)
        doca_dpa_dev_device_set((doca_dpa_dev_t)s->dpa_dev);

    if (!s->stop && !s->done) {
        if (s->mode == MUX_PROBE_POLL) {
            if (s->cal_us == 0)
                calibrate(s);
            poll_chunk(s);
            if (s->cursor >= MUX_PROBE_VARIANTS * MUX_PROBE_R_STEPS)
                s->done = 1;
        } else if (s->mode == MUX_PROBE_FAULT) {
            fault_slice(s);
        } else {
            s->done = 1;
        }
    }
    __dpa_thread_window_writeback();
    if (s->stop || s->done) {
        s->done = 1;
        __dpa_thread_window_writeback();
        doca_dpa_dev_thread_finish();
        return;
    }
    doca_dpa_dev_thread_retrigger();
}
