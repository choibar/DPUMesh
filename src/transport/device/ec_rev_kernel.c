/* DPU -> host through the worker's reverse DPA thread: DPU-side device code. */
#include "doca_dpa_dev.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"

/*
 * Layout and protocol in common/dpa_common.h (struct dmesh_ec_*).
 *
 * Every access to ARM or host memory takes a fresh external pointer: a pointer
 * kept across another mmap's pointer can stall the thread.
 */
#define EC_ACTIVATION_US 2000u

static inline volatile struct dmesh_ec_ctl *ec_ctl(struct dmesh_ec_tls *t)
{
    return (volatile struct dmesh_ec_ctl *)doca_dpa_dev_mmap_get_external_ptr((doca_dpa_dev_mmap_t)t->ctl_mmap,
                                                                             t->ctl_addr);
}

/* The batch's copy completed (or it has none): descriptor, wake, completion. */
static void ec_publish(struct dmesh_ec_tls *t, const struct dmesh_ec_ent *e)
{
    uint32_t pos = (uint32_t)(e->pos_len >> 32), len = (uint32_t)e->pos_len;
    uint64_t slot = e->win_addr + (e->seq % DMESH_EC_DESC_N) * DMESH_EC_DESC_SZ;
    volatile uint32_t *pl = (volatile uint32_t *)doca_dpa_dev_mmap_get_external_ptr(
        (doca_dpa_dev_mmap_t)e->win_mmap, slot + 8);
    pl[0] = pos;
    pl[1] = len;
    __dpa_thread_window_writeback();
    volatile uint64_t *sq = (volatile uint64_t *)doca_dpa_dev_mmap_get_external_ptr(
        (doca_dpa_dev_mmap_t)e->win_mmap, slot);
    *sq = e->seq;
    __dpa_thread_window_writeback();
    t->publishes++;
    /* Unique per store: flows share a slot word over time, and two workers'
     * threads may serve one slot. */
    volatile uint64_t *se = (volatile uint64_t *)doca_dpa_dev_mmap_get_external_ptr(
        (doca_dpa_dev_mmap_t)e->se_mmap, e->se_addr);
    *se = t->wid << 48 | t->publishes;
    __dpa_thread_window_writeback();
    volatile struct dmesh_ec_done *d = &ec_ctl(t)->done[e->conn % DMESH_EC_CONNS];
    d->bytes = e->cum_bytes;
    __dpa_thread_window_writeback();
    d->seq = e->seq;
    __dpa_thread_window_writeback();
}

__dpa_global__ void run_ec_rev(void)
{
    struct dmesh_ec_tls *t = (void *)doca_dpa_dev_thread_get_local_storage();
    doca_dpa_dev_completion_element_t ce;
    uint64_t start = __dpa_thread_time();

    t->activations++;
    for (;;) {
        __dpa_thread_window_read_inv();
        volatile struct dmesh_ec_ctl *c = ec_ctl(t);
        uint64_t tail = c->tail, stop = c->stop, head0 = t->head;
        /* New batches: start each copy, in ring order. */
        while (t->head != tail && t->ff_tail - t->ff_head < DMESH_EC_FIFO) {
            struct dmesh_ec_ent *f = &t->ff[t->ff_tail % DMESH_EC_FIFO];
            volatile struct dmesh_ec_ent *e = &ec_ctl(t)->ring[t->head % DMESH_EC_RING];
            f->conn = e->conn;
            f->src_mmap = e->src_mmap;
            f->win_mmap = e->win_mmap;
            f->se_mmap = e->se_mmap;
            f->seq = e->seq;
            f->pos_len = e->pos_len;
            f->src_addr = e->src_addr;
            f->win_addr = e->win_addr;
            f->se_addr = e->se_addr;
            f->cum_bytes = e->cum_bytes;
            t->head++;
            uint32_t len = (uint32_t)f->pos_len;
            if (len != 0) {
                doca_dpa_dev_post_memcpy((doca_dpa_dev_async_ops_t)t->ops, (doca_dpa_dev_mmap_t)f->win_mmap,
                                         f->win_addr + DMESH_EC_DATA_OFF + (f->pos_len >> 32),
                                         (doca_dpa_dev_mmap_t)f->src_mmap, f->src_addr, len,
                                         DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
                t->copies++;
            }
            t->ff_tail++;
        }
        if (t->head != head0) {
            /* Ring slots the ARM may reuse. */
            ec_ctl(t)->head = t->head;
            __dpa_thread_window_writeback();
        }
        /* Publish in order: a batch with a copy waits for its completion
         * (one queue completes in submission order), the end of stream none. */
        while (t->ff_head != t->ff_tail) {
            struct dmesh_ec_ent *f = &t->ff[t->ff_head % DMESH_EC_FIFO];
            if ((uint32_t)f->pos_len != 0) {
                if (!doca_dpa_dev_get_completion((doca_dpa_dev_completion_t)t->acomp, &ce))
                    break;
                doca_dpa_dev_completion_ack((doca_dpa_dev_completion_t)t->acomp, 1);
            }
            ec_publish(t, f);
            t->ff_head++;
        }
        t->passes++;
        if (stop && t->ff_head == t->ff_tail) {
            volatile struct dmesh_ec_ctl *s = ec_ctl(t);
            s->head = t->head;
            s->stopped = 1;
            __dpa_thread_window_writeback();
            doca_dpa_dev_thread_finish();
            return;
        }
        if (__dpa_thread_time() - start > EC_ACTIVATION_US)
            break;
    }
    {
        volatile struct dmesh_ec_ctl *s = ec_ctl(t);
        s->head = t->head;
        s->copies = t->copies;
        s->publishes = t->publishes;
        s->passes = t->passes;
        s->activations = t->activations;
        __dpa_thread_window_writeback();
    }
    /* No completion wakes this thread: poll again at once. */
    doca_dpa_dev_thread_retrigger();
}
