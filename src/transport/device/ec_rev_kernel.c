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
static void ec_publish(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint32_t conn, const struct dmesh_ec_batch *b)
{
    uint64_t seq = q->head;
    uint32_t pos = (uint32_t)(b->pos_len >> 32), len = (uint32_t)b->pos_len;
    uint64_t slot = q->win_addr + (seq % DMESH_EC_DESC_N) * DMESH_EC_DESC_SZ;
    volatile uint32_t *pl = (volatile uint32_t *)doca_dpa_dev_mmap_get_external_ptr(
        (doca_dpa_dev_mmap_t)q->win_mmap, slot + 8);
    pl[0] = pos;
    pl[1] = len;
    __dpa_thread_window_writeback();
    volatile uint64_t *sq = (volatile uint64_t *)doca_dpa_dev_mmap_get_external_ptr(
        (doca_dpa_dev_mmap_t)q->win_mmap, slot);
    *sq = seq;
    __dpa_thread_window_writeback();
    t->publishes++;
    /* Unique per store: flows share a slot word over time, and two workers'
     * threads may serve one slot. */
    volatile uint64_t *se = (volatile uint64_t *)doca_dpa_dev_mmap_get_external_ptr(
        (doca_dpa_dev_mmap_t)q->se_mmap, q->se_addr);
    *se = t->wid << 48 | t->publishes;
    __dpa_thread_window_writeback();
    volatile struct dmesh_ec_done *d = &ec_ctl(t)->done[conn];
    d->bytes = b->cum_bytes;
    __dpa_thread_window_writeback();
    d->seq = (uint64_t)q->gen << DMESH_EC_GEN_SHIFT | seq;
    __dpa_thread_window_writeback();
}

/* Publishes the flow's ready batches in order: a batch with a copy waits for
 * its completion (a queue completes in submission order), the end of stream
 * for none. */
static void ec_flush(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint32_t conn)
{
    while (q->head != q->tail) {
        const struct dmesh_ec_batch *b = &q->b[q->head % DMESH_EC_FLOW_Q];
        if ((uint32_t)b->pos_len != 0) {
            if (q->done == 0)
                return;
            q->done--;
        }
        q->head++;
        ec_publish(t, q, conn, b);
    }
}

/* The flow stops here: the worker reads `failed` and closes it. */
static void ec_fail(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint32_t conn)
{
    q->failed = 1;
    ec_ctl(t)->done[conn].failed = q->gen;
    __dpa_thread_window_writeback();
}

/* The slot's first entry of a new flow: take the flow's line and drop the last
 * flow's batches. That flow's queue's late completions no longer match the id. */
static void ec_load(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint32_t conn, uint32_t gen)
{
    volatile struct dmesh_ec_flow *l = &ec_ctl(t)->flow[conn];
    q->ops = l->ops;
    q->gen = l->gen;
    q->id = l->id;
    q->src_mmap = l->src_mmap;
    q->win_mmap = l->win_mmap;
    q->se_mmap = l->se_mmap;
    q->win_addr = l->win_addr;
    q->se_addr = l->se_addr;
    q->head = q->tail = q->done = 0;
    q->failed = 0;
    if (q->gen != gen) {
        q->gen = gen;
        ec_fail(t, q, conn);
    }
}

/* New entries: each goes to its flow, and its copy starts on the flow's queue. */
static void ec_take(struct dmesh_ec_tls *t, uint64_t tail)
{
    while (t->head != tail) {
        volatile struct dmesh_ec_ent *e = &ec_ctl(t)->ring[t->head % DMESH_EC_RING];
        uint32_t conn = e->conn % DMESH_EC_CONNS, gen = e->gen;
        uint64_t seq = e->seq, pos_len = e->pos_len, src = e->src_addr, cum = e->cum_bytes;
        struct dmesh_ec_fq *q = &t->f[conn];

        t->head++;
        if (seq == 0) {
            /* The worker retires the slot's queue: forget the flow. */
            q->gen = q->id = 0;
            q->head = q->tail = q->done = 0;
            q->failed = 0;
            continue;
        }
        if (gen != q->gen)
            ec_load(t, q, conn, gen);
        if (q->failed)
            continue;
        /* The worker numbers a flow's batches from 1 and keeps fewer than
         * DMESH_EC_FLOW_Q in flight. */
        if (seq != q->tail + 1 || q->tail - q->head >= DMESH_EC_FLOW_Q) {
            ec_fail(t, q, conn);
            continue;
        }
        q->b[q->tail % DMESH_EC_FLOW_Q] = (struct dmesh_ec_batch){pos_len, cum};
        q->tail++;
        if ((uint32_t)pos_len == 0) {
            ec_flush(t, q, conn);
            continue;
        }
        doca_dpa_dev_post_memcpy((doca_dpa_dev_async_ops_t)q->ops, (doca_dpa_dev_mmap_t)q->win_mmap,
                                 q->win_addr + DMESH_EC_DATA_OFF + (pos_len >> 32),
                                 (doca_dpa_dev_mmap_t)q->src_mmap, src, (uint32_t)pos_len,
                                 DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        t->copies++;
    }
}

/* Completions of every queue: each names its flow's queue. */
static void ec_complete(struct dmesh_ec_tls *t)
{
    doca_dpa_dev_completion_element_t ce;

    while (doca_dpa_dev_get_completion((doca_dpa_dev_completion_t)t->acomp, &ce)) {
        uint32_t id = doca_dpa_dev_get_completion_user_data(ce);
        int ok = doca_dpa_dev_get_completion_type(ce) == DOCA_DPA_DEV_COMP_SEND;
        uint32_t conn = id % DMESH_EC_CONNS;
        struct dmesh_ec_fq *q = &t->f[conn];

        doca_dpa_dev_completion_ack((doca_dpa_dev_completion_t)t->acomp, 1);
        if (id != q->id || q->failed) {
            t->stale_cqes++;
            continue;
        }
        if (!ok) {
            t->err_cqes++;
            ec_fail(t, q, conn);
            continue;
        }
        q->done++;
        ec_flush(t, q, conn);
    }
}

__dpa_global__ void run_ec_rev(void)
{
    struct dmesh_ec_tls *t = (void *)doca_dpa_dev_thread_get_local_storage();
    uint64_t start = __dpa_thread_time();

    t->activations++;
    for (;;) {
        __dpa_thread_window_read_inv();
        uint64_t tail = ec_ctl(t)->tail, head0 = t->head;
        ec_take(t, tail);
        if (t->head != head0) {
            /* Ring slots the ARM may reuse. */
            ec_ctl(t)->head = t->head;
            __dpa_thread_window_writeback();
        }
        ec_complete(t);
        t->passes++;
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
        s->stale_cqes = t->stale_cqes;
        s->err_cqes = t->err_cqes;
        __dpa_thread_window_writeback();
    }
    /* No completion wakes this thread: poll again at once. */
    doca_dpa_dev_thread_retrigger();
}
