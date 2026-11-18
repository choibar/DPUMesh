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
/* A waiting flow reads the host's cursor again after this long, doubling
 * while it finds no room (a read costs about 1 us of the thread). */
#define EC_CURSOR_MIN_US 4u
#define EC_CURSOR_MAX_US 64u

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
    q->pub_bytes = b->cum_bytes;
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

/* Publishes the flow's started batches in order: a batch with a copy waits
 * for its completion (a queue completes in submission order), the end of
 * stream for none. */
static void ec_flush(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint32_t conn)
{
    while (q->head != q->issued) {
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
    t->waiting &= ~(1ull << conn);
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
    q->ring = l->ring;
    q->end = q->gap = 0;
    q->gap_seq = 0;
    q->head = q->issued = q->tail = q->done = 0;
    q->pub_bytes = q->cseq = q->cbytes = 0;
    q->cur_us = 0;
    q->cur_gap = EC_CURSOR_MIN_US;
    q->failed = 0;
    t->waiting &= ~(1ull << conn);
    if (q->gen != gen || q->ring == 0) {
        q->gen = gen;
        ec_fail(t, q, conn);
    }
}

/* What the host consumed, as its cursor says, but no more than was published. */
static void ec_cursor(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint64_t now)
{
    __dpa_thread_window_read_inv();
    volatile uint64_t *c = (volatile uint64_t *)doca_dpa_dev_mmap_get_external_ptr(
        (doca_dpa_dev_mmap_t)q->win_mmap, q->win_addr + DMESH_EC_CURSOR_OFF);
    uint64_t magic = c[0], seq = c[1], bytes = c[2];

    q->cur_us = now;
    t->cursor_reads++;
    if (magic != DMESH_EC_FC_MAGIC)
        return;
    if (seq > q->head)
        seq = q->head;
    if (bytes > q->pub_bytes)
        bytes = q->pub_bytes;
    if (seq > q->cseq)
        q->cseq = seq;
    if (bytes > q->cbytes)
        q->cbytes = bytes;
}

/* The host has a free descriptor slot for batch seq and room for its bytes:
 * what it has not read, this batch included, and the unused end of the ring
 * before a wrap it has not read past (this batch's own, if it wraps). */
static int ec_room(const struct dmesh_ec_fq *q, uint64_t seq, uint64_t pos_len, uint64_t cum)
{
    uint32_t gap = 0;

    if ((uint32_t)pos_len == 0)
        return seq - q->cseq <= DMESH_EC_DESC_N - 1;
    if ((uint32_t)(pos_len >> 32) < q->end)
        gap = q->ring - q->end;
    else if (q->gap_seq != 0 && q->cseq + 1 < q->gap_seq)
        gap = q->gap;
    return seq - q->cseq <= DMESH_EC_DESC_N - 2 && cum - q->cbytes + gap <= q->ring;
}

/* Batch seq has room: a copy on the flow's queue, or the end of stream's
 * publish. */
static void ec_start(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint32_t conn, uint64_t seq, uint64_t pos_len,
                     uint64_t src)
{
    uint32_t pos = (uint32_t)(pos_len >> 32), len = (uint32_t)pos_len;

    q->issued = seq;
    if (len == 0) {
        ec_flush(t, q, conn);
        return;
    }
    if (pos < q->end) {
        q->gap = q->ring - q->end;
        q->gap_seq = seq;
    }
    q->end = pos + len;
    doca_dpa_dev_post_memcpy((doca_dpa_dev_async_ops_t)q->ops, (doca_dpa_dev_mmap_t)q->win_mmap,
                             q->win_addr + DMESH_EC_DATA_OFF + pos, (doca_dpa_dev_mmap_t)q->src_mmap, src, len,
                             DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
    t->copies++;
}

/* Starts, in order, the batches of a waiting flow the host has room for. */
static void ec_issue(struct dmesh_ec_tls *t, struct dmesh_ec_fq *q, uint32_t conn, uint64_t now)
{
    while (q->issued != q->tail) {
        const struct dmesh_ec_batch *b = &q->b[q->issued % DMESH_EC_FLOW_Q];
        uint64_t seq = q->issued + 1;

        if (!ec_room(q, seq, b->pos_len, b->cum_bytes)) {
            if (now - q->cur_us < q->cur_gap)
                return;
            ec_cursor(t, q, now);
            if (!ec_room(q, seq, b->pos_len, b->cum_bytes)) {
                q->cur_gap = q->cur_gap >= EC_CURSOR_MAX_US / 2 ? EC_CURSOR_MAX_US : q->cur_gap * 2;
                return;
            }
            q->cur_gap = EC_CURSOR_MIN_US;
        }
        ec_start(t, q, conn, seq, b->pos_len, q->src[q->issued % DMESH_EC_FLOW_Q]);
    }
    t->waiting &= ~(1ull << conn);
}

/* New entries: each goes to its flow. A flow with nothing waiting starts the
 * batch at once when the host has room; otherwise the batch waits. */
static void ec_take(struct dmesh_ec_tls *t, uint64_t tail, uint64_t now)
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
            q->head = q->issued = q->tail = q->done = 0;
            q->failed = 0;
            t->waiting &= ~(1ull << conn);
            continue;
        }
        if (gen != q->gen)
            ec_load(t, q, conn, gen);
        if (q->failed)
            continue;
        /* The worker numbers a flow's batches from 1, keeps fewer than
         * DMESH_EC_FLOW_Q unpublished and places them inside the ring. */
        if (seq != q->tail + 1 || q->tail - q->head >= DMESH_EC_FLOW_Q ||
            (pos_len >> 32) + (uint32_t)pos_len > q->ring) {
            ec_fail(t, q, conn);
            continue;
        }
        q->b[q->tail % DMESH_EC_FLOW_Q] = (struct dmesh_ec_batch){pos_len, cum};
        q->tail = seq;
        if (q->issued + 1 == seq && ec_room(q, seq, pos_len, cum)) {
            ec_start(t, q, conn, seq, pos_len, src);
            continue;
        }
        q->src[(seq - 1) % DMESH_EC_FLOW_Q] = src;
        if (!(t->waiting & 1ull << conn)) {
            t->waiting |= 1ull << conn;
            ec_issue(t, q, conn, now);
        }
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
        uint64_t now = __dpa_thread_time();
        __dpa_thread_window_read_inv();
        uint64_t tail = ec_ctl(t)->tail, head0 = t->head;
        ec_take(t, tail, now);
        if (t->head != head0) {
            /* Ring slots the ARM may reuse. */
            ec_ctl(t)->head = t->head;
            __dpa_thread_window_writeback();
        }
        ec_complete(t);
        /* Flows waiting for the host's room. */
        for (uint64_t w = t->waiting; w != 0; w &= w - 1) {
            uint32_t conn = (uint32_t)__builtin_ctzll(w);
            ec_issue(t, &t->f[conn], conn, now);
        }
        t->passes++;
        if (now - start > EC_ACTIVATION_US)
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
        s->cursor_reads = t->cursor_reads;
        __dpa_thread_window_writeback();
    }
    /* No completion wakes this thread: poll again at once. */
    doca_dpa_dev_thread_retrigger();
}
