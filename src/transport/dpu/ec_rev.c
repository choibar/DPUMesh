/* DPU -> host through the worker's reverse DPA thread. See ec_rev.h and
 * common/dpa_common.h. */
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <doca_dpa.h>
#include <doca_log.h>
#include <doca_mmap.h>

#include "buffer.h"
#include "comch_common.h"
#include "dpa.h"
#include "dpa_common.h"
#include "dpa_runtime.h"
#include "ec_rev.h"
#include "object.h"

#ifdef DOCA_ARCH_DPU

DOCA_LOG_REGISTER(EC_REV);

extern doca_dpa_func_t run_ec_rev;
extern doca_dpa_func_t mux_kick_rpc;

_Static_assert(DMESH_EC_CONNS == DMESH_MAX_CONNECTIONS, "one completion line per connection slot");
_Static_assert(DMESH_EC_DESC_N == DMESH_PUSH_DESC_N && DMESH_EC_DATA_OFF == DMESH_PUSH_DATA_OFF &&
               DMESH_EC_DESC_SZ == sizeof(struct dmesh_push_desc) &&
               offsetof(struct dmesh_push_desc, pos) == 8 && offsetof(struct dmesh_push_desc, len) == 12,
               "the DPA writes the host's push layout");
_Static_assert(DMESH_EC_CURSOR_OFF == DMESH_PUSH_CURSOR_OFF && DMESH_EC_FC_MAGIC == DMESH_PUSH_FC_MAGIC &&
               offsetof(struct dmesh_push_cursor, consumed_seq) == 8 &&
               offsetof(struct dmesh_push_cursor, consumed_bytes) == 16,
               "the DPA reads the host's cursor");

#define EC_QUIESCE_NS 1000000000ull
#define EC_RETIRE_NS  100000000ull      /* the thread takes an entry within a pass */

/* A slot's queue on the thread. It serves the slot's flows one after another,
 * and is replaced when a flow ends with copies the thread still holds. */
struct dmesh_ec_queue {
    struct doca_dpa_async_ops *ops;
    doca_dpa_dev_async_ops_t h;
    uint32_t id;                        /* slot + DMESH_EC_CONNS * serial: its completions' user data */
    uint32_t serial, gen;
};

struct dmesh_ec_worker {
    struct doca_dpa *dpa;
    int worker_idx;
    struct doca_dpa_thread *thread;
    uint64_t tls;
    struct doca_dpa_completion *acomp;
    struct doca_dpa_notification_completion *notify;
    struct doca_mmap *ctl_mmap;
    struct dmesh_ec_ctl *ctl;
    uint32_t ctl_h;
    uint64_t tail;                      /* entries posted (the ARM's copy of ctl->tail) */
    struct dmesh_ec_queue q[DMESH_EC_CONNS];
};

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint32_t slot_of(const struct dmesh_conn *conn)
{
    return (uint32_t)(conn - conn->objs->conns);
}

doca_error_t dmesh_ec_init(struct objects *objs)
{
    struct dmesh_ec_worker *w;
    struct dmesh_ec_tls *tl = NULL;
    doca_dpa_dev_notification_completion_t notify_h;
    doca_dpa_dev_completion_t acomp_h;
    uint64_t rpc_ret = 0;
    const char *step = "";
    doca_error_t r;

    if (objs->ec != NULL)
        return DOCA_SUCCESS;
    if (objs->dpa_pool == NULL || objs->dpa_pool->dpa == NULL)
        return DOCA_ERROR_BAD_STATE;
    w = calloc(1, sizeof(*w));
    tl = aligned_alloc(64, sizeof(*tl));    /* the flows' state is laid out by cache line */
    if (tl != NULL)
        memset(tl, 0, sizeof(*tl));
    if (w == NULL || tl == NULL) {
        free(w);
        free(tl);
        return DOCA_ERROR_NO_MEMORY;
    }
    w->dpa = objs->dpa_pool->dpa;
    w->worker_idx = objs->worker_idx;
#define EC_TRY(call, name) do { step = name; if ((r = (call)) != DOCA_SUCCESS) goto fail; } while (0)
    EC_TRY(alloc_buffer_and_set_mmap(&w->ctl_mmap, objs->dev, (void **)&w->ctl, sizeof(struct dmesh_ec_ctl),
                                     DOCA_ACCESS_FLAG_PCI_READ_WRITE), "ctl memory");
    memset(w->ctl, 0, sizeof(*w->ctl));
    EC_TRY(DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(w->ctl_mmap, objs->dev, &w->ctl_h)), "ctl handle");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_mem_alloc(w->dpa, sizeof(struct dmesh_ec_tls), &w->tls)), "tls");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_thread_create(w->dpa, &w->thread)), "thread create");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_thread_set_func_arg(w->thread, run_ec_rev, 0)), "thread func");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_thread_set_local_storage(w->thread, w->tls)), "thread storage");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_thread_start(w->thread)), "thread start");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_create(w->dpa, DMESH_EC_CQ, &w->acomp)), "completion create");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_set_thread(w->acomp, w->thread)), "completion thread");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_start(w->acomp)), "completion start");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_create(w->dpa, w->thread, &w->notify)), "notify create");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_start(w->notify)), "notify start");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_get_dpa_handle(w->notify, &notify_h)), "notify handle");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(w->acomp, &acomp_h)), "completion handle");
    tl->acomp = acomp_h;
    tl->wid = (uint64_t)objs->worker_idx + 1;
    tl->ctl_mmap = w->ctl_h;
    tl->ctl_addr = (uint64_t)(uintptr_t)w->ctl;
    EC_TRY(DMESH_DPA_CALL(doca_dpa_h2d_memcpy(w->dpa, w->tls, tl, sizeof(*tl))), "tls copy");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_thread_run(w->thread)), "thread run");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_rpc(w->dpa, mux_kick_rpc, &rpc_ret, notify_h)), "first activation");
#undef EC_TRY
    free(tl);
    objs->ec = w;
    DOCA_LOG_INFO("worker %d: reverse DPA thread running", objs->worker_idx);
    return DOCA_SUCCESS;
fail:
    DOCA_LOG_ERR("worker %d: reverse DPA thread: %s failed: %s", objs->worker_idx, step, doca_error_get_name(r));
    free(tl);
    /* Left allocated: the worker fails its start and the process exits. */
    return r;
}

/* The slot's queue, created on its first flow and after a replacement. */
static doca_error_t ec_queue_get(struct dmesh_ec_worker *w, uint32_t slot)
{
    struct dmesh_ec_queue *q = &w->q[slot];
    struct doca_dpa_async_ops *ops = NULL;
    doca_dpa_dev_async_ops_t h;
    uint32_t id;
    doca_error_t r;

    if (q->ops != NULL)
        return DOCA_SUCCESS;
    id = slot + DMESH_EC_CONNS * (q->serial + 1);
    r = DMESH_DPA_CALL(doca_dpa_async_ops_create(w->dpa, DMESH_EC_FLOW_Q, id, &ops));
    if (r == DOCA_SUCCESS)
        r = DMESH_DPA_CALL(doca_dpa_async_ops_attach(ops, w->acomp));
    if (r == DOCA_SUCCESS)
        r = DMESH_DPA_CALL(doca_dpa_async_ops_start(ops));
    if (r == DOCA_SUCCESS)
        r = DMESH_DPA_CALL(doca_dpa_async_ops_get_dpa_handle(ops, &h));
    if (r != DOCA_SUCCESS) {
        DOCA_LOG_ERR("worker %d slot %u: reverse DPA queue: %s", w->worker_idx, slot, doca_error_get_name(r));
        if (ops != NULL)
            (void)DMESH_DPA_CALL(doca_dpa_async_ops_destroy(ops));
        return r;
    }
    q->ops = ops;
    q->h = h;
    q->id = id;
    q->serial++;
    return DOCA_SUCCESS;
}

/* Posts the entry that retires the slot's queue and waits for the thread to
 * take it: after that the thread posts nothing to the queue and drops its
 * completions. */
static bool ec_retire(struct dmesh_ec_worker *w, uint32_t slot)
{
    struct dmesh_ec_ctl *c = w->ctl;
    uint64_t deadline = now_ns() + EC_RETIRE_NS;

    while (w->tail - __atomic_load_n(&c->head, __ATOMIC_ACQUIRE) >= DMESH_EC_RING - 1)
        if (now_ns() > deadline)
            return false;
    memset(&c->ring[w->tail % DMESH_EC_RING], 0, sizeof(struct dmesh_ec_ent));
    c->ring[w->tail % DMESH_EC_RING].conn = slot;
    w->tail++;
    __atomic_store_n(&c->tail, w->tail, __ATOMIC_RELEASE);
    while (__atomic_load_n(&c->head, __ATOMIC_ACQUIRE) < w->tail)
        if (now_ns() > deadline)
            return false;
    return true;
}

/* The slot's flow ended with copies the thread holds: they may never complete
 * (the host process died), and nothing behind them on the queue would. The
 * next flow gets a new queue. Once the thread retired this one, destroying it
 * flushes the copies, and no completion of it reaches a flow. */
static void ec_queue_replace(struct dmesh_ec_worker *w, uint32_t slot)
{
    struct dmesh_ec_queue *q = &w->q[slot];
    doca_error_t r = DOCA_ERROR_BAD_STATE;
    bool retired;

    if (q->ops == NULL)
        return;
    retired = ec_retire(w, slot);
    if (retired) {
        r = DMESH_DPA_CALL(doca_dpa_async_ops_stop(q->ops));
        if (r == DOCA_SUCCESS)
            r = DMESH_DPA_CALL(doca_dpa_async_ops_destroy(q->ops));
    }
    if (r != DOCA_SUCCESS)
        DOCA_LOG_ERR("worker %d slot %u: reverse DPA queue %u left allocated (%s)", w->worker_idx, slot, q->id,
                     retired ? doca_error_get_name(r) : "the thread did not retire it");
    else
        DOCA_LOG_INFO("worker %d slot %u: reverse DPA queue %u replaced", w->worker_idx, slot, q->id);
    q->ops = NULL;
}

doca_error_t dmesh_ec_conn_setup(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    struct dmesh_ec_worker *w = objs->ec;
    void *base = NULL;
    size_t len = 0;
    doca_error_t r;

    if (w == NULL || conn->pending_tails == NULL || conn->flow_id == 0 || conn->flow_id > 32)
        return DOCA_ERROR_BAD_STATE;
    r = ec_queue_get(w, slot_of(conn));
    if (r != DOCA_SUCCESS)
        return r;
    r = doca_mmap_create_from_export(NULL, conn->pending_tails->desc, conn->pending_tails->desc_len, objs->dev,
                                     &conn->ec_se_mmap);
    if (r == DOCA_SUCCESS)
        r = doca_mmap_start(conn->ec_se_mmap);
    if (r == DOCA_SUCCESS)
        r = doca_mmap_get_memrange(conn->ec_se_mmap, &base, &len);
    if (r == DOCA_SUCCESS && len < DMESH_TAILS_ALLOC)
        r = DOCA_ERROR_INVALID_VALUE;   /* the page holds the slots' sync event words */
    if (r == DOCA_SUCCESS)
        r = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->ec_se_mmap, objs->dev, &conn->ec_se_h));
    if (r != DOCA_SUCCESS) {
        DOCA_LOG_ERR("flow %u: tail table page for the reverse DPA thread: %s", conn->flow_id,
                     doca_error_get_name(r));
        if (conn->ec_se_mmap != NULL) {
            (void)doca_mmap_destroy(conn->ec_se_mmap);
            conn->ec_se_mmap = NULL;
        }
        return r;
    }
    conn->ec_se_addr = (uint64_t)(uintptr_t)base + DMESH_EC_SE_OFF(conn->flow_id);
    conn->ec = true;
    conn->ec_ready = false;
    conn->ec_issued_seq = conn->ec_issued_bytes = 0;
    conn->ec_quiesce_ns = 0;
    /* A generation the slot's completion line has not carried. */
    struct dmesh_ec_queue *q = &w->q[slot_of(conn)];
    q->gen = (q->gen + 1) & DMESH_EC_GEN_MASK;
    if (q->gen == 0)
        q->gen = 1;
    conn->ec_gen = q->gen;
    return DOCA_SUCCESS;
}

doca_error_t dmesh_ec_conn_handles(struct dmesh_conn *conn, uint32_t ring)
{
    struct objects *objs = conn->objs;
    struct dmesh_ec_worker *w = objs->ec;
    uint32_t src_h, win_h;
    doca_error_t r;

    if (conn->ec_ready)
        return DOCA_SUCCESS;
    if (!conn->ec || w == NULL || conn->tx_staging_mmap == NULL || conn->rcvbuf.mmap == NULL)
        return DOCA_ERROR_BAD_STATE;
    r = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->tx_staging_mmap, objs->dev, &src_h));
    if (r == DOCA_SUCCESS)
        r = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->rcvbuf.mmap, objs->dev, &win_h));
    if (r != DOCA_SUCCESS) {
        DOCA_LOG_ERR("flow %u: DPA handles for the reverse thread: %s", conn->flow_id, doca_error_get_name(r));
        return r;
    }
    /* The flow's line, before its first entry (whose tail store releases it). */
    const struct dmesh_ec_queue *q = &w->q[slot_of(conn)];
    struct dmesh_ec_flow *l = &w->ctl->flow[slot_of(conn)];
    l->ops = q->h;
    l->gen = conn->ec_gen;
    l->id = q->id;
    l->src_mmap = src_h;
    l->win_mmap = win_h;
    l->se_mmap = conn->ec_se_h;
    l->ring = ring;
    l->win_addr = (uint64_t)(uintptr_t)conn->rcvbuf.buf;
    l->se_addr = conn->ec_se_addr;
    conn->ec_ready = true;
    return DOCA_SUCCESS;
}

struct dmesh_ec_ctl *dmesh_ec_ctl_of(struct objects *objs)
{
    return objs->ec != NULL ? objs->ec->ctl : NULL;
}

doca_error_t dmesh_ec_post(struct dmesh_conn *conn, uint64_t seq, uint32_t pos, uint32_t len, uint64_t src_addr,
                           uint64_t cum_bytes)
{
    struct dmesh_ec_worker *w = conn->objs->ec;
    struct dmesh_ec_ctl *c = w->ctl;
    if (w->tail - __atomic_load_n(&c->head, __ATOMIC_ACQUIRE) >= DMESH_EC_RING - 1)
        return DOCA_ERROR_AGAIN;
    struct dmesh_ec_ent *e = &c->ring[w->tail % DMESH_EC_RING];
    e->conn = slot_of(conn);
    e->gen = conn->ec_gen;
    e->seq = seq;
    e->pos_len = (uint64_t)pos << 32 | len;
    e->src_addr = src_addr;
    e->cum_bytes = cum_bytes;
    w->tail++;
    __atomic_store_n(&c->tail, w->tail, __ATOMIC_RELEASE);
    return DOCA_SUCCESS;
}

int dmesh_ec_reap(struct dmesh_conn *conn)
{
    struct dmesh_ec_ctl *c = dmesh_ec_ctl_of(conn->objs);
    if (c == NULL || !conn->ec)
        return 0;
    struct dmesh_ec_done *d = &c->done[slot_of(conn)];
    if (__atomic_load_n(&d->failed, __ATOMIC_ACQUIRE) == conn->ec_gen)
        return -1;
    uint64_t word = __atomic_load_n(&d->seq, __ATOMIC_ACQUIRE);
    uint64_t seq = word & ((1ull << DMESH_EC_GEN_SHIFT) - 1);
    if ((word >> DMESH_EC_GEN_SHIFT) != conn->ec_gen || seq <= conn->push_seq || seq > conn->ec_issued_seq)
        return 0;
    conn->push_seq = seq;
    conn->pushed_bytes = __atomic_load_n(&d->bytes, __ATOMIC_ACQUIRE);
    return 1;
}

doca_error_t dmesh_ec_quiesce_step(struct dmesh_conn *conn)
{
    if (!conn->ec || conn->ec_issued_seq == 0)
        return DOCA_SUCCESS;
    if (dmesh_ec_reap(conn) < 0)
        return DOCA_SUCCESS;            /* failed: nothing more completes */
    if (conn->push_seq == conn->ec_issued_seq)
        return DOCA_SUCCESS;
    if (conn->ec_quiesce_ns == 0)
        conn->ec_quiesce_ns = now_ns();
    if (now_ns() - conn->ec_quiesce_ns < EC_QUIESCE_NS)
        return DOCA_ERROR_AGAIN;
    DOCA_LOG_ERR("flow %u: reverse DPA thread still holds batches %lu..%lu after 1 s; releasing", conn->flow_id,
                 (unsigned long)conn->push_seq + 1, (unsigned long)conn->ec_issued_seq);
    return DOCA_SUCCESS;
}

void dmesh_ec_conn_reset(struct dmesh_conn *conn)
{
    struct dmesh_ec_worker *w = conn->objs->ec;

    if (conn->ec && w != NULL) {
        if (conn->ec_issued_seq != 0)
            DOCA_LOG_INFO("flow %u: %lu batches (%lu bytes) through the reverse DPA thread, %lu completed; "
                          "the thread read host cursors %lu times so far",
                          conn->flow_id, (unsigned long)conn->ec_issued_seq, (unsigned long)conn->ec_issued_bytes,
                          (unsigned long)conn->push_seq, (unsigned long)w->ctl->cursor_reads);
        if (dmesh_ec_reap(conn) < 0 || conn->push_seq != conn->ec_issued_seq)
            ec_queue_replace(w, slot_of(conn));
    }
    if (conn->ec_se_mmap != NULL) {
        (void)doca_mmap_destroy(conn->ec_se_mmap);
        conn->ec_se_mmap = NULL;
    }
    conn->ec = false;
    conn->ec_ready = false;
    conn->ec_se_h = 0;
    conn->ec_se_addr = 0;
    conn->ec_gen = 0;
    conn->ec_issued_seq = conn->ec_issued_bytes = 0;
    conn->ec_quiesce_ns = 0;
}

#endif /* DOCA_ARCH_DPU */
