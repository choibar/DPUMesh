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

#define EC_QUIESCE_NS 1000000000ull

struct dmesh_ec_worker {
    struct doca_dpa *dpa;
    struct doca_dpa_thread *thread;
    uint64_t tls;
    struct doca_dpa_completion *acomp;
    struct doca_dpa_async_ops *ops;
    struct doca_dpa_notification_completion *notify;
    struct doca_mmap *ctl_mmap;
    struct dmesh_ec_ctl *ctl;
    uint32_t ctl_h;
    uint64_t tail;                      /* entries posted (the ARM's copy of ctl->tail) */
};

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

doca_error_t dmesh_ec_init(struct objects *objs)
{
    struct dmesh_ec_worker *w;
    struct dmesh_ec_tls tl;
    doca_dpa_dev_async_ops_t ops_h;
    doca_dpa_dev_notification_completion_t notify_h;
    uint64_t rpc_ret = 0;
    const char *step = "";
    doca_error_t r;

    if (objs->ec != NULL)
        return DOCA_SUCCESS;
    if (objs->dpa_pool == NULL || objs->dpa_pool->dpa == NULL)
        return DOCA_ERROR_BAD_STATE;
    w = calloc(1, sizeof(*w));
    if (w == NULL)
        return DOCA_ERROR_NO_MEMORY;
    w->dpa = objs->dpa_pool->dpa;
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
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_create(w->dpa, 1024, &w->acomp)), "completion create");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_set_thread(w->acomp, w->thread)), "completion thread");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_start(w->acomp)), "completion start");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_async_ops_create(w->dpa, 256, (uint64_t)objs->worker_idx, &w->ops)), "async ops create");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_async_ops_attach(w->ops, w->acomp)), "async ops attach");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_async_ops_start(w->ops)), "async ops start");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_create(w->dpa, w->thread, &w->notify)), "notify create");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_start(w->notify)), "notify start");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_get_dpa_handle(w->notify, &notify_h)), "notify handle");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_async_ops_get_dpa_handle(w->ops, &ops_h)), "async ops handle");
    memset(&tl, 0, sizeof(tl));
    tl.ops = ops_h;
    EC_TRY(DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(w->acomp, &tl.acomp)), "completion handle");
    tl.wid = (uint64_t)objs->worker_idx + 1;
    tl.ctl_mmap = w->ctl_h;
    tl.ctl_addr = (uint64_t)(uintptr_t)w->ctl;
    EC_TRY(DMESH_DPA_CALL(doca_dpa_h2d_memcpy(w->dpa, w->tls, &tl, sizeof(tl))), "tls copy");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_thread_run(w->thread)), "thread run");
    EC_TRY(DMESH_DPA_CALL(doca_dpa_rpc(w->dpa, mux_kick_rpc, &rpc_ret, notify_h)), "first activation");
#undef EC_TRY
    objs->ec = w;
    DOCA_LOG_INFO("worker %d: reverse DPA thread running", objs->worker_idx);
    return DOCA_SUCCESS;
fail:
    DOCA_LOG_ERR("worker %d: reverse DPA thread: %s failed: %s", objs->worker_idx, step, doca_error_get_name(r));
    /* Left allocated: the worker fails its start and the process exits. */
    return r;
}

doca_error_t dmesh_ec_conn_setup(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    void *base = NULL;
    size_t len = 0;
    doca_error_t r;

    if (objs->ec == NULL || conn->pending_tails == NULL || conn->flow_id == 0 || conn->flow_id > 32)
        return DOCA_ERROR_BAD_STATE;
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
        return r;
    }
    conn->ec_se_addr = (uint64_t)(uintptr_t)base + DMESH_EC_SE_OFF(conn->flow_id);
    conn->ec = true;
    conn->ec_ready = false;
    conn->ec_issued_seq = conn->ec_issued_bytes = 0;
    conn->ec_quiesce_ns = 0;
    struct dmesh_ec_done *d = &objs->ec->ctl->done[conn - objs->conns];
    d->seq = 0;
    d->bytes = 0;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    return DOCA_SUCCESS;
}

doca_error_t dmesh_ec_conn_handles(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    doca_error_t r;

    if (conn->ec_ready)
        return DOCA_SUCCESS;
    if (conn->tx_staging_mmap == NULL || conn->rcvbuf.mmap == NULL)
        return DOCA_ERROR_BAD_STATE;
    r = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->tx_staging_mmap, objs->dev, &conn->ec_src_h));
    if (r == DOCA_SUCCESS)
        r = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->rcvbuf.mmap, objs->dev, &conn->ec_win_h));
    if (r != DOCA_SUCCESS) {
        DOCA_LOG_ERR("flow %u: DPA handles for the reverse thread: %s", conn->flow_id, doca_error_get_name(r));
        return r;
    }
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
    e->conn = (uint32_t)(conn - conn->objs->conns);
    e->src_mmap = conn->ec_src_h;
    e->win_mmap = conn->ec_win_h;
    e->se_mmap = conn->ec_se_h;
    e->seq = seq;
    e->pos_len = (uint64_t)pos << 32 | len;
    e->src_addr = src_addr;
    e->win_addr = (uint64_t)(uintptr_t)conn->rcvbuf.buf;
    e->se_addr = conn->ec_se_addr;
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
    struct dmesh_ec_done *d = &c->done[conn - conn->objs->conns];
    uint64_t seq = __atomic_load_n(&d->seq, __ATOMIC_ACQUIRE);
    if (seq <= conn->push_seq || seq > conn->ec_issued_seq)
        return 0;
    conn->push_seq = seq;
    conn->pushed_bytes = __atomic_load_n(&d->bytes, __ATOMIC_ACQUIRE);
    return 1;
}

doca_error_t dmesh_ec_quiesce_step(struct dmesh_conn *conn)
{
    if (!conn->ec || conn->ec_issued_seq == 0)
        return DOCA_SUCCESS;
    (void)dmesh_ec_reap(conn);
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
    if (conn->ec && conn->ec_issued_seq)
        DOCA_LOG_INFO("flow %u: %lu batches (%lu bytes) through the reverse DPA thread, %lu completed",
                      conn->flow_id, (unsigned long)conn->ec_issued_seq, (unsigned long)conn->ec_issued_bytes,
                      (unsigned long)conn->push_seq);
    if (conn->ec_se_mmap != NULL) {
        (void)doca_mmap_destroy(conn->ec_se_mmap);
        conn->ec_se_mmap = NULL;
    }
    conn->ec = false;
    conn->ec_ready = false;
    conn->ec_src_h = conn->ec_win_h = conn->ec_se_h = 0;
    conn->ec_se_addr = 0;
    conn->ec_issued_seq = conn->ec_issued_bytes = 0;
    conn->ec_quiesce_ns = 0;
}

#endif /* DOCA_ARCH_DPU */
