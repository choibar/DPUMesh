#include "../common/dpa_runtime.h"
/* ARM setup / descriptor publication only. NIC WQEs, CQEs, IMM RQ repost
 * and (Sync mode only) event reads belong exclusively to the DPA engine. */
#define _POSIX_C_SOURCE 200809L
#include "peer_write_sync_dpa.h"
#include "peer_write_sync_dpa_shared.h"
#include "peer_write_completion.h"
#include "peer_sync_event.h"
#include "peer_sync_native.h"
#include <pthread.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>
#include <doca_mmap.h>
#include <doca_version.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

_Static_assert(DOCA_DPA_COMPLETION_LOG_MAX_USER_DATA >= 24,
               "async wait identity requires 24-bit completion user data");

struct peer_wsd_qp {
    struct peer_wsd *ctx;
    struct peer_sync_native *native;
    struct doca_mmap *remote_map;
    struct doca_dpa_async_ops *wait_ops[2];
    uint8_t wait_started[2];
    uint32_t qpn, generation;
    unsigned index;
    uint64_t tail;
    uint64_t fence_report_ns;
    struct peer_write_completion data_completion;
    uint64_t pending[WSD_CONTROLS], wrid[WSD_CONTROLS];
    uint16_t pending_slots[WSD_CONTROLS], pending_count;
    /* Worker-owned intrusive list; MsgQ callbacks never mutate QP state. */
    struct peer_wsd_qp *completion_prev, *completion_next;
    uint8_t installed, active, removed, error_reported, completion_queued;
};

struct peer_wsd {
    struct doca_dpa *base, *extended;
    struct doca_dev *sf;
    struct wsd_source *sources;
    struct peer_wsd *registry_next;
    uint32_t cookie;

    struct {
        uint32_t generation;
        uint64_t tail, head;
    } cache[WSD_LANES];
    struct doca_dpa_completion *cq;
    struct doca_dpa_completion *event_cq;
    struct doca_mmap *map;
    struct doca_buf_arr *array;
    struct wsd_shared *shared;
    struct peer_wsd_qp *qp[WSD_LANES];
    struct peer_wsd_qp *completion_first, *completion_last;
    struct peer_dpa_runtime runtime;
    uint64_t device_state, tail, data_tail, id, wait_epoch;
    uint64_t fence_stats_period_ns;
    unsigned eu;
    uint32_t wait_serial;
    int attached, paused, immediate, event_wait;
};

struct wsd_source {
    struct wsd_source *next;
    struct doca_mmap *map;
    uint32_t key, handle;
    uint64_t address;
    size_t bytes;
};

static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;
static struct peer_wsd *registry;
static uint32_t next_cookie;

int peer_wsd_arm_worker(unsigned worker)
{
    int rc = 0;

    pthread_mutex_lock(&registry_lock);
    for (struct peer_wsd *t = registry; t; t = t->registry_next) {
        if (t->runtime.owner_worker != worker || !t->shared || !t->attached)
            continue;

        uint64_t seen = __atomic_load_n(&t->shared->publication.sequence, __ATOMIC_ACQUIRE);
        if (seen == UINT64_MAX || t->wait_epoch == UINT64_MAX) {
            rc = -1;
            break;
        }

        __atomic_store_n(&t->shared->wait.seen, seen, __ATOMIC_RELAXED);
        __atomic_store_n(&t->shared->wait.epoch, ++t->wait_epoch, __ATOMIC_RELEASE);
    }

    /* Complete the request stores before the caller's final status scan.
     * DPA reads ARM's line through its invalidate on every bounded pass. */
    __atomic_thread_fence(__ATOMIC_SEQ_CST);

    if (rc)
        for (struct peer_wsd *t = registry; t; t = t->registry_next)
            if (t->runtime.owner_worker == worker && t->shared)
                __atomic_store_n(&t->shared->wait.epoch, 0, __ATOMIC_RELEASE);

    pthread_mutex_unlock(&registry_lock);

    return rc;
}

void peer_wsd_disarm_worker(unsigned worker)
{
    pthread_mutex_lock(&registry_lock);
    for (struct peer_wsd *t = registry; t; t = t->registry_next)
        if (t->runtime.owner_worker == worker && t->shared)
            __atomic_store_n(&t->shared->wait.epoch, 0, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&registry_lock);
}

/* MsgQ PE owners may differ from the data owner. Copy only counters under the
 * registry lock: no transport progress or QP dereference on that thread. */
void peer_wsd_counter_message(const struct dpa_rdma_counter_msg *m)
{
    if (!m || m->lane >= WSD_LANES)
        return;

    pthread_mutex_lock(&registry_lock);
    for (struct peer_wsd *t = registry; t; t = t->registry_next) {
        if (t->cookie != m->cookie || t->eu != m->eu_index ||
            t->runtime.owner_worker != m->worker || t->cache[m->lane].generation != m->generation)
            continue;

        if (m->tail >= t->cache[m->lane].tail && m->head >= t->cache[m->lane].head) {
            __atomic_store_n(&t->cache[m->lane].tail, m->tail, __ATOMIC_RELEASE);
            __atomic_store_n(&t->cache[m->lane].head, m->head, __ATOMIC_RELEASE);
        }
        break;
    }
    pthread_mutex_unlock(&registry_lock);
}

static int check(doca_error_t e, const char *stage)
{
    if (e == DOCA_SUCCESS)
        return 0;

    fprintf(stderr, "WRITE_SYNC_DPA stage=%s error=%s\n", stage, doca_error_get_descr(e));
    return -1;
}

#define TRY(x)                                                                                     \
    do {                                                                                           \
        if (check((x), #x))                                                                        \
            goto fail;                                                                             \
    } while (0)

static uint64_t now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + t.tv_nsec;
}

static int fence_stats_period(uint64_t *period)
{
    const char *value = getenv("DPUMESH_DPA_FENCE_STATS_MS");
    if (!value || !*value) {
        *period = 0;
        return 0;
    }
    char *end;
    unsigned long ms = strtoul(value, &end, 10);
    if (*end || *value == '-' || ms > 60000) {
        fprintf(stderr, "DPUMESH_DPA_FENCE_STATS_MS must be 0..60000 (0 disables)\n");
        return -1;
    }
    *period = (uint64_t)ms * 1000000ull;
    return 0;
}

static int event_wait_mode(int immediate)
{
    if (immediate)
        return WSD_EVENT_GET;

    const char *value = getenv("DPUMESH_DPA_SYNC_WAIT");
    if (!value || !strcmp(value, "async"))
        return WSD_EVENT_ASYNC;
    if (!strcmp(value, "get"))
        return WSD_EVENT_GET;

    fprintf(stderr, "DPUMESH_DPA_SYNC_WAIT must be async or get\n");
    return -1;
}

static int wait_ops_free(struct peer_wsd_qp *c)
{
    /* REMOVE ACK fences DPA rearming. Cancel waits before freeing their event
     * storage; RESUME subsequently drains their already-generated CQEs. */
    for (unsigned i = 0; i < 2; i++) {
        if (c->wait_started[i]) {
            if (check(DMESH_DPA_CALL(doca_dpa_async_ops_stop(c->wait_ops[i])), "event-wait-stop"))
                return -1;
            c->wait_started[i] = 0;
        }
        if (c->wait_ops[i]) {
            if (check(DMESH_DPA_CALL(doca_dpa_async_ops_destroy(c->wait_ops[i])), "event-wait-destroy"))
                return -1;
            c->wait_ops[i] = NULL;
        }
    }

    return 0;
}

static int control(struct peer_wsd *t, unsigned op, struct peer_wsd_qp *c)
{
    if (t->id == UINT64_MAX || t->tail == UINT64_MAX)
        return -1;

    uint64_t head = __atomic_load_n(&t->shared->control_consumed, __ATOMIC_ACQUIRE);
    if (t->tail - head >= WSD_RING)
        return -1;

    struct wsd_request *d = &t->shared->control[t->tail % WSD_RING];
    *d = (struct wsd_request){.op = op,
                              .sequence = ++t->id,
                              .lane = c ? c->index : 0,
                              .generation = c ? c->generation : 0,
                              .control.data_fence = t->data_tail};
    __atomic_store_n(&d->ticket, ++t->tail, __ATOMIC_RELEASE);

    uint64_t deadline = now() + 5000000000ull;
    uint64_t *ack = op == WSD_STOP ? &t->shared->stopped : &t->shared->control_ack;

    while (__atomic_load_n(ack, __ATOMIC_ACQUIRE) != t->id) {
        if (now() > deadline)
            return -1;

        struct timespec delay = {.tv_nsec = 100000};
        nanosleep(&delay, NULL);
    }

    return 0;
}

static int fence_stats_print(struct peer_wsd_qp *c, const char *phase)
{
    const struct wsd_fence_snapshot *snapshot = &c->ctx->shared->fence_snapshot;
    if (snapshot->lane != c->index || snapshot->generation != c->generation)
        return -1;

    const struct peer_write_fence_stats stats = snapshot->stats;
    uint64_t stamp = now();
    struct timespec wall;
    clock_gettime(CLOCK_REALTIME, &wall);
    fprintf(stderr,
        "WRITE_SYNC_FENCE_STATS mode=%s worker=%u eu=%u cookie=%u lane=%u generation=%u phase=%s sample=post_commit unix_ns=%llu window_ms=%.3f capacity=%u current=%u samples=%llu depth_sum=%llu avg=%.3f max=%u full_stalls=%llu hist_1=%u hist_2_4=%u hist_5_8=%u hist_9_16=%u hist_17_plus=%u\n",
        c->ctx->immediate ? "write-imm" : "sync", c->ctx->runtime.owner_worker,
        c->ctx->eu, c->ctx->cookie, c->index, c->generation, phase,
        (unsigned long long)((uint64_t)wall.tv_sec * 1000000000ull + wall.tv_nsec),
        (double)(stamp - c->fence_report_ns) / 1000000.0,
        WSD_FENCES, snapshot->depth,
        (unsigned long long)stats.samples, (unsigned long long)stats.depth_sum,
        stats.samples ? (double)stats.depth_sum / stats.samples : 0.0,
        stats.peak, (unsigned long long)stats.full_stalls,
        stats.histogram[0], stats.histogram[1], stats.histogram[2],
        stats.histogram[3], stats.histogram[4]);
    c->fence_report_ns = stamp;
    return 0;
}

static int fence_stats_report(struct peer_wsd_qp *c)
{
    if (!c->ctx->fence_stats_period_ns || !c->active || c->removed || c->ctx->paused ||
        now() - c->fence_report_ns < c->ctx->fence_stats_period_ns)
        return 0;
    /* Opt-in only. Use the existing serialized control owner for an immutable
     * snapshot, never memcpy live DPA counters or print from the DPA loop. */
    if (control(c->ctx, WSD_FENCE_SNAPSHOT, c))
        return -1;
    return fence_stats_print(c, "interval");
}

static int enqueue(struct peer_wsd_qp *c, struct wsd_request *r)
{
    struct wsd_status *o = &c->ctx->shared->status[c->index];
    if (__atomic_load_n(&o->error, __ATOMIC_ACQUIRE) || c->tail == UINT64_MAX ||
        c->ctx->data_tail == UINT64_MAX ||
        c->ctx->data_tail -
                __atomic_load_n(&c->ctx->shared->data_credit.consumed, __ATOMIC_ACQUIRE) >=
            WSD_RING ||
        c->removed)
        return -1;

    r->lane = c->index;
    r->generation = c->generation;
    r->qpn = c->qpn;
    r->sequence = ++c->tail;

    struct wsd_request *d = &c->ctx->shared->data.entry[c->ctx->data_tail % WSD_RING];
    /* Reuse never bulk-writes the live ticket. Publish QPN and descriptor
     * together before its context-wide release sequence. */
    memcpy((uint8_t *)d + sizeof(d->ticket),
           (const uint8_t *)r + sizeof(r->ticket),
           sizeof(*d) - sizeof(d->ticket));
    __atomic_store_n(&d->ticket, ++c->ctx->data_tail, __ATOMIC_RELEASE);

    return 0;
}

static int wsd_new(struct peer_sync_engine *engine,
                   const struct peer_dpa_runtime *rt,
                   unsigned eu,
                   struct peer_wsd **out,
                   int immediate)
{
    if (!engine || !rt || !rt->dpa || !rt->dev || !rt->attach || !out)
        return -1;

    *out = NULL;

    struct peer_wsd *t = calloc(1, sizeof(*t));
    if (!t)
        return -1;

    t->base = rt->dpa;
    t->runtime = *rt;
    t->eu = eu;
    t->immediate = immediate;
    t->event_wait = event_wait_mode(immediate);
    if (t->event_wait < 0 || fence_stats_period(&t->fence_stats_period_ns)) {
        free(t);
        return -1;
    }

    struct doca_dev *sf = peer_sync_engine_dev(engine);
    t->sf = sf;

    struct wsd_device *s = aligned_alloc(64, sizeof(*s));
    if (!s) {
        free(t);
        return -1;
    }

    memset(s, 0, sizeof(*s));
    s->engine_kind = WSD_ENGINE;
    s->owner_worker = rt->owner_worker;
    s->event_wait = t->event_wait;
    s->fence_stats_enabled = t->fence_stats_period_ns != 0;

    pthread_mutex_lock(&registry_lock);

    if (next_cookie == UINT32_MAX) {
        pthread_mutex_unlock(&registry_lock);

        free(s);
        free(t);
        return -1;
    }

    s->cookie = t->cookie = ++next_cookie;
    t->registry_next = registry;
    registry = t;
    pthread_mutex_unlock(&registry_lock);

    TRY(DMESH_DPA_CALL(doca_dpa_device_extend(t->base, sf, &t->extended)));
    TRY(DMESH_DPA_CALL(doca_dpa_get_dpa_handle(t->extended, &s->cq_device)));
    TRY(DMESH_DPA_CALL(doca_dpa_get_dpa_handle(t->base, &s->base_device)));

    TRY(DMESH_DPA_CALL(doca_dpa_completion_create(t->extended, immediate ? WSD_IMM_CQ : WSD_CQ, &t->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_start(t->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(t->cq, &s->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_get_queue_size(t->cq, &s->cq_capacity)));
    if (s->cq_capacity < WSD_LANES * (immediate ? (WSD_IMM_RQ + WSD_FENCES + WSD_CONTROLS)
                                                : (WSD_FENCES + WSD_CONTROLS)))
        goto fail;

    if (t->event_wait == WSD_EVENT_ASYNC) {
        TRY(DMESH_DPA_CALL(doca_dpa_completion_create(t->extended, WSD_EVENT_CQ, &t->event_cq)));
        TRY(DMESH_DPA_CALL(doca_dpa_completion_start(t->event_cq)));
        TRY(DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(t->event_cq, &s->event_cq)));
        TRY(DMESH_DPA_CALL(doca_dpa_completion_get_queue_size(t->event_cq, &s->event_cq_capacity)));
        if (s->event_cq_capacity < 2 * WSD_LANES)
            goto fail;
    }

    t->shared = aligned_alloc(64, sizeof(*t->shared));
    if (!t->shared)
        goto fail;

    memset(t->shared, 0, sizeof(*t->shared));
    t->shared->wait.enabled = 1;

    TRY(doca_mmap_create(&t->map));
    TRY(doca_mmap_set_memrange(t->map, t->shared, sizeof(*t->shared)));
    TRY(doca_mmap_set_permissions(t->map, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE));
    TRY(doca_mmap_add_dev(t->map, rt->dev));
    TRY(doca_mmap_start(t->map));

    TRY(doca_buf_arr_create(1, &t->array));
    TRY(doca_buf_arr_set_target_dpa(t->array, t->base));
    TRY(doca_buf_arr_set_params(t->array, t->map, sizeof(*t->shared), 0));
    TRY(doca_buf_arr_start(t->array));
    TRY(doca_buf_arr_get_dpa_handle(t->array, &s->buffers));

    TRY(DMESH_DPA_CALL(doca_dpa_mem_alloc(t->base, sizeof(*s), &t->device_state)));
    if (t->device_state % 64)
        goto fail;
    TRY(DMESH_DPA_CALL(doca_dpa_h2d_memcpy(t->base, t->device_state, s, sizeof(*s))));

    /* An attach failure can still have published the state to a staged/running
     * thread. Mark before calling, so cancellation/STOP is always required. */
    t->attached = 1;
    if (rt->attach(rt->attach_ctx, eu, t->device_state))
        goto fail;

    free(s);
    if (!immediate)
        fprintf(stderr,
                "WRITE_SYNC_DPA event_wait=%s\n",
                t->event_wait == WSD_EVENT_ASYNC ? "async" : "get");
    *out = t;
    return 0;

fail:
    free(s);

    if (peer_wsd_free(t))
        *out = t;

    return -1;
}

int peer_wsd_new(struct peer_sync_engine *engine,
                 const struct peer_dpa_runtime *rt,
                 unsigned eu,
                 struct peer_wsd **out)
{
    return wsd_new(engine, rt, eu, out, 0);
}

int peer_wsd_new_imm(struct peer_sync_engine *engine,
                     const struct peer_dpa_runtime *rt,
                     unsigned eu,
                     struct peer_wsd **out)
{
    return wsd_new(engine, rt, eu, out, 1);
}

/* Only connected QPs with outstanding control operations belong here.
 * Bootstrap RECV may be posted before connect; activation queues it later. */
static void completion_enqueue(struct peer_wsd_qp *c)
{
    struct peer_wsd *t = c->ctx;
    if (c->completion_queued || !c->installed || !c->active || c->removed ||
        c->error_reported || !c->pending_count || t->qp[c->index] != c)
        return;

    c->completion_prev = t->completion_last;
    c->completion_next = NULL;
    if (t->completion_last)
        t->completion_last->completion_next = c;
    else
        t->completion_first = c;

    t->completion_last = c;
    c->completion_queued = 1;
}

static void completion_unlink(struct peer_wsd_qp *c)
{
    if (!c->completion_queued)
        return;

    struct peer_wsd *t = c->ctx;
    if (c->completion_prev)
        c->completion_prev->completion_next = c->completion_next;
    else
        t->completion_first = c->completion_next;

    if (c->completion_next)
        c->completion_next->completion_prev = c->completion_prev;
    else
        t->completion_last = c->completion_prev;

    c->completion_prev = c->completion_next = NULL;
    c->completion_queued = 0;
}

int peer_wsd_qp_new(struct peer_wsd *t,
                    unsigned index,
                    uint32_t gen,
                    uint32_t capacity,
                    uint32_t gid,
                    uint16_t mtu,
                    struct peer_verbs_endpoint *ep,
                    struct peer_wsd_qp **out)
{
    if (!out || !ep)
        return -1;

    *out = NULL;

    if (index >= WSD_LANES || t->qp[index] || t->paused || !gen)
        return -1;

    struct peer_wsd_qp *c = calloc(1, sizeof(*c));
    if (!c)
        return -1;

    c->ctx = t;
    c->index = index;
    c->generation = gen;
    t->qp[index] = c;

    if (peer_write_completion_init(&c->data_completion, capacity))
        goto fail;

    int rc = t->immediate
                 ? peer_sync_native_new_imm(t->sf, t->extended, t->cq, gid, mtu, &c->native)
                 : peer_sync_native_new(t->sf, t->extended, t->cq, gid, mtu, &c->native);
    if (rc)
        goto fail;

    const void *blob;
    size_t bytes;
    if (peer_sync_native_export(c->native, &blob, &bytes) || bytes > PEER_NATIVE_SETUP_MAX ||
        peer_sync_native_endpoint(c->native, &ep->qpn, &ep->psn))
        goto fail;

    ep->native.bytes = bytes;
    memcpy(ep->native.data, blob, bytes);
    c->qpn = ep->qpn;

    struct wsd_binding binding = {.generation = gen,
                                  .capacity = capacity,
                                  .qpn = c->qpn,
                                  .native = 1,
                                  .mode = t->immediate ? WSD_IMMEDIATE : WSD_SYNC};
    doca_dpa_dev_sync_event_t local[2];
    doca_dpa_dev_sync_event_remote_net_t remote[2];
    if (peer_sync_native_handles(c->native, &binding.qp, &binding.connection, local, remote))
        goto fail;

    binding.local_tail = local[0];
    binding.local_head = local[1];
    if (t->event_wait == WSD_EVENT_ASYNC) {
        if (t->wait_serial == WSD_WAIT_SERIAL_MAX)
            goto fail;
        binding.wait_tag = (++t->wait_serial << WSD_WAIT_TAG_SHIFT) | (index << 1);
        for (unsigned i = 0; i < 2; i++) {
            TRY(DMESH_DPA_CALL(doca_dpa_async_ops_create(t->extended, 4, binding.wait_tag | i, &c->wait_ops[i])));
            TRY(DMESH_DPA_CALL(doca_dpa_async_ops_attach(c->wait_ops[i], t->event_cq)));
            TRY(DMESH_DPA_CALL(doca_dpa_async_ops_start(c->wait_ops[i])));
            c->wait_started[i] = 1;
            TRY(DMESH_DPA_CALL(doca_dpa_async_ops_get_dpa_handle(c->wait_ops[i], &binding.wait_ops[i])));
        }
    }
    TRY(DMESH_DPA_CALL(doca_dpa_get_dpa_handle(t->extended, &binding.device)));

    pthread_mutex_lock(&registry_lock);
    t->cache[index].generation = gen;
    t->cache[index].tail = t->cache[index].head = 0;
    pthread_mutex_unlock(&registry_lock);

    t->shared->binding[index] = binding;
    c->installed = 1;

    if (control(t, WSD_INSTALL, c) || t->shared->status[index].error)
        goto fail;

    if (t->fence_stats_period_ns)
        c->fence_report_ns = now();
    *out = c;
    return 0;

fail:
    if (peer_wsd_destroy(c))
        *out = c;

    return -1;
}

int peer_wsd_activate(struct peer_wsd_qp *c, const struct peer_verbs_endpoint *ep, int protected)
{
    if (!c || !ep || protected != 1 || !ep->native.bytes ||
        ep->native.bytes > PEER_NATIVE_SETUP_MAX)
        return -1;

    if (!c->installed || c->removed || c->active || c->ctx->qp[c->index] != c ||
        __atomic_load_n(&c->ctx->shared->status[c->index].error, __ATOMIC_ACQUIRE) ||
        peer_sync_native_connect(c->native, ep->native.data, ep->native.bytes))
        return -1;

    c->active = 1;
    completion_enqueue(c);
    return 0;
}

uint32_t peer_wsd_qpn(struct peer_wsd_qp *c)
{
    return c->qpn;
}

/* INIT/RTR/RTS transitions are owned by native doca_rdma, never verbs_modify. */
int peer_wsd_modify(struct peer_wsd_qp *c, struct ibv_qp_attr *a, int mask)
{
    (void)c;
    return (mask & IBV_QP_STATE) && a->qp_state == IBV_QPS_INIT ? 0 : -1;
}

int peer_wsd_register(struct peer_wsd *t, void *address, size_t bytes, uint32_t key)
{
    for (struct wsd_source *s = t->sources; s; s = s->next)
        if (s->key == key)
            return s->address == (uintptr_t)address && s->bytes == bytes ? 0 : -1;

    struct wsd_source *s = calloc(1, sizeof(*s));
    if (!s)
        return -1;

    s->address = (uintptr_t)address;
    s->bytes = bytes;
    s->key = key;

    TRY(doca_mmap_create(&s->map));
    TRY(doca_mmap_set_memrange(s->map, address, bytes));
    TRY(doca_mmap_set_permissions(s->map, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE));
    TRY(doca_mmap_add_dev(s->map, t->sf));
    TRY(doca_mmap_start(s->map));
    TRY(doca_mmap_dev_get_dpa_handle(s->map, t->sf, &s->handle));

    s->next = t->sources;
    t->sources = s;

    return 0;

fail:
    if (s->map && doca_mmap_destroy(s->map) != DOCA_SUCCESS) {
        s->next = t->sources;
        t->sources = s;
        return -1;
    }
    free(s);

    return -1;
}

int peer_wsd_unregister(struct peer_wsd *t, uint32_t key)
{
    struct wsd_source **p = &t->sources;
    while (*p && (*p)->key != key)
        p = &(*p)->next;

    if (!*p)
        return 0;

    struct wsd_source *s = *p;
    if (check(doca_mmap_destroy(s->map), "source-map-destroy"))
        return -1;

    *p = s->next;
    free(s);

    return 0;
}

static int source_handle(
    struct peer_wsd_qp *c, uint64_t address, unsigned bytes, uint32_t key, uint32_t *handle)
{
    for (struct wsd_source *s = c->ctx->sources; s; s = s->next)
        if (s->key == key && address >= s->address && bytes <= s->bytes &&
            address - s->address <= s->bytes - bytes) {
            *handle = s->handle;
            return 0;
        }
    return -1;
}

unsigned peer_wsd_hello_size(const struct peer_wsd_qp *c)
{
    return wsd_hello_size(c->ctx->immediate ? WSD_IMMEDIATE : WSD_SYNC);
}

int peer_wsd_event_export(struct peer_wsd_qp *c, void *out, size_t cap)
{
    if (c->ctx->immediate) {
        memset(out, 0, cap);
        return 0;
    }

    uint8_t *p = out;
    size_t used = 0;

    for (unsigned i = 0; i < 2; i++) {
        const void *data;
        size_t bytes;
        if (peer_sync_native_event_export(c->native, i, &data, &bytes) || bytes > 65535 ||
            cap - used < 2 || bytes > cap - used - 2)
            return -1;

        p[used++] = bytes >> 8;
        p[used++] = bytes;
        memcpy(p + used, data, bytes);
        used += bytes;
    }

    memset(p + used, 0, cap - used);

    return 0;
}

int peer_wsd_configure_native(struct peer_wsd_qp *c,
                              uint64_t base,
                              uint32_t key,
                              uint32_t capacity,
                              const void *data,
                              size_t bytes)
{
    const uint8_t *p = data;
    size_t used = 0;
    if (c->remote_map || !base || base > UINT64_MAX - 2ull * capacity)
        return -1;

    for (unsigned i = 0; !c->ctx->immediate && i < 2; i++) {
        if (bytes - used < 2)
            return -1;

        size_t n = (size_t)p[used] << 8 | p[used + 1];
        used += 2;

        if (!n || n > bytes - used || peer_sync_native_event_import(c->native, i, p + used, n))
            return -1;

        used += n;
    }

    while (used < bytes)
        if (p[used++])
            return -1;

    TRY(doca_mmap_create_from_mr(NULL,
                                 base,
                                 2ull * capacity,
                                 key,
                                 DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_RDMA_WRITE,
                                 c->ctx->sf,
                                 &c->remote_map));

    uint32_t handle;
    TRY(doca_mmap_dev_get_dpa_handle(c->remote_map, c->ctx->sf, &handle));

    doca_dpa_dev_rdma_t rdma;
    uint32_t connection;
    doca_dpa_dev_sync_event_t local[2];
    doca_dpa_dev_sync_event_remote_net_t remote[2];
    if (peer_sync_native_handles(c->native, &rdma, &connection, local, remote))
        return -1;

    return peer_wsd_configure(c, base, handle, capacity, remote[0], remote[1], 0);

fail:
    return -1;
}

int peer_wsd_destroy(struct peer_wsd_qp *c)
{
    if (!c)
        return 0;

    struct peer_wsd *t = c->ctx;
    /* Remove before any fallible teardown step. Retried destruction must not
     * leave a quarantined QP reachable by completion polling. */
    completion_unlink(c);
    c->active = 0;

    if (c->installed && !c->removed) {
        if (control(t, WSD_REMOVE, c))
            return -1;

        t->paused = 1;
        c->removed = 1;

        if (t->fence_stats_period_ns && fence_stats_print(c, "remove"))
            fprintf(stderr, "WRITE_SYNC_FENCE_STATS error=snapshot-identity lane=%u\n", c->index);

        struct wsd_status *o = &t->shared->status[c->index];
        fprintf(
            stderr,
            "WRITE_SYNC_NATIVE_RECEIPT mode=%s lane=%u generation=%u writes=%llu tail_sets=%llu head_sets=%llu cqes=%llu rx_cqes=%llu rq_posts=%llu error=%u notify=armed-epoch-v1 publication=%llu wait_epochs=%llu event_wait=%s event_reads=%llu event_posts=%llu event_cqes=%llu\n",
            t->immediate ? "write-imm" : "sync",
            c->index,
            c->generation,
            (unsigned long long)o->data_posts,
            (unsigned long long)o->tail_sets,
            (unsigned long long)o->head_sets,
            (unsigned long long)o->send_cqes,
            (unsigned long long)o->recv_cqes,
            (unsigned long long)o->rq_posts,
            o->error,
            (unsigned long long)__atomic_load_n(&t->shared->publication.sequence, __ATOMIC_ACQUIRE),
            (unsigned long long)t->wait_epoch,
            t->immediate ? "none" : (t->event_wait == WSD_EVENT_ASYNC ? "async" : "get"),
            (unsigned long long)o->event_reads,
            (unsigned long long)o->event_posts,
            (unsigned long long)o->event_completions);
    }

    if (wait_ops_free(c))
        return -1;

    if (c->native) {
        if (peer_sync_native_free(c->native))
            return -1;

        c->native = NULL;
    }

    if (c->remote_map) {
        if (check(doca_mmap_destroy(c->remote_map), "remote-map-destroy"))
            return -1;

        c->remote_map = NULL;
    }

    pthread_mutex_lock(&registry_lock);
    t->cache[c->index].generation = 0;
    pthread_mutex_unlock(&registry_lock);

    if (t->paused) {
        if (control(t, WSD_RESUME, NULL))
            return -1;

        t->paused = 0;
    }

    t->qp[c->index] = NULL;
    peer_write_completion_free(&c->data_completion);
    free(c);

    return 0;
}

int peer_wsd_recv(struct peer_wsd_qp *c, const struct ibv_recv_wr *wr)
{
    if (!wr || wr->next || wr->num_sge != 1 || wr->sg_list->length != peer_wsd_hello_size(c) ||
        c->pending[WSD_RECV])
        return -1;

    struct wsd_request r = {.op = WSD_RECEIVE,
                            .data.src = wr->sg_list->addr,
                            .data.key = wr->sg_list->lkey,
                            .bytes = wr->sg_list->length};
    if (source_handle(c, r.data.src, r.bytes, r.data.key, &r.data.key) || enqueue(c, &r))
        return -1;

    c->pending[WSD_RECV] = r.sequence;
    c->wrid[WSD_RECV] = wr->wr_id;
    c->pending_slots[c->pending_count++] = WSD_RECV;
    completion_enqueue(c);

    return 0;
}

static int submit_control(struct peer_wsd_qp *c, struct wsd_request *r, uint64_t wrid)
{
    unsigned slot = wsd_control_slot(r->op);
    if (slot >= WSD_CONTROLS || c->pending[slot])
        return -1;
    if (r->op != WSD_HEAD && source_handle(c, r->data.src, r->bytes, r->data.key, &r->data.key))
        return -1;
    if (enqueue(c, r))
        return -1;

    c->pending[slot] = r->sequence;
    c->wrid[slot] = wrid;
    c->pending_slots[c->pending_count++] = (uint16_t)slot;
    completion_enqueue(c);

    return 0;
}

/* DPA publishes the last completed logical DATA ID. Keep only a two-byte
 * length per original WRITE on ARM to recover the cumulative byte prefix.
 * DATA never occupies a control pending slot or produces synthetic CQEs. */
int peer_wsd_data(
    struct peer_wsd_qp *c, uint64_t end, uint64_t source, uint32_t key, uint32_t bytes)
{
    struct wsd_request r = {
        .op = WSD_DATA, .data.delta = end, .data.src = source, .data.key = key, .bytes = bytes};
    if (source_handle(c, source, bytes, key, &r.data.key))
        return -1;
    if (peer_write_completion_post(&c->data_completion, bytes, end))
        return -1;
    r.data.id = c->data_completion.posted_id;
    if (enqueue(c, &r)) {
        /* enqueue fails only before publishing a descriptor. */
        peer_write_completion_cancel(&c->data_completion);
        return -1;
    }
    return 0;
}

int peer_wsd_admit(struct peer_wsd_qp *c, unsigned entries)
{
    struct wsd_status *o = &c->ctx->shared->status[c->index];
    if (!entries || entries > WSD_RING || c->removed ||
        __atomic_load_n(&o->error, __ATOMIC_ACQUIRE) || c->tail == UINT64_MAX ||
        c->ctx->data_tail == UINT64_MAX)
        return -1;
    return c->ctx->data_tail -
               __atomic_load_n(&c->ctx->shared->data_credit.consumed, __ATOMIC_ACQUIRE) <=
           WSD_RING - entries;
}

int peer_wsd_head(struct peer_wsd_qp *c, uint64_t id, uint64_t delta)
{
    struct wsd_request r = {.op = WSD_HEAD, .data.src = 1, .bytes = 8, .data.delta = delta};
    return submit_control(c, &r, id);
}

int peer_wsd_send(struct peer_wsd_qp *c, const struct ibv_send_wr *wr)
{
    if (!wr || wr->next || wr->num_sge != 1 || wr->opcode != IBV_WR_SEND ||
        wr->sg_list->length != peer_wsd_hello_size(c))
        return -1;

    struct wsd_request r = {.op = WSD_SEND_BOOT,
                            .data.src = wr->sg_list->addr,
                            .bytes = wr->sg_list->length,
                            .data.key = wr->sg_list->lkey};
    return submit_control(c, &r, wr->wr_id);
}

int peer_wsd_configure(struct peer_wsd_qp *c,
                       uint64_t base,
                       uint32_t key,
                       uint32_t capacity,
                       uint64_t tail,
                       uint64_t head,
                       uint32_t event_key)
{
    struct wsd_request r = {.op = WSD_CONFIGURE,
                            .config.base = base,
                            .config.key = key,
                            .bytes = capacity,
                            .config.tail_event = tail,
                            .config.head_event = head,
                            .config.event_key = event_key};
    return enqueue(c, &r);
}

int peer_wsd_poll(struct peer_wsd *t, int max, struct ibv_wc *out)
{
    int n = 0;
    for (struct peer_wsd_qp *c = t->completion_first, *next; c && n < max; c = next) {
        next = c->completion_next;
        unsigned i = c->index;
        struct wsd_status *o = &t->shared->status[i];
        if (__atomic_load_n(&o->error, __ATOMIC_ACQUIRE)) {
            completion_unlink(c);
            c->error_reported = 1;

            /* Preserve all source holds until QP destroy on any fault. */
            out[n++] = (struct ibv_wc){.wr_id = (uint64_t)c->generation << 39 | (uint64_t)i << 32,
                                       .qp_num = c->qpn,
                                       .status = IBV_WC_GENERAL_ERR};
            continue;
        }

        for (unsigned at = 0; at < c->pending_count && n < max;) {
            unsigned slot = c->pending_slots[at];
            uint64_t id = __atomic_load_n(&o->done[slot].id, __ATOMIC_ACQUIRE);
            if (c->pending[slot] != id) {
                at++;
                continue;
            }

            out[n++] = (struct ibv_wc){.wr_id = c->wrid[slot],
                                       .qp_num = c->qpn,
                                       .status = IBV_WC_SUCCESS,
                                       .opcode = slot == WSD_RECV   ? IBV_WC_RECV
                                                 : slot == WSD_BOOT ? IBV_WC_SEND
                                                                    : IBV_WC_RDMA_WRITE,
                                       /* DPA validated the fixed hello length before publishing
                 * its ID. Do not acquire a SECOND external cache line for
                 * bytes: ID and bytes can become visible independently. */
                                       .byte_len = slot == WSD_RECV ? peer_wsd_hello_size(c) : 0};
            c->pending[slot] = 0;
            c->pending_slots[at] = c->pending_slots[--c->pending_count];
        }

        if (!c->pending_count)
            completion_unlink(c);
    }
    return n;
}

int peer_wsd_counters(
    struct peer_wsd_qp *c, uint64_t *tail, uint64_t *head, uint64_t *completed, uint64_t *consumed)
{
    struct wsd_status *o = &c->ctx->shared->status[c->index];

    /* MsgQ is a wake hint, never the sole source of progress. The DPA writes
     * status back before publishing this sequence. Each counter is monotonic
     * and independently validated by the native engine and wire adapter. */
    if (__atomic_load_n(&c->ctx->shared->publication.sequence, __ATOMIC_ACQUIRE) == UINT64_MAX ||
        __atomic_load_n(&o->generation, __ATOMIC_ACQUIRE) != c->generation ||
        __atomic_load_n(&o->error, __ATOMIC_ACQUIRE))
        return -1;

    *tail = __atomic_load_n(&o->tail, __ATOMIC_ACQUIRE);
    *head = __atomic_load_n(&o->head, __ATOMIC_ACQUIRE);
    if (peer_write_completion_done(&c->data_completion,
                                   __atomic_load_n(&o->completed_id, __ATOMIC_ACQUIRE), completed))
        return -1;
    *consumed = __atomic_load_n(&o->consumed, __ATOMIC_ACQUIRE);
    if (*consumed > c->tail)
        return -1;
    return fence_stats_report(c);
}

int peer_wsd_free(struct peer_wsd *t)
{
    if (!t)
        return 0;

    for (unsigned i = 0; i < WSD_LANES; i++)
        if (t->qp[i] && peer_wsd_destroy(t->qp[i]))
            return -1;

    if (t->attached) {
        int cancelled =
            t->runtime.cancel_attach
                ? t->runtime.cancel_attach(t->runtime.attach_ctx, t->eu, t->device_state)
                : 0;
        if (cancelled < 0 || (!cancelled && control(t, WSD_STOP, NULL)))
            return -1;

        t->attached = 0;
    }

    while (t->sources)
        if (peer_wsd_unregister(t, t->sources->key))
            return -1;

    pthread_mutex_lock(&registry_lock);

    struct peer_wsd **entry = &registry;
    while (*entry && *entry != t)
        entry = &(*entry)->registry_next;

    if (*entry)
        *entry = t->registry_next;
    pthread_mutex_unlock(&registry_lock);

    /* STOP writeback is the last DPA read/write of shared/state/events. */
    if (t->device_state) {
        if (check(DMESH_DPA_CALL(doca_dpa_mem_free(t->base, t->device_state)), "state-free"))
            return -1;

        t->device_state = 0;
    }

    if (t->array) {
        if (check(doca_buf_arr_destroy(t->array), "array-free"))
            return -1;

        t->array = NULL;
    }

    if (t->map) {
        if (check(doca_mmap_destroy(t->map), "map-free"))
            return -1;

        t->map = NULL;
    }

    if (t->cq) {
        if (check(DMESH_DPA_CALL(doca_dpa_completion_destroy(t->cq)), "CQ-free"))
            return -1;

        t->cq = NULL;
    }

    if (t->event_cq) {
        if (check(DMESH_DPA_CALL(doca_dpa_completion_destroy(t->event_cq)), "event-CQ-free"))
            return -1;
        t->event_cq = NULL;
    }

    if (t->extended) {
        if (check(DMESH_DPA_CALL(doca_dpa_destroy(t->extended)), "extended-free"))
            return -1;

        t->extended = NULL;
    }

    free(t->shared);
    free(t);

    return 0;
}

void peer_wsd_debug(struct peer_wsd_qp *c)
{
    if (!c)
        return;

    struct peer_wsd *t = c->ctx;
    struct wsd_status *o = &t->shared->status[c->index];
    fprintf(
        stderr,
        "WRITE_SYNC_NATIVE_STATE lane=%u generation=%u data=%llu/%llu error=%u tail=%llu head=%llu writes=%llu tail_sets=%llu head_sets=%llu cqes=%llu dpa_error=%s\n",
        c->index,
        c->generation,
        (unsigned long long)o->consumed,
        (unsigned long long)c->tail,
        o->error,
        (unsigned long long)o->tail,
        (unsigned long long)o->head,
        (unsigned long long)o->data_posts,
        (unsigned long long)o->tail_sets,
        (unsigned long long)o->head_sets,
        (unsigned long long)o->send_cqes,
        doca_error_get_name(DMESH_DPA_CALL(doca_dpa_peek_at_last_error(t->base))));
}
