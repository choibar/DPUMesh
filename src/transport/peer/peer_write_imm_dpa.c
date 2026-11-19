#include "../common/dpa_runtime.h"
/* ARM setup / descriptor publication only. NIC WQEs, CQEs, IMM RQ repost
 * belong exclusively to the DPA engine. */
#define _POSIX_C_SOURCE 200809L
#include "peer_write_imm_dpa.h"
#include "peer_dpa_owner.h"
#include <infiniband/verbs.h>
#include "device/peer_write_imm_dpa_internal.h"
#include "peer_imm_native.h"
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

/* ---- Logical DATA-ID completion lengths ------------------------------- */

/* ARM-only lengths for logical DATA requests, before DPA WRITE coalescing.
 * IDs count DATA only, wrap modulo 2^32, and are implicit in FIFO positions.
 * Store wire bytes (header included) minus one so 65536 fits in two bytes. */
struct peer_write_completion_length {
    uint16_t bytes_minus_one;
};

struct peer_write_completion {
    struct peer_write_completion_length *length;
    uint32_t capacity, limit;
    uint32_t posted_id, completed_id;
    uint64_t posted_bytes, completed_bytes;
};

_Static_assert(sizeof(struct peer_write_completion_length) == 2, "two bytes per DATA request");

/* The local byte window bounds outstanding requests: every wire frame is >=4B.
 * A window below 2^31 requests also makes modulo-ID completion unambiguous. */
static inline int peer_write_completion_init(struct peer_write_completion *q, uint32_t window)
{
    if (window < PEER_DATA_HEADER_BYTES || q->length || q->limit)
        return -1;
    q->limit = window / PEER_DATA_HEADER_BYTES;
    return 0;
}

/* Reserve a length before publishing the descriptor. A failed enqueue must
 * cancel this last entry; no source reference is released by this FIFO. */
static inline int
peer_write_completion_post(struct peer_write_completion *q, uint32_t bytes, uint64_t end)
{
    uint32_t pending = q->posted_id - q->completed_id;
    if (bytes < PEER_DATA_HEADER_BYTES || bytes > 65536 || pending >= q->limit ||
        q->posted_bytes > UINT64_MAX - bytes || end != q->posted_bytes + bytes ||
        q->completed_bytes > q->posted_bytes ||
        q->posted_bytes - q->completed_bytes > (uint64_t)q->limit * PEER_DATA_HEADER_BYTES ||
        bytes > (uint64_t)q->limit * PEER_DATA_HEADER_BYTES - (q->posted_bytes - q->completed_bytes))
        return -1;

    if (pending == q->capacity) {
        uint32_t capacity = q->capacity ? q->capacity * 2 : 32;
        struct peer_write_completion_length *length = malloc((size_t)capacity * sizeof(*length));
        if (!length)
            return -1;
        for (uint32_t n = 0; n < pending; n++) {
            uint32_t id = q->completed_id + n;
            length[id & (capacity - 1)] = q->length[id & (q->capacity - 1)];
        }
        free(q->length);
        q->length = length;
        q->capacity = capacity;
    }

    q->length[q->posted_id & (q->capacity - 1)].bytes_minus_one = (uint16_t)(bytes - 1);
    q->posted_id++;
    q->posted_bytes = end;
    return 0;
}

static inline void peer_write_completion_cancel(struct peer_write_completion *q)
{
    q->posted_id--;
    q->posted_bytes -= (uint32_t)q->length[q->posted_id & (q->capacity - 1)].bytes_minus_one + 1;
}

/* Validate the whole completion prefix before advancing either cursor.
 * A repeated ID is harmless; a stale or future ID must not release sources. */
static inline int
peer_write_completion_done(struct peer_write_completion *q, uint32_t id, uint64_t *end)
{
    uint32_t pending = q->posted_id - q->completed_id;
    uint32_t count = id - q->completed_id;
    if (pending > q->limit || count > pending)
        return -1;

    uint64_t next = q->completed_bytes;
    for (uint32_t n = 0; n < count; n++) {
        uint32_t at = (q->completed_id + n) & (q->capacity - 1);
        uint32_t bytes = (uint32_t)q->length[at].bytes_minus_one + 1;
        if (bytes < PEER_DATA_HEADER_BYTES || next > q->posted_bytes || bytes > q->posted_bytes - next)
            return -1;
        next += bytes;
    }

    q->completed_id = id;
    *end = q->completed_bytes = next;
    return 0;
}

static inline void peer_write_completion_free(struct peer_write_completion *q)
{
    free(q->length);
    *q = (struct peer_write_completion){0};
}

struct peer_wim_qp {
    struct peer_wim *ctx;
    struct peer_imm_native *native;
    struct doca_mmap *remote_map;
    uint32_t qpn, generation;
    unsigned index;
    uint64_t tail;
    uint64_t fence_report_ns;
    struct peer_write_completion data_completion;
    uint64_t pending[WIM_CONTROLS], wrid[WIM_CONTROLS];
    uint16_t pending_slots[WIM_CONTROLS], pending_count;
    /* Worker-owned intrusive list; MsgQ callbacks never mutate QP state. */
    struct peer_wim_qp *completion_prev, *completion_next;
    uint8_t installed, active, removed, error_reported, completion_queued;
};

struct peer_wim {
    struct doca_dpa *base, *extended;
    struct doca_dev *sf;
    struct wim_source *sources;
    struct peer_wim *registry_next;
    uint32_t cookie;

    struct {
        uint32_t generation;
        uint64_t tail, head;
    } cache[WIM_LANES];
    struct doca_dpa_completion *cq;
    struct doca_mmap *map;
    struct doca_buf_arr *array;
    struct wim_shared *shared;
    struct peer_wim_qp *qp[WIM_LANES];
    struct peer_wim_qp *completion_first, *completion_last;
    struct peer_dpa_runtime runtime;
    uint64_t device_state, tail, data_tail, id, wait_epoch;
    uint64_t fence_stats_period_ns;
    unsigned eu;
    int attached, paused;
};

struct wim_source {
    struct wim_source *next;
    struct doca_mmap *map;
    uint32_t key, handle;
    uint64_t address;
    size_t bytes;
};

static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;
static struct peer_wim *registry;
static uint32_t next_cookie;

int peer_wim_arm_worker(unsigned worker)
{
    int rc = 0;

    pthread_mutex_lock(&registry_lock);
    for (struct peer_wim *t = registry; t; t = t->registry_next) {
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
        for (struct peer_wim *t = registry; t; t = t->registry_next)
            if (t->runtime.owner_worker == worker && t->shared)
                __atomic_store_n(&t->shared->wait.epoch, 0, __ATOMIC_RELEASE);

    pthread_mutex_unlock(&registry_lock);

    return rc;
}

void peer_wim_disarm_worker(unsigned worker)
{
    pthread_mutex_lock(&registry_lock);
    for (struct peer_wim *t = registry; t; t = t->registry_next)
        if (t->runtime.owner_worker == worker && t->shared)
            __atomic_store_n(&t->shared->wait.epoch, 0, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&registry_lock);
}

/* MsgQ PE owners may differ from the data owner. Copy only counters under the
 * registry lock: no transport progress or QP dereference on that thread. */
void peer_wim_counter_message(const struct dpa_rdma_counter_msg *m)
{
    if (!m || m->lane >= WIM_LANES)
        return;

    pthread_mutex_lock(&registry_lock);
    for (struct peer_wim *t = registry; t; t = t->registry_next) {
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

    fprintf(stderr, "WRITE_IMM_DPA stage=%s error=%s\n", stage, doca_error_get_descr(e));
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

static int control(struct peer_wim *t, unsigned op, struct peer_wim_qp *c)
{
    if (t->id == UINT64_MAX || t->tail == UINT64_MAX)
        return -1;

    uint64_t head = __atomic_load_n(&t->shared->control_consumed, __ATOMIC_ACQUIRE);
    if (t->tail - head >= WIM_RING)
        return -1;

    struct wim_request *d = &t->shared->control[t->tail % WIM_RING];
    *d = (struct wim_request){.op = op,
                              .sequence = ++t->id,
                              .lane = c ? c->index : 0,
                              .generation = c ? c->generation : 0,
                              .control.data_fence = t->data_tail};
    __atomic_store_n(&d->ticket, ++t->tail, __ATOMIC_RELEASE);

    uint64_t deadline = now() + 5000000000ull;
    uint64_t *ack = op == WIM_STOP ? &t->shared->stopped : &t->shared->control_ack;

    while (__atomic_load_n(ack, __ATOMIC_ACQUIRE) != t->id) {
        if (now() > deadline)
            return -1;

        struct timespec delay = {.tv_nsec = 100000};
        nanosleep(&delay, NULL);
    }

    return 0;
}

static int fence_stats_print(struct peer_wim_qp *c, const char *phase)
{
    const struct wim_fence_snapshot *snapshot = &c->ctx->shared->fence_snapshot;
    if (snapshot->lane != c->index || snapshot->generation != c->generation)
        return -1;

    const struct peer_write_fence_stats stats = snapshot->stats;
    uint64_t stamp = now();
    struct timespec wall;
    clock_gettime(CLOCK_REALTIME, &wall);
    fprintf(stderr,
        "WRITE_IMM_FENCE_STATS mode=%s worker=%u eu=%u cookie=%u lane=%u generation=%u phase=%s sample=post_commit unix_ns=%llu window_ms=%.3f capacity=%u current=%u samples=%llu depth_sum=%llu avg=%.3f max=%u full_stalls=%llu hist_1=%u hist_2_4=%u hist_5_8=%u hist_9_16=%u hist_17_plus=%u\n",
        "write-imm", c->ctx->runtime.owner_worker,
        c->ctx->eu, c->ctx->cookie, c->index, c->generation, phase,
        (unsigned long long)((uint64_t)wall.tv_sec * 1000000000ull + wall.tv_nsec),
        (double)(stamp - c->fence_report_ns) / 1000000.0,
        WIM_FENCES, snapshot->depth,
        (unsigned long long)stats.samples, (unsigned long long)stats.depth_sum,
        stats.samples ? (double)stats.depth_sum / stats.samples : 0.0,
        stats.peak, (unsigned long long)stats.full_stalls,
        stats.histogram[0], stats.histogram[1], stats.histogram[2],
        stats.histogram[3], stats.histogram[4]);
    c->fence_report_ns = stamp;
    return 0;
}

static int fence_stats_report(struct peer_wim_qp *c)
{
    if (!c->ctx->fence_stats_period_ns || !c->active || c->removed || c->ctx->paused ||
        now() - c->fence_report_ns < c->ctx->fence_stats_period_ns)
        return 0;
    /* Opt-in only. Use the existing serialized control owner for an immutable
     * snapshot, never memcpy live DPA counters or print from the DPA loop. */
    if (control(c->ctx, WIM_FENCE_SNAPSHOT, c))
        return -1;
    return fence_stats_print(c, "interval");
}

static int enqueue(struct peer_wim_qp *c, struct wim_request *r)
{
    struct wim_status *o = &c->ctx->shared->status[c->index];
    if (__atomic_load_n(&o->error, __ATOMIC_ACQUIRE) || c->tail == UINT64_MAX ||
        c->ctx->data_tail == UINT64_MAX ||
        c->ctx->data_tail -
                __atomic_load_n(&c->ctx->shared->data_credit.consumed, __ATOMIC_ACQUIRE) >=
            WIM_RING ||
        c->removed)
        return -1;

    r->lane = c->index;
    r->generation = c->generation;
    r->qpn = c->qpn;
    r->sequence = ++c->tail;

    struct wim_request *d = &c->ctx->shared->data.entry[c->ctx->data_tail % WIM_RING];
    /* Reuse never bulk-writes the live ticket. Publish QPN and descriptor
     * together before its context-wide release sequence. */
    memcpy((uint8_t *)d + sizeof(d->ticket),
           (const uint8_t *)r + sizeof(r->ticket),
           sizeof(*d) - sizeof(d->ticket));
    __atomic_store_n(&d->ticket, ++c->ctx->data_tail, __ATOMIC_RELEASE);

    return 0;
}

int peer_wim_new(struct peer_rdma_device *engine,
                   const struct peer_dpa_runtime *rt,
                   unsigned eu,
                   struct peer_wim **out)
{
    if (!engine || !rt || !rt->dpa || !rt->dev || !rt->attach || !out)
        return -1;

    *out = NULL;

    struct peer_wim *t = calloc(1, sizeof(*t));
    if (!t)
        return -1;

    t->base = rt->dpa;
    t->runtime = *rt;
    t->eu = eu;
    if (fence_stats_period(&t->fence_stats_period_ns)) {
        free(t);
        return -1;
    }

    struct doca_dev *sf = peer_rdma_device_dev(engine);
    t->sf = sf;

    struct wim_device *s = aligned_alloc(64, sizeof(*s));
    if (!s) {
        free(t);
        return -1;
    }

    memset(s, 0, sizeof(*s));
    s->owner_worker = rt->owner_worker;
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

    TRY(DMESH_DPA_CALL(doca_dpa_completion_create(t->extended, WIM_IMM_CQ, &t->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_start(t->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(t->cq, &s->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_get_queue_size(t->cq, &s->cq_capacity)));
    if (s->cq_capacity < WIM_LANES * (WIM_IMM_RQ + WIM_FENCES + WIM_CONTROLS))
        goto fail;

    t->shared = aligned_alloc(64, sizeof(*t->shared));
    if (!t->shared)
        goto fail;

    memset(t->shared, 0, sizeof(*t->shared));

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

    *out = t;
    return 0;

fail:
    free(s);

    if (peer_wim_free(t))
        *out = t;

    return -1;
}

/* Only connected QPs with outstanding control operations belong here.
 * Bootstrap RECV may be posted before connect; activation queues it later. */
static void completion_enqueue(struct peer_wim_qp *c)
{
    struct peer_wim *t = c->ctx;
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

static void completion_unlink(struct peer_wim_qp *c)
{
    if (!c->completion_queued)
        return;

    struct peer_wim *t = c->ctx;
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

int peer_wim_qp_new(struct peer_wim *t,
                    unsigned index,
                    uint32_t gen,
                    uint32_t capacity,
                    uint32_t gid,
                    uint16_t mtu,
                    struct peer_verbs_endpoint *ep,
                    struct peer_wim_qp **out)
{
    if (!out || !ep)
        return -1;

    *out = NULL;

    if (index >= WIM_LANES || t->qp[index] || t->paused || !gen)
        return -1;

    struct peer_wim_qp *c = calloc(1, sizeof(*c));
    if (!c)
        return -1;

    c->ctx = t;
    c->index = index;
    c->generation = gen;
    t->qp[index] = c;

    if (peer_write_completion_init(&c->data_completion, capacity))
        goto fail;

    int rc = peer_imm_native_new(t->sf, t->extended, t->cq, gid, mtu, &c->native);
    if (rc)
        goto fail;

    const void *blob;
    size_t bytes;
    if (peer_imm_native_export(c->native, &blob, &bytes) || bytes > PEER_NATIVE_SETUP_MAX ||
        peer_imm_native_endpoint(c->native, &ep->qpn, &ep->psn))
        goto fail;

    ep->native.bytes = bytes;
    memcpy(ep->native.data, blob, bytes);
    c->qpn = ep->qpn;

    struct wim_binding binding = {.generation = gen,
                                  .capacity = capacity,
                                  .qpn = c->qpn};
    if (peer_imm_native_handles(c->native, &binding.qp, &binding.connection))
        goto fail;

    TRY(DMESH_DPA_CALL(doca_dpa_get_dpa_handle(t->extended, &binding.device)));

    pthread_mutex_lock(&registry_lock);
    t->cache[index].generation = gen;
    t->cache[index].tail = t->cache[index].head = 0;
    pthread_mutex_unlock(&registry_lock);

    t->shared->binding[index] = binding;
    c->installed = 1;

    if (control(t, WIM_INSTALL, c) || t->shared->status[index].error)
        goto fail;

    if (t->fence_stats_period_ns)
        c->fence_report_ns = now();
    *out = c;
    return 0;

fail:
    if (peer_wim_destroy(c))
        *out = c;

    return -1;
}

int peer_wim_activate(struct peer_wim_qp *c, const struct peer_verbs_endpoint *ep, int protected)
{
    if (!c || !ep || protected != 1 || !ep->native.bytes ||
        ep->native.bytes > PEER_NATIVE_SETUP_MAX)
        return -1;

    if (!c->installed || c->removed || c->active || c->ctx->qp[c->index] != c ||
        __atomic_load_n(&c->ctx->shared->status[c->index].error, __ATOMIC_ACQUIRE) ||
        peer_imm_native_connect(c->native, ep->native.data, ep->native.bytes))
        return -1;

    c->active = 1;
    completion_enqueue(c);
    return 0;
}

/* INIT/RTR/RTS transitions are owned by native doca_rdma, never verbs_modify. */

int peer_wim_register(struct peer_wim *t, void *address, size_t bytes, uint32_t key)
{
    for (struct wim_source *s = t->sources; s; s = s->next)
        if (s->key == key)
            return s->address == (uintptr_t)address && s->bytes == bytes ? 0 : -1;

    struct wim_source *s = calloc(1, sizeof(*s));
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

int peer_wim_unregister(struct peer_wim *t, uint32_t key)
{
    struct wim_source **p = &t->sources;
    while (*p && (*p)->key != key)
        p = &(*p)->next;

    if (!*p)
        return 0;

    struct wim_source *s = *p;
    if (check(doca_mmap_destroy(s->map), "source-map-destroy"))
        return -1;

    *p = s->next;
    free(s);

    return 0;
}

static int source_handle(
    struct peer_wim_qp *c, uint64_t address, unsigned bytes, uint32_t key, uint32_t *handle)
{
    for (struct wim_source *s = c->ctx->sources; s; s = s->next)
        if (s->key == key && address >= s->address && bytes <= s->bytes &&
            address - s->address <= s->bytes - bytes) {
            *handle = s->handle;
            return 0;
        }
    return -1;
}

unsigned peer_wim_hello_size(const struct peer_wim_qp *c)
{
    (void)c;
    return PEER_WRITE_IMM_HELLO;
}

int peer_wim_configure_native(struct peer_wim_qp *c,
                              uint64_t base, uint32_t key, uint32_t capacity)
{
    if (!c || c->remote_map || !base || !peer_write_capacity_in_range(capacity) ||
        base > UINT64_MAX - 2ull * capacity)
        return -1;

    TRY(doca_mmap_create_from_mr(NULL, base, 2ull * capacity, key,
                                 DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_RDMA_WRITE,
                                 c->ctx->sf, &c->remote_map));
    uint32_t handle;
    TRY(doca_mmap_dev_get_dpa_handle(c->remote_map, c->ctx->sf, &handle));
    struct wim_request r = {.op = WIM_CONFIGURE, .config.base = base,
                            .config.key = handle, .bytes = capacity};
    return enqueue(c, &r);
fail:
    return -1;
}

int peer_wim_destroy(struct peer_wim_qp *c)
{
    if (!c)
        return 0;

    struct peer_wim *t = c->ctx;
    /* Remove before any fallible teardown step. Retried destruction must not
     * leave a quarantined QP reachable by completion polling. */
    completion_unlink(c);
    c->active = 0;

    if (c->installed && !c->removed) {
        if (control(t, WIM_REMOVE, c))
            return -1;

        t->paused = 1;
        c->removed = 1;

        if (t->fence_stats_period_ns && fence_stats_print(c, "remove"))
            fprintf(stderr, "WRITE_IMM_FENCE_STATS error=snapshot-identity lane=%u\n", c->index);

        struct wim_status *o = &t->shared->status[c->index];
        fprintf(
            stderr,
            "WRITE_IMM_NATIVE_RECEIPT mode=%s lane=%u generation=%u writes=%llu tail_sets=%llu head_sets=%llu cqes=%llu rx_cqes=%llu rq_posts=%llu error=%u notify=armed-epoch-v1 publication=%llu wait_epochs=%llu\n",
            "write-imm",
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
            (unsigned long long)t->wait_epoch);
    }

    if (c->native) {
        if (peer_imm_native_free(c->native))
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
        if (control(t, WIM_RESUME, NULL))
            return -1;

        t->paused = 0;
    }

    t->qp[c->index] = NULL;
    peer_write_completion_free(&c->data_completion);
    free(c);

    return 0;
}

int peer_wim_recv(struct peer_wim_qp *c, const struct ibv_recv_wr *wr)
{
    if (!wr || wr->next || wr->num_sge != 1 || wr->sg_list->length != peer_wim_hello_size(c) ||
        c->pending[WIM_RECV])
        return -1;

    struct wim_request r = {.op = WIM_RECEIVE,
                            .data.src = wr->sg_list->addr,
                            .data.key = wr->sg_list->lkey,
                            .bytes = wr->sg_list->length};
    if (source_handle(c, r.data.src, r.bytes, r.data.key, &r.data.key) || enqueue(c, &r))
        return -1;

    c->pending[WIM_RECV] = r.sequence;
    c->wrid[WIM_RECV] = wr->wr_id;
    c->pending_slots[c->pending_count++] = WIM_RECV;
    completion_enqueue(c);

    return 0;
}

static int submit_control(struct peer_wim_qp *c, struct wim_request *r, uint64_t wrid)
{
    unsigned slot = wim_control_slot(r->op);
    if (slot >= WIM_CONTROLS || c->pending[slot])
        return -1;
    if (r->op != WIM_HEAD && source_handle(c, r->data.src, r->bytes, r->data.key, &r->data.key))
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
int peer_wim_data(
    struct peer_wim_qp *c, uint64_t end, uint64_t source, uint32_t key, uint32_t bytes)
{
    struct wim_request r = {
        .op = WIM_DATA, .data.delta = end, .data.src = source, .data.key = key, .bytes = bytes};
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

int peer_wim_admit(struct peer_wim_qp *c, unsigned entries)
{
    struct wim_status *o = &c->ctx->shared->status[c->index];
    if (!entries || entries > WIM_RING || c->removed ||
        __atomic_load_n(&o->error, __ATOMIC_ACQUIRE) || c->tail == UINT64_MAX ||
        c->ctx->data_tail == UINT64_MAX)
        return -1;
    return c->ctx->data_tail -
               __atomic_load_n(&c->ctx->shared->data_credit.consumed, __ATOMIC_ACQUIRE) <=
           WIM_RING - entries;
}

int peer_wim_head(struct peer_wim_qp *c, uint64_t id, uint64_t delta)
{
    struct wim_request r = {.op = WIM_HEAD, .data.src = 1, .bytes = 8, .data.delta = delta};
    return submit_control(c, &r, id);
}

int peer_wim_send(struct peer_wim_qp *c, const struct ibv_send_wr *wr)
{
    if (!wr || wr->next || wr->num_sge != 1 || wr->opcode != IBV_WR_SEND ||
        wr->sg_list->length != peer_wim_hello_size(c))
        return -1;

    struct wim_request r = {.op = WIM_SEND_BOOT,
                            .data.src = wr->sg_list->addr,
                            .bytes = wr->sg_list->length,
                            .data.key = wr->sg_list->lkey};
    return submit_control(c, &r, wr->wr_id);
}

int peer_wim_poll(struct peer_wim *t, int max, struct ibv_wc *out)
{
    int n = 0;
    for (struct peer_wim_qp *c = t->completion_first, *next; c && n < max; c = next) {
        next = c->completion_next;
        unsigned i = c->index;
        struct wim_status *o = &t->shared->status[i];
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
                                       .opcode = slot == WIM_RECV   ? IBV_WC_RECV
                                                 : slot == WIM_BOOT ? IBV_WC_SEND
                                                                    : IBV_WC_RDMA_WRITE,
                                       /* DPA validated the fixed hello length before publishing
                 * its ID. Do not acquire a SECOND external cache line for
                 * bytes: ID and bytes can become visible independently. */
                                       .byte_len = slot == WIM_RECV ? peer_wim_hello_size(c) : 0};
            c->pending[slot] = 0;
            c->pending_slots[at] = c->pending_slots[--c->pending_count];
        }

        if (!c->pending_count)
            completion_unlink(c);
    }
    return n;
}

int peer_wim_counters(
    struct peer_wim_qp *c, uint64_t *tail, uint64_t *head, uint64_t *completed, uint64_t *consumed)
{
    struct wim_status *o = &c->ctx->shared->status[c->index];

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

int peer_wim_free(struct peer_wim *t)
{
    if (!t)
        return 0;

    for (unsigned i = 0; i < WIM_LANES; i++)
        if (t->qp[i] && peer_wim_destroy(t->qp[i]))
            return -1;

    if (t->attached) {
        int cancelled =
            t->runtime.cancel_attach
                ? t->runtime.cancel_attach(t->runtime.attach_ctx, t->eu, t->device_state)
                : 0;
        if (cancelled < 0 || (!cancelled && control(t, WIM_STOP, NULL)))
            return -1;

        t->attached = 0;
    }

    while (t->sources)
        if (peer_wim_unregister(t, t->sources->key))
            return -1;

    pthread_mutex_lock(&registry_lock);

    struct peer_wim **entry = &registry;
    while (*entry && *entry != t)
        entry = &(*entry)->registry_next;

    if (*entry)
        *entry = t->registry_next;
    pthread_mutex_unlock(&registry_lock);

    /* STOP writeback is the last DPA read/write of shared/state. */
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

    if (t->extended) {
        if (check(DMESH_DPA_CALL(doca_dpa_destroy(t->extended)), "extended-free"))
            return -1;

        t->extended = NULL;
    }

    free(t->shared);
    free(t);

    return 0;
}

void peer_wim_debug(struct peer_wim_qp *c)
{
    if (!c)
        return;

    struct peer_wim *t = c->ctx;
    struct wim_status *o = &t->shared->status[c->index];
    fprintf(
        stderr,
        "WRITE_IMM_NATIVE_STATE lane=%u generation=%u data=%llu/%llu error=%u tail=%llu head=%llu writes=%llu tail_sets=%llu head_sets=%llu cqes=%llu dpa_error=%s\n",
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
