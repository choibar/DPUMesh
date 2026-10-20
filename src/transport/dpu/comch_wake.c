/* Comch fast-path wake, DPU side: one producer per (worker, session). See
 * comch_wake.h. */
#include "comch_wake.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <doca_comch_producer.h>
#include <doca_ctx.h>
#include <doca_log.h>
#include <doca_pe.h>

#include "object.h"
#include "dispatcher.h"
#include "dma.h"

DOCA_LOG_REGISTER(COMCH_WAKE);

/* One descriptor is in flight per flow; a worker holds at most
 * DMESH_MAX_CONNECTIONS flows. */
#define WAKE_SEND_TASKS DMESH_MAX_CONNECTIONS
#define WAKE_STOP_NS 200000000ull

enum wake_state { WAKE_FREE = 0, WAKE_STARTING, WAKE_RUNNING, WAKE_STOPPING, WAKE_FAILED };

struct dmesh_wake_producer {
    struct objects *objs;
    uint64_t session_id, session_epoch;
    struct doca_comch_producer *producer;
    uint32_t consumer;
    unsigned refs;
    enum wake_state state;
    bool idle;                 /* the context reached IDLE after a stop */
};

bool dmesh_wake_enabled(void)
{
    const char *v = getenv("DMESH_COMCH_WAKE");
    return !(v && strcmp(v, "0") == 0);
}

static uint64_t wake_now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

/* The flow a completion belongs to: the task carries (slot, generation) and
 * the flow must still be bound to this producer. */
static struct dmesh_conn *wake_task_conn(struct dmesh_wake_producer *p, union doca_data task_data)
{
    uint32_t slot = (uint32_t)(task_data.u64 >> 32), generation = (uint32_t)task_data.u64;
    if (slot >= DMESH_MAX_CONNECTIONS)
        return NULL;
    struct dmesh_conn *c = &p->objs->conns[slot];
    return c->wake == p && c->generation == generation ? c : NULL;
}

static void wake_send_done(struct doca_comch_producer_task_send *task, union doca_data task_data,
                           union doca_data ctx_data)
{
    struct dmesh_wake_producer *p = ctx_data.ptr;
    doca_task_free(doca_comch_producer_task_send_as_task(task));
    struct dmesh_conn *c = wake_task_conn(p, task_data);
    if (c != NULL && c->push_state == 2)
        dmesh_dma_push_desc_sent(c);
}

static void wake_send_failed(struct doca_comch_producer_task_send *task, union doca_data task_data,
                             union doca_data ctx_data)
{
    struct dmesh_wake_producer *p = ctx_data.ptr;
    doca_error_t status = doca_task_get_status(doca_comch_producer_task_send_as_task(task));
    doca_task_free(doca_comch_producer_task_send_as_task(task));
    struct dmesh_conn *c = wake_task_conn(p, task_data);
    if (c != NULL && c->push_state == 2) {
        DOCA_LOG_ERR("fast-path descriptor send failed (flow %u): %s", c->flow_id,
                     doca_error_get_name(status));
        dmesh_dma_push_desc_failed(c);
    }
}

static void wake_state_changed(const union doca_data user_data, struct doca_ctx *ctx,
                               enum doca_ctx_states prev, enum doca_ctx_states next)
{
    struct dmesh_wake_producer *p = user_data.ptr;
    (void)ctx;
    if (next == DOCA_CTX_STATE_RUNNING) {
        if (p->state == WAKE_STARTING)
            p->state = WAKE_RUNNING;
    } else if (next == DOCA_CTX_STATE_IDLE) {
        p->idle = true;
        /* The usual cause is the host leaving: its consumer expires and the
         * SDK stops the producer before the flows' teardown detaches it. A
         * flow that still sends gets BAD_STATE and fails like a desc DMA. */
        if (p->state == WAKE_STARTING || p->state == WAKE_RUNNING) {
            DOCA_LOG_INFO("fast-path producer went idle with %u flows (peer gone?), from state %d",
                          p->refs, (int)prev);
            p->state = WAKE_FAILED;
        }
    }
}

static doca_error_t wake_start(struct dmesh_wake_producer *p, struct doca_comch_connection *connection)
{
    struct objects *o = p->objs;
    union doca_data user_data = {.ptr = p};
    doca_error_t result = doca_comch_producer_create(connection, &p->producer);
    if (result != DOCA_SUCCESS)
        return result;
    struct doca_ctx *ctx = doca_comch_producer_as_ctx(p->producer);
    if ((result = doca_pe_connect_ctx(o->consumer_pe, ctx)) != DOCA_SUCCESS ||
        (result = doca_comch_producer_task_send_set_conf(p->producer, wake_send_done, wake_send_failed,
                                                         WAKE_SEND_TASKS)) != DOCA_SUCCESS ||
        (result = doca_ctx_set_user_data(ctx, user_data)) != DOCA_SUCCESS ||
        (result = doca_ctx_set_state_changed_cb(ctx, wake_state_changed)) != DOCA_SUCCESS)
        goto destroy;
    p->state = WAKE_STARTING;
    p->idle = false;
    result = doca_ctx_start(ctx);
    if (result == DOCA_SUCCESS || result == DOCA_ERROR_IN_PROGRESS)
        return DOCA_SUCCESS;
destroy:
    (void)doca_comch_producer_destroy(p->producer);
    p->producer = NULL;
    p->state = WAKE_FREE;
    return result;
}

/* Stop and destroy; the stop flushes nothing (one descriptor per flow, and
 * the last flow is gone). Bounded: a producer that does not reach IDLE stays
 * STOPPING and is retried from cleanup. */
static void wake_stop(struct dmesh_wake_producer *p)
{
    struct objects *o = p->objs;
    pthread_mutex_t *lock = dmesh_dispatch_sdk_lock(o);
    if (p->producer == NULL) {
        memset(p, 0, sizeof(*p));
        return;
    }
    struct doca_ctx *ctx = doca_comch_producer_as_ctx(p->producer);
    if (p->state != WAKE_STOPPING) {
        p->state = WAKE_STOPPING;
        if (lock) pthread_mutex_lock(lock);
        doca_error_t r = doca_ctx_stop(ctx);
        if (lock) pthread_mutex_unlock(lock);
        if (r != DOCA_SUCCESS && r != DOCA_ERROR_IN_PROGRESS && r != DOCA_ERROR_BAD_STATE)
            DOCA_LOG_WARN("fast-path producer stop: %s", doca_error_get_name(r));
    }
    uint64_t deadline = wake_now_ns() + WAKE_STOP_NS;
    enum doca_ctx_states st = DOCA_CTX_STATE_STOPPING;
    while (!p->idle && (doca_ctx_get_state(ctx, &st) != DOCA_SUCCESS || st != DOCA_CTX_STATE_IDLE) &&
           wake_now_ns() < deadline)
        (void)doca_pe_progress(o->consumer_pe);
    if (!p->idle && st != DOCA_CTX_STATE_IDLE)
        return;
    if (lock) pthread_mutex_lock(lock);
    doca_error_t r = doca_comch_producer_destroy(p->producer);
    if (lock) pthread_mutex_unlock(lock);
    if (r != DOCA_SUCCESS) {
        DOCA_LOG_WARN("fast-path producer destroy: %s", doca_error_get_name(r));
        return;
    }
    memset(p, 0, sizeof(*p));
}

doca_error_t dmesh_wake_attach(struct dmesh_conn *c, struct doca_comch_connection *connection,
                               uint32_t consumer)
{
    struct objects *o = c->objs;
    if (o->consumer_pe == NULL || connection == NULL || consumer == 0)
        return DOCA_ERROR_INVALID_VALUE;
    if (o->wake_producers == NULL) {
        o->wake_producers = calloc(DMESH_MAX_SESSIONS, sizeof(*o->wake_producers));
        if (o->wake_producers == NULL)
            return DOCA_ERROR_NO_MEMORY;
    }
    struct dmesh_wake_producer *p = NULL, *free_entry = NULL;
    for (unsigned i = 0; i < DMESH_MAX_SESSIONS; ++i) {
        struct dmesh_wake_producer *e = &o->wake_producers[i];
        if (e->state != WAKE_FREE && e->session_id == c->key.session_id &&
            e->session_epoch == c->key.session_epoch && e->consumer == consumer) {
            p = e;
            break;
        }
        if (e->state == WAKE_FREE && free_entry == NULL)
            free_entry = e;
    }
    if (p != NULL && (p->state == WAKE_FAILED || p->state == WAKE_STOPPING))
        return DOCA_ERROR_BAD_STATE;
    if (p == NULL) {
        if (free_entry == NULL)
            return DOCA_ERROR_FULL;
        p = free_entry;
        memset(p, 0, sizeof(*p));
        p->objs = o;
        p->session_id = c->key.session_id;
        p->session_epoch = c->key.session_epoch;
        p->consumer = consumer;
        pthread_mutex_t *lock = dmesh_dispatch_sdk_lock(o);
        if (lock) pthread_mutex_lock(lock);
        doca_error_t r = wake_start(p, connection);
        if (lock) pthread_mutex_unlock(lock);
        if (r != DOCA_SUCCESS) {
            DOCA_LOG_ERR("fast-path producer for session %lu: %s", (unsigned long)p->session_id,
                         doca_error_get_name(r));
            memset(p, 0, sizeof(*p));
            return r;
        }
    }
    p->refs++;
    c->wake = p;
    c->wake_desc_pending = false;
    return DOCA_SUCCESS;
}

void dmesh_wake_detach(struct dmesh_conn *c)
{
    struct dmesh_wake_producer *p = c->wake;
    if (p == NULL)
        return;
    c->wake = NULL;
    c->wake_desc_pending = false;
    if (p->refs > 0 && --p->refs == 0)
        wake_stop(p);
}

doca_error_t dmesh_wake_send_desc(struct dmesh_conn *c, uint64_t seq, uint32_t pos, uint32_t len)
{
    struct dmesh_wake_producer *p = c->wake;
    if (p == NULL)
        return DOCA_ERROR_BAD_STATE;
    if (p->state == WAKE_STARTING)
        return DOCA_ERROR_AGAIN;
    if (p->state != WAKE_RUNNING)
        return DOCA_ERROR_BAD_STATE;
    /* The task keeps a pointer to its immediate data: the flow owns it until
     * the completion, and holds one descriptor in flight at a time. */
    c->wake_imm = (struct dmesh_wake_desc){.flow_id = c->flow_id, .generation = c->generation,
                                           .seq = seq, .pos = pos, .len = len};
    struct doca_comch_producer_task_send *task;
    doca_error_t r = doca_comch_producer_task_send_alloc_init(p->producer, NULL, (uint8_t *)&c->wake_imm,
                                                              DMESH_WAKE_IMM_LEN, p->consumer, &task);
    if (r == DOCA_ERROR_NO_MEMORY)
        return DOCA_ERROR_AGAIN;
    if (r != DOCA_SUCCESS)
        return r;
    union doca_data task_data = {.u64 = (uint64_t)(uint32_t)(c - p->objs->conns) << 32 | c->generation};
    doca_task_set_user_data(doca_comch_producer_task_send_as_task(task), task_data);
    r = doca_task_submit(doca_comch_producer_task_send_as_task(task));
    if (r != DOCA_SUCCESS)
        doca_task_free(doca_comch_producer_task_send_as_task(task));
    return r;
}

void dmesh_wake_cleanup(struct objects *o)
{
    if (o->wake_producers == NULL)
        return;
    for (unsigned i = 0; i < DMESH_MAX_SESSIONS; ++i)
        if (o->wake_producers[i].state != WAKE_FREE)
            wake_stop(&o->wake_producers[i]);
    for (unsigned i = 0; i < DMESH_MAX_SESSIONS; ++i)
        if (o->wake_producers[i].state != WAKE_FREE)
            return;                     /* retained: a producer did not stop */
    free(o->wake_producers);
    o->wake_producers = NULL;
}
