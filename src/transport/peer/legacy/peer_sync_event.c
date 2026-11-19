#define _POSIX_C_SOURCE 200809L
#include "peer_sync_event.h"
#include "peer_profile.h"
#include <doca_dev.h>
#include <doca_ctx.h>
#include <doca_pe.h>
#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_mmap.h>
#include <doca_rdma_bridge.h>
#include <doca_sync_event.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct peer_sync_engine {
    struct doca_dev *dev;
    struct doca_dpa *dpa;
    struct ibv_pd *pd; /* borrowed from dev, never ibv_dealloc_pd */
    struct doca_pe *pe;
    doca_notification_handle_t fd;
    unsigned live;
    int failed, armed;
    struct peer_profile profile;
};

struct sync_value {
    struct peer_sync_events *owner;
    struct doca_sync_event *event;
    struct doca_buf *buf;
    struct doca_sync_event_task_wait_neq *wait;
    uint64_t observed, profile_observed;
    int busy, started, stopping;
};

struct peer_sync_events {
    struct peer_sync_engine *engine;
    struct doca_mmap *map;
    struct doca_buf_inventory *inventory;
    uint8_t *memory;
    uint32_t key;
    struct sync_value value[2];
};

static int checked(doca_error_t rc, const char *stage)
{
    if (rc == DOCA_SUCCESS)
        return 0;

    fprintf(stderr, "PEER_SYNC_EVENT stage=%s error=%s\n", stage, doca_error_get_name(rc));
    return -1;
}

#define TRY(call)                                                                                  \
    do {                                                                                           \
        if (checked((call), #call))                                                                \
            goto fail;                                                                             \
    } while (0)

static void
wait_done(struct doca_sync_event_task_wait_neq *task, union doca_data data, union doca_data context)
{
    (void)task;
    (void)context;

    struct sync_value *v = data.ptr;
    peer_profile_note(&v->owner->engine->profile,
                        "wait_done",
                        0,
                        (uint64_t)(v - v->owner->value),
                        v->observed,
                        v->owner->engine->armed,
                        0,
                        0,
                        0);
    v->busy = 0;

    /* Re-arm in the owner's next get, after observing the authoritative full
     * counter. Reading the backing through an unrelated C pointer would
     * bypass the Sync Event API's CPU/device visibility contract. */
}

static void wait_error(struct doca_sync_event_task_wait_neq *task,
                       union doca_data data,
                       union doca_data context)
{
    (void)task;
    (void)context;

    struct sync_value *v = data.ptr;
    peer_profile_note(&v->owner->engine->profile,
                        "wait_error",
                        1,
                        (uint64_t)(v - v->owner->value),
                        v->observed,
                        v->stopping,
                        0,
                        0,
                        0);
    v->busy = 0;

    if (!v->stopping)
        v->owner->engine->failed = 1;
}

static int engine_new(const char *name, struct doca_dpa *dpa, struct peer_sync_engine **out)
{
    if (!name || !out)
        return -1;

    *out = NULL;

    struct peer_sync_engine *e = calloc(1, sizeof(*e));
    struct doca_devinfo **list = NULL;
    uint32_t count = 0;
    if (!e)
        return -1;

    e->dpa = dpa;
    e->fd = -1;
    peer_profile_init(&e->profile, "sync_engine");
    TRY(doca_devinfo_create_list(&list, &count));

    for (uint32_t i = 0; i < count; i++) {
        char candidate[DOCA_DEVINFO_IBDEV_NAME_SIZE] = {0};
        if (doca_devinfo_get_ibdev_name(list[i], candidate, sizeof(candidate)) != DOCA_SUCCESS ||
            strcmp(name, candidate))
            continue;

        TRY(doca_sync_event_cap_task_wait_neq_is_supported(list[i]));
        TRY(doca_dev_open(list[i], &e->dev));
        break;
    }

    doca_devinfo_destroy_list(list);
    list = NULL;

    if (!e->dev)
        goto fail;

    TRY(doca_rdma_bridge_get_dev_pd(e->dev, &e->pd));
    TRY(doca_pe_create(&e->pe));
    TRY(doca_pe_set_event_mode(e->pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL));
    TRY(doca_pe_get_notification_handle(e->pe, &e->fd));
    *out = e;
    return 0;

fail:
    if (list)
        doca_devinfo_destroy_list(list);
    (void)peer_sync_engine_free(e);

    return -1;
}

int peer_sync_engine_new(const char *name, struct peer_sync_engine **out)
{
    return engine_new(name, NULL, out);
}

int peer_sync_engine_new_dpa(const char *name, struct doca_dpa *dpa, struct peer_sync_engine **out)
{
    return dpa ? engine_new(name, dpa, out) : -1;
}

struct doca_dev *peer_sync_engine_dev(struct peer_sync_engine *e)
{
    return e->dev;
}

struct ibv_pd *peer_sync_engine_pd(struct peer_sync_engine *e)
{
    return e->pd;
}

int peer_sync_engine_fd(struct peer_sync_engine *e)
{
    return e->fd;
}

void peer_sync_engine_notification(struct peer_sync_engine *e)
{
    peer_profile_add(&e->profile, PEER_PROFILE_pe_notify, 1);
    peer_profile_note(&e->profile, "pe_notify", 0, e->armed, 0, 0, 0, 0, 0);
    e->armed = 0;
}

int peer_sync_engine_progress(struct peer_sync_engine *e)
{
    if (e->failed)
        return -1;

    int moved = 0;

    /* Poll while active. Event-channel work belongs at the idle boundary. */
    for (unsigned i = 0; i < 128; i++) {
        uint64_t stamp = peer_profile_begin(&e->profile);
        int progress = doca_pe_progress(e->pe);
        peer_profile_end(&e->profile, PEER_PROFILE_PE_PROGRESS, stamp);
        peer_profile_add(&e->profile, PEER_PROFILE_pe_calls, 1);
        peer_profile_add(&e->profile, PEER_PROFILE_pe_moved, progress != 0);

        if (!progress)
            break;

        moved = 1;
    }

    return e->failed ? -1 : moved;
}

int peer_sync_engine_arm(struct peer_sync_engine *e)
{
    if (e->failed)
        return -1;

    /* A still-armed PE needs no clear/rearm syscall on each active iteration.
     * The carrier reports readable notification FDs before progressing us. */
    if (!e->armed) {
        uint64_t stamp = peer_profile_begin(&e->profile);
        peer_profile_add(&e->profile, PEER_PROFILE_pe_arms, 1);

        if (checked(doca_pe_request_notification(e->pe), "arm"))
            e->failed = 1;
        else
            e->armed = 1;

        peer_profile_end(&e->profile, PEER_PROFILE_PE_ARM, stamp);
        peer_profile_note(&e->profile, "pe_arm", 0, e->armed, e->failed, 0, 0, 0, 0);
    } else
        peer_profile_add(&e->profile, PEER_PROFILE_pe_arm_skip, 1);
    return e->failed ? -1 : 0;
}

int peer_sync_events_new(struct peer_sync_engine *e, struct peer_sync_events **out)
{
    if (!e || !out || e->failed || e->dpa)
        return -1;

    *out = NULL;

    struct peer_sync_events *s = calloc(1, sizeof(*s));
    if (!s)
        return -1;

    s->engine = e;
    e->live++;

    if (posix_memalign((void **)&s->memory, 64, 128))
        goto fail;

    memset(s->memory, 0, 128);
    TRY(doca_mmap_create(&s->map));
    TRY(doca_mmap_set_memrange(s->map, s->memory, 128));
    TRY(doca_mmap_set_permissions(s->map,
                                  DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_RDMA_READ |
                                      DOCA_ACCESS_FLAG_RDMA_WRITE | DOCA_ACCESS_FLAG_RDMA_ATOMIC));
    TRY(doca_mmap_add_dev(s->map, e->dev));
    TRY(doca_mmap_start(s->map));

    TRY(doca_rdma_bridge_get_mmap_mkey_from_pd(s->map, e->pd, &s->key));
    TRY(doca_buf_inventory_create(2, &s->inventory));
    TRY(doca_buf_inventory_start(s->inventory));

    for (unsigned i = 0; i < 2; i++) {
        struct sync_value *v = &s->value[i];
        v->owner = s;
        TRY(doca_buf_inventory_buf_get_by_data(
            s->inventory, s->map, s->memory + 64 * i, 8, &v->buf));
        TRY(doca_sync_event_create(&v->event));
        TRY(doca_sync_event_add_publisher_location_remote_net(v->event));
        TRY(doca_sync_event_add_subscriber_location_cpu(v->event, e->dev));
        TRY(doca_sync_event_set_doca_buf(v->event, v->buf));
        TRY(doca_sync_event_task_wait_neq_set_conf(v->event, wait_done, wait_error, 1));
        TRY(doca_pe_connect_ctx(e->pe, doca_sync_event_as_ctx(v->event)));
        TRY(doca_ctx_start(doca_sync_event_as_ctx(v->event)));
        v->started = 1;

        TRY(doca_sync_event_task_wait_neq_alloc_init(
            v->event, 0, UINT64_MAX, (union doca_data){.ptr = v}, &v->wait));
        TRY(doca_task_submit(doca_sync_event_task_wait_neq_as_doca_task(v->wait)));
        v->busy = 1;
        peer_profile_add(&e->profile, PEER_PROFILE_wait_submit, 1);
    }

    e->armed = 0; /* new contexts/waits must participate in the next arm */
    *out = s;
    return 0;

fail:
    if (peer_sync_events_free(s))
        *out = s; /* preserve failed retirement */

    return -1;
}

uint64_t peer_sync_events_address(struct peer_sync_events *s, unsigned which)
{
    return which < 2 ? (uintptr_t)(s->memory + 64 * which) : 0;
}

uint32_t peer_sync_events_key(struct peer_sync_events *s)
{
    return s->key;
}

int peer_sync_events_get(struct peer_sync_events *s, uint64_t *tail, uint64_t *head)
{
    uint64_t values[2];
    if (!s || s->engine->failed || s->engine->dpa)
        return -1;

    for (unsigned i = 0; i < 2; i++) {
        struct sync_value *v = &s->value[i];
        TRY(doca_sync_event_get(v->event, &values[i]));

        if (s->engine->profile.enabled && values[i] != v->profile_observed) {
            peer_profile_add(&s->engine->profile, i ? PEER_PROFILE_head_changes : PEER_PROFILE_tail_changes, 1);
            peer_profile_note(&s->engine->profile,
                                "sync_value",
                                0,
                                i,
                                v->profile_observed,
                                values[i],
                                v->busy,
                                s->engine->armed,
                                0);
            v->profile_observed = values[i];
        }

        if (!v->busy) {
            v->observed = values[i];
            doca_sync_event_task_wait_neq_set_wait_val(v->wait, v->observed);
            TRY(doca_task_submit(doca_sync_event_task_wait_neq_as_doca_task(v->wait)));
            v->busy = 1;
            peer_profile_add(&s->engine->profile, PEER_PROFILE_wait_submit, 1);
            peer_profile_note(
                &s->engine->profile, "wait_submit", 0, i, v->observed, s->engine->armed, 0, 0, 0);

            /* Rearm after submitting a replacement wait, even if its old
             * completion was found by active polling before the FD hint. */
            s->engine->armed = 0;
        }
    }

    *tail = values[0];
    *head = values[1];
    return 0;

fail:
    s->engine->failed = 1;

    return -1;
}

int peer_sync_events_free(struct peer_sync_events *s)
{
    if (!s)
        return 0;

    for (unsigned i = 0; i < 2; i++) {
        struct sync_value *v = &s->value[i];
        v->stopping = 1;

        if (v->event && v->started) {
            /* 3.5 drains an outstanding wait on stop; it does not cancel an
             * unsatisfied wait. After the QP fence ONLY, satisfy our private
             * wait using the real Sync Event API, then retire the task. The
             * generation is dead and this counter is never exposed again. */
            if (v->wait && v->busy &&
                checked(doca_sync_event_update_set(v->event, v->observed ^ UINT64_MAX),
                        "retired-wait-wake"))
                return -1;

            for (unsigned n = 0; v->busy && n < 100000; n++)
                (void)doca_pe_progress(s->engine->pe);

            if (v->busy)
                return -1;

            if (v->wait && !v->busy) {
                doca_task_free(doca_sync_event_task_wait_neq_as_doca_task(v->wait));
                v->wait = NULL;
            }

            doca_error_t rc = doca_ctx_stop(doca_sync_event_as_ctx(v->event));
            if (rc != DOCA_SUCCESS && rc != DOCA_ERROR_IN_PROGRESS)
                return checked(rc, "stop");

            enum doca_ctx_states state;
            unsigned budget = 100000;

            do {
                (void)doca_pe_progress(s->engine->pe);

                if (v->wait && !v->busy) {
                    doca_task_free(doca_sync_event_task_wait_neq_as_doca_task(v->wait));
                    v->wait = NULL;
                }
                if (checked(doca_ctx_get_state(doca_sync_event_as_ctx(v->event), &state),
                            "stop-state"))
                    return -1;
            } while (state != DOCA_CTX_STATE_IDLE && --budget);

            if (state != DOCA_CTX_STATE_IDLE) {
                fprintf(stderr,
                        "PEER_SYNC_EVENT stop-pending index=%u state=%d busy=%d\n",
                        i,
                        (int)state,
                        v->busy);
                return -1;
            } /* quarantine; never free on timeout */

            v->started = 0;
        }

        if (v->wait) {
            doca_task_free(doca_sync_event_task_wait_neq_as_doca_task(v->wait));
            v->wait = NULL;
        }

        if (v->event) {
            if (checked(doca_sync_event_destroy(v->event), "event-destroy"))
                return -1;

            v->event = NULL;
        }

        if (v->buf) {
            if (checked(doca_buf_dec_refcount(v->buf, NULL), "buf-release"))
                return -1;

            v->buf = NULL;
        }
    }

    if (s->inventory) {
        if (checked(doca_buf_inventory_destroy(s->inventory), "inventory-destroy"))
            return -1;

        s->inventory = NULL;
    }

    if (s->map) {
        if (checked(doca_mmap_destroy(s->map), "mmap-destroy"))
            return -1;

        s->map = NULL;
    }

    free(s->memory);
    s->engine->live--;
    free(s);

    return 0;
}

int peer_sync_engine_free(struct peer_sync_engine *e)
{
    if (!e)
        return 0;
    if (e->live)
        return -1;

    if (e->pe) {
        if (doca_pe_destroy(e->pe) != DOCA_SUCCESS)
            return -1;

        e->pe = NULL;
    }

    if (e->dev) {
        if (doca_dev_close(e->dev) != DOCA_SUCCESS)
            return -1;

        e->dev = NULL;
    }

    peer_profile_dump(&e->profile, stderr);
    peer_profile_free(&e->profile);
    free(e);

    return 0;
}
