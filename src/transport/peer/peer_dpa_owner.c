#include "../common/dpa_runtime.h"
#include "peer_dpa_owner.h"
#include "peer_dpa_abi.h"
#include "peer_write_sync_dpa.h"
#include <doca_dpa.h>
#include <doca_comch_msgq.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_ctx.h>
#include <doca_pe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CREDITS 128u
extern doca_dpa_func_t dpumesh_peer_run, dpumesh_peer_yield, dpumesh_peer_start;

struct peer_dpa_owner {
    struct peer_dpa_runtime runtime;
    struct doca_dpa_thread *thread, *helper;
    struct doca_dpa_eu_affinity *affinity;
    struct doca_dpa_notification_completion *resume, *yield;
    struct doca_dpa_completion *completion;
    struct doca_pe *pe;
    struct doca_comch_msgq *msgq;
    struct doca_comch_consumer *consumer;
    struct doca_comch_producer *producer;
    struct peer_dpa_thread_arg arg;
    uint64_t address, resume_handle;
    doca_notification_handle_t fd;
    unsigned posted;
    int running, stopping, failed;
    int thread_started, helper_started, resume_started, yield_started;
    int completion_started, msgq_started;
};

static int checked(doca_error_t rc, const char *operation)
{
    if (rc == DOCA_SUCCESS)
        return 0;
    fprintf(stderr, "peer DPA %s: %s\n", operation, doca_error_get_descr(rc));
    return -1;
}
#define TRY(call) do { if (checked((call), #call)) return -1; } while (0)

static void receive(struct doca_comch_consumer_task_post_recv *recv,
                    union doca_data task_data, union doca_data ctx_data)
{
    (void)task_data;
    struct peer_dpa_owner *o = ctx_data.ptr;
    struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv);
    o->posted--;
    if (o->stopping) {
        doca_task_free(task);
        return;
    }

    const void *data = doca_comch_consumer_task_post_recv_get_imm_data(recv);
    uint32_t bytes = doca_comch_consumer_task_post_recv_get_imm_data_len(recv);
    unsigned worker = o->runtime.owner_worker, workers = o->runtime.workers;
    int valid = 0;
    if (data && bytes == sizeof(struct dpa_rdma_counter_msg)) {
        struct dpa_rdma_counter_msg m;
        memcpy(&m, data, sizeof(m));
        valid = dpa_rdma_counter_valid(&m, bytes, worker, workers, workers);
        if (valid)
            peer_wsd_counter_message(&m);
    } else if (data && bytes == sizeof(struct dpa_rdma_notify_msg)) {
        struct dpa_rdma_notify_msg m;
        memcpy(&m, data, sizeof(m));
        valid = dpa_rdma_notify_valid(&m, bytes, worker, workers, workers);
    }
    if (!valid)
        o->failed = 1;

    if (doca_task_submit(task) == DOCA_SUCCESS)
        o->posted++;
    else {
        o->failed = 1;
        doca_task_free(task);
    }
}

static void receive_error(struct doca_comch_consumer_task_post_recv *recv,
                          union doca_data task_data, union doca_data ctx_data)
{
    (void)task_data;
    struct peer_dpa_owner *o = ctx_data.ptr;
    o->posted--;
    if (!o->stopping)
        o->failed = 1;
    doca_task_free(doca_comch_consumer_task_post_recv_as_task(recv));
}

static int attach(void *ctx, unsigned eu, uint64_t state)
{
    struct peer_dpa_owner *o = ctx;
    if (eu != o->runtime.owner_worker || o->running || o->arg.rdma_state || !state)
        return -1;

    o->arg.rdma_state = state;
    TRY(DMESH_DPA_CALL(doca_dpa_h2d_memcpy(o->runtime.dpa, o->address, &o->arg, sizeof(o->arg))));
    /* Conservatively retain on a failed run/RPC; activation may have reached
     * hardware. Transport STOP is the only later permission to free state. */
    o->running = 1;
    TRY(DMESH_DPA_CALL(doca_dpa_thread_run(o->thread)));
    uint64_t result;
    TRY(DMESH_DPA_CALL(doca_dpa_rpc(o->runtime.dpa, dpumesh_peer_start, &result, o->resume_handle)));
    return result ? -1 : 0;
}

static int cancel_attach(void *ctx, unsigned eu, uint64_t state)
{
    struct peer_dpa_owner *o = ctx;
    if (eu != o->runtime.owner_worker)
        return -1;
    if (o->running)
        return 0;
    if (o->arg.rdma_state && o->arg.rdma_state != state)
        return -1;
    o->arg.rdma_state = 0;
    return 1;
}

int peer_dpa_owner_new(struct doca_dev *dev, struct doca_dpa *dpa, unsigned worker,
                       unsigned workers, unsigned eu, struct peer_dpa_owner **out)
{
    if (!dev || !dpa || !out || !workers || workers > PEER_RDMA_MAX_WORKERS ||
        workers > DPA_RDMA_CONTEXTS || worker >= workers)
        return -1;
    struct peer_dpa_owner *o = calloc(1, sizeof(*o));
    if (!o)
        return -1;
    *out = o; /* Even a partial failure remains reachable for checked cleanup. */
    o->fd = -1;
    o->runtime = (struct peer_dpa_runtime){.dev = dev, .dpa = dpa, .workers = workers,
        .rdma_threads = workers, .owner_worker = worker, .attach = attach,
        .cancel_attach = cancel_attach, .attach_ctx = o};
    o->arg.eu_index = worker;

    TRY(DMESH_DPA_CALL(doca_dpa_mem_alloc(dpa, sizeof(o->arg), &o->address)));
    TRY(DMESH_DPA_CALL(doca_dpa_eu_affinity_create(dpa, &o->affinity)));
    TRY(DMESH_DPA_CALL(doca_dpa_eu_affinity_set(o->affinity, eu)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_create(dpa, &o->thread)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_set_func_arg(o->thread, dpumesh_peer_run, o->address)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_set_affinity(o->thread, o->affinity)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_start(o->thread)));
    o->thread_started = 1;
    TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_create(dpa, o->thread, &o->resume)));
    TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_start(o->resume)));
    o->resume_started = 1;
    TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_get_dpa_handle(o->resume, &o->resume_handle)));

    TRY(DMESH_DPA_CALL(doca_dpa_thread_create(dpa, &o->helper)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_set_func_arg(o->helper, dpumesh_peer_yield, o->resume_handle)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_set_affinity(o->helper, o->affinity)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_start(o->helper)));
    o->helper_started = 1;
    TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_create(dpa, o->helper, &o->yield)));
    TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_start(o->yield)));
    o->yield_started = 1;
    TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_get_dpa_handle(o->yield, &o->arg.yield_notification)));
    TRY(DMESH_DPA_CALL(doca_dpa_thread_run(o->helper)));

    TRY(doca_pe_create(&o->pe));
    TRY(doca_pe_get_notification_handle(o->pe, &o->fd));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_create(dpa, CREDITS, &o->completion)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_start(o->completion)));
    o->completion_started = 1;
    TRY(DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(o->completion, &o->arg.dpa_producer_comp)));
    TRY(doca_comch_msgq_create(dev, &o->msgq));
    TRY(doca_comch_msgq_set_max_num_consumers(o->msgq, 1));
    TRY(doca_comch_msgq_set_max_num_producers(o->msgq, 1));
    TRY(doca_comch_msgq_set_dpa_producer(o->msgq, dpa));
    TRY(doca_comch_msgq_start(o->msgq));
    o->msgq_started = 1;
    TRY(doca_comch_msgq_consumer_create(o->msgq, &o->consumer));
    TRY(doca_ctx_set_user_data(doca_comch_consumer_as_ctx(o->consumer), (union doca_data){.ptr = o}));
    TRY(doca_comch_consumer_set_imm_data_len(o->consumer, sizeof(struct dpa_rdma_counter_msg)));
    TRY(doca_pe_connect_ctx(o->pe, doca_comch_consumer_as_ctx(o->consumer)));
    TRY(doca_comch_consumer_task_post_recv_set_conf(o->consumer, receive, receive_error, CREDITS));
    TRY(doca_ctx_start(doca_comch_consumer_as_ctx(o->consumer)));
    TRY(doca_comch_consumer_get_id(o->consumer, &o->arg.dpu_consumer_id));
    TRY(doca_comch_msgq_producer_create(o->msgq, &o->producer));
    TRY(doca_ctx_set_datapath_on_dpa(doca_comch_producer_as_ctx(o->producer), dpa));
    TRY(doca_comch_producer_set_dev_max_num_send(o->producer, CREDITS));
    TRY(doca_comch_producer_dpa_completion_attach(o->producer, o->completion));
    TRY(doca_ctx_start(doca_comch_producer_as_ctx(o->producer)));
    TRY(doca_comch_producer_get_dpa_handle(o->producer, &o->arg.dpa_producer));
    for (unsigned i = 0; i < CREDITS; i++) {
        struct doca_comch_consumer_task_post_recv *recv;
        TRY(doca_comch_consumer_task_post_recv_alloc_init(o->consumer, NULL, &recv));
        struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv);
        if (checked(doca_task_submit(task), "initial receive")) {
            doca_task_free(task);
            return -1;
        }
        o->posted++;
    }
    return 0;
}

const struct peer_dpa_runtime *peer_dpa_owner_runtime(struct peer_dpa_owner *o) { return &o->runtime; }
int peer_dpa_owner_fd(struct peer_dpa_owner *o) { return o->fd; }
int peer_dpa_owner_progress(struct peer_dpa_owner *o)
{
    unsigned work = 0;
    while (work < 2 * CREDITS && doca_pe_progress(o->pe))
        work++;
    return o->failed ? -1 : !!work;
}
int peer_dpa_owner_arm(struct peer_dpa_owner *o)
{
    if (o->failed)
        return -1;
    TRY(doca_pe_request_notification(o->pe));
    return peer_wsd_arm_worker(o->runtime.owner_worker);
}
int peer_dpa_owner_disarm(struct peer_dpa_owner *o)
{
    peer_wsd_disarm_worker(o->runtime.owner_worker);
    return checked(doca_pe_clear_notification(o->pe, o->fd), "clear notification");
}

static int stop_context(struct peer_dpa_owner *o, struct doca_ctx *ctx)
{
    doca_error_t rc = doca_ctx_stop(ctx);
    if (rc != DOCA_SUCCESS && rc != DOCA_ERROR_IN_PROGRESS)
        return checked(rc, "context stop");
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
        enum doca_ctx_states state;
        TRY(doca_ctx_get_state(ctx, &state));
        if (state == DOCA_CTX_STATE_IDLE)
            return 0;
        doca_pe_progress(o->pe);
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (now.tv_sec - start.tv_sec >= 5)
            return -1;
    }
}

int peer_dpa_owner_free(struct peer_dpa_owner **owner)
{
    struct peer_dpa_owner *o = *owner;
    if (!o)
        return 0;
    struct doca_dpa *dpa = o->runtime.dpa;
    if (o->running) {
        struct peer_dpa_thread_arg arg;
        TRY(DMESH_DPA_CALL(doca_dpa_d2h_memcpy(dpa, &arg, o->address, sizeof(arg))));
        if (arg.rdma_state)
            return -1;
        for (unsigned i = 0; i < DPA_RDMA_CONTEXTS; i++)
            if (arg.rdma_states[i])
                return -1;
    }
    if (o->resume_started) { TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_stop(o->resume))); o->resume_started = 0; }
    if (o->yield_started) { TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_stop(o->yield))); o->yield_started = 0; }
    if (o->resume) { TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_destroy(o->resume))); o->resume = NULL; }
    if (o->yield) { TRY(DMESH_DPA_CALL(doca_dpa_notification_completion_destroy(o->yield))); o->yield = NULL; }
    if (o->thread_started) { TRY(DMESH_DPA_CALL(doca_dpa_thread_stop(o->thread))); o->thread_started = 0; o->running = 0; }
    if (o->helper_started) { TRY(DMESH_DPA_CALL(doca_dpa_thread_stop(o->helper))); o->helper_started = 0; }
    o->stopping = 1;
    if (o->producer) {
        if (stop_context(o, doca_comch_producer_as_ctx(o->producer))) return -1;
        TRY(doca_comch_producer_destroy(o->producer)); o->producer = NULL;
    }
    if (o->consumer) {
        if (stop_context(o, doca_comch_consumer_as_ctx(o->consumer)) || o->posted) return -1;
        TRY(doca_comch_consumer_destroy(o->consumer)); o->consumer = NULL;
    }
    if (o->msgq_started) { TRY(doca_comch_msgq_stop(o->msgq)); o->msgq_started = 0; }
    if (o->msgq) { TRY(doca_comch_msgq_destroy(o->msgq)); o->msgq = NULL; }
    if (o->completion_started) { TRY(DMESH_DPA_CALL(doca_dpa_completion_stop(o->completion))); o->completion_started = 0; }
    if (o->completion) { TRY(DMESH_DPA_CALL(doca_dpa_completion_destroy(o->completion))); o->completion = NULL; }
    if (o->thread) { TRY(DMESH_DPA_CALL(doca_dpa_thread_destroy(o->thread))); o->thread = NULL; }
    if (o->helper) { TRY(DMESH_DPA_CALL(doca_dpa_thread_destroy(o->helper))); o->helper = NULL; }
    if (o->affinity) { TRY(DMESH_DPA_CALL(doca_dpa_eu_affinity_destroy(o->affinity))); o->affinity = NULL; }
    if (o->address) { TRY(DMESH_DPA_CALL(doca_dpa_mem_free(dpa, o->address))); o->address = 0; }
    if (o->pe) { TRY(doca_pe_destroy(o->pe)); o->pe = NULL; }
    free(o);
    *owner = NULL;
    return 0;
}
