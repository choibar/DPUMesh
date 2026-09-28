#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Test real fences, receive callbacks and destruction order. SDK substitutes
 * below only model device state/failures; no DPA/device is created or run. */
#include "../src/transport/common/dpa.c"

enum operation {
    OP_NONE, OP_WRITE_STOP, OP_READ_STOPPED, OP_READ_COUNT, OP_CTX_QUERY,
    OP_CTX_STOP, OP_PRODUCER_DESTROY, OP_CONSUMER_DESTROY, OP_MSGQ_STOP,
    OP_MSGQ_DESTROY, OP_CONSUMER_COMP_STOP, OP_CONSUMER_COMP_DESTROY,
    OP_PRODUCER_COMP_STOP, OP_PRODUCER_COMP_DESTROY, OP_THREAD_DESTROY,
    OP_ARG_FREE, OP_REPOST,
};
enum resource {
    SEND_PRODUCER, SEND_CONSUMER, SEND_MSGQ, RECV_PRODUCER, RECV_CONSUMER,
    RECV_MSGQ, CONSUMER_COMP, PRODUCER_COMP, RESOURCE_COUNT,
};
struct fake_resource {
    enum doca_ctx_states state;
    bool started, destroyed;
    unsigned stop_calls, destroy_calls;
};
struct fake_recv {
    struct comch_dma_comp_msg message;
    uint32_t length;
    bool submitted, freed;
};
struct fixture {
    struct objects objs;
    struct dmesh_doca_dpa_thread thread;
    struct dpa_thread_arg args;
    struct fake_resource resources[RESOURCE_COUNT];
    struct fake_recv recv;
    enum operation fail;
    void *fail_handle;
    bool hold_contexts, dpa_fatal;
    unsigned deliveries, progress_calls, stop_writes, reads, reposts, task_frees;
    unsigned thread_destroy_calls, arg_free_calls;
    bool thread_destroyed, arg_freed;
};
static struct fixture *f;

static bool fails(enum operation op, const void *handle)
{
    return f->fail == op && (f->fail_handle == NULL || f->fail_handle == handle);
}
static struct dmesh_conn *conn(void) { return &f->objs.conns[0]; }

struct doca_task *doca_comch_consumer_task_post_recv_as_task(struct doca_comch_consumer_task_post_recv *task)
{
    return (void *)task;
}
const uint8_t *doca_comch_consumer_task_post_recv_get_imm_data(const struct doca_comch_consumer_task_post_recv *task)
{
    return (const void *)&((const struct fake_recv *)task)->message;
}
uint32_t doca_comch_consumer_task_post_recv_get_imm_data_len(const struct doca_comch_consumer_task_post_recv *task)
{
    return ((const struct fake_recv *)task)->length;
}
doca_error_t doca_task_submit(struct doca_task *task)
{
    struct fake_recv *recv = (void *)task;
    assert(!recv->submitted && !recv->freed);
    ++f->reposts;
    if (fails(OP_REPOST, task)) return DOCA_ERROR_DRIVER;
    recv->submitted = true;
    return DOCA_SUCCESS;
}
void doca_task_free(struct doca_task *task)
{
    struct fake_recv *recv = (void *)task;
    assert(!recv->submitted && !recv->freed);
    recv->freed = true;
    ++f->task_frees;
}

doca_error_t doca_dpa_h2d_memcpy(struct doca_dpa *dpa, doca_dpa_dev_uintptr_t dst,
                                void *src, size_t size)
{
    assert(dpa == (void *)f);
    assert(dst == f->thread.arg + offsetof(struct dpa_thread_arg, stop));
    assert(size == sizeof(uint32_t) && *(uint32_t *)src == 1);
    ++f->stop_writes;
    if (fails(OP_WRITE_STOP, NULL)) return DOCA_ERROR_DRIVER;
    memcpy((void *)(uintptr_t)dst, src, size);
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_peek_at_last_error(const struct doca_dpa *dpa)
{
    assert(dpa == (void *)f);
    return f->dpa_fatal ? DOCA_ERROR_BAD_STATE : DOCA_SUCCESS;
}
doca_error_t doca_dpa_d2h_memcpy(struct doca_dpa *dpa, void *dst,
                                doca_dpa_dev_uintptr_t src, size_t size)
{
    assert(dpa == (void *)f);
    enum operation op = src == f->thread.arg + offsetof(struct dpa_thread_arg, stopped)
                      ? OP_READ_STOPPED : OP_READ_COUNT;
    if (op == OP_READ_COUNT)
        assert(src == f->thread.arg + offsetof(struct dpa_thread_arg, dma_submitted));
    ++f->reads;
    if (fails(op, NULL)) return DOCA_ERROR_DRIVER;
    memcpy(dst, (const void *)(uintptr_t)src, size);
    return DOCA_SUCCESS;
}

struct doca_ctx *doca_comch_producer_as_ctx(struct doca_comch_producer *producer) { return (void *)producer; }
struct doca_ctx *doca_comch_consumer_as_ctx(struct doca_comch_consumer *consumer) { return (void *)consumer; }
doca_error_t doca_ctx_get_state(const struct doca_ctx *ctx, enum doca_ctx_states *state)
{
    if (fails(OP_CTX_QUERY, ctx)) return DOCA_ERROR_DRIVER;
    *state = ((const struct fake_resource *)ctx)->state;
    return DOCA_SUCCESS;
}
doca_error_t doca_ctx_stop(struct doca_ctx *ctx)
{
    struct fake_resource *r = (void *)ctx;
    ++r->stop_calls;
    if (fails(OP_CTX_STOP, ctx)) return DOCA_ERROR_DRIVER;
    r->state = DOCA_CTX_STATE_STOPPING;
    return DOCA_ERROR_IN_PROGRESS;
}
uint8_t doca_pe_progress(struct doca_pe *pe)
{
    assert(pe == (void *)&f->objs);
    ++f->progress_calls;
    if (!f->hold_contexts)
        for (int i = 0; i < RESOURCE_COUNT; ++i)
            if (f->resources[i].state == DOCA_CTX_STATE_STOPPING)
                f->resources[i].state = DOCA_CTX_STATE_IDLE;
    if (f->deliveries) {
        --f->deliveries;
        f->recv.submitted = false;
        dmesh_doca_dpa_msgq_recv_cb((void *)&f->recv, (union doca_data){0},
                                   (union doca_data){.ptr = conn()});
        return 1;
    }
    return 0;
}
static doca_error_t destroy_endpoint(void *handle, enum operation op)
{
    struct fake_resource *r = handle;
    assert(r->state == DOCA_CTX_STATE_IDLE && !r->destroyed);
    ++r->destroy_calls;
    if (fails(op, handle)) return DOCA_ERROR_DRIVER;
    r->destroyed = true;
    return DOCA_SUCCESS;
}
doca_error_t doca_comch_producer_destroy(struct doca_comch_producer *p)
{
    return destroy_endpoint(p, OP_PRODUCER_DESTROY);
}
doca_error_t doca_comch_consumer_destroy(struct doca_comch_consumer *c)
{
    return destroy_endpoint(c, OP_CONSUMER_DESTROY);
}
static doca_error_t stop_resource(void *handle, enum operation op)
{
    struct fake_resource *r = handle;
    assert(r->started && !r->destroyed);
    ++r->stop_calls;
    if (fails(op, handle)) return DOCA_ERROR_DRIVER;
    r->started = false;
    return DOCA_SUCCESS;
}
static doca_error_t destroy_resource(void *handle, enum operation op)
{
    struct fake_resource *r = handle;
    assert(!r->started && !r->destroyed);
    ++r->destroy_calls;
    if (fails(op, handle)) return DOCA_ERROR_DRIVER;
    r->destroyed = true;
    return DOCA_SUCCESS;
}
doca_error_t doca_comch_msgq_stop(struct doca_comch_msgq *q) { return stop_resource(q, OP_MSGQ_STOP); }
doca_error_t doca_comch_msgq_destroy(struct doca_comch_msgq *q)
{
    struct dmesh_doca_dpa_msgq *msgq = q == (void *)&f->resources[SEND_MSGQ]
                                   ? &conn()->dpa_comch->send : &conn()->dpa_comch->recv;
    assert(msgq->producer == NULL && msgq->consumer == NULL);
    return destroy_resource(q, OP_MSGQ_DESTROY);
}
doca_error_t doca_comch_consumer_completion_stop(struct doca_comch_consumer_completion *c)
{
    assert(!conn()->dpa_comch->send.msgq && !conn()->dpa_comch->recv.msgq);
    return stop_resource(c, OP_CONSUMER_COMP_STOP);
}
doca_error_t doca_comch_consumer_completion_destroy(struct doca_comch_consumer_completion *c)
{
    return destroy_resource(c, OP_CONSUMER_COMP_DESTROY);
}
doca_error_t doca_dpa_completion_stop(struct doca_dpa_completion *c)
{
    assert(!conn()->dpa_comch->send.msgq && !conn()->dpa_comch->recv.msgq);
    return stop_resource(c, OP_PRODUCER_COMP_STOP);
}
doca_error_t doca_dpa_completion_destroy(struct doca_dpa_completion *c)
{
    return destroy_resource(c, OP_PRODUCER_COMP_DESTROY);
}
doca_error_t doca_dpa_thread_destroy(struct doca_dpa_thread *thread)
{
    assert(thread == (void *)&f->thread && !f->thread_destroyed);
    ++f->thread_destroy_calls;
    if (fails(OP_THREAD_DESTROY, thread)) return DOCA_ERROR_DRIVER;
    f->thread_destroyed = true;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_mem_free(struct doca_dpa *dpa, doca_dpa_dev_uintptr_t ptr)
{
    assert(dpa == (void *)f && ptr == (uintptr_t)&f->args);
    assert(f->thread_destroyed && !f->arg_freed);
    ++f->arg_free_calls;
    if (fails(OP_ARG_FREE, NULL)) return DOCA_ERROR_DRIVER;
    f->arg_freed = true;
    return DOCA_SUCCESS;
}

static void create_fixture(void)
{
    f = calloc(1, sizeof(*f));
    assert(f);
    conn()->objs = &f->objs;
    conn()->dpa_thread = &f->thread;
    conn()->dpa_comch = calloc(1, sizeof(*conn()->dpa_comch));
    assert(conn()->dpa_comch);
    f->objs.consumer_pe = (void *)&f->objs;
    f->thread.dpa = (void *)f;
    f->thread.thread = (void *)&f->thread;
    f->thread.arg = (uintptr_t)&f->args;
    f->thread.running = true;
    f->args.stopped = 1;
    f->recv.message = (struct comch_dma_comp_msg){.type = COMCH_MSG_TYPE_DMA_COMPLETED,
                                                 .pos = 8, .length = 32, .count = 4};
    f->recv.length = sizeof(f->recv.message);
}
static void release_fixture(void)
{
    free(conn()->dpa_comch);
    free(f);
    f = NULL;
}
static void populate_resources(void)
{
    struct dmesh_doca_dpa_comch *c = conn()->dpa_comch;
    for (int i = 0; i < RESOURCE_COUNT; ++i) {
        f->resources[i].state = DOCA_CTX_STATE_RUNNING;
        f->resources[i].started = true;
    }
    c->send.producer = (void *)&f->resources[SEND_PRODUCER];
    c->send.consumer = (void *)&f->resources[SEND_CONSUMER];
    c->send.msgq = (void *)&f->resources[SEND_MSGQ];
    c->send.started = true;
    c->recv.producer = (void *)&f->resources[RECV_PRODUCER];
    c->recv.consumer = (void *)&f->resources[RECV_CONSUMER];
    c->recv.msgq = (void *)&f->resources[RECV_MSGQ];
    c->recv.started = true;
    c->consumer_comp = (void *)&f->resources[CONSUMER_COMP];
    c->consumer_comp_started = true;
    c->producer_comp = (void *)&f->resources[PRODUCER_COMP];
    c->producer_comp_started = true;
}
static void *retained_handle(enum resource r)
{
    struct dmesh_doca_dpa_comch *c = conn()->dpa_comch;
    switch (r) {
    case SEND_PRODUCER: return c->send.producer;
    case SEND_CONSUMER: return c->send.consumer;
    case SEND_MSGQ: return c->send.msgq;
    case RECV_PRODUCER: return c->recv.producer;
    case RECV_CONSUMER: return c->recv.consumer;
    case RECV_MSGQ: return c->recv.msgq;
    case CONSUMER_COMP: return c->consumer_comp;
    case PRODUCER_COMP: return c->producer_comp;
    default: abort();
    }
}

static void test_quiesce_requires_copy_completion(void)
{
    create_fixture();
    f->args.dma_submitted = 2;
    conn()->dpa_comch->dma_completed = 1;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_TIME_OUT);
    assert(!f->thread.quiesced && f->args.stop == 1);
    assert(f->thread.thread && f->thread.arg && conn()->dpa_comch);
    assert(f->thread_destroy_calls == 0 && f->task_frees == 0);
    f->deliveries = 1;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_SUCCESS);
    assert(f->thread.quiesced && conn()->dpa_comch->dma_completed == 2);
    assert(f->reposts == 1 && f->objs.recv_msg_cnt == 4);
    unsigned writes = f->stop_writes;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_SUCCESS);
    assert(f->stop_writes == writes);
    release_fixture();
}

static void test_quiesce_faults(void)
{
    static const enum operation errors[] = {OP_WRITE_STOP, OP_READ_STOPPED, OP_READ_COUNT};
    for (size_t i = 0; i < sizeof(errors) / sizeof(errors[0]); ++i) {
        create_fixture();
        f->fail = errors[i];
        assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_DRIVER);
        assert(!f->thread.quiesced && f->thread.thread && f->thread.arg);
        f->fail = OP_NONE;
        assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_SUCCESS);
        release_fixture();
    }
    create_fixture();
    f->args.stopped = 0;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_TIME_OUT);
    assert(!f->thread.quiesced);
    f->args.stopped = 1;
    conn()->dpa_comch->completion_error = true;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_IO_FAILED);
    assert(!f->thread.quiesced);
    conn()->dpa_comch->completion_error = false;
    conn()->dpa_comch->dma_completed = 1;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_IO_FAILED);
    assert(!f->thread.quiesced);
    release_fixture();
}

static void test_quiesce_step_never_waits(void)
{
    create_fixture();
    f->args.stopped = 0;
    assert(dmesh_doca_dpa_quiesce_step(conn()) == DOCA_ERROR_AGAIN);
    assert(f->stop_writes == 1 && f->reads == 1 && f->progress_calls == 1);
    assert(dmesh_doca_dpa_quiesce_step(conn()) == DOCA_ERROR_AGAIN);
    assert(f->stop_writes == 1 && f->reads == 2);
    f->args.stopped = 1;
    f->args.dma_submitted = 1;
    assert(dmesh_doca_dpa_quiesce_step(conn()) == DOCA_ERROR_AGAIN);
    assert(f->reads == 4 && !f->thread.quiesced);
    f->deliveries = 1;
    assert(dmesh_doca_dpa_quiesce_step(conn()) == DOCA_SUCCESS);
    assert(f->thread.quiesced && f->stop_writes == 1 && f->reads == 4);
    release_fixture();

    create_fixture();
    f->args.stopped = 0;
    assert(dmesh_doca_dpa_quiesce_step(conn()) == DOCA_ERROR_AGAIN);
    f->thread.quiesce_deadline_ns = 0;
    assert(dmesh_doca_dpa_quiesce_step(conn()) == DOCA_ERROR_TIME_OUT);
    assert(!f->thread.quiesced && f->stop_writes == 1);
    release_fixture();
}

static void test_failed_dpa_is_not_polled(void)
{
    create_fixture();
    f->dpa_fatal = true;
    assert(dmesh_doca_dpa_quiesce_step(conn()) == DOCA_ERROR_BAD_STATE);
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_BAD_STATE);
    assert(f->stop_writes == 0 && f->reads == 0 && !f->thread.quiesced);
    f->dpa_fatal = false;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_SUCCESS);
    release_fixture();
}

static void test_never_run_needs_no_fence(void)
{
    create_fixture();
    assert(dmesh_doca_dpa_quiesce_checked(NULL) == DOCA_SUCCESS);
    conn()->dpa_thread = NULL;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_SUCCESS);
    conn()->dpa_thread = &f->thread;
    f->thread.running = false;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_SUCCESS);
    assert(f->stop_writes == 0 && f->reads == 0 && f->progress_calls == 0);
    f->thread.running = true;
    f->thread.thread = NULL;
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_BAD_STATE);
    assert(!f->thread.quiesced);
    release_fixture();
}

static void test_recv_counter_and_shutdown(void)
{
    create_fixture();
    f->recv.message.count = 9;
    dmesh_doca_dpa_msgq_recv_cb((void *)&f->recv, (union doca_data){0},
                               (union doca_data){.ptr = conn()});
    assert(conn()->dpa_comch->dma_completed == 1 && f->objs.recv_msg_cnt == 9);
    assert(f->reposts == 1 && f->task_frees == 0);
    f->recv.submitted = false;
    f->recv.message.type = (enum comch_msg_type)UINT16_MAX;
    dmesh_doca_dpa_msgq_recv_cb((void *)&f->recv, (union doca_data){0},
                               (union doca_data){.ptr = conn()});
    assert(conn()->dpa_comch->dma_completed == 1 && f->objs.recv_msg_cnt == 9);
    assert(f->reposts == 2 && conn()->dpa_comch->completion_error);
    conn()->dpa_comch->stopping = true;
    f->recv.submitted = false;
    dmesh_doca_dpa_msgq_recv_cb((void *)&f->recv, (union doca_data){0},
                               (union doca_data){.ptr = conn()});
    assert(f->reposts == 2 && f->task_frees == 1);
    release_fixture();

    create_fixture();
    dmesh_doca_dpa_msgq_recv_error_cb((void *)&f->recv, (union doca_data){0},
                                     (union doca_data){.ptr = conn()});
    assert(conn()->dpa_comch->completion_error && f->task_frees == 1);
    assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_IO_FAILED);
    release_fixture();
    create_fixture();
    conn()->dpa_comch->stopping = true;
    dmesh_doca_dpa_msgq_recv_error_cb((void *)&f->recv, (union doca_data){0},
                                     (union doca_data){.ptr = conn()});
    assert(!conn()->dpa_comch->completion_error && f->task_frees == 1);
    release_fixture();
}

static void test_malformed_completion_cannot_satisfy_fence(void)
{
    static const uint32_t lengths[] = {0, sizeof(enum comch_msg_type),
                                      sizeof(struct comch_dma_comp_msg) - 1,
                                      sizeof(struct comch_dma_comp_msg) + 1};
    for (size_t i = 0; i < sizeof(lengths) / sizeof(lengths[0]); ++i) {
        create_fixture();
        f->recv.length = lengths[i];
        dmesh_doca_dpa_msgq_recv_cb((void *)&f->recv, (union doca_data){0},
                                   (union doca_data){.ptr = conn()});
        assert(conn()->dpa_comch->completion_error);
        assert(conn()->dpa_comch->dma_completed == 0 && f->objs.recv_msg_cnt == 0);
        assert(dmesh_doca_dpa_quiesce_checked(conn()) == DOCA_ERROR_IO_FAILED);
        assert(!f->thread.quiesced);
        release_fixture();
    }
}

static void test_msgq_destroy_faults(void)
{
    static const struct { enum operation op; enum resource resource; } errors[] = {
        {OP_CTX_QUERY, SEND_PRODUCER}, {OP_CTX_STOP, SEND_PRODUCER},
        {OP_PRODUCER_DESTROY, SEND_PRODUCER}, {OP_CONSUMER_DESTROY, SEND_CONSUMER},
        {OP_MSGQ_STOP, SEND_MSGQ}, {OP_MSGQ_DESTROY, SEND_MSGQ},
        {OP_CTX_QUERY, RECV_CONSUMER}, {OP_CTX_STOP, RECV_CONSUMER},
        {OP_PRODUCER_DESTROY, RECV_PRODUCER}, {OP_CONSUMER_DESTROY, RECV_CONSUMER},
        {OP_MSGQ_STOP, RECV_MSGQ}, {OP_MSGQ_DESTROY, RECV_MSGQ},
        {OP_CONSUMER_COMP_STOP, CONSUMER_COMP}, {OP_CONSUMER_COMP_DESTROY, CONSUMER_COMP},
        {OP_PRODUCER_COMP_STOP, PRODUCER_COMP}, {OP_PRODUCER_COMP_DESTROY, PRODUCER_COMP},
    };
    for (size_t i = 0; i < sizeof(errors) / sizeof(errors[0]); ++i) {
        create_fixture();
        populate_resources();
        assert(dmesh_doca_dpa_comch_destroy_checked(conn()) == DOCA_ERROR_BAD_STATE);
        assert(f->progress_calls == 0);
        f->thread.quiesced = true;
        struct dmesh_doca_dpa_comch *original = conn()->dpa_comch;
        f->fail = errors[i].op;
        f->fail_handle = &f->resources[errors[i].resource];
        assert(dmesh_doca_dpa_comch_destroy_checked(conn()) == DOCA_ERROR_DRIVER);
        assert(conn()->dpa_comch == original && original->stopping);
        assert(retained_handle(errors[i].resource) == f->fail_handle);
        assert(!f->resources[errors[i].resource].destroyed);
        f->fail = OP_NONE;
        assert(dmesh_doca_dpa_comch_destroy_checked(conn()) == DOCA_SUCCESS);
        assert(conn()->dpa_comch == NULL);
        for (int r = 0; r < RESOURCE_COUNT; ++r) assert(f->resources[r].destroyed);
        assert(dmesh_doca_dpa_comch_destroy_checked(conn()) == DOCA_SUCCESS);
        release_fixture();
    }
}

static void test_context_stop_timeout_retry(void)
{
    create_fixture();
    populate_resources();
    f->thread.quiesced = true;
    f->hold_contexts = true;
    assert(dmesh_doca_dpa_comch_destroy_checked(conn()) == DOCA_ERROR_TIME_OUT);
    assert(conn()->dpa_comch && retained_handle(SEND_PRODUCER));
    assert(!f->resources[SEND_PRODUCER].destroyed);
    unsigned stops = f->resources[SEND_PRODUCER].stop_calls;
    f->hold_contexts = false;
    assert(dmesh_doca_dpa_comch_destroy_checked(conn()) == DOCA_SUCCESS);
    assert(f->resources[SEND_PRODUCER].stop_calls == stops);
    release_fixture();
}

static void test_thread_destroy_retry(void)
{
    create_fixture();
    assert(dmesh_doca_dpa_thread_destroy_checked(NULL) == DOCA_SUCCESS);
    assert(dmesh_doca_dpa_thread_destroy_checked(&f->thread) == DOCA_ERROR_BAD_STATE);
    assert(f->thread_destroy_calls == 0 && f->arg_free_calls == 0);
    f->thread.quiesced = true;
    f->fail = OP_THREAD_DESTROY;
    assert(dmesh_doca_dpa_thread_destroy_checked(&f->thread) == DOCA_ERROR_DRIVER);
    assert(f->thread.thread && f->thread.arg && f->thread.running);
    f->fail = OP_ARG_FREE;
    assert(dmesh_doca_dpa_thread_destroy_checked(&f->thread) == DOCA_ERROR_DRIVER);
    assert(!f->thread.thread && !f->thread.running && f->thread.arg);
    unsigned destroys = f->thread_destroy_calls;
    f->fail = OP_NONE;
    assert(dmesh_doca_dpa_thread_destroy_checked(&f->thread) == DOCA_SUCCESS);
    assert(!f->thread.arg && !f->thread.quiesced && f->arg_freed);
    assert(f->thread_destroy_calls == destroys);
    assert(dmesh_doca_dpa_thread_destroy_checked(&f->thread) == DOCA_SUCCESS);
    release_fixture();
}

int main(void)
{
    test_quiesce_requires_copy_completion();
    test_quiesce_faults();
    test_quiesce_step_never_waits();
    test_failed_dpa_is_not_polled();
    test_never_run_needs_no_fence();
    test_recv_counter_and_shutdown();
    test_malformed_completion_cannot_satisfy_fence();
    test_msgq_destroy_faults();
    test_context_stop_timeout_retry();
    test_thread_destroy_retry();
    puts("dpa_cleanup_test: PASS");
    return 0;
}
