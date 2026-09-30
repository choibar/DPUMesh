#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Real cleanup, queue management and completion callbacks; only the SDK is
 * replaced. A submitted fake task cannot be freed, and an IDLE transition
 * requires every allocated task to have been returned, like the SDK contract. */
#define DMESH_DMA_STOP_MAX_POLLS 4u
#include "../src/transport/dpu/dma.c"

struct test_buffer { int refs, releases; };
struct test_task {
    struct dma_task_entry *entry;
    const struct doca_buf *src;
    struct doca_buf *dst;
    bool submitted, freed, fail_completion;
};
struct test_fixture {
    struct objects objs;
    struct test_task tasks[3];
    struct test_buffer buffers[6];
    enum doca_ctx_states state;
    doca_error_t query_error, stop_error, destroy_error, inventory_error, submit_error;
    struct doca_buf *release_error;
    bool complete_on_progress;
    unsigned lend_buffers;  /* buffers[4..] handed to chained submissions */
    unsigned task_count, free_count, progress_count, submit_count, destroy_count, inventory_count;
};
static struct test_fixture *fixture;

struct doca_ctx *doca_dma_as_ctx(struct doca_dma *dma) { return (void *)dma; }
struct doca_task *doca_dma_task_memcpy_as_task(struct doca_dma_task_memcpy *task) { return (void *)task; }
union doca_data doca_task_get_user_data(const struct doca_task *task)
{
    return (union doca_data){.ptr = ((const struct test_task *)task)->entry};
}
doca_error_t doca_task_get_status(const struct doca_task *task)
{
    return ((const struct test_task *)task)->fail_completion ? DOCA_ERROR_UNEXPECTED : DOCA_SUCCESS;
}
const struct doca_buf *doca_dma_task_memcpy_get_src(const struct doca_dma_task_memcpy *task)
{
    return ((const struct test_task *)task)->src;
}
struct doca_buf *doca_dma_task_memcpy_get_dst(const struct doca_dma_task_memcpy *task)
{
    return ((const struct test_task *)task)->dst;
}
void doca_dma_task_memcpy_set_src(struct doca_dma_task_memcpy *task, const struct doca_buf *src)
{
    ((struct test_task *)task)->src = src;
}
void doca_dma_task_memcpy_set_dst(struct doca_dma_task_memcpy *task, struct doca_buf *dst)
{
    ((struct test_task *)task)->dst = dst;
}
doca_error_t doca_buf_dec_refcount(struct doca_buf *buf, uint16_t *refcount)
{
    struct test_buffer *buffer = (void *)buf;
    if (fixture->release_error == buf) return DOCA_ERROR_IN_USE;
    assert(buffer->refs > 0);
    --buffer->refs;
    ++buffer->releases;
    if (refcount) *refcount = (uint16_t)buffer->refs;
    return DOCA_SUCCESS;
}
void doca_task_free(struct doca_task *task)
{
    struct test_task *t = (void *)task;
    assert(!t->submitted && !t->freed);
    assert(t->src == NULL && t->dst == NULL);
    t->freed = true;
    ++fixture->free_count;
}
doca_error_t doca_task_submit(struct doca_task *task)
{
    struct test_task *t = (void *)task;
    assert(!t->submitted && !t->freed);
    ++fixture->submit_count;
    if (fixture->submit_error) return fixture->submit_error;
    t->submitted = true;
    return DOCA_SUCCESS;
}
doca_error_t doca_ctx_get_state(const struct doca_ctx *ctx, enum doca_ctx_states *state)
{
    assert(ctx == (void *)fixture);
    if (fixture->query_error) return fixture->query_error;
    *state = fixture->state;
    return DOCA_SUCCESS;
}
doca_error_t doca_ctx_stop(struct doca_ctx *ctx)
{
    assert(ctx == (void *)fixture);
    if (fixture->stop_error) return fixture->stop_error;
    fixture->state = DOCA_CTX_STATE_STOPPING;
    return DOCA_ERROR_IN_PROGRESS;
}
uint8_t doca_pe_progress(struct doca_pe *pe)
{
    assert(pe == (void *)&fixture->objs);
    ++fixture->progress_count;
    if (!fixture->complete_on_progress) return 0;
    for (unsigned i = 0; i < fixture->task_count; ++i) {
        struct test_task *t = &fixture->tasks[i];
        if (!t->submitted) continue;
        t->submitted = false;
        union doca_data task_data = {.ptr = t->entry};
        union doca_data ctx_data = {.ptr = &fixture->objs.conns[0]};
        if (t->fail_completion)
            dmesh_doca_dpa_dma_task_error_cb((void *)t, task_data, ctx_data);
        else
            dmesh_doca_dpa_dma_task_completed_cb((void *)t, task_data, ctx_data);
        return 1;
    }
    if (fixture->free_count == fixture->task_count)
        fixture->state = DOCA_CTX_STATE_IDLE;
    return 0;
}
doca_error_t doca_dma_destroy(struct doca_dma *dma)
{
    assert(dma == (void *)fixture && fixture->state == DOCA_CTX_STATE_IDLE);
    assert(fixture->free_count == fixture->task_count);
    ++fixture->destroy_count;
    return fixture->destroy_error;
}
doca_error_t doca_buf_inventory_destroy(struct doca_buf_inventory *inventory)
{
    assert(inventory == (void *)fixture);
    assert(fixture->objs.conns[0].dma_ctx == NULL);
    for (unsigned i = 0; i < fixture->task_count * 2; ++i)
        assert(fixture->buffers[i].refs == 0);
    ++fixture->inventory_count;
    return fixture->inventory_error;
}
doca_error_t doca_buf_inventory_buf_get_by_args(struct doca_buf_inventory *inventory,
                                                struct doca_mmap *mmap,
                                                void *addr, size_t len,
                                                void *data, size_t data_len,
                                                struct doca_buf **buf)
{
    (void)inventory; (void)mmap; (void)addr; (void)len;
    (void)data; (void)data_len;
    assert(fixture->lend_buffers > 0 && "completion chained another DMA during shutdown");
    struct test_buffer *lent = &fixture->buffers[6 - fixture->lend_buffers--];
    ++lent->refs;
    *buf = (void *)lent;
    return DOCA_SUCCESS;
}

static struct dmesh_conn *create_fixture(unsigned count)
{
    fixture = calloc(1, sizeof(*fixture));
    assert(fixture && count <= 3);
    struct dmesh_conn *conn = &fixture->objs.conns[0];
    fixture->state = DOCA_CTX_STATE_RUNNING;
    fixture->complete_on_progress = true;
    fixture->task_count = count;
    fixture->objs.consumer_pe = (void *)&fixture->objs;
    conn->objs = &fixture->objs;
    conn->state = DMESH_CONN_RUNNING;
    conn->dma_ctx = (void *)fixture;
    conn->buf_inv = (void *)fixture;
    conn->num_dma_tasks = (int)count;
    conn->dma_task_entries = calloc(count ? count : 1, sizeof(*conn->dma_task_entries));
    conn->dma_pending = calloc(1, sizeof(*conn->dma_pending));
    conn->recv_segs = calloc(1, sizeof(*conn->recv_segs));
    assert(conn->dma_task_entries && conn->dma_pending && conn->recv_segs);
    TAILQ_INIT(&conn->free_dma_tasks);
    TAILQ_INIT(&conn->submission_dma_tasks);
    for (unsigned i = 0; i < count; ++i) {
        struct dma_task_entry *entry = &conn->dma_task_entries[i];
        struct test_task *t = &fixture->tasks[i];
        t->entry = entry;
        t->src = (void *)&fixture->buffers[2 * i];
        t->dst = (void *)&fixture->buffers[2 * i + 1];
        fixture->buffers[2 * i].refs = fixture->buffers[2 * i + 1].refs = 1;
        entry->owner = conn;
        entry->task = (void *)t;
        entry->in_flight = true;
        t->submitted = true;
    }
    return conn;
}
static void finish_fixture(struct dmesh_conn *conn)
{
    assert(cleanup_dma_tasks(conn) == DOCA_SUCCESS);
    assert(conn->dma_closing && !conn->dma_ctx && !conn->buf_inv);
    assert(!conn->dma_task_entries && !conn->dma_pending && !conn->recv_segs);
    assert(conn->num_dma_tasks == 0 && conn->num_free_dma_tasks == 0);
    assert(conn->num_submission_dma_tasks == 0);
    assert(cleanup_dma_tasks(conn) == DOCA_SUCCESS);
    free(fixture);
    fixture = NULL;
}

static void test_flush_and_dependency_order(void)
{
    struct dmesh_conn *conn = create_fixture(3);
    fixture->tasks[0].entry->kind = DMESH_TASK_PUSH_DATA;
    fixture->tasks[1].fail_completion = true;
    fixture->tasks[2].entry->in_flight = false;
    fixture->tasks[2].submitted = false;
    assert(put_submission_dma_task(conn, (void *)&fixture->tasks[2]) == DOCA_SUCCESS);
    conn->push_state = 1;
    conn->dma_pending_cnt = 1;
    assert(cleanup_dma_tasks(conn) == DOCA_SUCCESS);
    assert(fixture->free_count == 3 && fixture->submit_count == 0);
    assert(fixture->progress_count > 0 && fixture->inventory_count == 1);
    for (unsigned i = 0; i < 6; ++i)
        assert(fixture->buffers[i].refs == 0 && fixture->buffers[i].releases == 1);
    finish_fixture(conn);
}

static void test_stop_and_query_failure(void)
{
    struct dmesh_conn *conn = create_fixture(1);
    struct dma_task_entry *entries = conn->dma_task_entries;
    fixture->query_error = DOCA_ERROR_UNEXPECTED;
    assert(cleanup_dma_tasks(conn) == DOCA_ERROR_UNEXPECTED);
    assert(conn->dma_task_entries == entries && conn->dma_ctx && conn->buf_inv);
    assert(fixture->free_count == 0 && fixture->tasks[0].submitted);
    fixture->query_error = DOCA_SUCCESS;
    fixture->stop_error = DOCA_ERROR_NOT_CONNECTED;
    assert(cleanup_dma_tasks(conn) == DOCA_ERROR_NOT_CONNECTED);
    assert(fixture->free_count == 0 && fixture->destroy_count == 0);
    fixture->stop_error = DOCA_SUCCESS;
    finish_fixture(conn);
}

static void test_timeout_preserves_dma_and_blocks_admission(void)
{
    struct dmesh_conn *conn = create_fixture(1);
    fixture->complete_on_progress = false;
    assert(cleanup_dma_tasks(conn) == DOCA_ERROR_TIME_OUT);
    assert(fixture->progress_count == DMESH_DMA_STOP_MAX_POLLS);
    assert(fixture->tasks[0].submitted && !fixture->tasks[0].freed);
    assert(fixture->buffers[0].refs == 1 && fixture->buffers[1].refs == 1);
    assert(conn->dma_ctx && conn->buf_inv && conn->dma_task_entries && conn->recv_segs);
    assert(fixture->destroy_count == 0 && fixture->inventory_count == 0);
    const struct doca_buf *src = fixture->tasks[0].src;
    struct doca_buf *dst = fixture->tasks[0].dst;
    assert(submit_dma_task(conn, src, dst) == DOCA_ERROR_BAD_STATE);
    assert(enqueue_dma_task(conn, src, dst) == DOCA_ERROR_BAD_STATE);
    int submitted = -1;
    assert(progress_dma_submission_queue(conn, 0, &submitted) == DOCA_ERROR_BAD_STATE);
    assert(submitted == 0 && get_free_dma_task(conn) == NULL);
    assert(dmesh_dma_copy_to_rcvbuf(conn, 0, 1) == DOCA_ERROR_BAD_STATE);
    assert(dmesh_dma_push_staged(conn, 0, 1) == -(int)DOCA_ERROR_BAD_STATE);
    dmesh_dma_defer_copy(conn, 0, 1);
    assert(conn->dma_pending_cnt == 0 && fixture->submit_count == 0);
    fixture->complete_on_progress = true;
    finish_fixture(conn);
}

static void test_buffer_release_retry(void)
{
    struct dmesh_conn *conn = create_fixture(1);
    struct test_task *t = &fixture->tasks[0];
    t->submitted = t->entry->in_flight = false;
    assert(put_submission_dma_task(conn, (void *)t) == DOCA_SUCCESS);
    fixture->release_error = t->dst;
    assert(cleanup_dma_tasks(conn) == DOCA_ERROR_IN_USE);
    assert(!t->freed && t->src == NULL && t->dst == fixture->release_error);
    assert(fixture->buffers[0].releases == 1 && fixture->buffers[1].releases == 0);
    assert(t->entry->in_submission_queue && conn->num_submission_dma_tasks == 1);
    fixture->release_error = NULL;
    finish_fixture(conn); /* source ref must not be released a second time */
}

static void test_destroy_failures_are_retryable(void)
{
    struct dmesh_conn *conn = create_fixture(1);
    fixture->destroy_error = DOCA_ERROR_IN_USE;
    assert(cleanup_dma_tasks(conn) == DOCA_ERROR_IN_USE);
    assert(conn->dma_ctx && conn->buf_inv && conn->dma_task_entries);
    assert(fixture->free_count == 1 && fixture->inventory_count == 0);
    fixture->destroy_error = DOCA_SUCCESS;
    fixture->inventory_error = DOCA_ERROR_IN_USE;
    assert(cleanup_dma_tasks(conn) == DOCA_ERROR_IN_USE);
    assert(!conn->dma_ctx && conn->buf_inv && conn->dma_task_entries && conn->dma_pending);
    unsigned destroys = fixture->destroy_count;
    fixture->inventory_error = DOCA_SUCCESS;
    assert(cleanup_dma_tasks(conn) == DOCA_SUCCESS);
    assert(fixture->destroy_count == destroys);
    finish_fixture(conn);
}

static void test_error_callback_keeps_sibling_tasks(void)
{
    struct dmesh_conn *conn = create_fixture(2);
    struct test_task *t = &fixture->tasks[0];
    t->submitted = false;
    t->fail_completion = true;
    dmesh_doca_dpa_dma_task_error_cb((void *)t, (union doca_data){.ptr = t->entry},
                                    (union doca_data){.ptr = conn});
    assert(conn->state == DMESH_CONN_ERROR && conn->dma_closing);
    assert(fixture->free_count == 0 && fixture->tasks[1].submitted);
    assert(fixture->buffers[2].refs == 1 && fixture->buffers[3].refs == 1);
    finish_fixture(conn);
}

static void test_submission_failure_does_not_release_caller_buffers(void)
{
    struct dmesh_conn *conn = create_fixture(1);
    struct test_task *t = &fixture->tasks[0];
    const struct doca_buf *src = t->src;
    struct doca_buf *dst = t->dst;
    t->submitted = t->entry->in_flight = false;
    t->src = NULL;
    t->dst = NULL;
    assert(put_free_dma_task(conn, (void *)t) == DOCA_SUCCESS);
    fixture->submit_error = DOCA_ERROR_AGAIN;
    assert(submit_dma_task(conn, src, dst) == DOCA_ERROR_AGAIN);
    assert(conn->num_free_dma_tasks == 1 && !t->submitted);
    assert(fixture->buffers[0].refs == 1 && fixture->buffers[1].refs == 1);
    fixture->submit_error = DOCA_ERROR_UNEXPECTED;
    assert(submit_dma_task(conn, src, dst) == DOCA_ERROR_UNEXPECTED);
    assert(fixture->buffers[0].refs == 1 && fixture->buffers[1].refs == 1);
    assert(doca_buf_dec_refcount((struct doca_buf *)src, NULL) == DOCA_SUCCESS);
    assert(doca_buf_dec_refcount(dst, NULL) == DOCA_SUCCESS);
    finish_fixture(conn);
}

static void test_push_fin_follows_the_last_batch(void)
{
    struct dmesh_conn *conn = create_fixture(2);
    uint8_t staging[256], ring[64];
    struct test_task *last = &fixture->tasks[0], *spare = &fixture->tasks[1];
    last->entry->kind = DMESH_TASK_PUSH_DESC;
    spare->submitted = spare->entry->in_flight = false;
    assert(put_free_dma_task(conn, (void *)spare) == DOCA_SUCCESS);
    conn->tx_staging = staging;
    conn->tx_staging_len = sizeof(staging);
    conn->rcvbuf.mmap = (void *)fixture;
    conn->rcvbuf.buf = ring;
    conn->push_seq = 16;
    conn->push_pos = 40;
    conn->push_len = 8;
    conn->push_state = 2;

    conn->push_fin_requested = true;
    assert(dmesh_dma_push_staged(conn, 0, 1) == -(int)DOCA_ERROR_BAD_STATE);
    assert(dmesh_dma_push_fin(conn) == 0);
    assert(!conn->push_fin_sent && fixture->submit_count == 0);

    /* The last descriptor lands, then the FIN is published after it. */
    fixture->lend_buffers = 2;
    (void)doca_pe_progress(fixture->objs.consumer_pe);
    assert(conn->push_fin_sent && conn->push_state == 2 && fixture->submit_count == 1);
    assert(conn->push_seq == 17 && conn->push_shadow->seq == 18 && conn->push_shadow->pos == 48);
    assert(conn->push_shadow->len == 0 && fixture->lend_buffers == 0);
    assert(dmesh_dma_push_fin(conn) == 0 && fixture->submit_count == 1);
    assert(cleanup_dma_tasks(conn) == DOCA_SUCCESS);
    assert(fixture->buffers[4].refs == 0 && fixture->buffers[5].refs == 0);
    finish_fixture(conn);
}

static void test_push_publication_failure_retains_custody(void)
{
    struct dmesh_conn *conn = create_fixture(1);
    uint8_t staging[256], ring[64];
    fixture->tasks[0].submitted = fixture->tasks[0].entry->in_flight = false;
    assert(put_free_dma_task(conn, (void *)&fixture->tasks[0]) == DOCA_SUCCESS);
    fixture->submit_error = DOCA_ERROR_AGAIN;
    fixture->lend_buffers = 2;
    conn->tx_staging = staging;
    conn->tx_staging_len = sizeof(staging);
    conn->rcvbuf.mmap = (void *)fixture;
    conn->rcvbuf.buf = ring;
    conn->push_len = 8;
    dmesh_dma_push_submit_desc(conn);
    assert(conn->dma_closing && conn->state == DMESH_CONN_ERROR && conn->error_status == EIO);
    assert(conn->push_seq == 0 && conn->push_len == 8);
    assert(fixture->buffers[4].refs == 0 && fixture->buffers[5].refs == 0);
    finish_fixture(conn);
}

int main(void)
{
    assert(cleanup_dma_tasks(NULL) == DOCA_SUCCESS);
    test_flush_and_dependency_order();
    test_stop_and_query_failure();
    test_timeout_preserves_dma_and_blocks_admission();
    test_buffer_release_retry();
    test_destroy_failures_are_retryable();
    test_error_callback_keeps_sibling_tasks();
    test_submission_failure_does_not_release_caller_buffers();
    test_push_fin_follows_the_last_batch();
    test_push_publication_failure_retains_custody();
    puts("dma_cleanup_test: PASS");
    return 0;
}
