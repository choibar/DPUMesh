#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Real cleanup, queue management and completion callbacks; only the SDK is
 * replaced. A submitted fake task cannot be freed, and an IDLE transition
 * requires every allocated task to have been returned, like the SDK contract. */
#define DMESH_DMA_STOP_MAX_POLLS 4u
#define DMESH_EC_EXTERN
#include "../src/transport/dpu/dma.c"

/* The reverse DPA thread as dma.c sees it: the entries posted to its ring and
 * the flow's completion line. The thread itself is exercised on hardware. */
struct ec_entry { uint64_t seq; uint32_t pos, len; uint64_t cum; };
static struct ec_entry ec_posted[8];
static unsigned ec_posts;
static int ec_ring_full;
static uint64_t ec_done_seq, ec_done_bytes;
doca_error_t dmesh_ec_conn_handles(struct dmesh_conn *conn)
{
    conn->ec_ready = true;
    return DOCA_SUCCESS;
}
doca_error_t dmesh_ec_post(struct dmesh_conn *conn, uint64_t seq, uint32_t pos, uint32_t len,
                           uint64_t src_addr, uint64_t cum_bytes)
{
    (void)conn; (void)src_addr;
    if (ec_ring_full)
        return DOCA_ERROR_AGAIN;
    assert(ec_posts < sizeof(ec_posted) / sizeof(ec_posted[0]));
    ec_posted[ec_posts++] = (struct ec_entry){seq, pos, len, cum_bytes};
    return DOCA_SUCCESS;
}
int dmesh_ec_reap(struct dmesh_conn *conn)
{
    if (ec_done_seq <= conn->push_seq || ec_done_seq > conn->ec_issued_seq)
        return 0;
    conn->push_seq = ec_done_seq;
    conn->pushed_bytes = ec_done_bytes;
    return 1;
}

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
    unsigned event_reads; /* doca_pe_request_notification: once before each destroy */
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
doca_error_t doca_pe_request_notification(struct doca_pe *pe)
{
    /* The idle context's waiting events are read while it still exists. */
    assert(pe == (void *)&fixture->objs && fixture->state == DOCA_CTX_STATE_IDLE);
    assert(fixture->event_reads == fixture->destroy_count);
    ++fixture->event_reads;
    return DOCA_SUCCESS;
}
doca_error_t doca_dma_destroy(struct doca_dma *dma)
{
    assert(dma == (void *)fixture && fixture->state == DOCA_CTX_STATE_IDLE);
    assert(fixture->free_count == fixture->task_count);
    assert(fixture->event_reads == fixture->destroy_count + 1);
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
    fixture->tasks[0].entry->kind = DMESH_TASK_PULL_CURSOR;
    fixture->tasks[1].fail_completion = true;
    fixture->tasks[2].entry->in_flight = false;
    fixture->tasks[2].submitted = false;
    assert(put_submission_dma_task(conn, (void *)&fixture->tasks[2]) == DOCA_SUCCESS);
    conn->cursor_state = 1;
    conn->dma_pending_cnt = 1;
    assert(cleanup_dma_tasks(conn) == DOCA_SUCCESS);
    assert(conn->cursor_state == 0);
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

/* A flow over a 1 KiB data ring: batches of at most a quarter of it. */
static void push_flow(struct dmesh_conn *conn, uint8_t *staging, size_t staging_len, uint8_t *window)
{
    conn->tx_staging = staging;
    conn->tx_staging_len = staging_len;
    conn->tx_staging_mmap = (void *)fixture;
    conn->rcvbuf.mmap = (void *)fixture;
    conn->rcvbuf.buf = window;
    conn->rcvbuf.size = DMESH_PUSH_DATA_OFF + 1024;
    ec_posts = 0;
    ec_ring_full = 0;
    ec_done_seq = ec_done_bytes = 0;
}

/* Batches go to the reverse thread back to back, each at its own place in
 * the host's data ring; a full thread ring or the host's free slots hold the
 * next one, and a held flow pulls the host's cursor. */
static void test_push_batches_and_gate(void)
{
    struct dmesh_conn *conn = create_fixture(2);
    static uint8_t staging[4096], window[DMESH_PUSH_DATA_OFF + 1024];
    struct test_task *spare = &fixture->tasks[1];
    spare->submitted = spare->entry->in_flight = false;
    assert(put_free_dma_task(conn, (void *)spare) == DOCA_SUCCESS);
    push_flow(conn, staging, sizeof(staging), window);

    assert(dmesh_dma_push_staged(conn, 0, 1000) == 256);
    assert(dmesh_dma_push_staged(conn, 256, 744) == 256);
    assert(ec_posts == 2 && conn->ec_ready);
    assert(ec_posted[0].seq == 1 && ec_posted[0].pos == 0 && ec_posted[0].len == 256 && ec_posted[0].cum == 256);
    assert(ec_posted[1].seq == 2 && ec_posted[1].pos == 256 && ec_posted[1].cum == 512);
    assert(conn->ec_issued_seq == 2 && conn->push_seq == 0 && conn->pushed_bytes == 0);

    ec_ring_full = 1;
    assert(dmesh_dma_push_staged(conn, 512, 100) == 0 && conn->ec_issued_seq == 2);
    ec_ring_full = 0;

    /* The thread completed batch 1: its bytes may be reused. */
    ec_done_seq = 1;
    ec_done_bytes = 256;
    uint64_t bytes = 0;
    fixture->lend_buffers = 2;           /* an early completion pulls the cursor */
    assert(dmesh_dma_tx_completed(conn, &bytes) == DOCA_SUCCESS && bytes == 256 && conn->push_seq == 1);
    assert(conn->cursor_state == 1 && fixture->submit_count == 1 && fixture->lend_buffers == 0);
    (void)doca_pe_progress(fixture->objs.consumer_pe);   /* the fixture's first task */
    conn->cursor_shadow->magic = DMESH_PUSH_FC_MAGIC;
    conn->cursor_shadow->consumed_seq = 1;
    conn->cursor_shadow->consumed_bytes = 256;
    (void)doca_pe_progress(fixture->objs.consumer_pe);   /* the pull */
    assert(conn->cursor_state == 0 && conn->host_fc && conn->host_consumed_seq == 1);

    /* All but two of the host's slots in flight: held, with a fresh pull. */
    conn->ec_issued_seq = conn->host_consumed_seq + DMESH_PUSH_DESC_N - 2;
    fixture->lend_buffers = 2;
    unsigned posts = ec_posts, submits = fixture->submit_count;
    assert(dmesh_dma_push_staged(conn, 512, 100) == 0);
    assert(ec_posts == posts && fixture->submit_count == submits + 1 && conn->cursor_state == 1);
    (void)doca_pe_progress(fixture->objs.consumer_pe);
    assert(cleanup_dma_tasks(conn) == DOCA_SUCCESS);
    finish_fixture(conn);
}

/* The end of stream is a zero-length entry after the flow's last batch; the
 * flow takes no bytes after it and posts it once. */
static void test_push_fin_follows_the_last_batch(void)
{
    struct dmesh_conn *conn = create_fixture(0);
    static uint8_t staging[4096], window[DMESH_PUSH_DATA_OFF + 1024];
    push_flow(conn, staging, sizeof(staging), window);
    conn->ec_ready = true;
    conn->ec_issued_seq = 16;
    conn->ec_issued_bytes = 4000;
    conn->push_seq = 16 - (DMESH_PUSH_DESC_N - 1);   /* one slot short */
    conn->push_pos = 40;

    conn->push_fin_requested = true;
    assert(dmesh_dma_push_staged(conn, 0, 1) == -(int)DOCA_ERROR_BAD_STATE);
    assert(dmesh_dma_push_fin(conn) == 0 && !conn->push_fin_sent && ec_posts == 0);
    conn->push_seq = 15;
    assert(dmesh_dma_push_fin(conn) == 0 && conn->push_fin_sent && ec_posts == 1);
    assert(ec_posted[0].seq == 17 && ec_posted[0].pos == 40 && ec_posted[0].len == 0);
    assert(ec_posted[0].cum == 4000 && conn->ec_issued_seq == 17);
    assert(dmesh_dma_push_fin(conn) == 0 && ec_posts == 1);
    finish_fixture(conn);
}

/* What the proxy may reuse is what the reverse thread completed. */
static void test_tx_completion_sources(void)
{
    struct dmesh_conn conn = {.state = DMESH_CONN_RUNNING};
    uint64_t bytes = 999;
    ec_done_seq = ec_done_bytes = 0;
    assert(dmesh_dma_tx_completed(&conn, &bytes) == DOCA_SUCCESS && bytes == 0);
    conn.ec_issued_seq = 3;
    ec_done_seq = 2;
    ec_done_bytes = 192;
    assert(dmesh_dma_tx_completed(&conn, &bytes) == DOCA_SUCCESS && bytes == 192 && conn.push_seq == 2);
    ec_done_seq = 4;                      /* past what was issued: not this flow's */
    assert(dmesh_dma_tx_completed(&conn, &bytes) == DOCA_SUCCESS && bytes == 192 && conn.push_seq == 2);
    conn.state = DMESH_CONN_ERROR;
    assert(dmesh_dma_tx_completed(&conn, &bytes) == DOCA_ERROR_BAD_STATE);
    conn.state = DMESH_CONN_RUNNING;
    conn.dma_closing = true;
    assert(dmesh_dma_tx_completed(&conn, &bytes) == DOCA_ERROR_BAD_STATE && bytes == 192);
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
    test_push_batches_and_gate();
    test_push_fin_follows_the_last_batch();
    test_tx_completion_sources();
    puts("dma_cleanup_test: PASS");
    return 0;
}
