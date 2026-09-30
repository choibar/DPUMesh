/* Exercise the production host session and flow lifecycle with a fake Comch
 * peer. DOCA data-path branches link against the SDK but must not execute. */
#include <assert.h>
#include <stdio.h>
#include "src/transport/host/channel.c"
#ifdef OBJECT_H_
#error "Native host channels must not depend on the DPU objects layout"
#endif

static struct dmesh_comch_client *mock_control;
static unsigned client_creates, client_destroys, ring_allocs, ring_frees;
static unsigned opens, closes;
static unsigned control_progress;
static int next_close_status;
static uint8_t pending[DMESH_SESSION_MAX_FRAME];
static size_t pending_len;

enum cleanup_phase {
    FAIL_NONE, FAIL_QUIESCE, FAIL_COMCH, FAIL_BUF_ARRAY, FAIL_THREAD,
    FAIL_PE, FAIL_TX_IMPORT, FAIL_RING_IMPORT, FAIL_FORWARD_RING, FAIL_PHASES
};
static enum cleanup_phase fail_once;
static unsigned cleanup_attempts[FAIL_PHASES], cleanup_done[FAIL_PHASES];
static char fake_buf_array, fake_reverse_pe, fake_tx_import, fake_ring_import, fake_thread;

static doca_error_t cleanup_result(enum cleanup_phase phase)
{
    ++cleanup_attempts[phase];
    if (fail_once == phase) { fail_once = FAIL_NONE; return DOCA_ERROR_TIME_OUT; }
    ++cleanup_done[phase];
    return DOCA_SUCCESS;
}

void thread_init_rpc(void) { assert(!"unexpected DPA invocation"); }

static void reply(uint16_t type, uint32_t id, uint32_t generation, int status)
{
    assert(pending_len == 0);
    pending_len = dmesh_session_encode(pending, sizeof(pending), type, id,
                                       generation, status, NULL, 0);
    assert(pending_len != 0);
}

doca_error_t dmesh_comch_client_open(const char *server, struct dmesh_comch_client *objs)
{
    assert(strcmp(server, "unit-session") == 0);
    ++client_creates;
    mock_control = objs;
    objs->pe = (struct doca_pe *)objs;
    objs->cc_client = (struct doca_comch_client *)objs;
    objs->connection = (struct doca_comch_connection *)objs;
    return DOCA_SUCCESS;
}

doca_error_t dmesh_comch_client_send(struct dmesh_comch_client *objs, const char *data, size_t len)
{
    struct dmesh_session_header h;
    const uint8_t *payload;
    assert(objs == mock_control);
    assert(dmesh_session_decode(data, len, &h, &payload) == 0);
    switch (h.type) {
    case DMESH_SESSION_HELLO:
        reply(DMESH_SESSION_HELLO_ACK, 0, 0, 0);
        break;
    case DMESH_SESSION_OPEN:
        assert(h.payload_len == sizeof(struct dmesh_export_metadata_msg));
        ++opens;
        reply(DMESH_SESSION_READY, h.flow_id, h.generation, 0);
        break;
    case DMESH_SESSION_CLOSE: {
        struct channel_dev *dev = objs->owner;
        struct channel_conn *flow = dev->flows[h.flow_id];
        assert(flow && !flow->reverse && !flow->tx_mmap && !flow->ring_mmap);
        ++closes;
        reply(DMESH_SESSION_CLOSED, h.flow_id, h.generation, next_close_status);
        next_close_status = 0;
        break;
    }
    default:
        assert(!"unexpected host control message");
    }
    return DOCA_SUCCESS;
}

uint8_t doca_pe_progress(struct doca_pe *pe)
{
    if (pe == (struct doca_pe *)&fake_reverse_pe) return 0;
    assert(pe == (struct doca_pe *)mock_control);
    ++control_progress;
    if (!pending_len) return 0;
    size_t len = pending_len;
    pending_len = 0;
    mock_control->message(mock_control->owner, pending, len);
    return 1;
}
struct doca_ctx *doca_comch_client_as_ctx(struct doca_comch_client *client)
{ return (struct doca_ctx *)client; }
doca_error_t doca_ctx_stop(struct doca_ctx *ctx)
{ assert(ctx == (struct doca_ctx *)mock_control); return DOCA_SUCCESS; }
doca_error_t doca_ctx_get_state(const struct doca_ctx *ctx, enum doca_ctx_states *state)
{ assert(ctx == (struct doca_ctx *)mock_control); *state = DOCA_CTX_STATE_IDLE; return DOCA_SUCCESS; }
doca_error_t doca_comch_client_destroy(struct doca_comch_client *client)
{ assert(client == (struct doca_comch_client *)mock_control); ++client_destroys; return DOCA_SUCCESS; }
doca_error_t doca_pe_destroy(struct doca_pe *pe)
{
    if (pe == (struct doca_pe *)&fake_reverse_pe) return cleanup_result(FAIL_PE);
    assert(pe == (struct doca_pe *)mock_control); return DOCA_SUCCESS;
}
doca_error_t doca_buf_arr_destroy(struct doca_buf_arr *array)
{ assert(array == (struct doca_buf_arr *)&fake_buf_array); return cleanup_result(FAIL_BUF_ARRAY); }
doca_error_t doca_mmap_destroy(struct doca_mmap *mmap)
{
    if (mmap == (struct doca_mmap *)&fake_tx_import) return cleanup_result(FAIL_TX_IMPORT);
    assert(mmap == (struct doca_mmap *)&fake_ring_import);
    return cleanup_result(FAIL_RING_IMPORT);
}
doca_error_t doca_mmap_stop(struct doca_mmap *mmap)
{ (void)mmap; assert(!"imported mmaps must not be explicitly stopped"); return DOCA_ERROR_NOT_PERMITTED; }

static int fail_ring_alloc;
doca_error_t alloc_dma_ring(struct dma_ring **out, struct doca_dev *dev, size_t size)
{
    (void)dev;
    if (fail_ring_alloc) { fail_ring_alloc = 0; return DOCA_ERROR_NO_MEMORY; }
    struct dma_ring *ring = calloc(1, sizeof(*ring));
    assert(ring);
    ring->size = (uint32_t)size;
    ring->mmap = (struct doca_mmap *)ring;
    *out = ring;
    ++ring_allocs;
    return DOCA_SUCCESS;
}
doca_error_t destroy_mmap_and_free_buffer(struct doca_mmap *mmap, void *buffer)
{
    assert(mmap && !buffer);
    doca_error_t result = cleanup_result(FAIL_FORWARD_RING);
    if (result == DOCA_SUCCESS) ++ring_frees;
    return result;
}
doca_error_t dmesh_build_dma_metadata(struct doca_dev *dev, const struct dma_ring *ring,
    const struct dmesh_buffer *sndbuf, const struct dmesh_buffer *rcvbuf,
    const struct dmesh_flow_id *flow, struct dmesh_export_metadata_msg *msg)
{ (void)dev; (void)sndbuf; (void)rcvbuf; (void)flow; assert(ring); memset(msg, 0, sizeof(*msg)); return DOCA_SUCCESS; }
doca_error_t dmesh_validate_reverse_metadata(const struct dmesh_export_rcv_ring_msg *msg)
{ (void)msg; return DOCA_SUCCESS; }

/* These mocks expose each host cleanup phase; common checked-helper internals
 * have separate tests. Success represents the explicit DMA completion fence. */
doca_error_t dmesh_dpa_quiesce_checked(struct dmesh_doca_dpa_thread *t,
    struct dmesh_doca_dpa_comch *comch, struct doca_pe *pe)
{
    (void)comch; (void)pe;
    if (!t || !t->running || t->quiesced) return DOCA_SUCCESS;
    doca_error_t result = cleanup_result(FAIL_QUIESCE);
    if (result == DOCA_SUCCESS) t->quiesced = true;
    return result;
}
doca_error_t dmesh_dpa_comch_destroy_checked(struct dmesh_doca_dpa_thread *t,
    struct dmesh_doca_dpa_comch **comch, struct doca_pe *pe)
{
    (void)pe;
    if (!*comch) return DOCA_SUCCESS;
    assert(!t || !t->running || t->quiesced);
    doca_error_t result = cleanup_result(FAIL_COMCH);
    if (result == DOCA_SUCCESS) { free(*comch); *comch = NULL; }
    return result;
}
doca_error_t dmesh_doca_dpa_thread_destroy_checked(struct dmesh_doca_dpa_thread *t)
{
    if (!t->thread) return DOCA_SUCCESS;
    doca_error_t result = cleanup_result(FAIL_THREAD);
    if (result == DOCA_SUCCESS) t->thread = NULL;
    return result;
}
doca_error_t dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *t)
{ (void)t; assert(!"unexpected DPA creation"); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_dpa_endpoint_init_comch(struct dmesh_dpa_endpoint *c, struct doca_dev *dev)
{ (void)c; (void)dev; assert(!"unexpected DPA creation"); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *q, void *m, uint32_t n)
{ (void)q; (void)m; (void)n; assert(!"unexpected DPA send"); return DOCA_ERROR_NOT_SUPPORTED; }

static void dispatch(struct channel_dev *dev, uint16_t type, uint32_t id,
                     uint32_t generation, int status)
{
    uint8_t frame[DMESH_SESSION_MAX_FRAME];
    size_t len = dmesh_session_encode(frame, sizeof(frame), type, id, generation, status, NULL, 0);
    assert(len);
    pthread_mutex_lock(&dev->session_lock);
    session_message(dev, frame, len);
    pthread_mutex_unlock(&dev->session_lock);
}

static void attach_reverse(struct channel_conn *flow, int running)
{
    flow->reverse = calloc(1, sizeof(*flow->reverse));
    assert(flow->reverse);
    flow->reverse->dpa_thread = calloc(1, sizeof(*flow->reverse->dpa_thread));
    flow->reverse->dpa_comch = calloc(1, sizeof(*flow->reverse->dpa_comch));
    assert(flow->reverse->dpa_thread && flow->reverse->dpa_comch);
    flow->reverse->dpa_thread->thread = (struct doca_dpa_thread *)&fake_thread;
    flow->reverse->dpa_thread->running = running;
    flow->reverse->buf_arr = (struct doca_buf_arr *)&fake_buf_array;
    flow->reverse->pe = (struct doca_pe *)&fake_reverse_pe;
    flow->tx_mmap = (struct doca_mmap *)&fake_tx_import;
    flow->ring_mmap = (struct doca_mmap *)&fake_ring_import;
}

static void test_checked_close(void)
{
    struct channel_dev dev = {0};
    pthread_mutex_init(&dev.session_lock, NULL);
    assert(channel_session_open(&dev, "unit-session") == 0);
    struct channel_mem tx = {.buf = calloc(1, 8192), .bytes = 8192};
    struct channel_mem rx = {.buf = calloc(1, 2 * CHANNEL_WINDOW), .bytes = 2 * CHANNEL_WINDOW};
    assert(tx.buf && rx.buf);
    struct channel_conn_config cfg = {.tx = &tx, .rx = &rx, .mode = CHANNEL_MODE_CLIENT_DPU_DMA};

    /* Failed ring allocation never publishes OPEN and leaves no stale map. */
    cfg.flow_id = 1;
    struct channel_conn *unopened = NULL;
    unsigned before_open = opens;
    fail_ring_alloc = 1;
    assert(channel_conn_open(&dev, &cfg, &unopened) == -1 && errno == ENOMEM);
    assert(!unopened && !dev.flows[1] && opens == before_open);

    for (enum cleanup_phase phase = FAIL_QUIESCE; phase < FAIL_PHASES; ++phase) {
        dev.host_dpa = 0; /* Exercise OPEN separately from SDK reverse setup. */
        cfg.flow_id = 1; cfg.rx_offset = 0;
        struct channel_conn *a, *b;
        assert(channel_conn_open(&dev, &cfg, &a) == 0);
        cfg.flow_id = 2; cfg.rx_offset = CHANNEL_WINDOW;
        assert(channel_conn_open(&dev, &cfg, &b) == 0);
        attach_reverse(a, 1);
        dev.host_dpa = 1;
        memset(cleanup_attempts, 0, sizeof(cleanup_attempts));
        memset(cleanup_done, 0, sizeof(cleanup_done));
        unsigned before_close = closes, before_free = ring_frees;
        struct dmesh_comch_client *control = dev.control;
        fail_once = phase;
        assert(channel_conn_close(a) == -1 && errno == ETIMEDOUT);
        assert(dev.flows[1] == a && dev.flows[2] == b && dev.control == control);
        assert(ring_frees == before_free);
        assert(closes == before_close + (phase == FAIL_FORWARD_RING));
        assert(cleanup_attempts[phase] == 1 && cleanup_done[phase] == 0);
        /* Retried polling must not access a PE destroyed by an earlier phase. */
        (void)channel_conn_progress(a);
        /* A late application RX release must not write a freed thread arg. */
        if (phase != FAIL_QUIESCE)
            channel_conn_rx_consumed(a, CHANNEL_DESC_N / 2, CHANNEL_RD_POS_BATCH);
        assert(channel_conn_close(a) == 0);
        assert(!dev.flows[1] && dev.flows[2] == b && dev.control == control);
        assert(closes == before_close + 1 && ring_frees == before_free + 1);
        for (enum cleanup_phase step = FAIL_QUIESCE; step < FAIL_PHASES; ++step)
            assert(cleanup_done[step] == 1); /* No completed phase is repeated. */
        assert(channel_conn_close(b) == 0);
    }

    /* A never-run thread and a partially allocated connection need cleanup,
     * but cannot wait for a kernel stop acknowledgement that will never exist. */
    for (int missing_thread = 0; missing_thread < 2; ++missing_thread) {
        dev.host_dpa = 0; cfg.flow_id = 1; cfg.rx_offset = 0;
        struct channel_conn *flow;
        assert(channel_conn_open(&dev, &cfg, &flow) == 0);
        attach_reverse(flow, 0);
        if (missing_thread) { free(flow->reverse->dpa_thread); flow->reverse->dpa_thread = NULL; }
        dev.host_dpa = 1;
        unsigned attempts = cleanup_attempts[FAIL_QUIESCE];
        fail_once = FAIL_QUIESCE;
        assert(channel_conn_close(flow) == 0);
        assert(cleanup_attempts[FAIL_QUIESCE] == attempts && fail_once == FAIL_QUIESCE);
        fail_once = FAIL_NONE;
    }
    assert(channel_session_close(&dev) == 0);
    pthread_mutex_destroy(&dev.session_lock);
    free(tx.buf); free(rx.buf);
    assert(ring_allocs == ring_frees);
}

int main(void)
{
    struct channel_dev dev = {0};
    pthread_mutex_init(&dev.session_lock, NULL);
    assert(channel_session_open(&dev, "unit-session") == 0);
    assert(client_creates == 1 && dev.hello_ready);
    struct dmesh_comch_client *control = dev.control;
    struct channel_mem tx = {.buf = calloc(1, 8192), .bytes = 8192};
    struct channel_mem rx = {.buf = calloc(1, CHANNEL_WINDOW * 3), .bytes = CHANNEL_WINDOW * 3};
    assert(tx.buf && rx.buf);
    struct channel_conn_config cfg = {.flow_id = 1, .tx = &tx, .rx = &rx,
                                      .mode = CHANNEL_MODE_CLIENT_DPU_DMA};
    struct channel_conn *a, *b, *again;
    assert(channel_conn_open(&dev, &cfg, &a) == 0);
    cfg.flow_id = 2; cfg.rx_offset = CHANNEL_WINDOW;
    assert(channel_conn_open(&dev, &cfg, &b) == 0);
    assert(client_creates == 1 && opens == 2 && a->ready && b->ready);

    /* One session progress delivers replies for all flows; status checks must
     * neither re-progress that shared PE nor hide a sibling's error. */
    unsigned progress_before = control_progress;
    reply(DMESH_SESSION_ERROR, 1, a->generation, EIO);
    assert(channel_dev_progress(&dev) == 0);
    assert(control_progress == progress_before + 1);
    assert(channel_conn_status(a) == -1 && errno == EIO);
    assert(channel_conn_status(b) == 0);
    assert(control_progress == progress_before + 1);
    a->error = 0;

    /* A reply to a different incarnation cannot change the live flow. */
    dispatch(&dev, DMESH_SESSION_CLOSED, 1, a->generation + 1, 0);
    assert(!a->peer_closed && !b->peer_closed);
    dispatch(&dev, DMESH_SESSION_ERROR, 1, a->generation, EIO);
    assert(a->error == EIO && !b->error);
    a->error = 0;

    assert(channel_conn_close(a) == 0);
    assert(dev.flows[1] == NULL && dev.flows[2] == b);
    assert(dev.control == control && client_destroys == 0 && ring_frees == 1);
    cfg.flow_id = 1; cfg.rx_offset = 0;
    assert(channel_conn_open(&dev, &cfg, &again) == 0);
    assert(again->generation == 2 && client_creates == 1);
    dispatch(&dev, DMESH_SESSION_CLOSED, 1, 1, 0);
    assert(!again->peer_closed);

    /* A failed session is observed independently by every flow's poller. */
    control->peer_gone = 1;
    assert(channel_dev_progress(&dev) == -1 && errno == ECONNRESET);
    assert(channel_conn_status(again) == -1 && errno == ECONNRESET);
    assert(channel_conn_status(b) == -1 && errno == ECONNRESET);
    control->peer_gone = 0; dev.session_error = 0; /* Test-only reset. */

    /* A failed close retains the flow and its ring, without closing siblings. */
    unsigned freed = ring_frees;
    next_close_status = EBUSY;
    assert(channel_conn_close(b) == -1 && errno == EBUSY);
    assert(dev.flows[2] == b && ring_frees == freed && dev.control == control);
    assert(channel_conn_close(b) == 0);

    /* No wraparound to an old generation, and no RX-window overwrite on reject. */
    dev.generations[3] = UINT32_MAX;
    cfg.flow_id = 3; cfg.rx_offset = CHANNEL_WINDOW * 2;
    memset((char *)rx.buf + cfg.rx_offset, 0xa5, 64);
    struct channel_conn *unused = NULL;
    assert(channel_conn_open(&dev, &cfg, &unused) == -1 && errno == EOVERFLOW);
    assert(!unused && ((uint8_t *)rx.buf)[cfg.rx_offset] == 0xa5);

    /* Malformed control messages fail the session, not a random flow. */
    session_message(control->owner, (const uint8_t *)"bad", 3);
    assert(dev.session_error == EPROTO);
    assert(channel_conn_progress(again) == -1 && errno == EPROTO);
    dev.session_error = 0; /* Test-only reset to finish teardown. */
    control->peer_gone = 1;
    assert(channel_conn_progress(again) == -1 && errno == ECONNRESET);
    control->peer_gone = 0; dev.session_error = 0;

    assert(channel_session_close(&dev) == 0);
    assert(!dev.control && !dev.flows[1] && client_destroys == 1);
    assert(ring_allocs == ring_frees && closes == 4);
    pthread_mutex_destroy(&dev.session_lock);
    free(tx.buf); free(rx.buf);
    test_checked_close();
    puts("channel session: isolated flows, checked cleanup failures and retry verified");
    return 0;
}
