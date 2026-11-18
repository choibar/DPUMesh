/* Exercise the production host session and flow lifecycle with a fake Comch
 * peer. DOCA data-path branches link against the SDK but must not execute. */
#include <assert.h>
#include <stdio.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include "src/transport/host/channel.c"
#ifdef OBJECT_H_
#error "Native host channels must not depend on the DPU objects layout"
#endif

static struct dmesh_comch_client *mock_control;
static unsigned client_creates, client_destroys;
static unsigned opens, closes;
static unsigned shared_progress_calls;
static int next_close_status;
/* A slow DPU: OPEN or CLOSE gets no answer; a refused CLOSE is not queued. */
static int silent_open, silent_close, refuse_close;
static uint8_t pending[DMESH_SESSION_MAX_FRAME];
static size_t pending_len;
/* Control wake: the control PE notification. */
static unsigned pings_seen, notify_requests, notify_clears;
/* The session's tail table: allocated once, exported once per session. */
static unsigned tails_allocs, tails_seen;
static char fake_tails_mmap, fake_tails_desc[40];
static uint32_t fake_tails[DMESH_TAILS_ALLOC / 4] __attribute__((aligned(64))); /* the page: tails, then sync event words */
static doca_error_t send_result = DOCA_SUCCESS;
static int control_notify_fd = -1;

/* The forward ring's release after CLOSED can fail once. */

/* Flow wakes: every slot's PE and sync event, running at once (the flow wake
 * test models their states). */
#define FLOW_OBJS (DMESH_SESSION_MAX_FLOWS + 1)
static char fake_flow_pes[FLOW_OBJS], fake_events[FLOW_OBJS];
static unsigned flow_pes_live, events_live, next_flow_pe, next_event;
static int event_stopped[FLOW_OBJS];
static int is_flow_pe(const void *p)
{ return (const char *)p >= fake_flow_pes && (const char *)p < fake_flow_pes + FLOW_OBJS; }
static int is_event(const void *p)
{ return (const char *)p >= fake_events && (const char *)p < fake_events + FLOW_OBJS; }
doca_error_t doca_pe_create(struct doca_pe **pe)
{ *pe = (struct doca_pe *)&fake_flow_pes[next_flow_pe++ % FLOW_OBJS]; ++flow_pes_live; return DOCA_SUCCESS; }
doca_error_t doca_pe_set_event_mode(struct doca_pe *pe, enum doca_pe_event_mode mode)
{ assert(is_flow_pe(pe) && mode == DOCA_PE_EVENT_MODE_PROGRESS_ALL); return DOCA_SUCCESS; }
doca_error_t doca_sync_event_create(struct doca_sync_event **event)
{
    unsigned i = next_event++ % FLOW_OBJS;
    event_stopped[i] = 0;
    *event = (struct doca_sync_event *)&fake_events[i];
    ++events_live;
    return DOCA_SUCCESS;
}
doca_error_t doca_sync_event_add_publisher_location_remote_pci(struct doca_sync_event *event)
{ assert(is_event(event)); return DOCA_SUCCESS; }
doca_error_t doca_sync_event_add_subscriber_location_cpu(struct doca_sync_event *event, struct doca_dev *dev)
{ (void)dev; assert(is_event(event)); return DOCA_SUCCESS; }
doca_error_t doca_sync_event_set_addr(struct doca_sync_event *event, uint64_t *addr)
{
    assert(is_event(event));
    assert((uint8_t *)addr >= (uint8_t *)fake_tails + 1024 && (uint8_t *)addr < (uint8_t *)fake_tails + sizeof(fake_tails));
    return DOCA_SUCCESS;
}
struct doca_ctx *doca_sync_event_as_ctx(struct doca_sync_event *event) { return (struct doca_ctx *)event; }
doca_error_t doca_pe_connect_ctx(struct doca_pe *pe, struct doca_ctx *ctx)
{ assert(is_flow_pe(pe) && is_event(ctx)); return DOCA_SUCCESS; }
doca_error_t doca_sync_event_task_wait_neq_set_conf(struct doca_sync_event *event,
    doca_sync_event_task_wait_neq_completion_cb_t done, doca_sync_event_task_wait_neq_completion_cb_t failed,
    uint32_t n)
{ (void)done; (void)failed; (void)n; assert(is_event(event)); return DOCA_SUCCESS; }
doca_error_t doca_ctx_start(struct doca_ctx *ctx)
{ assert(is_event(ctx)); return DOCA_SUCCESS; }
doca_error_t doca_sync_event_destroy(struct doca_sync_event *event)
{ assert(is_event(event) && event_stopped[(char *)event - fake_events]); --events_live; return DOCA_SUCCESS; }

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
    if (send_result != DOCA_SUCCESS)
        return send_result;
    switch (h.type) {
    case DMESH_SESSION_HELLO:
        reply(DMESH_SESSION_HELLO_ACK, 0, 0, 0);
        break;
    case DMESH_SESSION_TAILS:
        assert(h.flow_id == 0 && h.generation == 0 && h.payload_len == sizeof(fake_tails_desc));
        assert(memcmp(payload, fake_tails_desc, sizeof(fake_tails_desc)) == 0);
        ++tails_seen;
        break;
    case DMESH_SESSION_PING:
        assert(h.flow_id == 0 && h.generation == 0 && h.payload_len == 0);
        ++pings_seen;
        break;
    case DMESH_SESSION_LISTEN:
        assert(h.flow_id == 0 && h.payload_len == 8);
        reply(DMESH_SESSION_LISTEN_ACK, 0, 1, 0);
        break;
    case DMESH_SESSION_BACKEND_REJECT:
        assert(h.status == ENOSPC && h.generation == 10);
        break;
    case DMESH_SESSION_OPEN:
        assert(h.payload_len == sizeof(struct dmesh_export_metadata_msg));
        ++opens;
        if (!silent_open) reply(DMESH_SESSION_READY, h.flow_id, h.generation, 0);
        break;
    case DMESH_SESSION_CLOSE: {
        if (refuse_close) return DOCA_ERROR_AGAIN;
        struct channel_dev *dev = objs->owner;
        struct channel_conn *flow = dev->flows[h.flow_id];
        assert(flow);
        ++closes;
        if (!silent_close) reply(DMESH_SESSION_CLOSED, h.flow_id, h.generation, next_close_status);
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
    if (is_flow_pe(pe)) return 0;
    assert(pe == (struct doca_pe *)mock_control);
    ++shared_progress_calls;
    if (!pending_len) return 0;
    size_t len = pending_len;
    pending_len = 0;
    mock_control->message(mock_control->owner, pending, len);
    return 1;
}
doca_error_t doca_pe_request_notification(struct doca_pe *pe)
{
    if (is_flow_pe(pe)) return DOCA_SUCCESS;
    assert(pe == (struct doca_pe *)mock_control);
    ++notify_requests;
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_get_notification_handle(const struct doca_pe *pe, doca_notification_handle_t *handle)
{
    if (is_flow_pe(pe)) { *handle = (doca_notification_handle_t)-1; return DOCA_SUCCESS; }
    assert(pe == (const struct doca_pe *)mock_control && control_notify_fd >= 0);
    *handle = (doca_notification_handle_t)control_notify_fd;
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_clear_notification(struct doca_pe *pe, doca_notification_handle_t handle)
{
    uint64_t v;
    assert(pe == (struct doca_pe *)mock_control && (int)handle == control_notify_fd);
    ++notify_clears;
    while (read(control_notify_fd, &v, sizeof(v)) > 0) {}
    return DOCA_SUCCESS;
}
struct doca_ctx *doca_comch_client_as_ctx(struct doca_comch_client *client)
{ return (struct doca_ctx *)client; }
doca_error_t doca_ctx_stop(struct doca_ctx *ctx)
{
    if (is_event(ctx)) { event_stopped[(char *)ctx - fake_events] = 1; return DOCA_SUCCESS; }
    assert(ctx == (struct doca_ctx *)mock_control); return DOCA_SUCCESS;
}
doca_error_t doca_ctx_get_state(const struct doca_ctx *ctx, enum doca_ctx_states *state)
{
    if (is_event(ctx)) {
        *state = event_stopped[(const char *)ctx - fake_events] ? DOCA_CTX_STATE_IDLE : DOCA_CTX_STATE_RUNNING;
        return DOCA_SUCCESS;
    }
    assert(ctx == (struct doca_ctx *)mock_control); *state = DOCA_CTX_STATE_IDLE; return DOCA_SUCCESS;
}
doca_error_t doca_comch_client_destroy(struct doca_comch_client *client)
{ assert(client == (struct doca_comch_client *)mock_control); ++client_destroys; return DOCA_SUCCESS; }
doca_error_t doca_pe_destroy(struct doca_pe *pe)
{
    if (is_flow_pe(pe)) { --flow_pes_live; return DOCA_SUCCESS; }
    assert(pe == (struct doca_pe *)mock_control); return DOCA_SUCCESS;
}

doca_error_t alloc_buffer_and_set_mmap(struct doca_mmap **mmap, struct doca_dev *dev,
                                       void **buffer, size_t size, uint32_t access)
{
    (void)dev; (void)access;
    assert(size == sizeof(fake_tails));
    ++tails_allocs;
    *mmap = (struct doca_mmap *)&fake_tails_mmap;
    *buffer = fake_tails;
    return DOCA_SUCCESS;
}
doca_error_t doca_mmap_export_pci(struct doca_mmap *mmap, const struct doca_dev *dev,
                                  const void **desc, size_t *len)
{
    (void)dev;
    assert(mmap == (struct doca_mmap *)&fake_tails_mmap);
    *desc = fake_tails_desc;
    *len = sizeof(fake_tails_desc);
    return DOCA_SUCCESS;
}
doca_error_t destroy_mmap_and_free_buffer(struct doca_mmap *mmap, void *buffer)
{
    /* A flow's ring lives in the channel's region: only the tail table is freed here. */
    assert(mmap == (struct doca_mmap *)&fake_tails_mmap && buffer == fake_tails);
    return DOCA_SUCCESS;
}
doca_error_t dmesh_build_dma_metadata(struct doca_dev *dev, const struct dma_ring *ring,
    const struct dmesh_buffer *sndbuf, const struct dmesh_buffer *rcvbuf,
    const struct dmesh_flow_id *flow, struct dmesh_export_metadata_msg *msg)
{ (void)dev; (void)sndbuf; (void)rcvbuf; (void)flow; assert(ring); memset(msg, 0, sizeof(*msg)); return DOCA_SUCCESS; }
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

/* Receives raise their slot's fd: the control fd is armed for control
 * messages only, sends nothing and is not progressed (the caller's next drain
 * progresses it). */
static void test_control_wake(struct channel_dev *dev)
{
    control_notify_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    assert(control_notify_fd >= 0 && channel_dev_fd(dev) == control_notify_fd);
    unsigned requests = notify_requests, progress = shared_progress_calls;
    assert(channel_dev_arm(dev) == 0 && channel_dev_arm(dev) == 0);
    assert(notify_requests == requests + 2 && shared_progress_calls == progress);

    /* A failed session cannot arm. */
    dev->session_error = ECONNRESET;
    assert(channel_dev_arm(dev) == -1 && errno == ECONNRESET);
    dev->session_error = 0;
    close(control_notify_fd);
    control_notify_fd = -1;
}

static void test_checked_close(void)
{
    struct channel_dev dev = {0};
    pthread_mutex_init(&dev.session_lock, NULL);
    assert(channel_session_open(&dev, "unit-session") == 0);
    struct channel_mem tx = {.buf = calloc(1, 8192), .bytes = 8192};
    struct channel_mem rx = {.buf = calloc(1, 2 * CHANNEL_WINDOW), .bytes = 2 * CHANNEL_WINDOW};
    struct channel_mem rings = {.buf = calloc(1, 2 * CHANNEL_RING_STRIDE), .bytes = 2 * CHANNEL_RING_STRIDE};
    assert(tx.buf && rx.buf && rings.buf);
    struct channel_conn_config cfg = {.tx = &tx, .rx = &rx, .rings = &rings, .mode = CHANNEL_MODE_CLIENT};

    /* A ring outside the ring region never publishes OPEN and leaves no stale map. */
    cfg.flow_id = 1; cfg.ring_offset = 2 * CHANNEL_RING_STRIDE;
    struct channel_conn *unopened = NULL;
    unsigned before_open = opens;
    assert(channel_conn_open(&dev, &cfg, &unopened) == -1 && errno == EINVAL);
    assert(!unopened && !dev.flows[1] && opens == before_open);

    /* Each flow's ring is its slot's part of the region. A closed flow leaves
     * it there without closing a sibling; the slot's next flow finds it empty. */
    cfg.flow_id = 1; cfg.rx_offset = 0; cfg.ring_offset = 0;
    struct channel_conn *a, *b, *again;
    assert(channel_conn_open(&dev, &cfg, &a) == 0);
    cfg.flow_id = 2; cfg.rx_offset = CHANNEL_WINDOW; cfg.ring_offset = CHANNEL_RING_STRIDE;
    assert(channel_conn_open(&dev, &cfg, &b) == 0);
    assert(a->forward_ring->buffer == rings.buf && a->forward_ring->mmap == rings.mmap);
    assert(b->forward_ring->buffer == (char *)rings.buf + CHANNEL_RING_STRIDE);
    a->forward_ring->ctrl->producer_tail = 7;
    a->forward_ring->ctrl->consumer_head = 5;
    unsigned before_close = closes;
    struct dmesh_comch_client *control = dev.control;
    assert(channel_conn_close(a) == 0);
    assert(!dev.flows[1] && dev.flows[2] == b && dev.control == control && closes == before_close + 1);
    cfg.flow_id = 1; cfg.rx_offset = 0; cfg.ring_offset = 0;
    assert(channel_conn_open(&dev, &cfg, &again) == 0);
    assert(again->forward_ring->buffer == rings.buf && again->forward_ring->ctrl->producer_tail == 0 &&
           again->forward_ring->ctrl->consumer_head == 0 && again->forward_ring->head == 0);
    assert(channel_conn_close(again) == 0);
    assert(channel_conn_close(b) == 0);
    assert(channel_session_close(&dev) == 0);
    pthread_mutex_destroy(&dev.session_lock);
    free(tx.buf); free(rx.buf); free(rings.buf);
}

/* An OPEN the DPU answers only after the host stopped waiting: the
 * unconfirmed close keeps the id, which channel_flow_free releases for the
 * next open once CLOSED arrives. */
static void test_late_open(void)
{
    struct channel_dev dev = {0};
    pthread_mutex_init(&dev.session_lock, NULL);
    assert(channel_session_open(&dev, "unit-session") == 0);
    struct channel_mem tx = {.buf = calloc(1, 8192), .bytes = 8192};
    struct channel_mem rx = {.buf = calloc(1, CHANNEL_WINDOW), .bytes = CHANNEL_WINDOW};
    struct channel_mem rings = {.buf = calloc(1, CHANNEL_RING_STRIDE), .bytes = CHANNEL_RING_STRIDE};
    assert(tx.buf && rx.buf && rings.buf);
    struct channel_conn_config cfg = {.flow_id = 1, .tx = &tx, .rx = &rx, .rings = &rings,
                                      .mode = CHANNEL_MODE_CLIENT};
    struct channel_conn *flow = NULL;

    silent_open = silent_close = 1;
    assert(channel_conn_open(&dev, &cfg, &flow) == -1 && errno == ETIMEDOUT && !flow);
    struct channel_conn *held = dev.flows[1];
    assert(held && held->abandoned && held->close_sent);
    assert(!channel_flow_free(&dev, 1));
    assert(channel_conn_open(&dev, &cfg, &flow) == -1 && errno == EBUSY);
    silent_open = silent_close = 0;
    dispatch(&dev, DMESH_SESSION_READY, 1, held->generation, 0);
    assert(!channel_flow_free(&dev, 1) && dev.flows[1] == held);
    dispatch(&dev, DMESH_SESSION_CLOSED, 1, held->generation, 0);
    assert(channel_flow_free(&dev, 1) && !dev.flows[1]);
    assert(channel_conn_open(&dev, &cfg, &flow) == 0 && flow->ready);
    assert(channel_conn_close(flow) == 0);

    /* A CLOSE the queue refused is sent by the next check. */
    silent_open = refuse_close = 1;
    assert(channel_conn_open(&dev, &cfg, &flow) == -1 && errno == ETIMEDOUT);
    held = dev.flows[1];
    assert(held && held->abandoned && !held->close_sent);
    silent_open = refuse_close = 0;
    unsigned sent = closes;
    assert(!channel_flow_free(&dev, 1) && held->close_sent && closes == sent + 1);
    assert(channel_dev_progress(&dev) == 0);
    assert(channel_flow_free(&dev, 1) && !dev.flows[1]);

    assert(channel_session_close(&dev) == 0);
    pthread_mutex_destroy(&dev.session_lock);
    free(tx.buf); free(rx.buf); free(rings.buf);
}

static void test_backend_requests(struct channel_dev *dev)
{
    unsigned before = opens;
    assert(channel_session_listen(dev, 0x0100510a, 8080) == 0);
    assert(opens == before); /* listener registration must not preallocate flows */
    uint8_t frame[DMESH_SESSION_HEADER_SIZE + 4], payload[4];
    dmesh_session_put_u32(payload, 3);
    size_t n = dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_BACKEND_REQUEST,
        0, 10, 0, payload, 4);
    session_message(dev, frame, n);
    session_message(dev, frame, n);
    uint32_t worker, token;
    assert(channel_backend_next(dev, &worker, &token) == 1 && worker == 3 && token == 10);
    assert(channel_backend_next(dev, &worker, &token) == 0);
    channel_backend_finish(dev, 3, 10, ENOSPC);
    assert(dev->backend_rejections == 1);
    assert(channel_dev_progress(dev) == 0 && dev->backend_rejections == 0);
    /* A session silent for CHANNEL_KEEPALIVE_MS PINGs once; a fresh send defers it. */
    unsigned pings = pings_seen;
    assert(channel_dev_progress(dev) == 0 && pings_seen == pings);
    dev->last_send_ms -= CHANNEL_KEEPALIVE_MS;
    assert(channel_dev_progress(dev) == 0 && pings_seen == pings + 1);
    assert(channel_dev_progress(dev) == 0 && pings_seen == pings + 1);
    session_message(dev, frame, n);
    assert(channel_backend_next(dev, &worker, &token) == 0); /* replay cannot allocate again */
}

int main(void)
{
    struct channel_dev dev = {0};
    pthread_mutex_init(&dev.session_lock, NULL);
    assert(channel_session_open(&dev, "unit-session") == 0);
    assert(client_creates == 1 && dev.hello_ready);
    assert(tails_allocs == 1 && tails_seen == 1 && dev.tails == fake_tails);
    /* Every flow slot has its wake once the session is open. */
    assert(flow_pes_live == DMESH_SESSION_MAX_FLOWS && events_live == DMESH_SESSION_MAX_FLOWS);
    test_backend_requests(&dev);
    struct dmesh_comch_client *control = dev.control;
    struct channel_mem tx = {.buf = calloc(1, 8192), .bytes = 8192};
    struct channel_mem rx = {.buf = calloc(1, CHANNEL_WINDOW * 3), .bytes = CHANNEL_WINDOW * 3};
    struct channel_mem rings = {.buf = calloc(1, CHANNEL_RING_STRIDE * 3), .bytes = CHANNEL_RING_STRIDE * 3};
    assert(tx.buf && rx.buf && rings.buf);
    struct channel_conn_config cfg = {.flow_id = 1, .tx = &tx, .rx = &rx, .rings = &rings,
                                      .mode = CHANNEL_MODE_CLIENT};
    struct channel_conn *a, *b, *again;
    assert(channel_conn_open(&dev, &cfg, &a) == 0);
    cfg.flow_id = 2; cfg.rx_offset = CHANNEL_WINDOW; cfg.ring_offset = CHANNEL_RING_STRIDE;
    assert(channel_conn_open(&dev, &cfg, &b) == 0);
    assert(client_creates == 1 && opens == 2 && a->ready && b->ready);
    /* Each flow publishes its tail in its own slot of the session's table. */
    assert(a->tail == &dev.tails[0] && b->tail == &dev.tails[1] && dev.tails[1] == 0);
    test_control_wake(&dev);

    /* One session progress delivers replies for all flows; status checks must
     * neither re-progress that shared PE nor hide a sibling's error. */
    unsigned progress_once = shared_progress_calls;
    reply(DMESH_SESSION_ERROR, 1, a->generation, EIO);
    assert(channel_dev_progress(&dev) == 0);
    assert(shared_progress_calls == progress_once + 1);
    assert(channel_conn_poll(a) == -1 && errno == EIO);
    assert(channel_conn_poll(b) == 0);
    assert(shared_progress_calls == progress_once + 1);
    a->error = 0;

    a->forward_ring->ctrl->error = 1;
    assert(channel_conn_poll(a) == -1 && errno == EIO);
    assert(channel_conn_poll(b) == 0);
    a->forward_ring->ctrl->error = 0; /* Test-only reset. */

    /* A reply to a different incarnation cannot change the live flow. */
    dispatch(&dev, DMESH_SESSION_CLOSED, 1, a->generation + 1, 0);
    assert(!a->peer_closed && !b->peer_closed);
    dispatch(&dev, DMESH_SESSION_ERROR, 1, a->generation, EIO);
    assert(a->error == EIO && !b->error);
    a->error = 0;

    assert(channel_conn_close(a) == 0);
    assert(dev.flows[1] == NULL && dev.flows[2] == b);
    assert(dev.control == control && client_destroys == 0);
    cfg.flow_id = 1; cfg.rx_offset = 0; cfg.ring_offset = 0;
    assert(channel_conn_open(&dev, &cfg, &again) == 0);
    assert(again->generation == 2 && client_creates == 1);
    dispatch(&dev, DMESH_SESSION_CLOSED, 1, 1, 0);
    assert(!again->peer_closed);

    /* A failed session is observed independently by every flow's poller. */
    control->peer_gone = 1;
    unsigned progress_before = shared_progress_calls;
    assert(channel_dev_progress(&dev) == -1 && errno == ECONNRESET);
    assert(channel_conn_poll(again) == -1 && errno == ECONNRESET);
    assert(channel_conn_poll(b) == -1 && errno == ECONNRESET);
    assert(shared_progress_calls == progress_before + 1);
    control->peer_gone = 0; dev.session_error = 0; /* Test-only reset. */

    /* A failed close retains the flow and its ring, without closing siblings. */
    next_close_status = EBUSY;
    assert(channel_conn_close(b) == -1 && errno == EBUSY);
    assert(dev.flows[2] == b && dev.control == control);
    assert(channel_conn_close(b) == 0);

    /* No wraparound to an old generation, and no RX-window overwrite on reject. */
    dev.generations[3] = UINT32_MAX;
    cfg.flow_id = 3; cfg.rx_offset = CHANNEL_WINDOW * 2; cfg.ring_offset = CHANNEL_RING_STRIDE * 2;
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
    assert(flow_pes_live == 0 && events_live == 0);
    assert(closes == 4);
    pthread_mutex_destroy(&dev.session_lock);
    free(tx.buf); free(rx.buf); free(rings.buf);
    test_checked_close();
    test_late_open();
    puts("channel session: isolated flows, checked cleanup failures and retry verified");
    return 0;
}
