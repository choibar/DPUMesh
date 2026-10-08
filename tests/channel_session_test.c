/* Exercise the production host session and flow lifecycle with a fake Comch
 * peer. DOCA data-path branches link against the SDK but must not execute. */
#include <assert.h>
#include <stdio.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include "src/transport/host/channel.c"
#ifdef OBJECT_H_
#error "Native host channels must not depend on the DPU objects layout"
#endif

static struct dmesh_comch_client *mock_control;
static unsigned client_creates, client_destroys, ring_allocs, ring_frees;
static unsigned opens, closes;
static unsigned shared_progress_calls;
static int next_close_status;
static uint8_t pending[DMESH_SESSION_MAX_FRAME];
static size_t pending_len;
/* Idle wake: the last ARM the host sent, and the control PE notification. */
static unsigned arms_seen, notify_requests, notify_clears;
static uint64_t arm_epoch_seen;
static uint32_t arm_count_seen;
static struct dmesh_session_arm_flow arm_flows_seen[DMESH_SESSION_MAX_FLOWS];
static doca_error_t send_result = DOCA_SUCCESS;
static int control_notify_fd = -1;

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
    if (send_result != DOCA_SUCCESS)
        return send_result;
    switch (h.type) {
    case DMESH_SESSION_ARM:
        ++arms_seen;
        assert(dmesh_session_arm_decode(payload, h.payload_len, &arm_epoch_seen,
                                        arm_flows_seen, &arm_count_seen) == 0);
        break;
    case DMESH_SESSION_HELLO:
        reply(DMESH_SESSION_HELLO_ACK, 0, 0, 0);
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
    ++shared_progress_calls;
    if (!pending_len) return 0;
    size_t len = pending_len;
    pending_len = 0;
    mock_control->message(mock_control->owner, pending, len);
    return 1;
}
doca_error_t doca_pe_request_notification(struct doca_pe *pe)
{
    if (pe == (struct doca_pe *)&fake_reverse_pe) return DOCA_SUCCESS;
    assert(pe == (struct doca_pe *)mock_control);
    ++notify_requests;
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_get_notification_handle(const struct doca_pe *pe, doca_notification_handle_t *handle)
{
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
    struct dma_ring *ring = calloc(1, sizeof(*ring) + sizeof(struct dma_ring_ctrl));
    assert(ring);
    ring->size = (uint32_t)size;
    ring->ctrl = (void *)(ring + 1);
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

static void dispatch_doorbell(struct channel_dev *dev)
{
    uint8_t frame[DMESH_SESSION_MAX_FRAME], epoch[DMESH_SESSION_DOORBELL_SIZE] = {0};
    size_t len = dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_DOORBELL, 0, 0, 0,
                                      epoch, sizeof(epoch));
    assert(len);
    pthread_mutex_lock(&dev->session_lock);
    session_message(dev, frame, len);
    pthread_mutex_unlock(&dev->session_lock);
}

/* ARM lists the unread descriptor of each live push flow, at most one ARM is
 * outstanding until a DOORBELL, and the control PE is armed then progressed. */
static void test_idle_wake(struct channel_dev *dev, struct channel_conn *a, struct channel_conn *b)
{
    control_notify_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    assert(control_notify_fd >= 0 && channel_dev_fd(dev) == control_notify_fd);
    unsigned arms = arms_seen, requests = notify_requests, progress = shared_progress_calls;
    b->expected = 5;
    assert(channel_dev_arm(dev) == 0);
    assert(arms_seen == arms + 1 && arm_epoch_seen == 1 && arm_count_seen == 2);
    assert(arm_flows_seen[0].flow_id == 1 && arm_flows_seen[0].generation == a->generation &&
           arm_flows_seen[0].expected_seq == 1);
    assert(arm_flows_seen[1].flow_id == 2 && arm_flows_seen[1].expected_seq == 5);
    assert(notify_requests == requests + 1 && shared_progress_calls == progress + 1);
    assert(dev->arm_outstanding);

    /* Outstanding: the next sleep re-arms the PE but sends nothing. */
    assert(channel_dev_arm(dev) == 0);
    assert(arms_seen == arms + 1 && notify_requests == requests + 2);

    /* DOORBELL releases it; ended, closed and failed flows are left out. */
    dispatch_doorbell(dev);
    assert(!dev->arm_outstanding && dev->doorbells == 1);
    b->rx_ended = 1;
    assert(channel_dev_arm(dev) == 0);
    assert(arms_seen == arms + 2 && arm_epoch_seen == 2 && arm_count_seen == 1 &&
           arm_flows_seen[0].flow_id == 1);
    dispatch_doorbell(dev);
    a->peer_closed = 1;
    assert(channel_dev_arm(dev) == 0);              /* no push flow: no ARM */
    assert(arms_seen == arms + 2 && !dev->arm_outstanding);
    a->peer_closed = 0;
    b->rx_ended = 0;

    /* A queue-full ARM fails the arm: the caller keeps polling. */
    send_result = DOCA_ERROR_AGAIN;
    assert(channel_dev_arm(dev) == -1 && errno == EAGAIN && !dev->arm_outstanding);
    send_result = DOCA_SUCCESS;

    /* host-dpa receives have their own doorbells: no ARM, PE still armed. */
    dev->host_dpa = 1;
    requests = notify_requests;
    assert(channel_dev_arm(dev) == 0 && arms_seen == arms + 2 && notify_requests == requests + 1);
    dev->host_dpa = 0;

    /* A raised fd is cleared once; an idle one is left alone. */
    uint64_t one = 1;
    assert(write(control_notify_fd, &one, sizeof(one)) == sizeof(one));
    unsigned clears = notify_clears;
    channel_dev_clear(dev);
    channel_dev_clear(dev);
    assert(notify_clears == clears + 1);

    /* A failed session cannot arm. */
    dev->session_error = ECONNRESET;
    assert(channel_dev_arm(dev) == -1 && errno == ECONNRESET);
    dev->session_error = 0;

    uint64_t sent = 0, rung = 0;
    channel_dev_wake_counters(dev, &sent, &rung);
    assert(sent == 2 && rung == 2);
    dev->arm_outstanding = 0;
    close(control_notify_fd);
    control_notify_fd = -1;
}

/* Broker side: the ARM a client listed goes out through the shared sender, one
 * at a time, and the DOORBELL raises the client's eventfd. */
static void test_broker_owner_wake(struct channel_dev *dev, struct channel_conn *a, struct channel_conn *b)
{
    struct dmesh_session_arm_flow listed[2] = {
        {.flow_id = 1, .generation = a->generation, .expected_seq = 7},
        {.flow_id = 2, .generation = b->generation, .expected_seq = 9},
    };
    unsigned arms = arms_seen;
    assert(channel_dev_send_arm(dev, listed, 2) == 0);
    assert(arms_seen == arms + 1 && arm_count_seen == 2 && arm_flows_seen[1].expected_seq == 9);
    assert(dev->arm_outstanding);
    assert(channel_dev_send_arm(dev, listed, 2) == 0 && arms_seen == arms + 1);

    int relay = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    assert(relay >= 0);
    dev->wake_relay = 1;
    dev->wake_relay_fd = relay;
    dispatch_doorbell(dev);
    uint64_t raised = 0;
    assert(read(relay, &raised, sizeof(raised)) == sizeof(raised) && raised == 1 && !dev->arm_outstanding);

    /* Nothing listed sends nothing; a failed session refuses the ARM. */
    assert(channel_dev_send_arm(dev, listed, 0) == 0 && arms_seen == arms + 1);
    dev->session_error = ECONNRESET;
    assert(channel_dev_send_arm(dev, listed, 2) == -1 && errno == ECONNRESET);
    dev->session_error = 0;
    dev->wake_relay = 0;
    close(relay);

    /* The broker arms only entries naming an incarnation it holds, and never
     * from a page the client is still writing. */
    struct broker_arm_page page = {0};
    struct dmesh_session_arm_flow flows[DMESH_SESSION_MAX_FLOWS];
    page.flow[1] = (struct broker_arm_flow){.generation = a->generation, .armed = 1, .expected = 3};
    page.flow[2] = (struct broker_arm_flow){.generation = b->generation + 1, .armed = 1, .expected = 4};
    page.flow[3] = (struct broker_arm_flow){.generation = 1, .armed = 1, .expected = 5};
    assert(channel_broker_arm_collect(&page, dev, flows) == 1);
    assert(flows[0].flow_id == 1 && flows[0].generation == a->generation && flows[0].expected_seq == 3);
    page.flow[1].armed = 0;
    assert(channel_broker_arm_collect(&page, dev, flows) == 0);
    page.seq = 1;
    assert(channel_broker_arm_collect(&page, dev, flows) == -1);
}

/* A broker that answers HELLO with its status and arm pages and eventfds, then
 * keeps the connection open: the client's liveness check watches it. */
struct fake_broker {
    int listener, sock;
    struct broker_status *status;
    struct broker_arm_page *arm;
    int status_fd, arm_fd, wake_fd, kick_fd;
};

static void *fake_broker_main(void *arg)
{
    struct fake_broker *f = arg;
    struct broker_request req;
    f->sock = accept(f->listener, NULL, NULL);
    assert(f->sock >= 0);
    assert(broker_ipc_recv(f->sock, &req, sizeof(req), NULL, 0) == 0 && req.type == BROKER_HELLO);
    assert(strcmp(req.name, "unit-broker") == 0);
    struct broker_reply rep = {.type = BROKER_REPLY, .version = BROKER_IPC_VERSION,
                               .fd_count = BROKER_HELLO_FDS, .bytes = sizeof(struct broker_status)};
    memcpy(rep.magic, BROKER_IPC_MAGIC, sizeof(rep.magic));
    int fds[BROKER_HELLO_FDS] = {f->status_fd, f->arm_fd, f->wake_fd, f->kick_fd};
    assert(broker_ipc_send(f->sock, &rep, sizeof(rep), fds, BROKER_HELLO_FDS) == 0);
    return NULL;
}

/* Answers the LISTEN, the refused backend CONN_OPEN and the BACKEND_FINISH
 * of a serving client. */
static void *fake_broker_backend(void *arg)
{
    struct fake_broker *f = arg;
    struct broker_request req;
    struct broker_reply rep = {.type = BROKER_REPLY, .version = BROKER_IPC_VERSION};
    memcpy(rep.magic, BROKER_IPC_MAGIC, sizeof(rep.magic));
    assert(broker_ipc_recv(f->sock, &req, sizeof(req), NULL, 0) == 0 && req.type == BROKER_LISTEN);
    assert(req.dst_ip == 0x0100510a && req.dst_port == 8080);
    assert(broker_ipc_send(f->sock, &rep, sizeof(rep), NULL, 0) == 0);
    assert(broker_ipc_recv(f->sock, &req, sizeof(req), NULL, 0) == 0 && req.type == BROKER_CONN_OPEN);
    assert(req.mode == CHANNEL_MODE_BACKEND_DPU_DMA && req.worker == 5 && req.token == 7);
    rep.status = ENOSPC;
    assert(broker_ipc_send(f->sock, &rep, sizeof(rep), NULL, 0) == 0);
    assert(broker_ipc_recv(f->sock, &req, sizeof(req), NULL, 0) == 0 && req.type == BROKER_BACKEND_FINISH);
    assert(req.worker == 5 && req.token == 7 && req.error == ENOSPC);
    rep.status = 0;
    assert(broker_ipc_send(f->sock, &rep, sizeof(rep), NULL, 0) == 0);
    return NULL;
}

/* Client side: the wake fd is the broker's eventfd, an ARM is the arm page
 * plus a kick, and a lost broker fails the arm so the caller keeps polling. */
static void test_broker_client_wake(void)
{
    char dir[] = "/tmp/channel-broker-XXXXXX", path[128];
    assert(mkdtemp(dir));
    snprintf(path, sizeof(path), "%s/broker.sock", dir);
    struct fake_broker f = {.sock = -1};
    f.listener = broker_ipc_listen(path);
    f.status = channel_shared_alloc(sizeof(*f.status), &f.status_fd);
    f.arm = channel_shared_alloc(sizeof(*f.arm), &f.arm_fd);
    f.wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    f.kick_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    assert(f.listener >= 0 && f.status && f.arm && f.wake_fd >= 0 && f.kick_fd >= 0);
    pthread_t thread;
    assert(pthread_create(&thread, NULL, fake_broker_main, &f) == 0);

    struct channel_dev *dev;
    assert(channel_broker_attach(path, &dev) == 0);
    assert(channel_session_open(dev, "unit-broker") == 0 && dev->hello_ready);
    assert(pthread_join(thread, NULL) == 0);

    int wake = channel_dev_fd(dev);
    struct pollfd ready = {.fd = wake, .events = POLLIN};
    uint64_t one = 1, kicks = 0;
    assert(wake >= 0 && poll(&ready, 1, 0) == 0);
    assert(write(f.wake_fd, &one, sizeof(one)) == sizeof(one));
    assert(poll(&ready, 1, 0) == 1);
    channel_dev_clear(dev);
    channel_dev_clear(dev);
    assert(poll(&ready, 1, 0) == 0);

    /* An open push flow is listed; one the broker reported CLOSED, or whose
     * stream ended, is not. */
    struct channel_conn a = {.dev = dev, .flow_id = 1, .generation = 4, .ready = 1, .expected = 11};
    struct channel_conn c = {.dev = dev, .flow_id = 3, .generation = 2, .ready = 1, .expected = 6};
    a.descs = c.descs = (volatile struct dmesh_push_desc *)f.arm;
    dev->flows[1] = &a;
    dev->flows[3] = &c;
    f.status->flow[1] = (struct broker_flow_status){.generation = 4, .state = 0};
    f.status->flow[3] = (struct broker_flow_status){.generation = 2, .state = 1};
    assert(channel_dev_arm(dev) == 0);
    assert(read(f.kick_fd, &kicks, sizeof(kicks)) == sizeof(kicks) && kicks == 1);
    assert(f.arm->seq == 2 && f.arm->flow[1].armed && f.arm->flow[1].generation == 4 &&
           f.arm->flow[1].expected == 11);
    assert(!f.arm->flow[2].armed && !f.arm->flow[3].armed);
    a.rx_ended = 1;
    assert(channel_dev_arm(dev) == 0 && f.arm->seq == 4 && !f.arm->flow[1].armed);
    a.rx_ended = 0;

    f.status->arms_sent = 5;
    f.status->doorbells = 3;
    uint64_t sent = 0, rung = 0;
    channel_dev_wake_counters(dev, &sent, &rung);
    assert(sent == 5 && rung == 3);

    /* The DPA's error flag in the shared ring fails the flow before the broker
     * publishes anything. */
    struct dma_ring_ctrl ctrl = {0};
    struct dma_ring ring = {.ctrl = &ctrl};
    a.forward_ring = &ring;
    assert(channel_conn_poll(&a) == 0);
    ctrl.error = 2;
    assert(channel_conn_poll(&a) == -1 && errno == EIO);
    a.forward_ring = NULL;

    /* Backend requests arrive in the status page, each taken once; the flow
     * and the outcome go back over the socket with the worker and token. */
    uint32_t worker, token;
    assert(channel_backend_next(dev, &worker, &token) == 0);
    f.status->backend_token[5] = 7;
    f.status->backend_requests = 1;
    assert(channel_backend_next(dev, &worker, &token) == 1 && worker == 5 && token == 7);
    assert(channel_backend_next(dev, &worker, &token) == 0);
    assert(pthread_create(&thread, NULL, fake_broker_backend, &f) == 0);
    assert(channel_session_listen(dev, 0x0100510a, 8080) == 0);
    struct channel_mem tx = {.shared = 1, .id = 0}, rx = {.shared = 1, .id = 1, .bytes = CHANNEL_WINDOW};
    struct channel_conn_config cfg = {.flow_id = 2, .mode = CHANNEL_MODE_BACKEND_DPU_DMA,
                                      .backend_worker = worker, .backend_token = token, .tx = &tx, .rx = &rx};
    struct channel_conn *flow;
    assert(channel_conn_open(dev, &cfg, &flow) == -1 && errno == ENOSPC);
    channel_backend_finish(dev, worker, token, ENOSPC);
    assert(pthread_join(thread, NULL) == 0);
    f.status->backend_token[2] = 3;
    f.status->backend_token[5] = 9;
    f.status->backend_requests = 3;
    assert(channel_backend_next(dev, &worker, &token) == 1 && worker == 2 && token == 3);
    assert(channel_backend_next(dev, &worker, &token) == 1 && worker == 5 && token == 9);
    assert(channel_backend_next(dev, &worker, &token) == 0);

    /* The liveness check runs every 20 ms (BROKER_CHECK_NS). */
    close(f.sock);
    const struct timespec pause = {.tv_nsec = 30 * 1000 * 1000};
    nanosleep(&pause, NULL);
    assert(channel_dev_arm(dev) == -1 && errno == ECONNRESET);
    assert(channel_dev_progress(dev) == -1 && errno == ECONNRESET);

    dev->flows[1] = dev->flows[3] = NULL;
    channel_broker_detach(dev);
    channel_shared_free(f.status, sizeof(*f.status), f.status_fd);
    channel_shared_free(f.arm, sizeof(*f.arm), f.arm_fd);
    close(f.wake_fd);
    close(f.kick_fd);
    close(f.listener);
    unlink(path);
    rmdir(dir);
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
            channel_conn_rx_consumed(a, CHANNEL_DESC_N / 2, CHANNEL_RX_CONSUMED_POS_BATCH);
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
    session_message(dev, frame, n);
    assert(channel_backend_next(dev, &worker, &token) == 0); /* replay cannot allocate again */
}

/* Broker side: each backend request the DPU sends is taken once and
 * published in the status page; a replay publishes nothing. */
static void test_broker_backend_publish(struct channel_dev *dev)
{
    struct broker_status status = {0};
    uint8_t frame[DMESH_SESSION_HEADER_SIZE + 4], payload[4];
    dmesh_session_put_u32(payload, 4);
    size_t n = dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_BACKEND_REQUEST,
        0, 12, 0, payload, 4);
    assert(channel_broker_backend_publish(dev, &status) == 0 && status.backend_requests == 0);
    session_message(dev, frame, n);
    assert(channel_broker_backend_publish(dev, &status) == 1);
    assert(status.backend_requests == 1 && status.backend_token[4] == 12 && dev->backend[4].state == 2);
    session_message(dev, frame, n);
    assert(channel_broker_backend_publish(dev, &status) == 0 && status.backend_requests == 1);
    channel_backend_finish(dev, 4, 12, 0);
    assert(dev->backend[4].state == 3 && dev->backend_rejections == 0);
}

int main(void)
{
    struct channel_dev dev = {0};
    pthread_mutex_init(&dev.session_lock, NULL);
    assert(channel_session_open(&dev, "unit-session") == 0);
    assert(client_creates == 1 && dev.hello_ready);
    test_backend_requests(&dev);
    test_broker_backend_publish(&dev);
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
    test_idle_wake(&dev, a, b);
    test_broker_owner_wake(&dev, a, b);
    test_broker_client_wake();

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
    assert(dev.control == control && client_destroys == 0 && ring_frees == 1);
    cfg.flow_id = 1; cfg.rx_offset = 0;
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
