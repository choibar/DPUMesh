#include "../src/core/carrier.c"
#include <assert.h>
#include <stdio.h>
static unsigned arm_calls;
static int arm_result;
static unsigned progress_calls;
int channel_dev_progress(struct channel_dev *dev)
{ (void)dev; ++progress_calls; return 0; }
/* Off unless a test turns it on: the stripes keep their slot epoll doorbells. */
static int flow_wake;
static unsigned flow_arm_calls, flow_progress_calls;
int channel_dev_flow_wake(const struct channel_dev *dev) { (void)dev; return flow_wake; }
int channel_flow_fd(struct channel_dev *dev, uint32_t flow_id) { (void)dev; (void)flow_id; return -1; }
void channel_flow_progress(struct channel_dev *dev, uint32_t flow_id)
{ (void)dev; assert(flow_id == 1); ++flow_progress_calls; }
int channel_flow_arm(struct channel_dev *dev, uint32_t flow_id)
{ (void)dev; assert(flow_id == 1); ++flow_arm_calls; return 0; }
uint32_t channel_dev_take_news(struct channel_dev *dev) { (void)dev; return 0; }

/* No DPU request unless a test queues one; its answer is recorded. */
static int backend_requests, backend_answer = -1;
int channel_backend_next(struct channel_dev *dev, uint32_t *worker, uint32_t *token)
{ (void)dev; if (!backend_requests) return 0; --backend_requests; *worker = 3; *token = 7; return 1; }
void channel_backend_finish(struct channel_dev *dev, uint32_t worker, uint32_t token, int error)
{ (void)dev; assert(worker == 3 && token == 7 && backend_answer == -1); backend_answer = error; }
/* Opens are refused; a test that expects one reads the flow id it got. */
static int expect_open;
static uint32_t held_ids, opened_id;   /* held: bit (flow id - 1) the channel keeps */
int dmesh_target_addr(int service, uint32_t *ip, uint16_t *port)
{ (void)service; assert(expect_open); *ip = 0; *port = 0; return 0; }
int channel_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out)
{ (void)dev; (void)out; assert(expect_open); opened_id = cfg->flow_id; errno = ECONNREFUSED; return -1; }
/* A started open is outstanding until a test lets READY arrive, or fails with open_error. */
static int open_outstanding, open_error;
int channel_conn_open_start(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out)
{ (void)dev; assert(expect_open); opened_id = cfg->flow_id; *out = (struct channel_conn *)cfg; return 0; }
int channel_conn_open_finish(struct channel_conn *conn)
{ (void)conn; if (open_error) { errno = open_error; return -1; } return open_outstanding; }
int channel_dev_host_dpa(const struct channel_dev *dev) { (void)dev; return 0; }
int channel_flow_free(struct channel_dev *dev, uint32_t flow_id)
{ (void)dev; return !(held_ids & (UINT32_C(1) << (flow_id - 1))); }
int channel_conn_fds(struct channel_conn *conn, int *fds, int max)
{ (void)conn; (void)fds; (void)max; return 0; }
void channel_conn_clear(struct channel_conn *conn, int fd) { (void)conn; (void)fd; }

static void test_closed_stripe_stays_pollable_until_retired(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[31];
    s->state = SLOT_CLOSED;
    s->fin_pending = 1;
    t->active_stripes = UINT32_C(1) << 31;
    assert(dmesh_native_progress(t) == (UINT32_C(1) << 31));
    assert(progress_calls == 1);
    /* Only retirement, not close, may remove the stripe from future drains. */
    slot_reap_locked(t, s);
    assert(dmesh_native_progress(t) == (UINT32_C(1) << 31));
    s->fin_pending = 0;
    slot_reap_locked(t, s);
    assert(dmesh_native_progress(t) == 0 && progress_calls == 3 && s->state == SLOT_FREE);
    free(t);
}

int channel_conn_arm(struct channel_conn *conn)
{
    (void)conn;
    ++arm_calls;
    return arm_result;
}


/* Only outstanding custody keeps a stripe polling: the shared control session
 * now wakes a sleeping EQ through the channel wake fd, and push receives
 * through the DPU's DOORBELL. The reverse doorbell stays one-shot. */
static void test_custody_polling(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0);
    assert(dmesh_native_stripe_arm(t, -1, 0) == 0);
    assert(dmesh_native_stripe_arm(t, SLOTS, 0) == 0);
    assert(dmesh_native_stripe_arm(t, 0, 0) == 0); /* no live flow */
    s->state = SLOT_OPEN;
    assert(dmesh_native_stripe_arm(t, 0, 0) == 0);
    assert(s->armed && arm_calls == 1);
    for (int i = 0; i < 4; ++i)
        assert(dmesh_native_stripe_arm(t, 0, 0) == 0);
    assert(arm_calls == 1); /* preserve one-shot doorbell arming */
    s->tickets[0] = (struct ticket){1, 1};
    s->t_tail = 1;               /* custody outstanding: no doorbell covers its ACK */
    assert(dmesh_native_stripe_arm(t, 0, 0) == DMESH_STRIPE_CUSTODY);
    s->t_head = 1;
    assert(dmesh_native_stripe_arm(t, 0, 0) == 0);
    s->armed = 0;
    arm_result = -1;
    assert(dmesh_native_stripe_arm(t, 0, 0) == 0 && !s->armed);
    s->state = SLOT_CLOSED;
    assert(dmesh_native_stripe_arm(t, 0, 0) == 0);
    s->fin_pending = 1;
    assert(dmesh_native_stripe_arm(t, 0, 0) == DMESH_STRIPE_RETIRING);
    s->fin_pending = 0;
    s->t_tail = 2;
    assert(dmesh_native_stripe_arm(t, 0, 0) == DMESH_STRIPE_RETIRING);
    pthread_mutex_destroy(&s->lock);
    free(t);
}

/* A flow wake's consumer outlives its flows: one raised as its flow closed is
 * cleared by the next request, which a closed or free slot still gets, after
 * its clear progresses away the descriptors its last flow left. */
static void test_flow_consumer_rearmed_after_close(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0);
    t->flow_wake = 1;
    s->state = SLOT_OPEN;
    assert(dmesh_native_stripe_arm(t, 0, 0) == 0 && s->armed && flow_arm_calls == 1);
    assert(dmesh_native_stripe_arm(t, 0, 1) == 0 && s->armed && flow_arm_calls == 2);
    assert(flow_progress_calls == 0);   /* an open slot's receives are its poll's */
    s->state = SLOT_CLOSED;
    assert(dmesh_native_stripe_arm(t, 0, 1) == 0 && s->armed && flow_arm_calls == 3);
    assert(flow_progress_calls == 1);
    s->state = SLOT_FREE;
    assert(dmesh_native_stripe_arm(t, 0, 1) == 0 && s->armed && flow_arm_calls == 4);
    assert(flow_progress_calls == 2);
    assert(dmesh_native_stripe_arm(t, 0, 0) == 0 && flow_arm_calls == 4); /* still one-shot */
    pthread_mutex_destroy(&s->lock);
    free(t);
}

/* A connection whose receives, custody and close the poll test scripts. */
static uint64_t conn_consumed, rx_next_seq = 1, rx_last;
static int conn_gone, conn_announce, conn_request;
int channel_conn_idle(const struct channel_conn *conn) { (void)conn; return rx_next_seq > rx_last && !conn_gone && !conn_announce; }
int channel_conn_announced(struct channel_conn *conn) { (void)conn; int a = conn_announce; conn_announce = 0; return a; }
int channel_conn_take_request(struct channel_conn *conn, int *kind, uint32_t *count, uint32_t *token)
{ (void)conn; *kind = conn_request; conn_request = 0; *count = 3; *token = 9; return *kind != 0; }
static unsigned spare_readies, scale_acks;
int channel_conn_spare_ready(struct channel_conn *conn) { (void)conn; ++spare_readies; return 0; }
int channel_conn_scale_ack(struct channel_conn *conn, uint32_t token, uint32_t applied)
{ (void)conn; assert(token == 9 && applied == 2); ++scale_acks; return 0; }
int channel_conn_poll(struct channel_conn *conn) { (void)conn; return conn_gone; }
uint64_t channel_conn_consumed(const struct channel_conn *conn) { (void)conn; return conn_consumed; }
int channel_conn_rx_next(struct channel_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len)
{
    (void)conn;
    if (rx_next_seq > rx_last) return 0;
    *seq = rx_next_seq; *pos = (uint32_t)(rx_next_seq - 1) * 128u; *len = 100;
    rx_next_seq++;
    return 1;
}

/* One poll hands out a batch under one hold of the slot: the flow wake is
 * progressed first, consumed tickets with consecutive sequences come as one
 * ACK before the receives, and the batch stops at max. A closed slot retires
 * its custody and then its FIN. */
static void test_batched_poll(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0 && pthread_mutex_init(&t->lock, NULL) == 0);
    struct dmesh_native_event ev[8];
    t->flow_wake = 1;
    s->state = SLOT_OPEN; s->port = 7;
    carrier_window_init(&s->window);
    unsigned progressed = flow_progress_calls;
    assert(dmesh_native_poll(t, 0, ev, 8) == 0 && flow_progress_calls == progressed + 1);
    /* Tickets 10, 11, 12 consumed, 14 (a gap in sequence) consumed, 15 not. */
    uint16_t seqs[] = {10, 11, 12, 14, 15};
    for (int i = 0; i < 5; ++i) s->tickets[i] = (struct ticket){seqs[i], (uint64_t)(i + 1) * 2};
    s->t_tail = 5;
    conn_consumed = 8;
    rx_last = 3;
    int n = dmesh_native_poll(t, 0, ev, 3);
    assert(n == 3);
    assert(ev[0].kind == DMESH_NATIVE_ACK && ev[0].port == 7 && ev[0].seq == 10 && ev[0].seq_count == 3);
    assert(ev[1].kind == DMESH_NATIVE_ACK && ev[1].seq == 14 && ev[1].seq_count == 1);
    assert(ev[2].kind == DMESH_NATIVE_RX && ev[2].desc.dst_port == 7 && ev[2].desc.body_len == 100);
    n = dmesh_native_poll(t, 0, ev, 8);
    assert(n == 2 && ev[0].kind == DMESH_NATIVE_RX && ev[1].kind == DMESH_NATIVE_RX);
    assert(ev[1].desc.seq == (uint16_t)(ev[0].desc.seq + 1));
    assert(dmesh_native_poll(t, 0, ev, 8) == 0);            /* ticket 15 still in custody */
    conn_consumed = 10;
    assert(dmesh_native_poll(t, 0, ev, 8) == 1 && ev[0].seq == 15 && ev[0].seq_count == 1);
    /* Closed with custody outstanding: retired without the consumer, then the FIN. */
    s->tickets[5] = (struct ticket){16, 99};
    s->tickets[6] = (struct ticket){17, 100};
    s->t_tail = 7;
    s->state = SLOT_CLOSED; s->fin_pending = 1; s->fin_seq = 18;
    t->port_slot[7] = 1;
    n = dmesh_native_poll(t, 0, ev, 8);
    assert(n == 2 && ev[0].seq == 16 && ev[0].seq_count == 2 && ev[1].seq == 18 && ev[1].seq_count == 1);
    /* Retired, it leaves its port; the three batches the application holds
     * keep the slot until the last is released. */
    assert(s->state == SLOT_CLOSED && t->port_slot[7] == 0);
    dmesh_native_release(t, CHANNEL_DATA_OFF + 128);
    dmesh_native_release(t, CHANNEL_DATA_OFF);
    assert(s->state == SLOT_CLOSED);
    dmesh_native_release(t, CHANNEL_DATA_OFF + 256);
    assert(s->state == SLOT_FREE);
    t->flow_wake = 0;
    pthread_mutex_destroy(&s->lock);
    free(t);
}

/* What the close tests script beyond the poll's connection. */
static unsigned shutdowns, closes;
int channel_conn_shutdown(struct channel_conn *conn) { (void)conn; ++shutdowns; return 0; }
int channel_conn_close(struct channel_conn *conn) { (void)conn; ++closes; return 0; }
/* A CLOSE is sent once (closes counts it) unless the session refuses it; it
 * is then outstanding until a test lets CLOSED arrive, or fails with close_errno. */
static int close_outstanding, close_refusals, close_errno, close_asked;
int channel_conn_close_start(struct channel_conn *conn)
{
    (void)conn;
    if (close_refusals) { --close_refusals; errno = EAGAIN; return -1; }
    if (!close_asked) { close_asked = 1; ++closes; }
    return 0;
}
int channel_conn_close_finish(struct channel_conn *conn)
{
    (void)conn;
    if (close_errno) { errno = close_errno; return -1; }
    if (close_outstanding) return 1;
    close_asked = 0;
    return 0;
}
uint32_t channel_conn_ring_free(const struct channel_conn *conn) { (void)conn; return CHANNEL_RING_SIZE; }
uint64_t channel_conn_post(struct channel_conn *conn, uint64_t addr, uint32_t bytes)
{ (void)conn; (void)addr; (void)bytes; return 0; }
void *channel_mem_base(const struct channel_mem *mem) { (void)mem; return NULL; }
void channel_conn_rx_consumed(struct channel_conn *conn, uint64_t seq, uint64_t bytes)
{ (void)conn; (void)seq; (void)bytes; }
int channel_session_close(struct channel_dev *dev) { (void)dev; return 0; }
void channel_mem_free(struct channel_mem *mem) { (void)mem; }
void channel_dev_close(struct channel_dev *dev) { (void)dev; }

/* A half close ends only the forward stream: its FIN is ACKed after the
 * custody ahead of it while receives go on. The QP's own FIN closes the flow;
 * retired, the slot leaves its port but is reused only once its held batch is
 * released, and the end of stream holds none. A held batch keeps the carrier
 * from closing. */
static void test_half_close_and_held_buffers(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0 && pthread_mutex_init(&t->lock, NULL) == 0);
    for (int i = 0; i < SLOTS; ++i) t->slots[i].epfd = -1;
    struct dmesh_native_event ev[8];
    s->state = SLOT_OPEN; s->port = 9; s->conn = (struct channel_conn *)t;
    t->port_slot[9] = 1; t->active_stripes = 1;
    carrier_window_init(&s->window);
    s->tickets[0] = (struct ticket){1, 5}; s->t_tail = 1;
    conn_consumed = 4;
    sw_descriptor_t fin = {.src_port = 9, .seq = 2, .flags = DMESH_DESC_HALF_CLOSE, .valid = 1};
    assert(dmesh_native_submit(t, &fin) == 0);
    assert(shutdowns == 1 && closes == 0 && s->state == SLOT_OPEN);
    rx_next_seq = rx_last = 1;                 /* a new flow: its first batch, at 0 */
    assert(dmesh_native_poll(t, 0, ev, 8) == 1 && ev[0].kind == DMESH_NATIVE_RX);
    int held = ev[0].desc.body_buf_slot;
    conn_consumed = 5;
    assert(dmesh_native_poll(t, 0, ev, 8) == 1);
    assert(ev[0].kind == DMESH_NATIVE_ACK && ev[0].seq == 1 && ev[0].seq_count == 2);
    conn_gone = 1;
    assert(dmesh_native_poll(t, 0, ev, 8) == 1 && ev[0].kind == DMESH_NATIVE_RX && ev[0].desc.body_len == 0);
    dmesh_native_release(t, ev[0].desc.body_buf_slot);
    assert(!carrier_window_empty(&s->window));
    conn_gone = 0;
    fin.seq = 3; fin.flags = 0;
    close_outstanding = 1;          /* the FIN does not wait for the DPU's CLOSED */
    assert(dmesh_native_submit(t, &fin) == 0 && closes == 1 && t->port_slot[9] == 1);
    assert(s->state == SLOT_CLOSING && dmesh_native_poll(t, 0, ev, 8) == 0 && s->state == SLOT_CLOSING);
    close_outstanding = 0;          /* CLOSED landed: the FIN is ACKed in the same poll */
    assert(dmesh_native_poll(t, 0, ev, 8) == 1 && ev[0].kind == DMESH_NATIVE_ACK && ev[0].seq == 3);
    assert(s->state == SLOT_CLOSED && t->port_slot[9] == 0 && t->active_stripes == 0);
    assert(dmesh_native_close(t) == -1 && errno == EBUSY && closes == 1);
    dmesh_native_release(t, held);
    assert(s->state == SLOT_FREE);
    pthread_mutex_destroy(&s->lock);
    assert(dmesh_native_close(t) == 0);
}

/* A free slot whose flow id the channel still holds (an open that timed
 * out, its close unconfirmed) is skipped, not retried into EBUSY. */
static void test_held_id_skipped(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    expect_open = 1;
    held_ids = 1;
    assert(dmesh_native_connect(t, 40000, 1) == -1 && errno == ECONNREFUSED && opened_id == 2);
    held_ids = 0;
    assert(dmesh_native_connect(t, 40000, 1) == -1 && opened_id == 1);
    held_ids = UINT32_MAX;
    opened_id = 0;
    assert(dmesh_native_connect(t, 40000, 1) == -1 && errno == ENOSPC && opened_id == 0);
    expect_open = 0; held_ids = 0;
    free(t);
}

/* A backend flow the DPU requests opens without waiting for READY: the
 * request is answered once a poll sees it, and that poll already reads the
 * batches published before it. */
static void test_backend_opens_async(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0 && pthread_mutex_init(&t->lock, NULL) == 0);
    struct dmesh_native_event ev[8];
    t->service_id = 1; t->backend_max = SLOTS; t->next_uport = UPORT_BASE;
    expect_open = 1; open_outstanding = 1; backend_requests = 1;
    (void)dmesh_native_progress(t);
    assert(s->state == SLOT_OPENING && opened_id == 1 && backend_answer == -1);
    assert(dmesh_native_poll(t, 0, ev, 8) == 0 && s->state == SLOT_OPENING && backend_answer == -1);
    open_outstanding = 0;
    rx_next_seq = rx_last = 1;      /* its first batch is published already */
    assert(dmesh_native_poll(t, 0, ev, 8) == 1 && ev[0].kind == DMESH_NATIVE_RX);
    assert(s->state == SLOT_OPEN && s->claimed && backend_answer == 0);
    backend_answer = -1;
    pthread_mutex_destroy(&s->lock);
    free(t);
}

/* A backend open that timed out keeps its slot, and so its flow id, until the
 * DPU confirms the close, however late: a refused CLOSE is sent again, and a
 * CLOSED after the deadline still frees the slot. */
static void test_backend_open_timeout_waits_for_closed(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0 && pthread_mutex_init(&t->lock, NULL) == 0);
    struct dmesh_native_event ev[8];
    t->service_id = 1; t->backend_max = SLOTS; t->next_uport = UPORT_BASE;
    backend_requests = 1; open_error = ETIMEDOUT; close_refusals = 2; close_outstanding = 1;
    unsigned before = closes;
    (void)dmesh_native_progress(t);
    assert(dmesh_native_poll(t, 0, ev, 8) == 0 && backend_answer == ETIMEDOUT);
    assert(s->state == SLOT_CLOSING && closes == before);          /* both sends of that pass refused */
    assert(dmesh_native_poll(t, 0, ev, 8) == 0 && closes == before + 1 && s->state == SLOT_CLOSING);
    close_errno = ETIMEDOUT;                                      /* no CLOSED in time */
    assert(dmesh_native_poll(t, 0, ev, 8) == 0 && s->state == SLOT_CLOSING && !s->close_failed);
    close_errno = 0; close_outstanding = 0;                       /* the late CLOSED */
    assert(dmesh_native_poll(t, 0, ev, 8) == 0 && s->state == SLOT_FREE && t->active_stripes == 0);
    assert(closes == before + 1);
    expect_open = 0; open_error = 0; backend_answer = -1;
    pthread_mutex_destroy(&s->lock);
    free(t);
}

/* A DPU request on a client flow is announced once by its stripe's poll and
 * taken by the port's QP; answers go out on the open client flow only. */
static void test_flow_requests(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0 && pthread_mutex_init(&t->lock, NULL) == 0);
    struct dmesh_native_event ev[8];
    s->state = SLOT_OPEN; s->port = 11; s->conn = (struct channel_conn *)t;
    t->port_slot[11] = 1;
    rx_next_seq = 1; rx_last = 0;
    conn_announce = 1; conn_request = CHANNEL_REQ_SCALE;
    assert(dmesh_native_poll(t, 0, ev, 8) == 1 && ev[0].kind == DMESH_NATIVE_REQUEST && ev[0].port == 11);
    assert(dmesh_native_poll(t, 0, ev, 8) == 0);           /* announced once */
    int kind; uint32_t ip = 1, count, token; uint16_t dst = 1;
    s->dst_ip = 0x0109630a; s->dst_port = 9000;            /* the destination it opened to */
    assert(dmesh_native_take_request(t, 11, &kind, &ip, &dst, &count, &token) == 1);
    assert(kind == DMESH_REQ_SCALE && count == 3 && token == 9 && ip == 0x0109630a && dst == 9000);
    assert(dmesh_native_take_request(t, 11, &kind, &ip, &dst, &count, &token) == 0);
    assert(dmesh_native_scale_ack(t, 11, 9, 2) == 0 && dmesh_native_spare_ready(t, 11) == 0);
    assert(scale_acks == 1 && spare_readies == 1);
    s->backend = 1;                                          /* never on a backend flow */
    assert(dmesh_native_spare_ready(t, 11) == -1 && errno == ENOTCONN);
    s->backend = 0; s->state = SLOT_CLOSING;
    assert(dmesh_native_scale_ack(t, 11, 9, 2) == -1 && errno == ENOTCONN);
    assert(dmesh_native_spare_ready(t, 12) == -1 && errno == ENOTCONN);
    pthread_mutex_destroy(&s->lock);
    free(t);
}

/* A Pod needs neither DPUMESH_POD_IP nor DPUMESH_SERVICE: its address is the
 * source of the route toward KUBERNETES_SERVICE_HOST, and DPUMESH_PORT alone
 * makes it serve that address. */
static void test_pod_identity_from_environment(void)
{
    uint32_t ip = 0;
    char buf[INET_ADDRSTRLEN + 6];
    unsetenv("DPUMESH_POD_IP");
    unsetenv("KUBERNETES_SERVICE_HOST");
    assert(pod_ip_lookup(&ip) == -1 && errno == EINVAL);
    setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1", 1);
    assert(pod_ip_lookup(&ip) == 0 && ip == htonl(INADDR_LOOPBACK));
    setenv("DPUMESH_POD_IP", "10.244.1.7", 1);        /* the override wins */
    assert(pod_ip_lookup(&ip) == 0 && ip == htonl(0x0af40107));
    setenv("DPUMESH_POD_IP", "pod", 1);
    assert(pod_ip_lookup(&ip) == -1 && errno == EINVAL);
    unsetenv("DPUMESH_POD_IP");
    unsetenv("KUBERNETES_SERVICE_HOST");

    assert(served_target(buf, sizeof(buf), NULL, ip, -1) == NULL);
    assert(served_target(buf, sizeof(buf), "", ip, -1) == NULL);
    assert(strcmp(served_target(buf, sizeof(buf), "", htonl(0x0af40107), 8080), "10.244.1.7:8080") == 0);
    assert(strcmp(served_target(buf, sizeof(buf), NULL, htonl(0xffffffff), 65535), "255.255.255.255:65535") == 0);
    assert(strcmp(served_target(buf, sizeof(buf), "echo:80", htonl(0x0af40107), 8080), "echo:80") == 0);
}

int main(void)
{
    test_closed_stripe_stays_pollable_until_retired();
    test_custody_polling();
    test_flow_consumer_rearmed_after_close();
    test_batched_poll();
    test_half_close_and_held_buffers();
    test_held_id_skipped();
    test_backend_opens_async();
    test_backend_open_timeout_waits_for_closed();
    test_flow_requests();
    test_pod_identity_from_environment();
    uint32_t p[2];
    assert(carrier_chunks(0, p) == 0);
    assert(carrier_chunks(1, p) == 1 && p[0] == 1);
    assert(carrier_chunks(128, p) == 1 && p[0] == 128);
    assert(carrier_chunks(129, p) == 2 && p[0] == 128 && p[1] == 1);
    assert(carrier_chunks(300, p) == 2 && p[0] == 256 && p[1] == 44);
    assert(carrier_chunks(8064, p) == 1 && p[0] == 8064);
    assert(carrier_chunks(8100, p) == 2 && p[0] == 8064 && p[1] == 36);
    assert(carrier_chunks(8192, p) == 2 && p[0] == 8064 && p[1] == 128);
    struct carrier_rx_window w; carrier_window_init(&w);
    uint64_t seq, bytes;
    assert(!carrier_window_advance(&w, &seq, &bytes) && seq == 0 && bytes == 0);
    assert(carrier_window_add(&w, 1, 0, 100) == 0);
    assert(carrier_window_add(&w, 2, 128, 200) == 0);
    assert(carrier_window_add(&w, 3, 384, 300) == 0);
    assert(carrier_window_release(&w, 128) == 0);           /* out of order */
    assert(!carrier_window_advance(&w, &seq, &bytes) && seq == 0);
    assert(carrier_window_release(&w, 0) == 0);
    assert(carrier_window_advance(&w, &seq, &bytes) && seq == 2 && bytes == 300);
    assert(carrier_window_release(&w, 999) == -1);
    assert(carrier_window_release(&w, 384) == 0);
    assert(carrier_window_advance(&w, &seq, &bytes) && seq == 3 && bytes == 600);
    assert(carrier_window_add(&w, 1 + CHANNEL_DESC_N, 0, 8) == 0);  /* slot reuse after retirement */
    /* In-order releases find their batch at the search's start. */
    struct carrier_rx_window v; carrier_window_init(&v);
    for (uint64_t k = 1; k <= 3 * CHANNEL_DESC_N; ++k) {
        assert(carrier_window_add(&v, k, (uint32_t)(k * 64u % 4096u), 64) == 0);
        assert(carrier_window_release(&v, (uint32_t)(k * 64u % 4096u)) == 0);
        assert(carrier_window_advance(&v, &seq, &bytes) && seq == k && bytes == k * 64u);
    }
    puts("carrier logic: chunking, release window, custody polling, flow consumer rearm, batched poll, half close, async close and open, held buffers, held flow ids (sync and async opens), flow requests and pod identity: PASS");
    return 0;
}
