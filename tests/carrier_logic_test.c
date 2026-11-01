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

int channel_backend_next(struct channel_dev *dev, uint32_t *worker, uint32_t *token)
{ (void)dev; (void)worker; (void)token; return 0; }
void channel_backend_finish(struct channel_dev *dev, uint32_t worker, uint32_t token, int error)
{ (void)dev; (void)worker; (void)token; (void)error; assert(!"unexpected request"); }
int dmesh_target_addr(int service, uint32_t *ip, uint16_t *port)
{ (void)service; (void)ip; (void)port; assert(!"unexpected open"); return -1; }
int channel_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out)
{ (void)dev; (void)cfg; (void)out; assert(!"unexpected open"); return -1; }
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
static int conn_gone;
int channel_conn_idle(const struct channel_conn *conn) { (void)conn; return rx_next_seq > rx_last && !conn_gone; }
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
    assert(dmesh_native_submit(t, &fin) == 0 && closes == 1 && t->port_slot[9] == 1);
    assert(dmesh_native_poll(t, 0, ev, 8) == 1 && ev[0].kind == DMESH_NATIVE_ACK && ev[0].seq == 3);
    assert(s->state == SLOT_CLOSED && t->port_slot[9] == 0 && t->active_stripes == 0);
    assert(dmesh_native_close(t) == -1 && errno == EBUSY && closes == 1);
    dmesh_native_release(t, held);
    assert(s->state == SLOT_FREE);
    pthread_mutex_destroy(&s->lock);
    assert(dmesh_native_close(t) == 0);
}

int main(void)
{
    test_closed_stripe_stays_pollable_until_retired();
    test_custody_polling();
    test_flow_consumer_rearmed_after_close();
    test_batched_poll();
    test_half_close_and_held_buffers();
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
    puts("carrier logic: chunking, release window, custody polling, flow consumer rearm, batched poll, half close and held buffers: PASS");
    return 0;
}
