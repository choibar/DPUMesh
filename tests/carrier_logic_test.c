#include "../src/core/carrier.c"
#include <assert.h>
#include <stdio.h>
static unsigned arm_calls;
static int arm_result;
static unsigned progress_calls;
int channel_dev_progress(struct channel_dev *dev)
{ (void)dev; ++progress_calls; return 0; }

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
    s->fin_pending = 0;
    slot_free(t, s);
    assert(dmesh_native_progress(t) == 0 && progress_calls == 2);
    free(t);
}

int channel_conn_arm(struct channel_conn *conn)
{
    (void)conn;
    ++arm_calls;
    return arm_result;
}

int channel_dev_host_dpa(const struct channel_dev *dev)
{
    (void)dev;
    return 1; /* The control-only wakeup regression affects idle host-DPA. */
}

static void test_shared_control_poll_tick(void)
{
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    assert(t);
    struct slot *s = &t->slots[0];
    assert(pthread_mutex_init(&s->lock, NULL) == 0);
    assert(dmesh_native_stripe_arm(t, -1) == 0);
    assert(dmesh_native_stripe_arm(t, SLOTS) == 0);
    assert(dmesh_native_stripe_arm(t, 0) == 0); /* no live flow */
    s->state = SLOT_OPEN;
    assert(dmesh_native_stripe_arm(t, 0) == 1);
    assert(s->armed && arm_calls == 1);
    /* No TX ticket or reverse DMA completion can wake the EQ. A session
     * ERROR/disconnect still needs polling after every subsequent empty poll. */
    for (int i = 0; i < 4; ++i)
        assert(dmesh_native_stripe_arm(t, 0) == 1);
    assert(arm_calls == 1); /* preserve one-shot doorbell arming */
    s->armed = 0;
    arm_result = -1;
    assert(dmesh_native_stripe_arm(t, 0) == 1 && !s->armed);
    s->state = SLOT_CLOSED;
    assert(dmesh_native_stripe_arm(t, 0) == 0);
    s->fin_pending = 1;
    assert(dmesh_native_stripe_arm(t, 0) == 1);
    pthread_mutex_destroy(&s->lock);
    free(t);
}

int main(void)
{
    test_closed_stripe_stays_pollable_until_retired();
    test_shared_control_poll_tick();
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
    puts("carrier logic: chunking, release window, shared-control polling: PASS");
    return 0;
}
