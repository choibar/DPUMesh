/* The channel as the listening socket over the memory carrier, with and
 * without flow wake. dmesh_accept does the channel's work and returns each
 * inbound stream as a QP whose own poll returns its first bytes. With flow
 * wake a QP's EQ advances and drains only its own stripe and never progresses
 * or arms the session, and the channel wakes it for its flow's control news;
 * without it the channel drains every stripe and hands each QP its receives. */
#include <assert.h>
#include <poll.h>
#include <stdio.h>
#include "../src/core/dmesh_core.c"
#include "support/native_memory_transport.c"

static int readable(int fd)
{
    struct pollfd p = {.fd = fd, .events = POLLIN};
    return poll(&p, 1, 0) == 1;
}

static void settle(dmesh_qp_t *qp)
{
    uint64_t v;
    while (read(qp->eq->notify_efd, &v, sizeof(v)) > 0) {}
}

static dmesh_channel_t *channel(const char *service)
{
    if (service) setenv("DPUMESH_SERVICE", service, 1);
    dmesh_channel_t *ch = dmesh_create_channel();
    unsetenv("DPUMESH_SERVICE");
    assert(ch && dmesh_channel_fd(ch) >= 0);
    /* Born readable: a poller that waits before its first accept wakes. */
    assert(readable(dmesh_channel_fd(ch)));
    return ch;
}

static dmesh_qp_t *dial(dmesh_channel_t *ch)
{
    dmesh_qp_t *qp = dmesh_create_qp(ch, "echo:80");
    assert(qp && dmesh_qp_fd(qp) >= 0);
    settle(qp);
    return qp;
}

static void send_bytes(dmesh_qp_t *qp, uint32_t n, unsigned char v)
{
    void *b = dmesh_alloc(qp, n);
    assert(b);
    memset(b, v, n);
    assert(dmesh_post_send(qp, b, n) == 0 && dmesh_flush(qp) == 0);
}

/* The next event of qp: one RECV of len bytes, released; then qp is empty. */
static void expect_recv(dmesh_channel_t *ch, dmesh_qp_t *qp, uint32_t len)
{
    dmesh_event_t ev;
    assert(dmesh_poll_qp(qp, &ev, 1) == 1);
    assert(ev.type == DMESH_EVENT_RECV && ev.qp == qp && ev.len == len);
    dmesh_release_rx_buffer(ch, &ev);
    assert(dmesh_poll_qp(qp, &ev, 1) == 0);
}

static void idle(dmesh_channel_t *ch)
{
    assert(dmesh_accept(ch) == NULL && errno == EAGAIN);
}

/* An inbound stream, with its fd handed out. */
static dmesh_qp_t *accept_one(dmesh_channel_t *ch)
{
    dmesh_qp_t *qp = dmesh_accept(ch);
    assert(qp && dmesh_qp_fd(qp) >= 0);
    return qp;
}

static uint32_t bit(dmesh_qp_t *qp) { return UINT32_C(1) << (qp->local_port % 8); }

static void teardown(dmesh_channel_t *server, dmesh_channel_t *client, dmesh_qp_t **qps, unsigned n)
{
    for (unsigned i = 0; i < n; ++i) assert(dmesh_destroy_qp(qps[i]) == 0);
    for (int round = 0; round < 4; ++round) {
        (void)dmesh_accept(server);
        (void)dmesh_accept(client);
    }
    assert(dmesh_destroy_channel(client) == 0 && dmesh_destroy_channel(server) == 0);
}

int main(void)
{
    dmesh_event_t ev;
    setenv("DPUMESH_RINGS_PER_POD", "8", 1);
    test_native_stripe_map(1);
    test_native_rx_signaled(1);        /* the Comch fast path */
    test_native_flow_wake(1);
    dmesh_channel_t *server = channel("echo:80"), *client = channel(NULL);
    assert(server->ctx->flow_wake && client->ctx->landing_stripes == 8);
    dmesh_qp_t *a = dial(client), *b = dial(client);
    assert(bit(a) != bit(b));
    assert(atomic_load(&a->eq->stripes) == bit(a) && atomic_load(&b->eq->stripes) == bit(b));

    /* A new stream waits for the channel; accepted, it is a QP owning its
     * stripe, whose own poll returns its first bytes. */
    send_bytes(a, 32, 1);
    dmesh_qp_t *sa = accept_one(server);
    assert(atomic_load(&sa->eq->stripes) == bit(sa));
    expect_recv(server, sa, 32);
    idle(server);

    /* Its later receives: its EQ advances its stripe alone; the channel and
     * the session leave it to that EQ. */
    send_bytes(a, 16, 2);
    (void)test_native_progressed();
    idle(server);
    assert((test_native_progressed() & bit(sa)) == 0);
    unsigned sessions = test_native_session_progress();
    expect_recv(server, sa, 16);
    assert(test_native_session_progress() == sessions && test_native_progressed() == bit(sa));

    /* A second stream, its first payload behind its arrival. */
    send_bytes(b, 8, 3);
    dmesh_qp_t *sb = accept_one(server);
    assert(dmesh_poll_qp(sa, &ev, 1) == 0);
    expect_recv(server, sb, 8);

    /* b's reply: drained by b's EQ alone; a's EQ and the channel neither
     * return it nor wake b's EQ for it. */
    settle(a); settle(b);
    send_bytes(sb, 8, 4);
    assert(dmesh_poll_qp(a, &ev, 1) == 0);
    idle(client);
    assert(!readable(b->eq->notify_efd));
    expect_recv(client, b, 8);

    /* Control news for a flow: the channel wakes the QP owning it. */
    settle(a); settle(b);
    test_native_news(bit(b));
    idle(client);
    assert(readable(b->eq->notify_efd) && !readable(a->eq->notify_efd));

    /* An empty QP arms its own stripe, never the session; the channel arms
     * the session and the stripes no QP owns, and its fd wakes on the
     * session's doorbell, which accepting acknowledges. */
    test_native_sleepable(1);
    unsigned arms = test_native_idle_arms();
    (void)test_native_armed();
    assert(dmesh_poll_qp(b, &ev, 1) == 0 && test_native_idle_arms() == arms);
    assert(test_native_armed() == bit(b));
    idle(client);
    assert(test_native_idle_arms() == arms + 1 && test_native_armed() == (0xffu & ~(bit(a) | bit(b))));
    int fd = dmesh_channel_fd(client);
    assert(!readable(fd));
    settle(b);
    test_native_raise_wake();
    assert(readable(fd) && !readable(b->eq->epfd));
    idle(client);
    assert(!readable(fd));
    test_native_sleepable(0);

    dmesh_qp_t *qps[] = {a, b, sa, sb};
    teardown(server, client, qps, 4);

    /* Without flow wake the session publishes every stripe: the channel
     * drains a QP's stripe, hands it the receive and wakes it. */
    test_native_flow_wake(0);
    server = channel("echo:80");
    client = channel(NULL);
    assert(!client->ctx->flow_wake);
    dmesh_qp_t *c = dial(client);
    send_bytes(c, 8, 5);
    dmesh_qp_t *sc = accept_one(server);
    expect_recv(server, sc, 8);
    send_bytes(sc, 24, 6);
    idle(client);
    assert(readable(c->eq->notify_efd));
    expect_recv(client, c, 24);
    dmesh_qp_t *qps2[] = {c, sc};
    teardown(server, client, qps2, 2);
    printf("native_listen_test: the channel accepts each stream as a QP with its own fd; a QP drains its "
           "own stripe, never the session, and gets its control news: PASS\n");
    return 0;
}
