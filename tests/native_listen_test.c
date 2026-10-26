/* A listening channel over the memory carrier, with and without flow wake.
 * Its EQs take no inbound streams and never progress or arm the session: the
 * channel's fd and dmesh_accept do its control work and bind each inbound
 * stream to the EQ named, which returns the stream's first bytes. With flow
 * wake an EQ advances and drains only its own stripes, and the channel wakes
 * it for its flows' control news; without it the channel drains every stripe
 * and hands each EQ its receives. */
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

static void settle(dmesh_eq_t *eq)
{
    uint64_t v;
    while (read(eq->notify_efd, &v, sizeof(v)) > 0) {}
}

static dmesh_eq_t *new_eq(dmesh_channel_t *ch)
{
    dmesh_eq_t *eq = dmesh_create_eq(ch);
    assert(eq && dmesh_eq_fd(eq) >= 0);
    settle(eq);
    return eq;
}

static dmesh_channel_t *listening(const char *service)
{
    if (service) setenv("DPUMESH_SERVICE", service, 1);
    dmesh_channel_t *ch = dmesh_create_channel();
    unsetenv("DPUMESH_SERVICE");
    assert(ch && dmesh_accept(ch, NULL) == NULL && errno == EINVAL);
    int fd = dmesh_channel_fd(ch);
    assert(fd >= 0 && dmesh_channel_fd(ch) == fd);
    return ch;
}

static void send_bytes(dmesh_qp_t *qp, uint32_t n, unsigned char v)
{
    void *b = dmesh_alloc(qp, n);
    assert(b);
    memset(b, v, n);
    assert(dmesh_post_send(qp, b, n) == 0 && dmesh_flush(qp) == 0);
}

/* The next event on eq: one RECV of len bytes for qp, released; then eq is
 * empty. */
static void expect_recv(dmesh_channel_t *ch, dmesh_eq_t *eq, dmesh_qp_t *qp, uint32_t len)
{
    dmesh_event_t ev;
    assert(dmesh_poll_eq(eq, &ev, 1) == 1);
    assert(ev.type == DMESH_EVENT_RECV && ev.qp == qp && ev.len == len);
    dmesh_release_rx_buffer(ch, &ev);
    assert(dmesh_poll_eq(eq, &ev, 1) == 0);
}

static void idle(dmesh_channel_t *ch)
{
    assert(dmesh_accept(ch, NULL) == NULL && errno == EAGAIN);
}

static uint32_t bit(dmesh_qp_t *qp) { return UINT32_C(1) << (qp->local_port % 8); }

static void teardown(dmesh_channel_t *server, dmesh_channel_t *client, dmesh_qp_t **qps, unsigned nqp,
                     dmesh_eq_t **eqs, unsigned neq)
{
    dmesh_event_t ev;
    for (unsigned i = 0; i < nqp; ++i) assert(dmesh_destroy_qp(qps[i]) == 0);
    for (int round = 0; round < 4; ++round) {
        (void)dmesh_accept(server, NULL);
        (void)dmesh_accept(client, NULL);
        for (unsigned i = 0; i < neq; ++i) (void)dmesh_poll_eq(eqs[i], &ev, 1);
    }
    for (unsigned i = 0; i < neq; ++i) assert(dmesh_destroy_eq(eqs[i]) == 0);
    assert(dmesh_destroy_channel(client) == 0 && dmesh_destroy_channel(server) == 0);
}

int main(void)
{
    dmesh_event_t ev;
    setenv("DPUMESH_RINGS_PER_POD", "8", 1);
    test_native_stripe_map(1);
    test_native_rx_signaled(1);        /* the Comch fast path: no naps or linger */

    /* A channel listens only while it has no EQ: one made before would go on
     * doing the channel's work. */
    dmesh_channel_t *late = dmesh_create_channel();
    dmesh_eq_t *early = dmesh_create_eq(late);
    assert(late && early && dmesh_channel_fd(late) == -1 && errno == EBUSY);
    assert(dmesh_destroy_eq(early) == 0 && dmesh_destroy_channel(late) == 0);

    test_native_flow_wake(1);
    dmesh_channel_t *server = listening("echo:80"), *client = listening(NULL);
    assert(server->ctx->flow_wake && client->ctx->landing_stripes == 8);
    dmesh_eq_t *own_a = new_eq(server), *own_b = new_eq(server);
    dmesh_eq_t *ea = new_eq(client), *eb = new_eq(client);
    assert(atomic_load(&own_a->no_accept) && atomic_load(&ea->no_accept));
    dmesh_qp_t *a = dmesh_create_qp(ea, "echo:80"), *b = dmesh_create_qp(eb, "echo:80");
    assert(a && b && bit(a) != bit(b));
    assert(atomic_load(&ea->stripes) == bit(a) && atomic_load(&eb->stripes) == bit(b));

    /* A new stream waits for the channel: no EQ returns or wakes for it.
     * Accepted onto an EQ, the stream is bound there and that EQ returns its
     * first bytes. */
    send_bytes(a, 32, 1);
    assert(dmesh_poll_eq(own_a, &ev, 1) == 0 && !readable(own_a->notify_efd));
    dmesh_qp_t *sa = dmesh_accept(server, own_a);
    assert(sa && sa->eq == own_a && atomic_load(&own_a->nqp) == 1);
    assert(atomic_load(&own_a->stripes) == bit(sa) && atomic_load(&own_b->stripes) == 0);
    assert(readable(own_a->notify_efd) && !readable(own_b->notify_efd));
    expect_recv(server, own_a, sa, 32);
    idle(server);

    /* Its later receives: its EQ advances its stripe alone; the channel and
     * the session leave it to that EQ. */
    send_bytes(a, 16, 2);
    (void)test_native_progressed();
    idle(server);
    assert((test_native_progressed() & bit(sa)) == 0);
    unsigned sessions = test_native_session_progress();
    expect_recv(server, own_a, sa, 16);
    assert(test_native_session_progress() == sessions && test_native_progressed() == bit(sa));

    /* A first payload behind the stream's arrival comes with it. */
    send_bytes(b, 8, 3);
    dmesh_qp_t *sb = dmesh_accept(server, own_b);
    assert(sb && sb->eq == own_b);
    assert(dmesh_poll_eq(own_a, &ev, 1) == 0);
    expect_recv(server, own_b, sb, 8);

    /* b's reply: drained by b's EQ alone; a's EQ and the channel neither
     * return it nor wake b's EQ for it. */
    settle(ea); settle(eb);
    send_bytes(sb, 8, 4);
    assert(dmesh_poll_eq(ea, &ev, 1) == 0);
    idle(client);
    assert(!readable(eb->notify_efd));
    expect_recv(client, eb, b, 8);

    /* Control news for a flow: the channel wakes the EQ owning it. */
    settle(ea); settle(eb);
    test_native_news(bit(b));
    idle(client);
    assert(readable(eb->notify_efd) && !readable(ea->notify_efd));

    /* An empty EQ arms its own stripes, never the session; the channel arms
     * the session and the stripes no EQ owns, and its fd wakes on the
     * session's doorbell, which accepting acknowledges. */
    test_native_sleepable(1);
    unsigned arms = test_native_idle_arms();
    (void)test_native_armed();
    assert(dmesh_poll_eq(eb, &ev, 1) == 0 && test_native_idle_arms() == arms);
    assert(test_native_armed() == bit(b));
    idle(client);
    assert(test_native_idle_arms() == arms + 1 && test_native_armed() == (0xffu & ~(bit(a) | bit(b))));
    int fd = dmesh_channel_fd(client);
    assert(!readable(fd));
    settle(eb);
    test_native_raise_wake();
    assert(readable(fd) && !readable(eb->epfd));
    idle(client);
    assert(!readable(fd));
    test_native_sleepable(0);

    dmesh_qp_t *qps[] = {a, b, sa, sb};
    dmesh_eq_t *eqs[] = {own_a, own_b, ea, eb};
    teardown(server, client, qps, 4, eqs, 4);

    /* Without flow wake the session publishes every stripe: the channel
     * drains an EQ's stripe, hands it the receive and wakes it. */
    test_native_flow_wake(0);
    server = listening("echo:80");
    client = listening(NULL);
    assert(!client->ctx->flow_wake);
    dmesh_eq_t *ec = new_eq(client), *own_c = new_eq(server);
    dmesh_qp_t *c = dmesh_create_qp(ec, "echo:80");
    assert(c);
    send_bytes(c, 8, 5);
    dmesh_qp_t *sc = dmesh_accept(server, own_c);
    assert(sc && sc->eq == own_c);
    expect_recv(server, own_c, sc, 8);
    settle(ec);
    send_bytes(sc, 24, 6);
    idle(client);
    assert(readable(ec->notify_efd));
    expect_recv(client, ec, c, 24);
    dmesh_qp_t *qps2[] = {c, sc};
    dmesh_eq_t *eqs2[] = {ec, own_c};
    teardown(server, client, qps2, 2, eqs2, 2);
    printf("native_listen_test: a listening channel accepts onto the EQ named; its EQs skip accepts "
           "and the session, drain their own stripes and get control news: PASS\n");
    return 0;
}
