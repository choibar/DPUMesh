/* Connection EQs over the memory carrier, with and without flow wake. With
 * it, a connection EQ is neither handed nor woken for inbound connections,
 * advances and drains only its own stripes, never progresses or arms the
 * session, and is woken by a session EQ for its flows' control news; a moved
 * QP takes its later and pending receives to its new EQ. Without it, a
 * session EQ drains every stripe and hands a connection EQ its receives. */
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

static dmesh_eq_t *new_eq(dmesh_channel_t *ch, int conn)
{
    dmesh_eq_t *eq = conn ? dmesh_create_conn_eq(ch) : dmesh_create_eq(ch);
    assert(eq && eq->conn == conn && dmesh_eq_fd(eq) >= 0);
    settle(eq);
    return eq;
}

static void send_bytes(dmesh_qp_t *qp, uint32_t n, unsigned char v)
{
    void *b = dmesh_alloc(qp, n);
    assert(b);
    memset(b, v, n);
    assert(dmesh_post_send(qp, b, n) == 0 && dmesh_flush(qp) == 0);
}

/* The next event on eq: one RECV of len bytes for qp, released; then eq is
 * empty, so its stream is no longer armed. */
static void expect_recv(dmesh_channel_t *ch, dmesh_eq_t *eq, dmesh_qp_t *qp, uint32_t len)
{
    dmesh_event_t ev;
    assert(dmesh_poll_eq(eq, &ev, 1) == 1);
    assert(ev.type == DMESH_EVENT_RECV && ev.qp == qp && ev.len == len);
    dmesh_release_rx_buffer(ch, &ev);
    assert(dmesh_poll_eq(eq, &ev, 1) == 0);
}

static uint32_t bit(dmesh_qp_t *qp) { return UINT32_C(1) << (qp->local_port % 8); }

int main(void)
{
    dmesh_event_t ev;
    setenv("DPUMESH_RINGS_PER_POD", "8", 1);
    test_native_stripe_map(1);
    test_native_rx_signaled(1);        /* the Comch fast path: no naps or linger */
    test_native_flow_wake(1);
    setenv("DPUMESH_SERVICE", "echo:80", 1);
    dmesh_channel_t *server = dmesh_create_channel();
    assert(server && server->ctx->flow_wake);
    unsetenv("DPUMESH_SERVICE");
    dmesh_channel_t *client = dmesh_create_channel();
    assert(client && client->ctx->landing_stripes == 8);
    dmesh_eq_t *listen = new_eq(server, 0), *own_a = new_eq(server, 1), *own_b = new_eq(server, 1);
    dmesh_eq_t *session = new_eq(client, 0), *ea = new_eq(client, 1), *eb = new_eq(client, 1);
    dmesh_qp_t *a = dmesh_create_qp(ea, "echo:80"), *b = dmesh_create_qp(eb, "echo:80");
    assert(a && b && bit(a) != bit(b));
    assert(atomic_load(&ea->stripes) == bit(a) && atomic_load(&eb->stripes) == bit(b));

    /* A new stream: only the session EQ takes it; no connection EQ wakes. */
    send_bytes(a, 32, 1);
    assert(dmesh_poll_eq(own_a, &ev, 1) == 0 && !readable(own_a->notify_efd));
    assert(dmesh_poll_eq(listen, &ev, 1) == 1 && ev.type == DMESH_EVENT_CONN_REQ);
    dmesh_qp_t *sa = ev.qp;
    expect_recv(server, listen, sa, 32);
    assert(!readable(own_a->notify_efd) && !readable(own_b->notify_efd));

    /* Moved to a connection EQ, the stream's receives arrive there only, and
     * that EQ advances its stripe alone without the session. */
    assert(dmesh_move_qp(sa, own_a) == 0 && sa->eq == own_a);
    assert(atomic_load(&listen->nqp) == 0 && atomic_load(&own_a->nqp) == 1);
    assert(atomic_load(&own_a->stripes) == bit(sa) && atomic_load(&listen->stripes) == 0);
    assert(dmesh_poll_eq(own_a, &ev, 1) == 0);            /* the move's arm emits nothing */
    send_bytes(a, 16, 2);
    assert(dmesh_poll_eq(listen, &ev, 1) == 0);
    unsigned sessions = test_native_session_progress();
    (void)test_native_progressed();
    expect_recv(server, own_a, sa, 16);
    assert(test_native_session_progress() == sessions && test_native_progressed() == bit(sa));
    assert(dmesh_move_qp(sa, own_a) == 0);                 /* already there */
    assert(dmesh_move_qp(sa, eb) == -1 && errno == EINVAL); /* another channel */

    /* A receive not yet returned when the QP moves is returned by its new EQ:
     * here the held first payload behind a one-event batch. */
    send_bytes(b, 8, 3);
    assert(dmesh_poll_eq(listen, &ev, 1) == 1 && ev.type == DMESH_EVENT_CONN_REQ);
    dmesh_qp_t *sb = ev.qp;
    assert(listen->drain_cur == sb);
    assert(dmesh_move_qp(sb, own_b) == 0 && listen->drain_cur == NULL);
    assert(dmesh_poll_eq(listen, &ev, 1) == 0);
    expect_recv(server, own_b, sb, 8);

    /* b's reply: drained by b's EQ alone, so neither a's EQ nor the session
     * EQ returns it or wakes b's EQ for it. */
    settle(ea); settle(eb);
    send_bytes(sb, 8, 4);
    assert(dmesh_poll_eq(ea, &ev, 1) == 0 && dmesh_poll_eq(session, &ev, 1) == 0);
    assert(!readable(eb->notify_efd));
    expect_recv(client, eb, b, 8);

    /* Control news for a flow: the session EQ wakes the EQ owning it. */
    settle(ea); settle(eb);
    test_native_news(bit(b));
    assert(dmesh_poll_eq(session, &ev, 1) == 0);
    assert(readable(eb->notify_efd) && !readable(ea->notify_efd));

    /* An empty connection EQ arms its own stripes, never the session. */
    test_native_sleepable(1);
    unsigned arms = test_native_idle_arms();
    (void)test_native_armed();
    assert(dmesh_poll_eq(eb, &ev, 1) == 0 && test_native_idle_arms() == arms);
    assert(test_native_armed() == bit(b));
    assert(dmesh_poll_eq(session, &ev, 1) == 0 && test_native_idle_arms() == arms + 1);
    test_native_sleepable(0);

    /* Each QP closes on its own EQ; their retiring stripes are anyone's. */
    dmesh_qp_t *qps[] = {a, b, sa, sb};
    for (unsigned i = 0; i < sizeof(qps) / sizeof(qps[0]); ++i) assert(dmesh_destroy_qp(qps[i]) == 0);
    dmesh_eq_t *eqs[] = {own_a, own_b, ea, eb, listen, session};
    for (int round = 0; round < 4; ++round)
        for (unsigned i = 0; i < sizeof(eqs) / sizeof(eqs[0]); ++i) (void)dmesh_poll_eq(eqs[i], &ev, 1);
    for (unsigned i = 0; i < sizeof(eqs) / sizeof(eqs[0]); ++i) assert(dmesh_destroy_eq(eqs[i]) == 0);
    assert(dmesh_destroy_channel(client) == 0 && dmesh_destroy_channel(server) == 0);

    /* Without flow wake the session publishes every stripe: a session EQ
     * drains a connection EQ's stripe, hands it the receive and wakes it. */
    test_native_flow_wake(0);
    setenv("DPUMESH_SERVICE", "echo:80", 1);
    server = dmesh_create_channel();
    unsetenv("DPUMESH_SERVICE");
    client = dmesh_create_channel();
    assert(server && client && !client->ctx->flow_wake);
    listen = new_eq(server, 0); session = new_eq(client, 0);
    dmesh_eq_t *ec = new_eq(client, 1), *listen_b = new_eq(server, 0), *own_c = new_eq(server, 1);
    dmesh_qp_t *c = dmesh_create_qp(ec, "echo:80");
    assert(c);
    send_bytes(c, 8, 5);
    assert(dmesh_poll_eq(listen, &ev, 1) == 1 && ev.type == DMESH_EVENT_CONN_REQ);
    dmesh_qp_t *sc = ev.qp;
    expect_recv(server, listen, sc, 8);

    /* A receive a sibling EQ handed to the QP's EQ before a move is drained
     * by the new EQ; the old one skips its stale ready entry. */
    send_bytes(c, 24, 7);
    assert(dmesh_poll_eq(listen_b, &ev, 1) == 0 && readable(listen->notify_efd));
    assert(dmesh_move_qp(sc, own_c) == 0);
    assert(dmesh_poll_eq(listen, &ev, 1) == 0);
    expect_recv(server, own_c, sc, 24);
    settle(ec);
    send_bytes(sc, 8, 6);
    assert(dmesh_poll_eq(session, &ev, 1) == 0 && readable(ec->notify_efd));
    expect_recv(client, ec, c, 8);
    assert(dmesh_destroy_qp(c) == 0 && dmesh_destroy_qp(sc) == 0);
    dmesh_eq_t *eqs2[] = {ec, session, listen, listen_b, own_c};
    for (int round = 0; round < 4; ++round)
        for (unsigned i = 0; i < sizeof(eqs2) / sizeof(eqs2[0]); ++i) (void)dmesh_poll_eq(eqs2[i], &ev, 1);
    for (unsigned i = 0; i < sizeof(eqs2) / sizeof(eqs2[0]); ++i) assert(dmesh_destroy_eq(eqs2[i]) == 0);
    assert(dmesh_destroy_channel(client) == 0 && dmesh_destroy_channel(server) == 0);
    printf("native_multi_eq_test: connection EQs skip accepts and the session, drain their own stripes, "
           "get control news, take moved QPs' receives: PASS\n");
    return 0;
}
