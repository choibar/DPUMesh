/* Idle-wake policy of the production core over the memory carrier. A QP's EQ
 * (dpumesh_eq_arm): the periodic backstop from the fd's hand-out, sleep with
 * only its own stripe armed, a send that leaves it asleep, re-polls while a
 * flow retires, and the backstop's missed-wake count. The channel
 * (dmesh_accept): it arms the session, and re-polls soon after a DOORBELL.
 * Timer values are read back from the timerfds, so no assertion depends on
 * scheduling. */
#include <assert.h>
#include <poll.h>
#include <stdio.h>
#include "../src/core/dmesh_core.c"
#include "support/native_memory_transport.c"

static struct itimerspec timer_of(int fd)
{
    struct itimerspec its;
    assert(timerfd_gettime(fd, &its) == 0);
    return its;
}

static uint64_t timer_ns(int fd)
{
    struct itimerspec its = timer_of(fd);
    return (uint64_t)its.it_value.tv_sec * 1000000000ull + (uint64_t)its.it_value.tv_nsec;
}

/* The EQ's timer holds the periodic backstop and no re-poll ahead of it. */
static int backstop_only(dmesh_eq_t *eq)
{
    struct itimerspec its = timer_of(eq->tick_fd);
    return !eq->repoll && its.it_interval.tv_sec == 0 && its.it_interval.tv_nsec == (long)DMESH_BACKSTOP_NS &&
           timer_ns(eq->tick_fd) > DMESH_REPOLL_NS;
}

static void empty_poll(dmesh_qp_t *qp)
{
    dmesh_event_t ev[4];
    assert(dmesh_poll_qp(qp, ev, 4) == 0);
}

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

/* Let only the EQ's backstop fire. */
static void backstop_fires(dmesh_eq_t *eq)
{
    struct itimerspec soon = {.it_value = {.tv_nsec = 1000}, .it_interval = {.tv_nsec = (long)DMESH_BACKSTOP_NS}};
    assert(timerfd_settime(eq->tick_fd, 0, &soon, NULL) == 0);
    struct pollfd p = {.fd = eq->epfd, .events = POLLIN};   /* dmesh_qp_fd would self-kick */
    assert(poll(&p, 1, 1000) == 1);
}

int main(void)
{
    test_native_stripe_map(1);                   /* the QP owns its stripe */
    setenv("DPUMESH_SERVICE", "echo:80", 1);     /* the peer that receives sends */
    dmesh_channel_t *server = dmesh_create_channel();
    assert(server);
    unsetenv("DPUMESH_SERVICE");
    dmesh_channel_t *ch = dmesh_create_channel();
    assert(ch);
    dpumesh_ctx_t *ctx = ch->ctx;
    dmesh_qp_t *qp = dmesh_create_qp(ch, "echo:80");
    assert(qp);
    dmesh_eq_t *eq = qp->eq;
    assert(timer_ns(eq->tick_fd) == 0);          /* poll-only: no backstop */
    assert(dmesh_qp_fd(qp) >= 0);
    assert(backstop_only(eq));                   /* handing out the fd starts it */
    uint32_t own = atomic_load(&eq->stripes);
    assert(own != 0);
    settle(eq);
    test_native_sleepable(1);

    /* An empty poll sleeps at once, with only its own stripe armed; the
     * session is the channel's, and the backstop is left as it was. */
    unsigned arms = test_native_idle_arms();
    (void)test_native_armed();
    uint64_t sleeps = atomic_load(&eq->st.arms);
    empty_poll(qp);
    assert(atomic_load(&eq->asleep) == 1 && atomic_load(&eq->st.arms) == sleeps + 1);
    assert(test_native_idle_arms() == arms && test_native_armed() == own);
    assert(backstop_only(eq));

    /* A send leaves the sleeping EQ asleep: its custody is reclaimed on the
     * next wake, and a sender that waits for the space wakes it. */
    void *buf = dmesh_alloc(qp, 64);
    assert(buf);
    memset(buf, 7, 64);
    assert(dmesh_post_send(qp, buf, 64) == 0);
    assert(!readable(eq->notify_efd));

    /* A closed flow still retiring keeps the EQ polling on a re-poll ahead of
     * the backstop; once retired it sleeps on the backstop alone. */
    test_native_retiring(1);
    uint64_t busy = atomic_load(&eq->st.busy);
    empty_poll(qp);
    assert(atomic_load(&eq->asleep) == 0 && eq->repoll && atomic_load(&eq->st.busy) == busy + 1);
    assert(timer_ns(eq->tick_fd) > 0 && timer_ns(eq->tick_fd) <= DMESH_REPOLL_NS);
    test_native_retiring(0);
    empty_poll(qp);
    assert(atomic_load(&eq->asleep) == 1 && backstop_only(eq));

    /* The channel arms the session. A DOORBELL raises its fd, not the QP's;
     * accepting acknowledges it and re-polls soon, since the descriptor may
     * become visible after the message, then the channel sleeps again. */
    int cfd = dmesh_channel_fd(ch);
    assert(dmesh_accept(ch) == NULL && errno == EAGAIN);
    assert(test_native_idle_arms() == arms + 1);
    assert(dmesh_accept(ch) == NULL && errno == EAGAIN);
    assert(timer_ns(ctx->listen_timer) > DMESH_REPOLL_NS);
    test_native_raise_wake();
    assert(readable(cfd) && !readable(eq->epfd));
    assert(dmesh_accept(ch) == NULL && errno == EAGAIN);
    assert(!readable(cfd));
    uint64_t t = timer_ns(ctx->listen_timer);
    assert(t > 0 && t <= DMESH_REPOLL_NS);
    assert(dmesh_accept(ch) == NULL && errno == EAGAIN);
    assert(timer_ns(ctx->listen_timer) > DMESH_REPOLL_NS);

    /* A backstop that found no work is counted, and is not a missed wake. */
    uint64_t wakes = atomic_load(&eq->st.backstop_wakes);
    backstop_fires(eq);
    empty_poll(qp);
    assert(atomic_load(&eq->st.backstop_wakes) == wakes + 1);
    assert(atomic_load(&eq->st.backstop_work) == 0);

    /* A backstop that reclaims only custody ACKs, left for the next wake on
     * purpose, is not a missed wake; one that hands the poller a receive is.
     * This carrier rings no stripe doorbell, so a receive is found only by
     * the backstop. */
    test_native_hold_acks(1);
    buf = dmesh_alloc(qp, 64);
    assert(buf);
    memset(buf, 9, 64);
    assert(dmesh_post_send(qp, buf, 64) == 0);
    test_native_hold_acks(0);
    backstop_fires(eq);
    empty_poll(qp);
    assert(atomic_load(&eq->st.backstop_wakes) == wakes + 2);
    assert(atomic_load(&eq->st.backstop_work) == 0);
    dmesh_qp_t *sq = NULL;
    for (int i = 0; i < 1000 && !sq; ++i) sq = dmesh_accept(server);
    assert(sq);
    dmesh_event_t ev[8];
    int n;
    while ((n = dmesh_poll_qp(sq, ev, 8)) > 0)
        for (int i = 0; i < n; ++i) dmesh_release_rx_buffer(server, &ev[i]);
    void *reply = dmesh_alloc(sq, 4);
    assert(reply);
    memcpy(reply, "pong", 4);
    assert(dmesh_post_send(sq, reply, 4) == 0);
    backstop_fires(eq);
    assert(dmesh_poll_qp(qp, ev, 8) == 1 && ev[0].type == DMESH_EVENT_RECV);
    dmesh_release_rx_buffer(ch, &ev[0]);
    empty_poll(qp);
    assert(atomic_load(&eq->st.backstop_wakes) == wakes + 3);
    assert(atomic_load(&eq->st.backstop_work) == 1);

    /* A re-poll's firing is not a backstop. */
    test_native_retiring(1);
    empty_poll(qp);
    assert(eq->repoll);
    test_native_retiring(0);
    struct pollfd p = {.fd = eq->epfd, .events = POLLIN};
    assert(poll(&p, 1, 1000) == 1);
    empty_poll(qp);
    assert(atomic_load(&eq->st.backstop_wakes) == wakes + 3 && backstop_only(eq));

    test_native_sleepable(0);
    assert(dmesh_destroy_qp(sq) == 0);
    assert(dmesh_destroy_qp(qp) == 0);
    assert(dmesh_destroy_channel(ch) == 0);
    assert(dmesh_destroy_channel(server) == 0);
    puts("native_polling_test: periodic backstop, sleep on its own stripe, quiet send, retiring re-poll, "
         "channel doorbell re-poll, backstop and missed-wake count: PASS");
}
