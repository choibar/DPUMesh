/* Idle-wake policy of the production core (dpumesh_eq_arm) over the memory
 * carrier: naps, linger, sleep with only the backstop, custody polling, the
 * submit wake and the DOORBELL restart. Timer values are read back from the
 * timerfd, so no assertion depends on scheduling. */
#include <assert.h>
#include <poll.h>
#include <stdio.h>
#include "../src/core/dmesh_core.c"
#include "support/native_memory_transport.c"

static uint64_t timer_ns(dmesh_eq_t *eq)
{
    struct itimerspec its;
    assert(timerfd_gettime(eq->tick_fd, &its) == 0);
    assert(its.it_interval.tv_sec == 0 && its.it_interval.tv_nsec == 0); /* one-shot */
    return (uint64_t)its.it_value.tv_sec * 1000000000ull + (uint64_t)its.it_value.tv_nsec;
}

/* An empty dmesh_poll_eq: returns the armed timer. */
static uint64_t empty_poll(dmesh_eq_t *eq)
{
    dmesh_event_t ev[4];
    assert(dmesh_poll_eq(eq, ev, 4) == 0);
    return timer_ns(eq);
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

int main(void)
{
    setenv("DPUMESH_LINGER_US", "1000000", 1);   /* 1 s: long enough to observe */
    setenv("DPUMESH_SPIN_US", "1000", 1);        /* ignored, with a warning */
    setenv("DPUMESH_SERVICE", "echo:80", 1);     /* the peer that receives sends */
    dmesh_channel_t *server = dmesh_create_channel();
    assert(server);
    unsetenv("DPUMESH_SERVICE");
    dmesh_channel_t *ch = dmesh_create_channel();
    assert(ch);
    dpumesh_ctx_t *ctx = ch->ctx;
    assert(ctx->nap_min_ns == 10000 && ctx->nap_cap_ns == 100000);
    assert(ctx->linger_ns == 1000000000ull && ctx->backstop_ns == 200000000ull);
    dmesh_eq_t *eq = dmesh_create_eq(ch);
    assert(eq && dmesh_eq_fd(eq) >= 0);
    dmesh_qp_t *qp = dmesh_create_qp(eq, "echo:80");
    assert(qp);
    settle(eq);
    test_native_sleepable(1);

    /* Born idle: the first empty poll sleeps at once, with only the backstop. */
    unsigned arms = test_native_idle_arms();
    uint64_t t = empty_poll(eq);
    assert(t > 150000000ull && t <= 200000000ull && eq->timer_backstop);
    assert(atomic_load(&eq->asleep) == 1 && test_native_idle_arms() == arms + 1);

    /* A send wakes the sleeping EQ (custody has no doorbell) and naps restart. */
    void *buf = dmesh_alloc(qp, 64);
    assert(buf);
    memset(buf, 7, 64);
    assert(dmesh_post_send(qp, buf, 64) == 0);
    assert(readable(eq->notify_efd));
    /* Model outstanding custody with a carrier that cannot sleep. */
    test_native_sleepable(0);
    const uint64_t naps[] = {10000, 20000, 40000, 80000};
    dmesh_event_t ev[8];
    int n;
    while ((n = dmesh_poll_eq(eq, ev, 8)) > 0)
        for (int i = 0; i < n; ++i) dmesh_release_rx_buffer(ch, &ev[i]);
    /* The poll that ran empty already took the first nap. */
    t = timer_ns(eq);
    assert(t > 0 && t <= naps[0] && !eq->timer_backstop && atomic_load(&eq->asleep) == 0);
    for (size_t i = 1; i < sizeof(naps) / sizeof(naps[0]); ++i) {
        t = empty_poll(eq);
        assert(t > naps[i - 1] && t <= naps[i]);
    }
    /* Naps spent, still within the linger: poll every nap_cap. */
    for (int i = 0; i < 3; ++i) {
        t = empty_poll(eq);
        assert(t > naps[3] && t <= ctx->nap_cap_ns && atomic_load(&eq->asleep) == 0);
    }
    /* Past the linger, outstanding custody still keeps the EQ polling. */
    ctx->linger_ns = 0;
    uint64_t busy = atomic_load(&wait_stats.busy);
    t = empty_poll(eq);
    assert(t <= ctx->nap_cap_ns && !eq->timer_backstop && atomic_load(&eq->asleep) == 0);
    assert(atomic_load(&wait_stats.busy) == busy + 1);

    /* Custody retired: sleep. */
    test_native_sleepable(1);
    arms = test_native_idle_arms();
    t = empty_poll(eq);
    assert(t > ctx->nap_cap_ns && eq->timer_backstop && atomic_load(&eq->asleep) == 1);
    assert(test_native_idle_arms() == arms + 1);

    /* A DOORBELL (the channel wake fd) restarts the naps and is acknowledged. */
    unsigned clears = test_native_wake_clears();
    test_native_raise_wake();
    assert(readable(eq->epfd));
    t = empty_poll(eq);
    assert(t > 0 && t <= naps[0] && test_native_wake_clears() == clears + 1);

    /* Walk back to sleep, then let only the backstop expire: counted, and a
     * backstop that found no work is not a missed wake. */
    for (size_t i = 1; i < 4; ++i) (void)empty_poll(eq);
    (void)empty_poll(eq);
    assert(eq->timer_backstop);
    uint64_t wakes = atomic_load(&wait_stats.backstop_wakes);
    struct itimerspec soon = {.it_value = {.tv_nsec = 1000}};
    assert(timerfd_settime(eq->tick_fd, 0, &soon, NULL) == 0);
    struct pollfd p = {.fd = eq->epfd, .events = POLLIN};   /* dmesh_eq_fd would self-kick */
    assert(poll(&p, 1, 1000) == 1);
    (void)empty_poll(eq);
    assert(atomic_load(&wait_stats.backstop_wakes) == wakes + 1);
    assert(atomic_load(&wait_stats.backstop_work) == 0);

    /* DPUMESH_NAP_US=0 skips the naps; with no linger the EQ sleeps. */
    ctx->nap_min_ns = 0;
    atomic_store(&eq->kick, 1);
    (void)empty_poll(eq);
    assert(eq->timer_backstop);

    /* Oversubscribed (more live EQs than allowed CPUs): no naps, no linger. */
    ctx->nap_min_ns = 10000;
    ctx->linger_ns = 1000000000ull;
    atomic_store(&ctx->live_eqs, 1 << 20);
    atomic_store(&eq->kick, 1);
    (void)empty_poll(eq);
    assert(eq->timer_backstop);
    atomic_store(&ctx->live_eqs, 1);

    test_native_sleepable(0);
    assert(dmesh_destroy_qp(qp) == 0);
    assert(dmesh_destroy_eq(eq) == 0);
    assert(dmesh_destroy_channel(ch) == 0);
    assert(dmesh_destroy_channel(server) == 0);
    unsetenv("DPUMESH_LINGER_US");
    unsetenv("DPUMESH_SPIN_US");
    puts("native_polling_test: naps, linger, custody polling, sleep, submit wake, doorbell, backstop: PASS");
}
