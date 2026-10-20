/* Idle wake with signaled receives (the Comch fast path) over the memory
 * carrier: no naps or linger by default, a send does not wake its EQ, open
 * custody alone does not keep an EQ polling, a sender waiting for space or a
 * retiring flow does, and arming that wait wakes a sleeping EQ. */
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

static int empty_poll(dmesh_eq_t *eq)
{
    dmesh_event_t ev[8];
    return dmesh_poll_eq(eq, ev, 8);
}

int main(void)
{
    unsetenv("DPUMESH_NAP_US");
    unsetenv("DPUMESH_LINGER_US");
    test_native_rx_signaled(1);
    setenv("DPUMESH_SERVICE", "echo:80", 1);
    dmesh_channel_t *server = dmesh_create_channel();
    assert(server);
    unsetenv("DPUMESH_SERVICE");
    dmesh_channel_t *ch = dmesh_create_channel();
    assert(ch);
    dpumesh_ctx_t *ctx = ch->ctx;
    assert(ctx->rx_signaled && ctx->nap_min_ns == 0 && ctx->linger_ns == 0);
    dmesh_eq_t *eq = dmesh_create_eq(ch);
    assert(eq && dmesh_eq_fd(eq) >= 0);
    dmesh_qp_t *qp = dmesh_create_qp(eq, "echo:80");
    assert(qp);
    settle(eq);
    test_native_sleepable(1);
    assert(empty_poll(eq) == 0 && atomic_load(&eq->asleep) == 1);

    /* A send leaves a sleeping EQ asleep: the reply's completion wakes it. */
    test_native_hold_acks(1);
    void *buf = dmesh_alloc(qp, 64);
    assert(buf);
    memset(buf, 7, 64);
    assert(dmesh_post_send(qp, buf, 64) == 0);
    assert(!readable(eq->notify_efd));

    /* Sleeping again soon keeps the armed backstop instead of re-arming it. */
    uint64_t until = eq->backstop_until_ns;
    assert(until != 0);
    eq_notify(eq);
    assert(empty_poll(eq) == 0 && atomic_load(&eq->asleep) == 1);
    assert(eq->backstop_until_ns == until && eq->timer_backstop);

    /* Open custody with no sender waiting: the EQ sleeps on its doorbells. */
    test_native_custody(1);
    uint64_t custody_sleeps = atomic_load(&wait_stats.custody_sleeps);
    eq_notify(eq);
    assert(empty_poll(eq) == 0);
    assert(atomic_load(&eq->asleep) == 1 && eq->timer_backstop);
    assert(atomic_load(&wait_stats.custody_sleeps) == custody_sleeps + 1);

    /* A sender waiting for space this custody frees: arming the wait wakes
     * the sleeping EQ, which then keeps polling. */
    struct dmesh_port_slot *psl = &ctx->ports[qp->local_port];
    settle(eq);
    tx_wait_arm(ctx, psl, qp->local_port, DMESH_TX_WAIT_SU_RECLAIM);
    assert(atomic_load(&ctx->tx_waiters) == 1);
    assert(readable(eq->notify_efd));
    uint64_t busy = atomic_load(&wait_stats.busy);
    assert(empty_poll(eq) == 0);
    assert(atomic_load(&eq->asleep) == 0 && !eq->timer_backstop);
    assert(atomic_load(&wait_stats.busy) == busy + 1);

    /* The wait ends: back to sleeping on open custody. */
    tx_wait_cancel(ctx, psl, qp->local_port);
    assert(atomic_load(&ctx->tx_waiters) == 0);
    eq_notify(eq);
    assert(empty_poll(eq) == 0 && atomic_load(&eq->asleep) == 1);

    /* A closed flow still retiring keeps polling regardless. */
    test_native_retiring(1);
    busy = atomic_load(&wait_stats.busy);
    eq_notify(eq);
    assert(empty_poll(eq) == 0 && atomic_load(&eq->asleep) == 0);
    assert(atomic_load(&wait_stats.busy) == busy + 1);
    test_native_retiring(0);

    /* Retire the held ACK so the QP closes cleanly. */
    test_native_custody(0);
    test_native_hold_acks(0);
    dmesh_event_t ev[8];
    int n;
    while ((n = dmesh_poll_eq(eq, ev, 8)) > 0)
        for (int i = 0; i < n; ++i) dmesh_release_rx_buffer(ch, &ev[i]);
    assert(dmesh_destroy_qp(qp) == 0);
    assert(dmesh_destroy_eq(eq) == 0);
    assert(dmesh_destroy_channel(ch) == 0);
    assert(dmesh_destroy_channel(server) == 0);
    test_native_rx_signaled(0);
    puts("native_signaled_test: no naps, quiet sends, kept backstop, custody sleeps, waiter wake and poll, retiring poll: PASS");
}
