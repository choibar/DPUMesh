/* A stream end to end over the memory carrier: dial, accept, a reservation
 * across fragments, custody ACKs, held RX buffers, FIN and teardown. */
#include <assert.h>
#include <stdio.h>
#include "../src/core/dmesh_core.c"
#include "support/native_memory_transport.c"

static dmesh_channel_t *server, *client;

/* Units in DPU custody: submitted, their ACK not yet seen. */
static int in_custody(dmesh_qp_t *qp) {
    return dmesh_tx_inflight_locked(&qp->ep->ctx->ports[qp->local_port]);
}

/* The channels' work, as each process's poller does it. */
static void pump(void) {
    assert(dmesh_accept(client) == NULL && errno == EAGAIN);
}
static dmesh_qp_t *accept_one(void) {
    for (int i = 0; i < 5000; ++i) {
        dmesh_qp_t *qp = dmesh_accept(server);
        if (qp) return qp;
        assert(errno == EAGAIN);
        usleep(1000);
    }
    assert(!"accept deadline exceeded"); return NULL;
}
static dmesh_event_t receive(dmesh_qp_t *qp, dmesh_event_type_t type) {
    dmesh_event_t ev;
    for (int i = 0; i < 5000; ++i) {
        (void)dmesh_accept(server);
        pump();
        int n = dmesh_poll_qp(qp, &ev, 1); assert(n >= 0);
        if (n) { assert(ev.type == type && ev.qp == qp); return ev; }
        usleep(1000);
    }
    assert(!"event deadline exceeded"); return ev;
}
int main(void) {
    setenv("DPUMESH_SERVICE", "echo:80", 1);
    server = dmesh_create_channel(); assert(server);
    unsetenv("DPUMESH_SERVICE");
    client = dmesh_create_channel(); assert(client);
    dmesh_qp_t *cq = dmesh_create_qp(client, "echo:80"); assert(cq);
    assert(dmesh_destroy_channel(client) == -1 && errno == EBUSY);
    test_native_hold_acks(1);
    unsigned char *tx = dmesh_alloc(cq, 16385); assert(tx);
    for (unsigned i = 0; i < 16385; ++i) tx[i] = (unsigned char)(i * 17);
    assert(!dmesh_alloc(cq, 1) && errno == EDEADLK);
    assert(dmesh_post_send(cq, tx, 16385) == 0);
    assert(dmesh_flush(cq) == 0);
    assert(in_custody(cq) > 0);
    dmesh_qp_t *sq = accept_one();
    size_t received = 0;
    dmesh_event_t held = {0};
    while (received < 16385) {
        dmesh_event_t ev = receive(sq, DMESH_EVENT_RECV);
        for (uint32_t i = 0; i < ev.len; ++i) assert(((unsigned char *)ev.buf)[i] == (unsigned char)((received + i) * 17));
        received += ev.len;
        if (!held.buf) held = ev; else dmesh_release_rx_buffer(server, &ev);
    }
    /* Payload landing does not stand in for the source's custody ACK. */
    assert(in_custody(cq) > 0);
    test_native_hold_acks(0);
    /* No thread reclaims custody in the background: the ACKs are polled. */
    for (int i = 0; i < 5000 && in_custody(cq); ++i) {
        dmesh_event_t none;
        pump();
        assert(dmesh_poll_qp(cq, &none, 1) == 0);
        usleep(1000);
    }
    assert(in_custody(cq) == 0);
    void *reply = dmesh_alloc(sq, 4); assert(reply); memcpy(reply, "pong", 4);
    assert(dmesh_post_send(sq, reply, 4) == 0); assert(dmesh_flush(sq) == 0);
    dmesh_event_t response = receive(cq, DMESH_EVENT_RECV);
    assert(response.len == 4 && !memcmp(response.buf, "pong", 4));
    dmesh_release_rx_buffer(client, &response); dmesh_release_rx_buffer(client, &response);
    /* RX bytes remain owned by the application across QP close. */
    assert(dmesh_destroy_qp(sq) == 0);
    for (unsigned i = 0; i < held.len; ++i) assert(((unsigned char *)held.buf)[i] == (unsigned char)(i * 17));
    dmesh_release_rx_buffer(server, &held); dmesh_release_rx_buffer(server, &held);
    dmesh_event_t fin = receive(cq, DMESH_EVENT_RECV_FIN); assert(fin.qp == cq);
    assert(dmesh_destroy_qp(cq) == 0);
    assert(dmesh_destroy_channel(client) == 0); assert(dmesh_destroy_channel(server) == 0);
    /* A failed channel transport shutdown must not free the public handle.
     * Retry uses the same context after the transport can quiesce again. */
    dmesh_channel_t *retry = dmesh_create_channel(); assert(retry);
    dpumesh_ctx_t *saved_ctx = retry->ctx;
    test_native_fail_close(1);
    assert(dmesh_destroy_channel(retry) == -1 && errno == EIO);
    assert(retry->ctx == saved_ctx);
    test_native_fail_close(0);
    assert(dmesh_destroy_channel(retry) == 0);
    puts("native_core_transport_test: PASS");
}
