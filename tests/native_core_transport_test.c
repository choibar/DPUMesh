#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "src/core/dmesh_core.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
void test_native_hold_acks(int);
void test_native_fail_close(int);

static dmesh_event_t receive(dmesh_eq_t *eq, dmesh_event_type_t type) {
    dmesh_event_t ev;
    for (int i = 0; i < 5000; ++i) {
        int n = dmesh_poll_eq(eq, &ev, 1); assert(n >= 0);
        if (n) { assert(ev.type == type); return ev; }
        usleep(1000);
    }
    assert(!"event deadline exceeded"); return ev;
}
int main(void) {
    setenv("DPUMESH_SERVICE", "echo:80", 1);
    dmesh_channel_t *server = dmesh_create_channel(); assert(server);
    unsetenv("DPUMESH_SERVICE");
    dmesh_channel_t *client = dmesh_create_channel(); assert(client);
    dmesh_eq_t *se = dmesh_create_eq(server), *ce = dmesh_create_eq(client);
    assert(se && ce);
    assert(dmesh_destroy_channel(client) == -1 && errno == EBUSY);
    dmesh_qp_t *cq = dmesh_create_qp(ce, "echo:80"); assert(cq);
    assert(dmesh_destroy_eq(ce) == -1 && errno == EBUSY);
    test_native_hold_acks(1);
    unsigned char *tx = dmesh_alloc(cq, 16385); assert(tx);
    for (unsigned i = 0; i < 16385; ++i) tx[i] = (unsigned char)(i * 17);
    assert(!dmesh_alloc(cq, 1) && errno == EDEADLK);
    assert(dmesh_post_send(cq, tx, 16385) == 0);
    assert(dmesh_flush(cq) == 0);
    assert(dmesh_tx_inflight(cq) > 0);
    dmesh_event_t incoming = receive(se, DMESH_EVENT_CONN_REQ);
    dmesh_qp_t *sq = incoming.qp; assert(sq);
    size_t received = 0;
    dmesh_event_t held = {0};
    while (received < 16385) {
        dmesh_event_t ev = receive(se, DMESH_EVENT_RECV);
        for (uint32_t i = 0; i < ev.len; ++i) assert(((unsigned char *)ev.buf)[i] == (unsigned char)((received + i) * 17));
        received += ev.len;
        if (!held.buf) held = ev; else dmesh_release_rx_buffer(server, &ev);
    }
    /* Payload landing does not stand in for the source's custody ACK. */
    assert(dmesh_tx_inflight(cq) > 0);
    test_native_hold_acks(0);
    /* No thread reclaims custody in the background: the ACKs are polled. */
    for (int i = 0; i < 5000 && dmesh_tx_inflight(cq); ++i) {
        dmesh_event_t none;
        assert(dmesh_poll_eq(ce, &none, 1) == 0);
        usleep(1000);
    }
    assert(dmesh_tx_inflight(cq) == 0);
    void *reply = dmesh_alloc(sq, 4); assert(reply); memcpy(reply, "pong", 4);
    assert(dmesh_post_send(sq, reply, 4) == 0); assert(dmesh_flush(sq) == 0);
    dmesh_event_t response = receive(ce, DMESH_EVENT_RECV);
    assert(response.len == 4 && !memcmp(response.buf, "pong", 4));
    dmesh_release_rx_buffer(client, &response); dmesh_release_rx_buffer(client, &response);
    /* RX bytes remain owned by the application across QP close. */
    assert(dmesh_destroy_qp(sq) == 0);
    for (unsigned i = 0; i < held.len; ++i) assert(((unsigned char *)held.buf)[i] == (unsigned char)(i * 17));
    dmesh_release_rx_buffer(server, &held); dmesh_release_rx_buffer(server, &held);
    dmesh_event_t fin = receive(ce, DMESH_EVENT_RECV_FIN); assert(fin.qp == cq);
    assert(dmesh_destroy_qp(cq) == 0);
    assert(dmesh_destroy_eq(ce) == 0); assert(dmesh_destroy_eq(se) == 0);
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
