/* Native allocation, posting, event polling, and RX-buffer release API.
 * Operations run on the caller thread and expose registered RX/TX memory. */
#include "src/core/dmesh_core.h"
#include <stdio.h>

#include <errno.h>

/* ===== Send ===== */

/* The reservation is the QP's open transmit call. */
void *dmesh_alloc(dmesh_qp_t *c, uint32_t len) {
    if (dmesh_tx_qp_valid(c) != 0) return NULL;
    return dpumesh_tx_reserve(c->ep->ctx, c->local_port, len);
}

/* Commit the open transmit call and publish it at once, a partial unit too. */
int dmesh_post_send(dmesh_qp_t *c, const void *buf, uint32_t len) {
    if (!dmesh_tx_call_active(c)) { errno = EINVAL; return -1; }
    if (dpumesh_tx_commit(c->ep->ctx, c->local_port, buf, len) != 0) return -1;
    return dmesh_tx_publish(c);
}

/* ===== Event polling ===== */

/* Emit one event for a landed message descriptor. A 0-length body is the
 * wire FIN marker: its landing credit is returned here (nothing to hand out) and
 * the conn latches EOF. */
static void eq_emit(dmesh_channel_t *s, dmesh_qp_t *c, dmesh_event_t *event,
                    int32_t slot, uint32_t body_len) {
    event->qp = c;
    if (body_len == 0) {                     /* FIN → EOF event */
        dpumesh_rx_free(s->ctx, slot);
        c->peer_closed = 1;
        event->type      = DMESH_EVENT_RECV_FIN;
        event->buf       = NULL;
        event->len       = 0;
        event->_rx_token = -1;
        return;
    }
    event->type      = DMESH_EVENT_RECV;
    event->buf       = dpumesh_rx_buf(s->ctx, slot);
    event->len       = body_len;
    event->_rx_token = slot;                 /* held until release_rx_buffer */
}

/* One payload-free event of the QP. */
static void emit_tx(dmesh_qp_t *c, dmesh_event_t *event, dmesh_event_type_t type) {
    event->qp        = c;
    event->type      = type;
    event->buf       = NULL;
    event->len       = 0;
    event->_rx_token = -1;
}

/* The QP's pending events into events[]. Flow faults the carrier reported are
 * terminal and take precedence, so an application never spins retrying a
 * failed stream. TX readiness (a QP whose automatically armed alloc(EAGAIN)
 * became retryable; a hint, not a reservation) comes before bulk RX, so a
 * read-heavy QP cannot starve writes. Then the accept-held first message and
 * the inbox; what does not fit stays for the next call. */
static int poll_qp_once(dmesh_qp_t *c, dmesh_event_t *events, int max_events) {
    dmesh_channel_t *s = c->ep;
    int n = 0;
    if (dpumesh_qp_take(c, DMESH_EQ_TX_ERROR))
        emit_tx(c, &events[n++], DMESH_EVENT_TX_ERROR);
    if (n < max_events && dpumesh_qp_take(c, DMESH_EQ_TX_READY))
        emit_tx(c, &events[n++], DMESH_EVENT_TX_READY);
    if (n < max_events && c->rx_slot >= 0) {
        eq_emit(s, c, &events[n++], c->rx_slot, c->rx_len);
        c->rx_slot = -1; c->rx_buf = NULL; c->rx_len = 0; c->rx_pos = 0;
    }
    sw_descriptor_t d;
    while (n < max_events && dpumesh_conn_recv(s->ctx, c->local_port, &d))
        eq_emit(s, c, &events[n++], d.body_buf_slot, d.body_len);
    return n;
}

/* An empty poll is what precedes the caller's sleep on dmesh_qp_fd: arm the
 * doorbell, then drain once more so a completion that landed between the
 * drain and the arm is not slept through. Anything it (or the channel
 * meanwhile) queued for this QP is consumed here: the drain does not write
 * the QP's own eventfd. */
int dmesh_poll_qp(dmesh_qp_t *qp, dmesh_event_t *events, int max_events) {
    if (!qp || !qp->eq || !events || max_events <= 0) { errno = EINVAL; return -1; }
    dmesh_eq_t *eq = qp->eq;
    dpumesh_eq_awake(eq);
    (void)dpumesh_eq_drain(eq);
    int n = poll_qp_once(qp, events, max_events);
    if (n != 0) {
        dpumesh_eq_note_work(eq);
        return n;
    }
    dpumesh_eq_arm(eq);
    if (dpumesh_eq_drain(eq) > 0 || dpumesh_qp_pending(qp)) {
        n = poll_qp_once(qp, events, max_events);
        if (n != 0) return n;
    }
    /* Events that landed before a loss are delivered first. */
    int lost = dpumesh_lost(eq->ch->ctx);
    if (lost) { errno = lost; return -1; }
    return 0;
}

void dmesh_release_rx_buffer(dmesh_channel_t *s, dmesh_event_t *event) {
    if (!s || !event || event->_rx_token < 0) return;
    dpumesh_rx_free(s->ctx, event->_rx_token);
    event->_rx_token = -1;                  /* idempotent */
    event->buf = NULL;
}
