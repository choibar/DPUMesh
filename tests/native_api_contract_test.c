#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>

#include "src/core/dmesh_core.h"

static uint8_t reservation[64];
static int reserve_calls;
static int reserve_error;
static int commit_calls;
static int publish_calls;
static int qp_valid_calls;
static int open_call;                     /* the stubbed core holds a reservation */
static int rx_free_calls;
static int last_rx_slot = -1;
static dmesh_qp_t *tx_ready_qp;
static dmesh_qp_t *tx_error_qp;

uint8_t *
dpumesh_tx_reserve(dpumesh_ctx_t *ctx, uint16_t port, uint32_t len)
{
    (void)ctx;
    assert(port == 17);
    assert(len == 32);
    reserve_calls++;
    if (reserve_error) { errno = reserve_error; return NULL; }
    open_call = 1;
    return reservation;
}

int
dpumesh_tx_commit(dpumesh_ctx_t *ctx, uint16_t port,
                  const void *buf, uint32_t len)
{
    (void)ctx;
    commit_calls++;
    if (port == 17 && buf == reservation && len == 32) { open_call = 0; return 0; }
    errno = EINVAL;
    return -1;
}

int dmesh_tx_qp_valid(dmesh_qp_t *qp)
{
    assert(qp != NULL);
    qp_valid_calls++;
    if (open_call) { errno = EDEADLK; return -1; }
    return 0;
}
int dmesh_tx_call_active(dmesh_qp_t *qp) { return qp != NULL && open_call; }
int dmesh_tx_publish(dmesh_qp_t *qp) { assert(qp != NULL); publish_calls++; return 0; }
int dpumesh_eq_drain(struct dmesh_eq *eq) { assert(eq != NULL); return 0; }
void dpumesh_eq_arm(struct dmesh_eq *eq) { assert(eq != NULL); }
int dpumesh_qp_pending(dmesh_qp_t *qp) { assert(qp != NULL); return 0; }
int dpumesh_lost(dpumesh_ctx_t *ctx) { (void)ctx; return 0; }
/* No DPU here: no request arrives, no spare is reported and no SCALE answered. */
int dpumesh_qp_requests(dmesh_qp_t *c, dmesh_event_t *events, int max) { (void)c; (void)events; (void)max; return 0; }
int dpumesh_spare_ready(dpumesh_ctx_t *ctx, uint16_t port) { (void)ctx; (void)port; return 0; }
int dpumesh_scale_ack(dpumesh_ctx_t *ctx, uint16_t port, uint32_t token, uint32_t applied) { (void)ctx; (void)port; (void)token; (void)applied; return 0; }

int dpumesh_qp_take(dmesh_qp_t *qp, unsigned event)
{
    dmesh_qp_t **raised = event == DMESH_EQ_TX_READY ? &tx_ready_qp : &tx_error_qp;
    if (*raised != qp) return 0;
    *raised = NULL;
    return 1;
}

int dpumesh_conn_recv(dpumesh_ctx_t *ctx, uint16_t port, sw_descriptor_t *out)
{
    (void)ctx;
    (void)port;
    (void)out;
    return 0;
}

uint8_t *dpumesh_rx_buf(dpumesh_ctx_t *ctx, int slot)
{
    (void)ctx;
    (void)slot;
    return NULL;
}

void dpumesh_rx_free(dpumesh_ctx_t *ctx, int slot)
{
    (void)ctx;
    rx_free_calls++;
    last_rx_slot = slot;
}

int
main(void)
{
    dmesh_channel_t channel = {0};
    dmesh_qp_t qp = {0};
    qp.ep = &channel;
    qp.local_port = 17;
    qp.rx_slot = -1;                      /* no accept-held first message */

    /* A reservation validates the handle and opens the transmit call; the
     * committing post_send publishes it at once. */
    assert(dmesh_alloc(&qp, 32) == reservation);
    assert(reserve_calls == 1 && qp_valid_calls == 1 && open_call);
    assert(dmesh_post_send(&qp, reservation, 32) == 0);
    assert(commit_calls == 1 && publish_calls == 1 && !open_call);

    reserve_error = EAGAIN;
    assert(dmesh_alloc(&qp, 32) == NULL && errno == EAGAIN);
    assert(!open_call && publish_calls == 1);
    reserve_error = 0;

    /* A rejected commit publishes nothing and leaves the call open, so the
     * next reservation reports it; a valid post then closes it. */
    errno = 0;
    assert(dmesh_alloc(&qp, 32) == reservation);
    assert(dmesh_post_send(&qp, reservation + 1, 32) == -1);
    assert(errno == EINVAL);
    assert(commit_calls == 2 && publish_calls == 1 && open_call);
    errno = 0;
    assert(dmesh_alloc(&qp, 32) == NULL && errno == EDEADLK);
    assert(dmesh_post_send(&qp, reservation, 32) == 0);
    assert(commit_calls == 3 && publish_calls == 2 && !open_call);

    /* Without an open transmit call, post_send rejects before committing. */
    errno = 0;
    assert(dmesh_post_send(&qp, reservation, 32) == -1);
    assert(errno == EINVAL && commit_calls == 3);

    /* poll_qp exposes the core one-shot as a payload-free API event. */
    struct dmesh_eq eq = {0};
    dmesh_event_t event = {0};
    eq.ch = &channel;
    qp.eq = &eq;
    assert(dmesh_poll_qp(&qp, &event, 1) == 0);
    tx_error_qp = &qp;
    assert(dmesh_poll_qp(&qp, &event, 1) == 1);
    assert(event.qp == &qp);
    assert(event.type == DMESH_EVENT_TX_ERROR);
    assert(event.buf == NULL && event.len == 0 && event._rx_token == -1);
    tx_ready_qp = &qp;
    assert(dmesh_poll_qp(&qp, &event, 1) == 1);
    assert(event.qp == &qp);
    assert(event.type == DMESH_EVENT_TX_READY);
    assert(event.buf == NULL && event.len == 0 && event._rx_token == -1);
    assert(dmesh_poll_qp(&qp, &event, 1) == 0);
    assert(dmesh_poll_qp(NULL, &event, 1) == -1 && errno == EINVAL);

    uint8_t payload = 0x5a;
    event = (dmesh_event_t){
        .qp = &qp,
        .type = DMESH_EVENT_RECV,
        .buf = &payload,
        .len = 1,
        ._rx_token = 23,
    };
    dmesh_release_rx_buffer(&channel, &event);
    assert(rx_free_calls == 1 && last_rx_slot == 23);
    assert(event.buf == NULL && event._rx_token == -1);
    assert(event.type == DMESH_EVENT_RECV && event.len == 1);
    dmesh_release_rx_buffer(&channel, &event);
    assert(rx_free_calls == 1);

    puts("native_api_contract_test: PASS");
    return 0;
}
