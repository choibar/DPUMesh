/* Manual SEND carrier with mocked verbs: no device is opened. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <assert.h>
#include <infiniband/verbs.h>
#include <stdint.h>
#include <string.h>

static struct ibv_send_wr posted_send;
static struct ibv_sge posted_sge;
static struct ibv_wc pending_wc;
static int pending, destroy_result, dereg_result;
static unsigned recv_posts, destroys, deregistrations;

static int mock_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad)
{
    (void)bad;
    assert(qp && !wr->next && wr->num_sge == 1 && wr->opcode == IBV_WR_SEND);
    posted_send = *wr;
    posted_sge = *wr->sg_list;
    return 0;
}

static int mock_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad)
{
    (void)bad;
    assert(qp && !wr->next && wr->num_sge == 1);
    assert(wr->sg_list->length == 65536);
    recv_posts++;
    return 0;
}

static int mock_poll(struct ibv_cq *cq, int max, struct ibv_wc *wc)
{
    (void)cq;
    assert(max > 0);
    if (!pending)
        return 0;
    *wc = pending_wc;
    pending = 0;
    return 1;
}

static int mock_destroy(struct ibv_qp *qp)
{
    assert(qp);
    destroys++;
    return destroy_result;
}

static int mock_dereg(struct ibv_mr *mr)
{
    assert(mr);
    deregistrations++;
    return dereg_result;
}

#define ibv_post_send mock_send
#define ibv_post_recv mock_recv
#define ibv_poll_cq mock_poll
#define ibv_destroy_qp mock_destroy
#define ibv_dereg_mr mock_dereg
#include "src/transport/peer/peer_wire_rdma.c"

static void complete(uint64_t id, uint32_t bytes)
{
    assert(!pending);
    pending_wc = (struct ibv_wc){.wr_id = id, .status = IBV_WC_SUCCESS, .byte_len = bytes};
    pending = 1;
}

static void retain(void *owner, void *cookie)
{
    (void)cookie;
    ++*(unsigned *)owner;
}

static void release(void *owner, void *cookie)
{
    (void)cookie;
    assert(*(unsigned *)owner > 0);
    --*(unsigned *)owner;
}

int main(void)
{
    struct rdma_ctx ctx = {0};
    struct ibv_qp qp = {0};
    struct ibv_mr mr = {.lkey = 7};
    struct rdma_conn *c = rdma_slot_take(&ctx);
    assert(c);
    c->qp = &qp;
    c->mr = &mr;
    c->state = RC_READY;

    /* A retired epoch's completion cannot fault or free the current lane. */
    complete(wr_pack(0, c->epoch + 1, c->index, 0), 0);
    assert(!rdma_poll_cq(&ctx) && c->state == RC_READY);

    /* RX remains posted until completion, and is reposted after view release. */
    assert(!rdma_post_recv(c, 0) && recv_posts == 1);
    memcpy(recv_slot(c, 0), "reply", 5);
    complete(wr_pack(1, c->epoch, c->index, 0), 5);
    assert(rdma_poll_cq(&ctx) == 1);
    struct peer_wire_rx_lease rx;
    assert(rdma_rx_acquire(c, &rx) == 1 && rx.len == 5);
    assert(!memcmp(rx.data, "reply", 5) && recv_posts == 1);
    assert(!rdma_rx_release(c, &rx) && !rx.data && recv_posts == 2);

    /* Borrowed arena bytes stay owned until SEND completion. */
    uint8_t *arena = calloc(1, PEER_WIRE_FRAME_BYTES);
    assert(arena);
    unsigned refs = 1;
    ctx.arena = arena;
    ctx.arena_len = PEER_WIRE_FRAME_BYTES;
    ctx.arena_mr = &mr;
    struct peer_wire_payload payload = {
        .data = arena + PEER_WIRE_HEADER_BYTES, .tx_header = arena,
        .len = PEER_WIRE_TX_BODY_BYTES, .region = arena, .region_len = PEER_WIRE_FRAME_BYTES,
        .owner = &refs, .retain = retain, .release = release};
    struct peer_wire_frame_lease frame;
    assert(rdma_frame_reserve(c, &frame) == 1);
    assert(rdma_frame_commit(c, &frame, payload.len, &payload) == 1 && refs == 2);
    assert(posted_sge.addr == (uintptr_t)arena && posted_sge.length == 65536);
    complete(posted_send.wr_id, 0);
    assert(rdma_poll_cq(&ctx) == 1 && refs == 1);

    /* Failed QP/MR fences quarantine outstanding arena references for retry. */
    assert(rdma_frame_reserve(c, &frame) == 1);
    assert(rdma_frame_commit(c, &frame, payload.len, &payload) == 1 && refs == 2);
    destroy_result = -1;
    assert(peer_wire_verbs_close(c) == -1);
    assert(c->in_use && c->qp && c->mr && c->buf && refs == 2);
    assert(destroys == 1 && deregistrations == 0);
    destroy_result = 0;
    dereg_result = -1;
    assert(peer_wire_verbs_close(c) == -1);
    assert(c->in_use && !c->qp && c->mr && c->buf && refs == 2);
    assert(destroys == 2 && deregistrations == 1);
    dereg_result = 0;
    assert(!peer_wire_verbs_close(c) && !c->in_use && refs == 1);
    assert(destroys == 2 && deregistrations == 2);
    free(arena);
    puts("peer verbs: stale CQ, RX leases, arena TX lifetime and failed teardown fences: PASS");
}
