/* Production carrier bootstrap/RQ layout with mocked verbs, no device. */
#define _GNU_SOURCE
#include <assert.h>
#include <infiniband/verbs.h>
static unsigned recv_calls, recv_buffers, recv_bytes, send_calls;
static uintptr_t recv_address, result_address;
static int mock_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad)
{
    (void)qp; (void)bad;
    recv_calls++;
    if (wr->num_sge) {
        assert(wr->num_sge == 1);
        recv_buffers++;
        recv_bytes += wr->sg_list->length;
        recv_address = wr->sg_list->addr;
    } else
        assert(!wr->sg_list);
    return 0;
}
static int mock_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad)
{
    (void)qp; (void)bad;
    for (; wr; wr = wr->next) {
        send_calls++;
        if (wr->opcode == IBV_WR_ATOMIC_FETCH_AND_ADD) {
            assert(wr->num_sge == 1 && wr->sg_list->length == 8);
            result_address = wr->sg_list->addr;
            *(uint64_t *)result_address = 42;
        }
    }
    return 0;
}
#define ibv_post_recv mock_recv
#define ibv_post_send mock_send
#include "src/transport/peer/peer_wire_write_sync.c"
#include "src/transport/peer/peer_wire_write_imm.c"

uint64_t peer_sync_events_address(struct peer_sync_events *e, unsigned which)
{ (void)e; return 0x200000 + 8u * which; }
uint32_t peer_sync_events_key(struct peer_sync_events *e)
{ (void)e; return 7; }

static void sync_layout(void)
{
    struct wsy_ctx t = {.capacity = 65536};
    struct wsy_conn *c = &t.conns[0];
    struct ibv_mr mr = {.lkey = 1, .rkey = 2};
    uint8_t *scratch = aligned_alloc(64, 128);
    uint8_t *tx = calloc(1, 65536);
    assert(scratch && tx && peer_write_scratch_size(0, 0) == 128);
    assert(peer_write_hello_size(0, 1) == 4096 && peer_write_scratch_size(0, 1) == 8192);
    assert(peer_write_hello_size(1, 1) == 64 && peer_write_scratch_size(1, 1) == 128);
    *c = (struct wsy_conn){.ctx = &t, .used = 1, .epoch = 1,
        .endpoint = {.qpn = 10}, .peer_qpn = 10, .state = WSY_CONNECTING,
        .scratch = scratch, .scratch_mr = &mr, .ring = tx, .ring_mr = &mr,
        .tx_ring = tx, .tx_ring_mr = &mr, .boot_busy = 1, .remote_capacity = 65536,
        .activated_ns = wsy_now()};
    assert(wsy_hello_size(c) == 64);
    assert(!wsy_hello_encode(c));
    uint8_t hello[64];
    memcpy(hello, scratch, sizeof(hello));
    memcpy(scratch + 64, scratch, 64);
    recv_calls = recv_buffers = recv_bytes = 0;
    assert(!wsy_post_recv(c, 0));
    assert(recv_calls == 1 && recv_bytes == 64 && recv_address == (uintptr_t)scratch + 64);
    struct ibv_wc wc = {.wr_id = wsy_id(c, WSY_RECV, 0), .status = IBV_WC_SUCCESS,
        .qp_num = 10, .opcode = IBV_WC_RECV, .byte_len = 64};
    wsy_completion(&t, &wc);
    assert(c->peer_ready && c->state == WSY_CONNECTING && c->boot_busy);
    c->rx_head = 8;
    send_calls = 0;
    assert(!wsy_credit(c, 1) && !send_calls); /* Local HELLO still owned by NIC. */
    const uint8_t fin[] = {0xa0, 1, 0, 0};
    assert(!wsy_send(c, fin, sizeof(fin)) && !send_calls);
    assert(!memcmp(scratch, hello, sizeof(hello)));
    wc.wr_id = wsy_id(c, WSY_BOOT, 0); wc.opcode = IBV_WC_SEND;
    wsy_completion(&t, &wc);
    assert(c->state == WSY_READY && !c->boot_busy);
    assert(wsy_credit(c, 1) == 1 && result_address == (uintptr_t)scratch + 8);
    assert(wsy_send(c, fin, sizeof(fin)) == 1 && result_address == (uintptr_t)scratch);
    assert(!memcmp(scratch + 64, hello, sizeof(hello))); /* RX HELLO remains separate. */
    free(tx); free(scratch);
}

static void imm_layout(void)
{
    struct wim_ctx t = {.capacity = 65536};
    struct wim_conn *c = &t.conns[0];
    struct ibv_mr mr = {.lkey = 1, .rkey = 2};
    uint8_t *scratch = aligned_alloc(64, 128);
    assert(scratch && WIM_SCRATCH == 128 && peer_write_scratch_size(1, 0) == 128);
    *c = (struct wim_conn){.ctx = &t, .used = 1, .epoch = 1,
        .endpoint = {.qpn = 10}, .peer_qpn = 10, .state = WIM_CONNECTING,
        .scratch = scratch, .scratch_mr = &mr, .ring = scratch, .ring_mr = &mr,
        .boot_busy = 1};
    wim_hello_encode(c);
    memcpy(scratch + 64, scratch, 64);
    recv_calls = recv_buffers = recv_bytes = 0;
    for (unsigned i = 0; i < WIM_RQ; i++) assert(!wim_post_recv(c, i));
    assert(recv_calls == WIM_RQ && recv_buffers == 1 && recv_bytes == 64);
    assert(recv_address == (uintptr_t)scratch + 64);
    struct ibv_wc wc = {.wr_id = wim_id(c, WIM_RECV, 0), .status = IBV_WC_SUCCESS,
        .qp_num = 10, .opcode = IBV_WC_RECV, .byte_len = 64};
    wim_completion(&t, &wc);
    assert(c->peer_ready && c->boot_busy && c->state == WIM_CONNECTING);
    assert(recv_buffers == 1 && recv_calls == WIM_RQ + 1); /* Zero-SGE repost. */
    wc.wr_id = wim_id(c, WIM_BOOT, 0); wc.opcode = IBV_WC_SEND;
    wim_completion(&t, &wc);
    assert(c->state == WIM_READY);
    wc.wr_id = wim_id(c, WIM_RECV, 1); wc.opcode = IBV_WC_RECV_RDMA_WITH_IMM;
    wc.wc_flags = IBV_WC_WITH_IMM; wc.byte_len = 5; wc.imm_data = htonl(5);
    wim_completion(&t, &wc);
    assert(c->rx_tail == 5 && c->state == WIM_READY && recv_buffers == 1);
    assert(recv_calls == WIM_RQ + 2 && !memcmp(scratch + 64, "DMWI", 4));
    free(scratch);
}

int main(void)
{
    sync_layout(); imm_layout();
    puts("peer_write_bootstrap_test: 64B ARM/IMM HELLO, 128B scratch, SEND completion barrier, zero-SGE RQ: PASS");
}
