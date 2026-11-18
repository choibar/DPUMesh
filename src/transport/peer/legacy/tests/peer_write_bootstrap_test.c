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
        .endpoint = {.qpn = 10}, .peer_qpn = 10, .state = PEER_WRITE_CONNECTING,
        .scratch = scratch, .scratch_mr = &mr, .ring = tx, .ring_mr = &mr,
        .tx_ring = tx, .tx_ring_mr = &mr, .boot_busy = 1, .remote_capacity = 65536,
        .activated_ns = peer_verbs_now_ns()};
    assert(wsy_hello_size(c) == 64);
    assert(!wsy_hello_encode(c));
    uint8_t hello[64];
    memcpy(hello, scratch, sizeof(hello));
    memcpy(scratch + 64, scratch, 64);
    recv_calls = recv_buffers = recv_bytes = 0;
    assert(!wsy_post_recv(c, 0));
    assert(recv_calls == 1 && recv_bytes == 64 && recv_address == (uintptr_t)scratch + 64);
    struct ibv_wc wc = {.wr_id = wsy_id(c, PEER_WRITE_RECV, 0), .status = IBV_WC_SUCCESS,
        .qp_num = 10, .opcode = IBV_WC_RECV, .byte_len = 64};
    wsy_completion(&t, &wc);
    assert(c->peer_ready && c->state == PEER_WRITE_CONNECTING && c->boot_busy);
    c->rx_head = 8;
    send_calls = 0;
    assert(!wsy_credit(c, 1) && !send_calls); /* Local HELLO still owned by NIC. */
    assert(!memcmp(scratch, hello, sizeof(hello)));
    wc.wr_id = wsy_id(c, PEER_WRITE_BOOT, 0); wc.opcode = IBV_WC_SEND;
    wsy_completion(&t, &wc);
    assert(c->state == PEER_WRITE_READY && !c->boot_busy);
    assert(wsy_credit(c, 1) == 1 && result_address == (uintptr_t)scratch + 8);
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
        .endpoint = {.qpn = 10}, .peer_qpn = 10, .state = PEER_WRITE_CONNECTING,
        .scratch = scratch, .scratch_mr = &mr, .ring = scratch, .ring_mr = &mr,
        .boot_busy = 1};
    wim_hello_encode(c);
    memcpy(scratch + 64, scratch, 64);
    recv_calls = recv_buffers = recv_bytes = 0;
    for (unsigned i = 0; i < WIM_RQ; i++) assert(!wim_post_recv(c, i));
    assert(recv_calls == WIM_RQ && recv_buffers == 1 && recv_bytes == 64);
    assert(recv_address == (uintptr_t)scratch + 64);
    struct ibv_wc wc = {.wr_id = wim_id(c, PEER_WRITE_RECV, 0), .status = IBV_WC_SUCCESS,
        .qp_num = 10, .opcode = IBV_WC_RECV, .byte_len = 64};
    wim_completion(&t, &wc);
    assert(c->peer_ready && c->boot_busy && c->state == PEER_WRITE_CONNECTING);
    assert(recv_buffers == 1 && recv_calls == WIM_RQ + 1); /* Zero-SGE repost. */
    wc.wr_id = wim_id(c, PEER_WRITE_BOOT, 0); wc.opcode = IBV_WC_SEND;
    wim_completion(&t, &wc);
    assert(c->state == PEER_WRITE_READY);
    wc.wr_id = wim_id(c, PEER_WRITE_RECV, 1); wc.opcode = IBV_WC_RECV_RDMA_WITH_IMM;
    wc.wc_flags = IBV_WC_WITH_IMM; wc.byte_len = 5; wc.imm_data = htonl(5);
    wim_completion(&t, &wc);
    assert(c->rx_tail == 5 && c->state == PEER_WRITE_READY && recv_buffers == 1);
    assert(recv_calls == WIM_RQ + 2 && !memcmp(scratch + 64, "DMWI", 4));
    free(scratch);
}

/* Pin the existing wire bytes independently of the shared encoder/decoder.
 * Exercise both production adapters so a common codec cannot hide a drift. */
static void hello_contract(void)
{
    uint8_t imm[64], sync[64], invalid[64];
    const uint8_t expected[40] = {
        'D', 'M', 'W', 'I', 0, 4, 0, 1,
        0, 1, 0, 0, 0x11, 0x22, 0x33, 0x44,
        0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
        0, 0, 0, 0, 1, 2, 3, 4,
        0, 0xaa, 0xbb, 0xcc, 0, 1, 0, 0
    };
    struct ibv_mr mr = {.rkey = 0x11223344};
    struct wim_ctx it = {.capacity = 65536};
    struct wsy_ctx st = {.capacity = 65536};
    struct wim_conn ic = {.ctx = &it, .scratch = imm, .ring_mr = &mr,
        .ring = (uint8_t *)(uintptr_t)UINT64_C(0x1020304050607080),
        .epoch = 0x01020304, .endpoint = {.qpn = 0xaabbcc}, .peer_qpn = 0xaabbcc};
    struct wsy_conn sc = {.ctx = &st, .scratch = sync, .ring_mr = &mr,
        .ring = ic.ring, .epoch = ic.epoch, .endpoint = ic.endpoint, .peer_qpn = ic.peer_qpn};
    wim_hello_encode(&ic);
    assert(!memcmp(imm, expected, sizeof(expected)));
    for (unsigned i = 40; i < sizeof(imm); i++) assert(!imm[i]);
    assert(!wsy_hello_encode(&sc));
    assert(!memcmp(sync, expected, 3) && sync[3] == 'S');
    assert(!memcmp(sync + 4, expected + 4, sizeof(expected) - 4));

    /* Bad magic/version/mode/capacity/QPN/frame size and reserved extension. */
    const unsigned corrupt[] = {0, 5, 7, 8, 9, 10, 32, 36, 63};
    for (unsigned i = 0; i < sizeof(corrupt) / sizeof(corrupt[0]); i++) {
        memcpy(invalid, imm, sizeof(invalid));
        invalid[corrupt[i]] ^= 1;
        assert(wim_hello_decode(&ic, invalid) == -1 && !ic.peer_ready);
        memcpy(invalid, sync, sizeof(invalid));
        invalid[corrupt[i]] ^= 1;
        assert(wsy_hello_decode(&sc, invalid) == -1 && !sc.peer_ready);
    }
    const unsigned zero[] = {16, 24}; /* Missing ring base or generation. */
    for (unsigned i = 0; i < sizeof(zero) / sizeof(zero[0]); i++) {
        memcpy(invalid, imm, sizeof(invalid));
        memset(invalid + zero[i], 0, 8);
        assert(wim_hello_decode(&ic, invalid) == -1);
        memcpy(invalid, sync, sizeof(invalid));
        memset(invalid + zero[i], 0, 8);
        assert(wsy_hello_decode(&sc, invalid) == -1);
    }
    memcpy(invalid, sync, sizeof(invalid));
    memcpy(invalid + 48, invalid + 40, 8); /* Aliased Sync counters. */
    assert(wsy_hello_decode(&sc, invalid) == -1);
    assert(wim_hello_decode(&ic, sync) == -1 && wsy_hello_decode(&sc, imm) == -1);
    assert(!wim_hello_decode(&ic, imm) && !wsy_hello_decode(&sc, sync));
    assert(ic.remote_capacity == 65536 && sc.remote_capacity == 65536);
    assert(ic.remote_key == mr.rkey && sc.remote_key == mr.rkey);
    assert(ic.remote_base == (uintptr_t)ic.ring && sc.remote_base == (uintptr_t)ic.ring);
    assert(ic.remote_generation == ic.epoch && sc.remote_generation == ic.epoch);
    assert(wim_hello_decode(&ic, imm) == -1 && wsy_hello_decode(&sc, sync) == -1);
}

static void mirrored_ring(void)
{
    uint8_t *ring = NULL;
    size_t capacity = PEER_DATA_FRAME_BYTES;
    assert(!peer_write_map_buffer(&ring, capacity, "peer-write-test"));
    /* Distinct virtual addresses alias physically; force ordered accesses
     * rather than letting C optimization assume disjoint array elements. */
    volatile uint8_t *alias = ring;
    alias[0] = 0x42;
    assert(alias[capacity] == 0x42);
    const uint8_t record[] = {1, 2, 3, 4};
    for (unsigned i = 0; i < sizeof(record); i++)
        alias[capacity - 2 + i] = record[i];
    assert(alias[capacity - 2] == 1 && alias[capacity - 1] == 2);
    assert(alias[0] == 3 && alias[1] == 4);
    assert(!munmap(ring, 2 * capacity));
}

int main(void)
{
    hello_contract(); mirrored_ring(); sync_layout(); imm_layout();
    puts("peer_write_bootstrap_test: HELLO wire/validation, mirrored ring, SEND barrier, zero-SGE RQ: PASS");
}
