/* Actual WRITE_WITH_IMM+DPA ARM bootstrap with a descriptor-only controller. */
#define _GNU_SOURCE
#include <assert.h>
#include "src/transport/peer/peer_carrier_write_imm.c"

static unsigned receives, sends, connects, configs, credits;
static uintptr_t recv_address;
static uint64_t recv_id;
static int configure_error;
int peer_wim_recv(struct peer_wim_qp *qp, const struct ibv_recv_wr *wr)
{
    assert(qp && wr->num_sge == 1 && wr->sg_list->length == 64);
    receives++;
    recv_address = wr->sg_list->addr;
    recv_id = wr->wr_id;
    return 0;
}
int peer_wim_send(struct peer_wim_qp *qp, const struct ibv_send_wr *wr)
{
    assert(qp && wr->opcode == IBV_WR_SEND && wr->num_sge == 1);
    assert(wr->sg_list->length == 64 && !wr->next && connects);
    sends++;
    return 0;
}
int peer_wim_activate(struct peer_wim_qp *qp, const struct peer_verbs_endpoint *ep, int protected)
{
    assert(qp && protected == 1 && ep->native.bytes);
    connects++;
    return 0;
}
int peer_wim_configure_native(struct peer_wim_qp *qp, uint64_t base, uint32_t key, uint32_t capacity)
{
    assert(qp && base && key == 2 && capacity == 65536);
    configs++;
    return configure_error;
}
int peer_wim_admit(struct peer_wim_qp *qp, unsigned entries)
{ assert(qp && entries); return 1; }
int peer_wim_head(struct peer_wim_qp *qp, uint64_t id, uint64_t delta)
{ assert(qp && peer_write_wr_kind(id) == PEER_WRITE_CREDIT && delta == 8); credits++; return 0; }

static unsigned holds, released;
static int post_error, destroy_error;
static uintptr_t expected_source;
static void retain(void *owner, void *cookie)
{ assert(owner == &holds && cookie == &released); holds++; }
static void release(void *owner, void *cookie)
{ assert(owner == &holds && cookie == &released && holds); holds--; released++; }
int peer_wim_data(struct peer_wim_qp *qp, uint64_t end, uint64_t source, uint32_t key, uint32_t bytes)
{
    assert(qp && holds == 1 && end == 5 && source == expected_source && key == 7 && bytes == 5);
    return post_error;
}
int peer_wim_destroy(struct peer_wim_qp *qp)
{ assert(qp); return destroy_error; }
int peer_wim_register(struct peer_wim *t, void *address, size_t bytes, uint32_t key)
{ (void)t; (void)address; (void)bytes; (void)key; assert(0 && "arena already registered"); return -1; }
int peer_wim_unregister(struct peer_wim *t, uint32_t key)
{ (void)t; (void)key; assert(0 && "no lane MR allocated by fixture"); return -1; }

static void borrowed_source(void)
{
    uint8_t frame[] = {0x60, 1, 0, 1, 0x42}; /* dmesh4 DATA, slot 1, one byte body. */
    struct ibv_mr arena_mr = {.lkey = 7};
    struct wim_carrier_ctx t = {.capacity = 65536, .arena = frame, .arena_len = sizeof(frame),
                             .arena_mr = &arena_mr, .dpa = (void *)1};
    struct wim_carrier_conn *c = &t.conns[0];
    struct peer_carrier_payload p = {.data=frame+4, .len=1, .tx_header=frame,
        .region=frame, .region_len=sizeof(frame), .owner=&holds, .cookie=&released,
        .retain=retain, .release=release};
    expected_source = (uintptr_t)frame;
    *c = (struct wim_carrier_conn){.ctx=&t, .used=1, .epoch=1, .remote_capacity=65536,
        .state=PEER_WRITE_READY, .dpa_qp=(void *)1};
    dmesh_live_add(&t.active, 0);
    assert(wim_carrier_post_data(c, sizeof(frame), &p) == 1 && holds == 1 && !released);
    assert(c->tx_tail == 5 && c->tx_source.tail == 5);
    assert(!peer_write_tx_ring_done(&c->tx_source, 5) && !holds && released == 1);
    assert(!wim_carrier_retire(c) && !t.active.count);

    *c = (struct wim_carrier_conn){.ctx=&t, .used=1, .epoch=2, .remote_capacity=65536,
        .state=PEER_WRITE_READY, .dpa_qp=(void *)1};
    dmesh_live_add(&t.active, 0);
    post_error = destroy_error = -1;
    assert(wim_carrier_post_data(c, sizeof(frame), &p) == -1 && holds == 1);
    assert(!c->tx_tail && !c->tx_source.tail && c->state == PEER_WRITE_DEAD);
    assert(wim_carrier_retire(c) == -1 && c->used && holds == 1 && released == 1);
    destroy_error = 0;
    assert(!wim_carrier_retire(c) && !c->used && !holds && released == 2);
    assert(!t.active.count);
}

/* These entry points belong to progress/teardown outside this fixture. */
void peer_wim_debug(struct peer_wim_qp *qp) { (void)qp; assert(0); }
int peer_wim_poll(struct peer_wim *t, int max, struct ibv_wc *wc)
{ (void)t; (void)max; (void)wc; assert(0); return -1; }
int peer_wim_counters(struct peer_wim_qp *qp, uint64_t *tail, uint64_t *head,
                      uint64_t *completed, uint64_t *consumed)
{ (void)qp; (void)tail; (void)head; (void)completed; (void)consumed; assert(0); return -1; }
int peer_wim_free(struct peer_wim *t) { (void)t; assert(0); return -1; }
int peer_rdma_device_free(struct peer_rdma_device *d) { (void)d; assert(0); return -1; }

static void bootstrap(void)
{
    struct wim_carrier_ctx t = {.capacity = 65536};
    struct ibv_mr mr = {.lkey = 1, .rkey = 2};
    uint8_t scratch[128] = {0}, ring[64] = {0};
    struct wim_carrier_conn *c = &t.conns[0];
    *c = (struct wim_carrier_conn){.ctx = &t, .used = 1, .epoch = 1,
        .state = PEER_WRITE_INIT, .dpa_qp = (void *)1,
        .scratch = scratch, .scratch_mr = &mr, .ring = ring, .ring_mr = &mr,
        .endpoint = {.qpn = 10, .mtu = 1024}};
    assert(!wim_carrier_post_recv(c, 0));
    assert(receives == 1 && recv_address == (uintptr_t)scratch + 64);
    struct peer_verbs_endpoint remote = {.qpn = 11, .mtu = 1024, .native = {.bytes = 1}};
    remote.gid[10] = remote.gid[11] = 0xff;
    assert(wim_carrier_activate(c, &remote, 0) == -1 && !connects && !sends);
    assert(!wim_carrier_activate(c, &remote, 1) && connects == 1 && sends == 1);
    assert(c->state == PEER_WRITE_CONNECTING && c->boot_busy);
    assert(!memcmp(scratch, "DMWS\0\4\0\3", 8));
    memcpy(scratch + 64, scratch, 64);
    peer_write_put(scratch + 64 + 32, remote.qpn, 4);
    struct ibv_wc wc = {.wr_id = recv_id, .status = IBV_WC_SUCCESS,
        .qp_num = 10, .opcode = IBV_WC_RECV, .byte_len = 64};
    wim_carrier_completion(&t, &wc);
    assert(configs == 1 && c->peer_ready && c->state == PEER_WRITE_CONNECTING);
    c->rx_head = 8;
    assert(!wim_carrier_credit(c, 1) && !credits); /* Local SEND still owns bootstrap. */
    wc.wr_id = wim_carrier_id(c, PEER_WRITE_BOOT, 0); wc.opcode = IBV_WC_SEND;
    wim_carrier_completion(&t, &wc);
    assert(c->state == PEER_WRITE_READY && !c->boot_busy);
    assert(wim_carrier_credit(c, 1) == 1 && credits == 1);
    assert(receives == 1); /* The controller/device owns subsequent IMM receives. */
}

static void wire_contract(void)
{
    uint8_t hello[64], invalid[64];
    const uint8_t expected[40] = {
        'D', 'M', 'W', 'S', 0, 4, 0, 3,
        0, 1, 0, 0, 0, 0, 0, 2,
        0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
        0, 0, 0, 0, 0, 0, 0, 1,
        0, 0, 0, 10, 0, 1, 0, 0};
    struct ibv_mr mr = {.rkey = 2};
    struct wim_carrier_ctx t = {.capacity = 65536};
    struct wim_carrier_conn c = {.ctx = &t, .scratch = hello, .ring_mr = &mr,
        .ring = (void *)(uintptr_t)UINT64_C(0x1020304050607080),
        .epoch = 1, .endpoint = {.qpn = 10}, .peer_qpn = 10, .dpa_qp = (void *)1};
    assert(!wim_carrier_hello_encode(&c));
    assert(!memcmp(hello, expected, sizeof(expected)));
    for (unsigned i = 40; i < sizeof(hello); i++) assert(!hello[i]);
    const unsigned corrupt[] = {0, 5, 7, 8, 9, 10, 32, 36, 40, 63};
    for (unsigned i = 0; i < sizeof(corrupt) / sizeof(corrupt[0]); i++) {
        memcpy(invalid, hello, sizeof(invalid)); invalid[corrupt[i]] ^= 1;
        assert(wim_carrier_hello_decode(&c, invalid) == -1 && !c.peer_ready);
    }
    for (unsigned mode = 1; mode <= 2; mode++) { /* Reject both legacy Sync encodings. */
        memcpy(invalid, hello, sizeof(invalid)); invalid[7] = mode;
        assert(wim_carrier_hello_decode(&c, invalid) == -1);
    }
    configure_error = -1;
    assert(wim_carrier_hello_decode(&c, hello) == -1 && !c.peer_ready);
    configure_error = 0;
    assert(!wim_carrier_hello_decode(&c, hello) && c.peer_ready && c.remote_capacity == 65536);
    assert(wim_carrier_hello_decode(&c, hello) == -1); /* No reconfiguration on live QP. */
}

static void mirrored_ring(void)
{
    uint8_t *ring = NULL;
    size_t capacity = PEER_DATA_FRAME_BYTES;
    assert(!peer_write_map_buffer(&ring, capacity, "peer-write-test"));
    volatile uint8_t *alias = ring;
    alias[0] = 0x42;
    assert(alias[capacity] == 0x42);
    alias[capacity - 1] = 1; alias[capacity] = 2;
    assert(alias[capacity - 1] == 1 && alias[0] == 2);
    assert(!munmap(ring, 2 * capacity));
}
int main(void)
{
    bootstrap(); wire_contract(); mirrored_ring(); borrowed_source();
    puts("peer_imm_bootstrap_test: protected connect, exact native HELLO, SEND barrier, ring wrap, source quarantine: PASS");
    return 0;
}
