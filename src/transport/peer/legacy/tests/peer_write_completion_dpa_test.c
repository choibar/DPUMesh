/* Production DPA WRITE with host SDK substitutes and delayed CQEs. */
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "../peer_write_completion.h"
#include "peer_write_sync_dpa_shared.h"
#include "support/dpa_sdk/peer_dpa_sdk.h"
enum { WRITE, IMM, SIGNAL, SEND };
static struct { unsigned kind; uint32_t bytes, immediate; } posts[32];
static unsigned nposts;
static unsigned nreceives, receive_bytes, receive_acks;
static unsigned writebacks;
void __dpa_thread_window_writeback(void) { writebacks++; }
void doca_dpa_dev_device_set(uint64_t dev) { (void)dev; }
static void record(unsigned kind, uint32_t bytes, uint32_t immediate) {
    assert(nposts < 32);
    posts[nposts].kind = kind;
    posts[nposts].bytes = bytes;
    posts[nposts++].immediate = immediate;
}
void doca_dpa_dev_rdma_post_write(uint64_t qp, uint32_t connection,
    uint32_t rkey, uint64_t remote, uint32_t key, uint64_t src, uint32_t bytes, unsigned flags) {
    (void)qp; (void)connection; (void)rkey; (void)remote; (void)key; (void)src; (void)flags;
    record(WRITE, bytes, 0);
}
void doca_dpa_dev_rdma_post_write_imm(uint64_t qp, uint32_t connection,
    uint32_t rkey, uint64_t remote, uint32_t key, uint64_t src, uint32_t bytes,
    uint32_t immediate, unsigned flags) {
    (void)qp; (void)connection; (void)rkey; (void)remote; (void)key; (void)src; (void)flags;
    record(IMM, bytes, immediate);
}
void doca_dpa_dev_rdma_signal_set(uint64_t qp, uint32_t connection,
    uint64_t remote, uint64_t value) {
    (void)qp; (void)connection; (void)remote;
    record(SIGNAL, 0, value);
}
void doca_dpa_dev_rdma_post_receive(uint64_t qp, uint32_t key, uint64_t src, uint32_t bytes) {
    (void)qp; (void)key; (void)src; (void)bytes;
    nreceives++; receive_bytes += bytes;
}
void doca_dpa_dev_rdma_receive_ack(uint64_t qp, unsigned count) {
    (void)qp; receive_acks += count;
}
void doca_dpa_dev_rdma_post_send(uint64_t qp, uint32_t connection,
    uint32_t key, uint64_t src, uint32_t bytes, unsigned flags) {
    (void)qp; (void)connection; (void)key; (void)src; (void)flags;
    record(SEND, bytes, 0);
}

/* Delayed CQEs are consumed by the complete production drain loop. */
static struct {
    uint32_t type, qpn, counter, bytes;
    int pending;
} completion;
static unsigned cq_acks;

void __dpa_thread_window_read_inv(void) {}
uint64_t doca_dpa_dev_buf_array_get_buf(uint64_t array, unsigned index)
{
    assert(index == 0);
    return array;
}
uint64_t doca_dpa_dev_buf_get_external_ptr(uint64_t buffer) { return buffer; }
int doca_dpa_dev_get_completion(uint64_t cq, doca_dpa_dev_completion_element_t *element)
{
    if (cq != 1 || !completion.pending)
        return 0;
    completion.pending = 0;
    *element = 1;
    return 1;
}
void doca_dpa_dev_completion_ack(uint64_t cq, unsigned count)
{
    assert(cq == 1 && count == 1);
    cq_acks += count;
}
uint32_t doca_dpa_dev_completion_element_get_qpn(doca_dpa_dev_completion_element_t element)
{ assert(element == 1); return completion.qpn; }
uint32_t doca_dpa_dev_completion_element_get_received_bytes(doca_dpa_dev_completion_element_t element)
{ assert(element == 1); return completion.bytes; }
uint32_t doca_dpa_dev_get_completion_type(doca_dpa_dev_completion_element_t element)
{ assert(element == 1); return completion.type; }
uint32_t doca_dpa_dev_rdma_completion_get_wr_index(doca_dpa_dev_completion_element_t element)
{ assert(element == 1); return completion.counter; }
uint32_t doca_dpa_dev_get_completion_immediate(doca_dpa_dev_completion_element_t element)
{ (void)element; assert(0 && "unexpected IMM completion"); return 0; }
uint32_t doca_dpa_dev_get_completion_user_data(doca_dpa_dev_completion_element_t element)
{ (void)element; assert(0 && "unexpected event completion"); return 0; }
void doca_dpa_dev_sync_event_get(uint64_t event, uint64_t *value)
{ (void)event; (void)value; assert(0 && "unexpected event GET"); }
void doca_dpa_dev_sync_event_post_wait_ne(uint64_t ops, uint64_t event, uint64_t value)
{ (void)ops; (void)event; (void)value; assert(0 && "unexpected event rearm"); }
int doca_dpa_dev_comch_producer_is_consumer_empty(uint64_t producer, unsigned consumer)
{ (void)producer; (void)consumer; assert(0 && "unexpected MsgQ query"); return 1; }
void doca_dpa_dev_comch_producer_post_send_imm_only(uint64_t producer, unsigned consumer,
                                                 const uint8_t *data, uint32_t bytes,
                                                 unsigned flags)
{
    (void)producer; (void)consumer; (void)data; (void)bytes; (void)flags;
    assert(0 && "unexpected MsgQ notification");
}

#include "src/transport/peer/device/peer_write_sync_dpa.c"

static void drain(struct wsd_device *s, struct wsd_shared *shared)
{
    struct wsd_lane *l = &s->lane[0];
    if (!l->binding.qpn) {
        l->binding.qpn = 10;
        wsd_qpn_add(s, 0);
    }
    s->count = 1;
    s->active[0] = 0;
    s->cq = 1;
    s->event_cq = 2;
    s->event_wait = WSD_EVENT_ASYNC;
    s->buffers = (uintptr_t)shared;
    struct peer_dpa_thread_arg arg = {0};
    peer_write_sync_dpa_drain(&arg, 0, (uintptr_t)s, PEER_WRITE_BUDGET);
}

static void receive_completion(struct wsd_device *s, struct wsd_shared *shared, uint32_t bytes)
{
    assert(!completion.pending);
    completion.type = DOCA_DPA_DEV_COMP_RECV_SEND;
    completion.qpn = 10;
    completion.bytes = bytes;
    completion.pending = 1;
    unsigned before = cq_acks;
    drain(s, shared);
    assert(!completion.pending && cq_acks == before + 1);
}

static void send_completion(struct wsd_device *s, struct wsd_shared *shared, uint32_t counter)
{
    assert(!completion.pending);
    completion.type = DOCA_DPA_DEV_COMP_SEND;
    completion.qpn = 10;
    completion.counter = counter;
    completion.pending = 1;
    unsigned before = cq_acks;
    drain(s, shared);
    assert(!completion.pending && cq_acks == before + 1);
}

static void queue_data(struct wsd_device *s)
{
    struct wsd_lane *l = &s->lane[0];
    peer_dpa_work_init(&s->pending);
    assert(!s->pending.head[0]);
    unsigned entry = peer_dpa_work_push(&s->pending, 0);
    assert(entry);
    s->pending_request[entry - 1] = (struct wsd_request){
        .op = WSD_DATA, .lane = 0, .qpn = l->binding.qpn,
        .generation = l->binding.generation, .sequence = l->consumed + 1, .bytes = 4,
        .data = {.src = 0xb00000, .delta = l->tx_tail + 4, .key = 2, .id = l->data_id + 1}};
}

static int can_admit(struct wsd_device *s, struct wsd_shared *shared)
{
    assert(s->pending.head[0]);
    drain(s, shared);
    return !s->pending.head[0];
}
static void bootstrap(unsigned mode, uint32_t bytes, int valid) {
    struct wsd_device *s = calloc(1, sizeof(*s));
    struct wsd_shared *shared = calloc(1, sizeof(*shared));
    assert(s && shared);
    struct wsd_status *o = &shared->status[0];
    struct wsd_lane *l = &s->lane[0];
    l->binding = (struct wsd_binding){.qp=1, .device=1, .generation=1, .mode=mode};
    nposts = nreceives = receive_bytes = receive_acks = 0;
    struct wsd_request r = {.op=WSD_RECEIVE, .generation=1, .sequence=1,
        .bytes=bytes, .data={.src=0x400000, .key=2}};
    unsigned flags = 0;
    assert(wsd_request(l, o, &r, &flags));
    if (!valid) {
        assert(l->failed && o->error && !nreceives);
        l->failed = 0; o->error = 0;
        r.op = WSD_SEND_BOOT;
        assert(wsd_request(l, o, &r, &flags));
        assert(l->failed && o->error && !nposts);
        free(shared); free(s); return;
    }
    assert(!l->failed && l->busy[WSD_RECV]);
    assert(nreceives == (mode == WSD_IMMEDIATE ? WSD_IMM_RQ : 1));
    assert(receive_bytes == bytes); /* Remaining IMM receives have no payload buffer. */
    receive_completion(s, shared, bytes);
    assert(!l->failed && !l->busy[WSD_RECV] && receive_acks == 1);
    assert(o->done[WSD_RECV].id == 1 && o->done[WSD_RECV].bytes == bytes);
    assert(receive_bytes == bytes); /* Reposted notification also has zero bytes. */
    r.op = WSD_SEND_BOOT; r.sequence = 2;
    assert(wsd_request(l, o, &r, &flags));
    assert(!l->failed && nposts == 1 && posts[0].kind == SEND && posts[0].bytes == bytes);
    /* The CQ length must match the mode before publishing completion. */
    l->busy[WSD_RECV] = 1; l->id[WSD_RECV] = 3;
    receive_completion(s, shared, mode == WSD_IMMEDIATE ? 4096 : 64);
    assert(l->failed && o->error && o->done[WSD_RECV].id == 1);
    free(shared);
    free(s);
}

static uint64_t sequence;
static void submit(struct wsd_device *s, struct wsd_shared *shared,
    struct peer_write_completion *q, struct wsd_commit_batch *batch,
    uint64_t src, uint32_t bytes) {
    struct wsd_lane *l = &s->lane[0];
    struct wsd_status *o = &shared->status[0];
    s->fence_stats_enabled = 1;
    uint64_t end = q->posted_bytes + bytes;
    assert(!peer_write_completion_post(q, bytes, end));
    struct wsd_request r = {.op=WSD_DATA, .generation=1, .sequence=++sequence,
        .bytes=bytes, .data={.src=src, .delta=end, .key=2, .id=q->posted_id}};
    unsigned commit = 0;
    assert(wsd_request(l, o, &r, &commit));
    assert(!l->failed && commit == WSD_COMMIT_SEND);
    wsd_defer_commit(s, batch, 0, commit);
}

static void run(unsigned mode, uint32_t initial_id) {
    struct wsd_device *s = calloc(1, sizeof(*s));
    struct wsd_shared *shared = calloc(1, sizeof(*shared));
    assert(s && shared);
    struct wsd_lane *l = &s->lane[0];
    struct wsd_status *o = &shared->status[0];
    l->binding = (struct wsd_binding){.qp=1, .device=1, .generation=1,
        .mode=mode, .capacity=262144};
    l->configured = 1;
    l->remote_base = 0x100000;
    l->remote_capacity = 262144;
    l->remote_key = 3;
    l->tail_addr = 0x200000;
    l->head_addr = 0x200008;
    l->recv_source = 0x300000;
    l->recv_key = 2;
    l->completion_base_valid = 1;
    l->completion_base = 0xabcd0000;
    l->post_index = l->completed_index = 65534; /* Cross raw16 CQ index wrap. */
    l->fence_first = WSD_FENCES - 1; /* Cross software fence ring wrap. */
    l->data_id = o->completed_id = initial_id;
    struct peer_write_completion q = {0};
    assert(!peer_write_completion_init(&q, 262144));
    q.posted_id = q.completed_id = initial_id;
    struct wsd_commit_batch batch = {0};
    nposts = 0;
    sequence = 0;

    submit(s, shared, &q, &batch, 0x400000, 1024);
    submit(s, shared, &q, &batch, 0x400400, 2048);
    assert(nposts == 0 && l->last_data.bytes == 3072); /* Two requests, one WRITE. */
    /* A control descriptor shares sequence numbering but never takes a DATA ID. */
    l->rx_tail = 64;
    struct wsd_request credit = {.op=WSD_HEAD, .generation=1, .sequence=++sequence,
        .data={.src=1, .delta=64}};
    unsigned flags = 0;
    assert(wsd_request(l, o, &credit, &flags) && flags == WSD_COMMIT_HEAD);
    wsd_defer_commit(s, &batch, 0, flags);
    submit(s, shared, &q, &batch, 0x500000, 512);
    assert(nposts == 1 && posts[0].kind == WRITE && posts[0].bytes == 3072);
    wsd_commit_pending(s, &batch, shared);
    assert(l->fence_count == 1);
    uint32_t first_counter = l->fence[WSD_FENCES - 1].counter;
    assert(l->fence[WSD_FENCES - 1].data_id == initial_id + 3u);
    uint64_t end;
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 0);
    if (mode == WSD_IMMEDIATE) {
        assert(nposts == 3 && posts[1].kind == IMM && posts[1].bytes == 512);
        assert(posts[1].immediate == 3584);
        assert(posts[2].kind == IMM && posts[2].bytes == 0 && posts[2].immediate == 64);
    }

    submit(s, shared, &q, &batch, 0x600000, 65536);
    submit(s, shared, &q, &batch, 0x700000, 64);
    wsd_commit_pending(s, &batch, shared);
    assert(l->fence_count == 2 && l->fence[0].data_id == initial_id + 5u);
    uint32_t last_counter = l->fence[0].counter;

    send_completion(s, shared, l->completion_base + first_counter);
    assert(!l->failed && l->fence_count == 1 && l->fence_first == 0);
    assert(o->completed_id == initial_id + 3u);
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 3584);
    send_completion(s, shared, l->completion_base + last_counter);
    assert(!l->failed && !l->fence_count && l->fence_first == 1);
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 69184);

    /* One later CQE can retire multiple batch fences. */
    submit(s, shared, &q, &batch, 0x800000, 64);
    wsd_commit_pending(s, &batch, shared);
    submit(s, shared, &q, &batch, 0x900000, 128);
    wsd_commit_pending(s, &batch, shared);
    assert(l->fence_count == 2);
    send_completion(s, shared, l->completion_base + l->fence[2].counter);
    assert(!l->failed && !l->fence_count);
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 69376);

    assert(l->fence_stats.samples == 4 && l->fence_stats.depth_sum == 6);
    assert(l->fence_stats.peak == 2);
    assert(l->fence_stats.histogram[0] == 2 && l->fence_stats.histogram[1] == 2);
    queue_data(s);
    l->fence_count = WSD_FENCES;
    assert(!can_admit(s, shared) && !can_admit(s, shared));
    assert(l->fence_stats.full_stalls == 2);
    s->fence_stats_enabled = 0;
    assert(!can_admit(s, shared) && l->fence_stats.full_stalls == 2);
    s->fence_stats_enabled = 1;
    l->fence_count = 0;
    l->post_index = l->completed_index + WSD_SQ;
    assert(!can_admit(s, shared) && l->fence_stats.full_stalls == 2); /* SQ stall is distinct. */
    l->post_index = l->completed_index;
    assert(peer_dpa_work_take(&s->pending) == 0);
    peer_dpa_work_pop(&s->pending, 0);
    /* The queued request was never consumed by a full fence/SQ. */
    unsigned before = writebacks;
    wsd_fence_snapshot(s, shared, 0);
    assert(writebacks == before + 1);
    assert(shared->fence_snapshot.stats.samples == 4);
    assert(shared->fence_snapshot.stats.full_stalls == 2);
    assert(!l->fence_stats.samples && !l->fence_stats.full_stalls);
    peer_write_fence_stats_sample(&l->fence_stats, 7);
    assert(shared->fence_snapshot.stats.samples == 4); /* Frozen until next request. */

    /* Reject missing/duplicate logical IDs before changing the DATA prefix. */
    struct wsd_request bad = {.op=WSD_DATA, .generation=1, .sequence=++sequence,
        .bytes=64, .data={.src=0xa00000, .delta=l->tx_tail+64, .key=2, .id=l->data_id+2u}};
    flags = 0;
    uint32_t previous = l->data_id;
    assert(wsd_request(l, o, &bad, &flags));
    assert(l->failed && o->error && !flags && l->data_id == previous);
    peer_write_completion_free(&q);
    free(shared);
    free(s);
}

static void small_frames(unsigned mode) {
    struct wsd_device *s = calloc(1, sizeof(*s));
    struct wsd_shared *shared = calloc(1, sizeof(*shared));
    assert(s && shared);
    struct wsd_lane *l = &s->lane[0];
    l->binding = (struct wsd_binding){.qp=1, .device=1, .generation=1,
        .mode=mode, .capacity=65536};
    l->configured = 1;
    l->remote_base = 0x100000;
    l->remote_capacity = 65536;
    l->remote_key = 3;
    l->tail_addr = 0x200000;
    l->completion_base_valid = 1;
    struct peer_write_completion q = {0};
    struct wsd_commit_batch batch = {0};
    assert(!peer_write_completion_init(&q, 65536));
    nposts = 0; sequence = 0;
    submit(s, shared, &q, &batch, 0x400000, 4); /* FIN / retirement frame. */
    submit(s, shared, &q, &batch, 0x400004, 5); /* Header + one byte DATA. */
    wsd_commit_pending(s, &batch, shared);
    assert(l->fence_count == 1 && l->fence[0].data_id == 2);
    send_completion(s, shared, l->fence[0].counter);
    uint64_t end = 0;
    assert(!l->failed && !peer_write_completion_done(&q, shared->status[0].completed_id, &end));
    assert(end == 9);
    queue_data(s);
    assert(can_admit(s, shared) && !l->failed);
    peer_write_completion_free(&q); free(shared); free(s);
}
int main(void) {
    bootstrap(WSD_IMMEDIATE, 64, 1);
    bootstrap(WSD_SYNC, 4096, 1);
    bootstrap(WSD_IMMEDIATE, 4096, 0);
    bootstrap(WSD_SYNC, 64, 0);
    run(WSD_IMMEDIATE, 0);
    run(WSD_SYNC, 0);
    run(WSD_IMMEDIATE, UINT32_MAX - 1);
    run(WSD_SYNC, UINT32_MAX - 1);
    small_frames(WSD_IMMEDIATE);
    small_frames(WSD_SYNC);
    puts("peer_write_completion_dpa_test: ok");
    return 0;
}
