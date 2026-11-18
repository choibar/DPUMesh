#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
/* Production DPA WRITE with host SDK substitutes and delayed CQEs. */
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "src/transport/peer/peer_write_imm_dpa.c"
#include "device/peer_write_imm_dpa_internal.h"
#include "support/dpa_sdk/peer_dpa_sdk.h"
static void *zero_cache_aligned(size_t bytes)
{
    void *p = aligned_alloc(64, (bytes + 63u) & ~(size_t)63u);
    assert(p);
    memset(p, 0, bytes);
    return p;
}
enum { WRITE, IMM, SEND };
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
    uint32_t type, qpn, counter, bytes, immediate;
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
{ assert(element == 1); return completion.immediate; }



static unsigned messages;
static int consumer_empty;
static uint64_t publication_at_send;
static struct wim_shared *notify_shared;
int doca_dpa_dev_comch_producer_is_consumer_empty(uint64_t producer, unsigned consumer)
{ assert(producer && consumer == 0); return consumer_empty; }
void doca_dpa_dev_comch_producer_post_send_imm_only(uint64_t producer, unsigned consumer,
                                                 const uint8_t *data, uint32_t bytes,
                                                 unsigned flags)
{
    assert(producer && consumer == 0 && flags == DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
    assert(bytes == sizeof(struct dpa_rdma_counter_msg) || bytes == sizeof(struct dpa_rdma_notify_msg));
    assert(data[0] == DPA_MSG_RDMA_COUNTER || data[0] == DPA_MSG_RDMA_READY);
    assert(notify_shared && notify_shared->publication.sequence == publication_at_send);
    messages++;
}

#include "src/transport/peer/device/peer_write_imm_dpa.c"

static void drain(struct wim_device *s, struct wim_shared *shared)
{
    struct wim_lane *l = &s->lane[0];
    if (!l->binding.qpn) {
        l->binding.qpn = 10;
        wim_qpn_add(s, 0);
    }
    s->count = 1;
    s->active[0] = 0;
    s->cq = 1;
    s->buffers = (uintptr_t)shared;
    struct peer_dpa_thread_arg arg = {0};
    peer_write_imm_dpa_drain(&arg, 0, (uintptr_t)s, PEER_WRITE_BUDGET);
}

static void receive_completion(struct wim_device *s, struct wim_shared *shared, uint32_t bytes)
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

static void send_completion(struct wim_device *s, struct wim_shared *shared, uint32_t counter)
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

static void queue_data(struct wim_device *s)
{
    struct wim_lane *l = &s->lane[0];
    peer_dpa_work_init(&s->pending);
    assert(!s->pending.head[0]);
    unsigned entry = peer_dpa_work_push(&s->pending, 0);
    assert(entry);
    s->pending_request[entry - 1] = (struct wim_request){
        .op = WIM_DATA, .lane = 0, .qpn = l->binding.qpn,
        .generation = l->binding.generation, .sequence = l->consumed + 1, .bytes = 4,
        .data = {.src = 0xb00000, .delta = l->tx_tail + 4, .key = 2, .id = l->data_id + 1}};
}

static int can_admit(struct wim_device *s, struct wim_shared *shared)
{
    assert(s->pending.head[0]);
    drain(s, shared);
    return !s->pending.head[0];
}
static void bootstrap(uint32_t bytes, int valid) {
    struct wim_device *s = zero_cache_aligned(sizeof(*s));
    struct wim_shared *shared = zero_cache_aligned(sizeof(*shared));
    assert(s && shared);
    struct wim_status *o = &shared->status[0];
    struct wim_lane *l = &s->lane[0];
    l->binding = (struct wim_binding){.qp=1, .device=1, .generation=1};
    nposts = nreceives = receive_bytes = receive_acks = 0;
    struct wim_request r = {.op=WIM_RECEIVE, .generation=1, .sequence=1,
        .bytes=bytes, .data={.src=0x400000, .key=2}};
    unsigned flags = 0;
    assert(wim_request(l, o, &r, &flags));
    if (!valid) {
        assert(l->failed && o->error && !nreceives);
        l->failed = 0; o->error = 0;
        r.op = WIM_SEND_BOOT;
        assert(wim_request(l, o, &r, &flags));
        assert(l->failed && o->error && !nposts);
        free(shared); free(s); return;
    }
    assert(!l->failed && l->busy[WIM_RECV]);
    assert(nreceives == WIM_IMM_RQ);
    assert(receive_bytes == bytes); /* Remaining IMM receives have no payload buffer. */
    receive_completion(s, shared, bytes);
    assert(!l->failed && !l->busy[WIM_RECV] && receive_acks == 1);
    assert(o->done[WIM_RECV].id == 1 && o->done[WIM_RECV].bytes == bytes);
    assert(receive_bytes == bytes); /* Reposted notification also has zero bytes. */
    r.op = WIM_SEND_BOOT; r.sequence = 2;
    assert(wim_request(l, o, &r, &flags));
    assert(!l->failed && nposts == 1 && posts[0].kind == SEND && posts[0].bytes == bytes);
    /* The CQ length must match the mode before publishing completion. */
    l->busy[WIM_RECV] = 1; l->id[WIM_RECV] = 3;
    receive_completion(s, shared, 4096);
    assert(l->failed && o->error && o->done[WIM_RECV].id == 1);
    free(shared);
    free(s);
}

static uint64_t sequence;
static void submit(struct wim_device *s, struct wim_shared *shared,
    struct peer_write_completion *q, struct wim_commit_batch *batch,
    uint64_t src, uint32_t bytes) {
    struct wim_lane *l = &s->lane[0];
    struct wim_status *o = &shared->status[0];
    s->fence_stats_enabled = 1;
    uint64_t end = q->posted_bytes + bytes;
    assert(!peer_write_completion_post(q, bytes, end));
    struct wim_request r = {.op=WIM_DATA, .generation=1, .sequence=++sequence,
        .bytes=bytes, .data={.src=src, .delta=end, .key=2, .id=q->posted_id}};
    unsigned commit = 0;
    assert(wim_request(l, o, &r, &commit));
    assert(!l->failed && commit == WIM_COMMIT_SEND);
    wim_defer_commit(s, batch, 0, commit);
}

static void run(uint32_t initial_id) {
    struct wim_device *s = zero_cache_aligned(sizeof(*s));
    struct wim_shared *shared = zero_cache_aligned(sizeof(*shared));
    assert(s && shared);
    struct wim_lane *l = &s->lane[0];
    struct wim_status *o = &shared->status[0];
    l->binding = (struct wim_binding){.qp=1, .device=1, .generation=1,
        .capacity=262144};
    l->configured = 1;
    l->remote_base = 0x100000;
    l->remote_capacity = 262144;
    l->remote_key = 3;
    l->recv_source = 0x300000;
    l->recv_key = 2;
    l->completion_base_valid = 1;
    l->completion_base = 0xabcd0000;
    l->post_index = l->completed_index = 65534; /* Cross raw16 CQ index wrap. */
    l->fence_first = WIM_FENCES - 1; /* Cross software fence ring wrap. */
    l->data_id = o->completed_id = initial_id;
    struct peer_write_completion q = {0};
    assert(!peer_write_completion_init(&q, 262144));
    q.posted_id = q.completed_id = initial_id;
    struct wim_commit_batch batch = {0};
    nposts = 0;
    sequence = 0;

    submit(s, shared, &q, &batch, 0x400000, 1024);
    submit(s, shared, &q, &batch, 0x400400, 2048);
    assert(nposts == 0 && l->last_data.bytes == 3072); /* Two requests, one WRITE. */
    /* A control descriptor shares sequence numbering but never takes a DATA ID. */
    l->rx_tail = 64;
    struct wim_request credit = {.op=WIM_HEAD, .generation=1, .sequence=++sequence,
        .data={.src=1, .delta=64}};
    unsigned flags = 0;
    assert(wim_request(l, o, &credit, &flags) && flags == WIM_COMMIT_HEAD);
    wim_defer_commit(s, &batch, 0, flags);
    submit(s, shared, &q, &batch, 0x500000, 512);
    assert(nposts == 1 && posts[0].kind == WRITE && posts[0].bytes == 3072);
    wim_commit_pending(s, &batch, shared);
    assert(l->fence_count == 1);
    uint32_t first_counter = l->fence[WIM_FENCES - 1].counter;
    assert(l->fence[WIM_FENCES - 1].data_id == initial_id + 3u);
    uint64_t end;
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 0);
    {
        assert(nposts == 3 && posts[1].kind == IMM && posts[1].bytes == 512);
        assert(posts[1].immediate == 3584);
        assert(posts[2].kind == IMM && posts[2].bytes == 0 && posts[2].immediate == 64);
    }

    submit(s, shared, &q, &batch, 0x600000, 65536);
    submit(s, shared, &q, &batch, 0x700000, 64);
    wim_commit_pending(s, &batch, shared);
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
    wim_commit_pending(s, &batch, shared);
    submit(s, shared, &q, &batch, 0x900000, 128);
    wim_commit_pending(s, &batch, shared);
    assert(l->fence_count == 2);
    send_completion(s, shared, l->completion_base + l->fence[2].counter);
    assert(!l->failed && !l->fence_count);
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 69376);

    assert(l->fence_stats.samples == 4 && l->fence_stats.depth_sum == 6);
    assert(l->fence_stats.peak == 2);
    assert(l->fence_stats.histogram[0] == 2 && l->fence_stats.histogram[1] == 2);
    queue_data(s);
    l->fence_count = WIM_FENCES;
    assert(!can_admit(s, shared) && !can_admit(s, shared));
    assert(l->fence_stats.full_stalls == 2);
    s->fence_stats_enabled = 0;
    assert(!can_admit(s, shared) && l->fence_stats.full_stalls == 2);
    s->fence_stats_enabled = 1;
    l->fence_count = 0;
    l->post_index = l->completed_index + WIM_SQ;
    assert(!can_admit(s, shared) && l->fence_stats.full_stalls == 2); /* SQ stall is distinct. */
    l->post_index = l->completed_index;
    assert(peer_dpa_work_take(&s->pending) == 0);
    peer_dpa_work_pop(&s->pending, 0);
    /* The queued request was never consumed by a full fence/SQ. */
    unsigned before = writebacks;
    wim_fence_snapshot(s, shared, 0);
    assert(writebacks == before + 1);
    assert(shared->fence_snapshot.stats.samples == 4);
    assert(shared->fence_snapshot.stats.full_stalls == 2);
    assert(!l->fence_stats.samples && !l->fence_stats.full_stalls);
    peer_write_fence_stats_sample(&l->fence_stats, 7);
    assert(shared->fence_snapshot.stats.samples == 4); /* Frozen until next request. */

    /* Reject missing/duplicate logical IDs before changing the DATA prefix. */
    struct wim_request bad = {.op=WIM_DATA, .generation=1, .sequence=++sequence,
        .bytes=64, .data={.src=0xa00000, .delta=l->tx_tail+64, .key=2, .id=l->data_id+2u}};
    flags = 0;
    uint32_t previous = l->data_id;
    assert(wim_request(l, o, &bad, &flags));
    assert(l->failed && o->error && !flags && l->data_id == previous);
    peer_write_completion_free(&q);
    free(shared);
    free(s);
}

static void small_frames(void) {
    struct wim_device *s = zero_cache_aligned(sizeof(*s));
    struct wim_shared *shared = zero_cache_aligned(sizeof(*shared));
    assert(s && shared);
    struct wim_lane *l = &s->lane[0];
    l->binding = (struct wim_binding){.qp=1, .device=1, .generation=1,
        .capacity=65536};
    l->configured = 1;
    l->remote_base = 0x100000;
    l->remote_capacity = 65536;
    l->remote_key = 3;
    l->completion_base_valid = 1;
    struct peer_write_completion q = {0};
    struct wim_commit_batch batch = {0};
    assert(!peer_write_completion_init(&q, 65536));
    nposts = 0; sequence = 0;
    submit(s, shared, &q, &batch, 0x400000, 4); /* FIN / retirement frame. */
    submit(s, shared, &q, &batch, 0x400004, 5); /* Header + one byte DATA. */
    wim_commit_pending(s, &batch, shared);
    assert(l->fence_count == 1 && l->fence[0].data_id == 2);
    send_completion(s, shared, l->fence[0].counter);
    uint64_t end = 0;
    assert(!l->failed && !peer_write_completion_done(&q, shared->status[0].completed_id, &end));
    assert(end == 9);
    queue_data(s);
    assert(can_admit(s, shared) && !l->failed);
    peer_write_completion_free(&q); free(shared); free(s);
}
static void immediate_completion(struct wim_device *s, struct wim_shared *shared,
                                 uint32_t bytes, uint32_t low)
{
    assert(!completion.pending);
    completion.type = DOCA_DPA_DEV_COMP_RECV_RDMA_WRITE_IMM;
    completion.qpn = 10; completion.bytes = bytes; completion.immediate = low;
    completion.pending = 1;
    unsigned before = cq_acks;
    drain(s, shared);
    assert(!completion.pending && cq_acks == before + 1);
}

static void immediate_counters(void)
{
    struct wim_device *s = zero_cache_aligned(sizeof(*s));
    struct wim_shared *q = zero_cache_aligned(sizeof(*q));
    assert(s && q);
    struct wim_lane *l = &s->lane[0];
    l->binding = (struct wim_binding){.qp=1, .device=1, .generation=1, .capacity=65536};
    l->recv_source = 0x300000; l->recv_key = 2; l->rq_live = WIM_IMM_RQ;
    l->rx_tail = l->rx_head = UINT32_MAX - 3ull;
    unsigned before = nreceives;
    immediate_completion(s, q, 8, 4); /* Raw32 wrap; cumulative tail extends to 2^32+4. */
    assert(!l->failed && l->rx_tail == (UINT64_C(1) << 32) + 4);
    assert(q->status[0].tail == l->rx_tail && nreceives == before + 1);
    l->tx_head = UINT32_MAX - 3ull; l->tx_tail = (UINT64_C(1) << 32) + 16;
    immediate_completion(s, q, 0, 12); /* Zero-length IMM returns credit, not DATA. */
    assert(!l->failed && l->tx_head == (UINT64_C(1) << 32) + 12);
    immediate_completion(s, q, 0, 12); /* Duplicate credit is harmless. */
    assert(!l->failed && q->status[0].head == l->tx_head);
    before = nreceives;
    immediate_completion(s, q, 0, 17); /* Credit cannot exceed posted data. */
    assert(l->failed && q->status[0].error && nreceives == before);
    l->failed = q->status[0].error = 0;
    immediate_completion(s, q, 4, (uint32_t)l->rx_tail - 1u); /* Stale DATA delta. */
    assert(l->failed && q->status[0].error);
    l->failed = q->status[0].error = 0; l->busy[WIM_RECV] = 1;
    immediate_completion(s, q, 4, (uint32_t)l->rx_tail + 4u); /* DATA before HELLO. */
    assert(l->failed && q->status[0].error);
    free(q); free(s);
}

static void wake_epoch(void)
{
    struct wim_device *s = zero_cache_aligned(sizeof(*s));
    struct wim_shared *q = zero_cache_aligned(sizeof(*q));
    assert(s && q);
    s->buffers = (uintptr_t)q;
    struct peer_dpa_thread_arg a = {.dpa_producer=1, .dpa_producer_comp=1, .rdma_states={1}};
    notify_shared = q;
    peer_write_imm_dpa_drain(&a, 0, (uintptr_t)s, 1);
    assert(!messages && q->publication.sequence == 1); /* No epoch armed yet. */
    publication_at_send = 1;
    q->wait.seen = 0; q->wait.epoch = 1;
    consumer_empty = 1;
    peer_write_imm_dpa_drain(&a, 0, (uintptr_t)s, 1);
    assert(!messages && !s->notified_epoch); /* No MsgQ credits: retain wake. */
    consumer_empty = 0;
    peer_write_imm_dpa_drain(&a, 0, (uintptr_t)s, 1);
    assert(messages == 1 && s->notified_epoch == 1); /* Idle retry sends it. */
    peer_write_imm_dpa_drain(&a, 0, (uintptr_t)s, 1);
    assert(messages == 1); /* One wake per epoch. */
    q->wait.seen = 1; q->wait.epoch = 2;
    peer_write_imm_dpa_drain(&a, 0, (uintptr_t)s, 1);
    assert(messages == 1); /* Already scanned status needs no wake. */
    q->control[0] = (struct wim_request){.ticket=1, .sequence=7, .op=WIM_STOP};
    unsigned wb = writebacks;
    assert(peer_write_imm_dpa_drain(&a, 0, (uintptr_t)s, 1) == 1);
    assert(!a.rdma_states[0] && q->stopped == 7 && writebacks == wb + 1 && messages == 1);
    free(q); free(s); notify_shared = NULL;
}

int main(void) {
    immediate_counters(); wake_epoch();
    bootstrap(64, 1);
    bootstrap(4096, 0);
    run(0);
    run(UINT32_MAX - 1);
    small_frames();
    puts("peer_write_completion_dpa_test: ok");
    return 0;
}
