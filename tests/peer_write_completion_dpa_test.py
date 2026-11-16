#!/usr/bin/env python3
"""Exercise production WRITE coalescing/commit/CQE code with delayed CQEs."""
from pathlib import Path
import os
import shlex
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
source = (root / "src/transport/peer/device/peer_write_sync_dpa.c").read_text()
definitions = source[source.index("#define WSD_REQUEST_BATCH"):source.index("/* GET mode")]
request = source[source.index("static int\nwsd_request("):source.index("\nunsigned\npeer_write_sync_dpa_drain(")]
completion = source[source.index("                    o->send_cqes++;"):
                    source.index("                } else\n                    wsd_fault(l, o);", source.index("                    o->send_cqes++;"))]
recv_begin = source.index("                    uint32_t bytes = doca_dpa_dev_completion_element_get_received_bytes(e);",
                          source.index("} else if (type == DOCA_DPA_DEV_COMP_RECV_SEND"))
recv_completion = source[recv_begin:source.index("                } else if (type == DOCA_DPA_DEV_COMP_SEND)", recv_begin)]
admission = source[source.index("            /* A stalled QP"):
                   source.index("            unsigned commit = 0;", source.index("            /* A stalled QP"))]

prefix = r'''
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "peer_write_completion.h"
#include "peer_write_sync_dpa_shared.h"
#define DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH 1u
#define DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS 2u
enum { WRITE, IMM, SIGNAL, SEND };
static struct { unsigned kind; uint32_t bytes, immediate; } posts[32];
static unsigned nposts;
static unsigned nreceives, receive_bytes, receive_acks;
static unsigned writebacks;
static void __dpa_thread_window_writeback(void) { writebacks++; }
static void doca_dpa_dev_device_set(uint64_t dev) { (void)dev; }
static void record(unsigned kind, uint32_t bytes, uint32_t immediate) {
    assert(nposts < 32);
    posts[nposts].kind = kind;
    posts[nposts].bytes = bytes;
    posts[nposts++].immediate = immediate;
}
static void doca_dpa_dev_rdma_post_write(uint64_t qp, uint32_t connection,
    uint32_t rkey, uint64_t remote, uint32_t key, uint64_t src, uint32_t bytes, unsigned flags) {
    (void)qp; (void)connection; (void)rkey; (void)remote; (void)key; (void)src; (void)flags;
    record(WRITE, bytes, 0);
}
static void doca_dpa_dev_rdma_post_write_imm(uint64_t qp, uint32_t connection,
    uint32_t rkey, uint64_t remote, uint32_t key, uint64_t src, uint32_t bytes,
    uint32_t immediate, unsigned flags) {
    (void)qp; (void)connection; (void)rkey; (void)remote; (void)key; (void)src; (void)flags;
    record(IMM, bytes, immediate);
}
static void doca_dpa_dev_rdma_signal_set(uint64_t qp, uint32_t connection,
    uint64_t remote, uint64_t value) {
    (void)qp; (void)connection; (void)remote;
    record(SIGNAL, 0, value);
}
static void doca_dpa_dev_rdma_post_receive(uint64_t qp, uint32_t key, uint64_t src, uint32_t bytes) {
    (void)qp; (void)key; (void)src; (void)bytes;
    nreceives++; receive_bytes += bytes;
}
static uint32_t doca_dpa_dev_completion_element_get_received_bytes(const uint32_t *e) { return *e; }
static void doca_dpa_dev_rdma_receive_ack(uint64_t qp, unsigned count) {
    (void)qp; receive_acks += count;
}
static void doca_dpa_dev_rdma_post_send(uint64_t qp, uint32_t connection,
    uint32_t key, uint64_t src, uint32_t bytes, unsigned flags) {
    (void)qp; (void)connection; (void)key; (void)src; (void)flags;
    record(SEND, bytes, 0);
}
'''

tests = r'''
static void bootstrap(unsigned mode, uint32_t bytes, int valid) {
    struct wsd_device *s = calloc(1, sizeof(*s));
    struct wsd_status o = {0};
    assert(s);
    struct wsd_lane *l = &s->lane[0];
    l->binding = (struct wsd_binding){.qp=1, .device=1, .generation=1, .mode=mode};
    nposts = nreceives = receive_bytes = receive_acks = 0;
    struct wsd_request r = {.op=WSD_RECEIVE, .generation=1, .sequence=1,
        .bytes=bytes, .data={.src=0x400000, .key=2}};
    unsigned flags = 0;
    assert(wsd_request(l, &o, &r, &flags));
    if (!valid) {
        assert(l->failed && o.error && !nreceives);
        l->failed = 0; o.error = 0;
        r.op = WSD_SEND_BOOT;
        assert(wsd_request(l, &o, &r, &flags));
        assert(l->failed && o.error && !nposts);
        free(s); return;
    }
    assert(!l->failed && l->busy[WSD_RECV]);
    assert(nreceives == (mode == WSD_IMMEDIATE ? WSD_IMM_RQ : 1));
    assert(receive_bytes == bytes); /* Remaining IMM receives have no payload buffer. */
    receive_completion(s, l, &o, bytes);
    assert(!l->failed && !l->busy[WSD_RECV] && receive_acks == 1);
    assert(o.done[WSD_RECV].id == 1 && o.done[WSD_RECV].bytes == bytes);
    assert(receive_bytes == bytes); /* Reposted notification also has zero bytes. */
    r.op = WSD_SEND_BOOT; r.sequence = 2;
    assert(wsd_request(l, &o, &r, &flags));
    assert(!l->failed && nposts == 1 && posts[0].kind == SEND && posts[0].bytes == bytes);
    /* The CQ length must match the mode before publishing completion. */
    l->busy[WSD_RECV] = 1; l->id[WSD_RECV] = 3;
    receive_completion(s, l, &o, mode == WSD_IMMEDIATE ? 4096 : 64);
    assert(l->failed && o.error && o.done[WSD_RECV].id == 1);
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

    send_completion(l, o, l->completion_base + first_counter);
    assert(!l->failed && l->fence_count == 1 && l->fence_first == 0);
    assert(o->completed_id == initial_id + 3u);
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 3584);
    send_completion(l, o, l->completion_base + last_counter);
    assert(!l->failed && !l->fence_count && l->fence_first == 1);
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 69184);

    /* One later CQE can retire multiple batch fences. */
    submit(s, shared, &q, &batch, 0x800000, 64);
    wsd_commit_pending(s, &batch, shared);
    submit(s, shared, &q, &batch, 0x900000, 128);
    wsd_commit_pending(s, &batch, shared);
    assert(l->fence_count == 2);
    send_completion(l, o, l->completion_base + l->fence[2].counter);
    assert(!l->failed && !l->fence_count);
    assert(!peer_write_completion_done(&q, o->completed_id, &end) && end == 69376);

    assert(l->fence_stats.samples == 4 && l->fence_stats.depth_sum == 6);
    assert(l->fence_stats.peak == 2);
    assert(l->fence_stats.histogram[0] == 2 && l->fence_stats.histogram[1] == 2);
    assert(can_admit(s, l));
    l->fence_count = WSD_FENCES;
    assert(!can_admit(s, l) && !can_admit(s, l));
    assert(l->fence_stats.full_stalls == 2);
    s->fence_stats_enabled = 0;
    assert(!can_admit(s, l) && l->fence_stats.full_stalls == 2);
    s->fence_stats_enabled = 1;
    l->fence_count = 0;
    l->post_index = l->completed_index + WSD_SQ;
    assert(!can_admit(s, l) && l->fence_stats.full_stalls == 2); /* SQ stall is distinct. */
    l->post_index = l->completed_index;
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
    send_completion(l, &shared->status[0], l->fence[0].counter);
    uint64_t end = 0;
    assert(!l->failed && !peer_write_completion_done(&q, shared->status[0].completed_id, &end));
    assert(end == 9);
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
'''

with tempfile.TemporaryDirectory(prefix="dpumesh-write-completion-") as tmp:
    path = Path(tmp)
    harness = path / "test.c"
    harness.write_text(prefix + definitions + request +
                       "\nstatic void receive_completion(struct wsd_device *s, struct wsd_lane *l, struct wsd_status *o, uint32_t received) {\nconst uint32_t *e = &received;\n" +
                       recv_completion + "\n}\n" +
                       "\nstatic void send_completion(struct wsd_lane *l, struct wsd_status *o, uint32_t completed) {\n" +
                       completion + "\n}\n" +
                       "static int can_admit(struct wsd_device *s, struct wsd_lane *l) { do {\n" +
                       admission + "return 1; } while (0); return 0; }\n" + tests)
    cc = shlex.split(os.environ.get("CC", "cc"))
    flags = shlex.split(os.environ.get("WRITE_COMPLETION_TEST_CFLAGS", ""))
    executable = path / "test"
    subprocess.run(cc + ["-std=gnu11", "-O2", "-g", "-Wall", "-Wextra", "-Werror",
                         "-I" + str(root / "src/transport/peer"),
                         "-I/opt/mellanox/doca/include"] + flags +
                   [str(harness), "-o", str(executable)], check=True)
    subprocess.run([str(executable)], check=True)
