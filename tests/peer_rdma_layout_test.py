#!/usr/bin/env python3
"""Compile shared D2D layouts and exercise production peer-node admission.

Only identity/clock/live-list dependencies are mocked. The allocator is the
production function, including duplicate-pair and occupied-slot handling.
No device is opened; this does not validate node-pool multiplexing.
"""
from pathlib import Path
import os
import shlex
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
peer = root / "src/transport/peer"
source = (peer / "peer_pair_transport.c").read_text()
allocate = source[source.index("static struct pair_conn *\nallocate("):
                  source.index("\nstatic void locate_channel(")]

prefix = r'''
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "peer_write_sync_dpa_shared.h"
struct dmesh_peer_pair { char local_uid[32], remote_uid[32]; };
struct peer_pair_offer {
    char node[32];
    uint32_t incarnation;
    struct dmesh_peer_pair pair;
};
struct peer_pair_ref { uint64_t runtime, generation; uint32_t slot; };
struct peer_pair_transport;
struct pair_conn {
    struct peer_pair_transport *rt;
    int used, incoming;
    struct peer_pair_offer offer;
    struct peer_pair_ref ref;
    uint64_t created;
};
struct peer_pair_transport {
    struct {
        unsigned connections, qps_per_peer_node;
        uint64_t (*now_ns)(void *);
        void *now_ctx;
    } cfg;
    struct pair_conn *conns;
    uint64_t generation, id;
    unsigned live;
};
static int identity_valid(struct peer_pair_transport *rt, const struct peer_pair_offer *o)
{ (void)rt; return o->pair.local_uid[0] && o->pair.remote_uid[0]; }
static void dmesh_live_add(unsigned *live, unsigned slot) { (void)slot; ++*live; }
static uint64_t now_ns(void *ctx) { (void)ctx; return 123; }
'''

tests = r'''
static void layout(void)
{
    assert(WSD_LANES == PEER_RDMA_CLUSTER_NODES - 1);
    assert(PEER_DPA_LANES == WSD_LANES && PEER_DPA_WORK_LANES == WSD_LANES);
    assert(sizeof(((struct wsd_device *)0)->lane) == WSD_LANES * sizeof(struct wsd_lane));
    assert(sizeof(((struct wsd_shared *)0)->status) == WSD_LANES * sizeof(struct wsd_status));
    assert(sizeof(((struct peer_dpa_device_state *)0)->lanes) ==
           WSD_LANES * sizeof(struct peer_dpa_lane_state));
    assert(sizeof(((struct peer_dpa_work *)0)->head) == WSD_LANES * sizeof(uint16_t));
    assert(WSD_CQ >= WSD_LANES * (WSD_FENCES + WSD_CONTROLS));
    assert(WSD_IMM_CQ >= WSD_LANES * (WSD_FENCES + WSD_CONTROLS + WSD_IMM_RQ));
    assert(WSD_EVENT_CQ >= WSD_LANES * 2);
    assert(PEER_DPA_CQ_SLOTS >= PEER_DPA_LANES * PEER_RDMA_CQ_SLOTS);
    assert(!(PEER_DPA_CQ_SLOTS & (PEER_DPA_CQ_SLOTS - 1)));
    assert(!(WSD_QPN_BUCKETS & (WSD_QPN_BUCKETS - 1)));
    assert(!(WSD_CQ & (WSD_CQ - 1)) && !(WSD_IMM_CQ & (WSD_IMM_CQ - 1)));
    assert(sizeof(struct wsd_device) % 64 == 0 && sizeof(struct wsd_shared) % 64 == 0);
    struct dpa_rdma_counter_msg msg = {.type=DPA_MSG_RDMA_COUNTER,
        .version=DPA_RDMA_NOTIFY_VERSION, .cookie=1, .generation=1,
        .lane=WSD_LANES-1};
    assert(dpa_rdma_counter_valid(&msg, sizeof(msg), 0, 1, 1));
    msg.lane = WSD_LANES;
    assert(!dpa_rdma_counter_valid(&msg, sizeof(msg), 0, 1, 1));
    printf("nodes=%u workers=%u lanes=%u device=%zu shared=%zu pending=%zu cq=%u imm_cq=%u\n",
           PEER_RDMA_CLUSTER_NODES, PEER_RDMA_MAX_WORKERS, WSD_LANES,
           sizeof(struct wsd_device), sizeof(struct wsd_shared), sizeof(struct peer_dpa_work),
           WSD_CQ, WSD_IMM_CQ);
}
static void admission(void)
{
    struct peer_pair_transport rt = {.cfg={.connections=PEER_RDMA_LANES,
        .qps_per_peer_node=PEER_RDMA_QPS_PER_PEER, .now_ns=now_ns}, .id=7};
    rt.conns = calloc(rt.cfg.connections, sizeof(*rt.conns));
    assert(rt.conns);
    struct peer_pair_offer o = {.node="peer-0", .incarnation=1,
        .pair={.local_uid="local", .remote_uid="remote"}};
    struct pair_conn *first = allocate(&rt, &o, 0);
    assert(first && first->created == 123);
    assert(!allocate(&rt, &o, 1)); /* Same pair cannot reserve a second slot. */
    strcpy(o.pair.remote_uid, "other-pod");
    assert(!allocate(&rt, &o, 1)); /* A second Pod pair on the same peer is rejected. */
    assert(rt.generation == 1 && rt.live == 1);
    for (unsigned i = 1; i < PEER_RDMA_LANES; i++) {
        snprintf(o.node, sizeof(o.node), "peer-%u", i);
        assert(allocate(&rt, &o, i & 1)); /* Both incoming and outgoing consume quota. */
    }
    strcpy(o.node, "overflow");
    assert(!allocate(&rt, &o, 0));
    strcpy(o.node, "peer-0");
    assert(!allocate(&rt, &o, 1)); /* Retained used slots continue to reserve the peer. */
    first->used = 0; /* Only retirement/FORGET permits reuse in production. */
    struct pair_conn *replacement = allocate(&rt, &o, 1);
    assert(replacement == first && replacement->incoming);
    assert(replacement->ref.generation == PEER_RDMA_LANES + 1);
    if (PEER_RDMA_LANES >= 2) {
        memset(rt.conns, 0, rt.cfg.connections * sizeof(*rt.conns));
        rt.cfg.qps_per_peer_node = 0; /* Explicit legacy callers retain pair admission. */
        assert(allocate(&rt, &o, 0));
        strcpy(o.pair.remote_uid, "third-pod");
        assert(allocate(&rt, &o, 1));
    }
    free(rt.conns);
}
int main(void) { layout(); admission(); return 0; }
'''

cc = shlex.split(os.environ.get("CC", "cc"))
with tempfile.TemporaryDirectory(prefix="peer-rdma-layout-") as directory:
    path = Path(directory)
    unit = path / "test.c"
    unit.write_text(prefix + allocate + tests)
    for nodes, workers in ((None, None), (2, 1), (8, 8), (129, 16)):
        output = path / f"test-{nodes}-{workers}"
        config = ([] if nodes is None else
                  [f"-DPEER_RDMA_CLUSTER_NODES={nodes}u", f"-DPEER_RDMA_MAX_WORKERS={workers}u"])
        subprocess.run(cc + ["-std=gnu11", "-Wall", "-Wextra", "-Werror",
                            "-I" + str(peer), "-I/opt/mellanox/doca/include"] +
                       config + [str(unit), "-o", str(output)],
                       check=True)
        subprocess.run([str(output)], check=True)
    for macro in ("PEER_RDMA_CLUSTER_NODES=1u", "PEER_RDMA_CLUSTER_NODES=130u",
                  "PEER_RDMA_MAX_WORKERS=0u", "PEER_RDMA_MAX_WORKERS=17u"):
        bad = subprocess.run(cc + ["-std=gnu11", "-I" + str(peer), "-D" + macro,
                                  "-x", "c", "-fsyntax-only", "-"],
                             input='#include "peer_rdma_limits.h"\n', text=True,
                             capture_output=True)
        assert bad.returncode, f"invalid configuration accepted: {macro}"
print("peer_rdma_layout_test: ok")
