/* Shared D2D layouts and the actual peer-node allocator, with local identity
 * callbacks. No device is opened; production structs and live lists are used. */
#include "src/transport/peer/peer_pair_transport.c"
#include "device/peer_write_imm_dpa_internal.h"
#include <assert.h>

static const struct dmesh_peer_registration registration = {
    .daemon = {1}, .nonce = {1}, .generation = 1};
static const uint8_t peer_key_bytes[32] = {1};

static int node_binding(void *ctx, const char *node, const uint8_t **key,
                        uint32_t *ip, uint16_t *port)
{
    (void)ctx; (void)node;
    *key = peer_key_bytes;
    *ip = 0x1234;
    *port = 4790;
    return 1;
}

static int pod_on_node(void *ctx, const char *uid, const char *node)
{
    (void)ctx;
    return !strcmp(uid, "local") ? !strcmp(node, "local-node") : strcmp(node, "local-node") != 0;
}

static int local_registration(void *ctx, const char *uid, struct dmesh_peer_registration *out)
{
    (void)ctx;
    assert(!strcmp(uid, "local"));
    *out = registration;
    return 1;
}

static const struct dmesh_peer_ops identity_ops = {
    .node_binding = node_binding, .pod_on_node = pod_on_node,
    .local_registration = local_registration};
static uint64_t now_ns(void *ctx) { (void)ctx; return 123; }

static void layout(void)
{
    assert(WIM_LANES == PEER_RDMA_CLUSTER_NODES - 1);
    assert(PEER_DPA_WORK_LANES == WIM_LANES);
    assert(sizeof(((struct wim_device *)0)->lane) == WIM_LANES * sizeof(struct wim_lane));
    assert(sizeof(((struct wim_shared *)0)->status) == WIM_LANES * sizeof(struct wim_status));
    assert(sizeof(((struct peer_dpa_work *)0)->head) == WIM_LANES * sizeof(uint16_t));
    assert(WIM_IMM_CQ >= WIM_LANES * (WIM_FENCES + WIM_CONTROLS + WIM_IMM_RQ));
    assert(!(WIM_QPN_BUCKETS & (WIM_QPN_BUCKETS - 1)));
    assert(!(WIM_IMM_CQ & (WIM_IMM_CQ - 1)));
    assert(sizeof(struct wim_device) % 64 == 0 && sizeof(struct wim_shared) % 64 == 0);
    struct dpa_rdma_counter_msg msg = {.type=DPA_MSG_RDMA_COUNTER,
        .version=DPA_RDMA_NOTIFY_VERSION, .cookie=1, .generation=1,
        .lane=WIM_LANES-1};
    assert(dpa_rdma_counter_valid(&msg, sizeof(msg), 0, 1, 1));
    msg.lane = WIM_LANES;
    assert(!dpa_rdma_counter_valid(&msg, sizeof(msg), 0, 1, 1));
    printf("nodes=%u workers=%u lanes=%u device=%zu shared=%zu pending=%zu imm_cq=%u\n",
           PEER_RDMA_CLUSTER_NODES, PEER_RDMA_MAX_WORKERS, WIM_LANES,
           sizeof(struct wim_device), sizeof(struct wim_shared), sizeof(struct peer_dpa_work),
           WIM_IMM_CQ);
}
static void admission(void)
{
    struct dmesh_peer_table *table = calloc(1, sizeof(*table));
    assert(table);
    table->ops = &identity_ops;
    strcpy(table->node_name, "local-node");
    struct peer_pair_transport rt = {.cfg={.connections=PEER_RDMA_LANES,
        .qps_per_peer_node=PEER_RDMA_QPS_PER_PEER, .now_ns=now_ns}, .id=7, .table=table};
    rt.conns = calloc(rt.cfg.connections, sizeof(*rt.conns));
    assert(rt.conns);
    struct peer_pair_offer o = {.node="peer-0", .incarnation=1,
        .key={1}, .ip_be=0x1234, .port=4790,
        .pair={.local_uid="local", .remote_uid="remote", .local=registration}};
    struct pair_conn *first = allocate(&rt, &o, 0);
    assert(first && first->created == 123);
    assert(!allocate(&rt, &o, 1)); /* Same pair cannot reserve a second slot. */
    strcpy(o.pair.remote_uid, "other-pod");
    assert(!allocate(&rt, &o, 1)); /* A second Pod pair on the same peer is rejected. */
    assert(rt.generation == 1 && rt.live.count == 1);
    for (unsigned i = 1; i < PEER_RDMA_LANES; i++) {
        snprintf(o.node, sizeof(o.node), "peer-%u", i);
        assert(allocate(&rt, &o, i & 1)); /* Both incoming and outgoing consume quota. */
    }
    strcpy(o.node, "overflow");
    assert(!allocate(&rt, &o, 0));
    strcpy(o.node, "peer-0");
    assert(!allocate(&rt, &o, 1)); /* Retained used slots continue to reserve the peer. */
    dmesh_live_remove(&rt.live, first->ref.slot);
    first->used = 0; /* Only retirement/FORGET permits reuse in production. */
    struct pair_conn *replacement = allocate(&rt, &o, 1);
    assert(replacement == first && replacement->incoming);
    assert(replacement->ref.generation == PEER_RDMA_LANES + 1);
    free(rt.conns);
    free(table);
}
int main(void)
{
    layout();
    admission();
    puts("peer_rdma_layout_test: ok");
    return 0;
}
