#ifndef DMESH_PEER_CONFIG_H
#define DMESH_PEER_CONFIG_H

#include <stddef.h>

/* Peer build-time configuration. No SDK or OS dependencies: paired-host,
 * ARM and DPA code share these definitions. Rebuild ARM and DPA together
 * after changing resource budgets that size their shared layouts. */

/* ==================== Common: identity and RDMA ==================== */

/* Paired-host registration and peer security bind the same complete values. */
#define DMESH_REG_NONCE_SIZE 32u
#define DMESH_POD_UID_MAX 64u
#define DMESH_K8S_NAMESPACE_MAX 64u
#define DMESH_K8S_NAME_MAX 254u
#define DMESH_SVC_NAME_MAX 64u
#define DMESH_DAEMON_INCARNATION_SIZE 16u

/* Cluster size includes this node. Each worker owns one RDMA engine with
 * one QP slot per remote node. The current authenticated transport admits
 * one pair per peer; these budgets do not implement Pod-pair multiplexing.
 * Host H2D DMA thread/flow capacities are independent. */
#ifndef PEER_RDMA_CLUSTER_NODES
#define PEER_RDMA_CLUSTER_NODES 4u
#endif
#ifndef PEER_RDMA_MAX_WORKERS
#define PEER_RDMA_MAX_WORKERS 16u
#endif
#define PEER_RDMA_QPS_PER_PEER 1u
#define PEER_RDMA_PEERS (PEER_RDMA_CLUSTER_NODES - 1u)
#define PEER_RDMA_LANES (PEER_RDMA_PEERS * PEER_RDMA_QPS_PER_PEER)
_Static_assert(PEER_RDMA_CLUSTER_NODES >= 2 && PEER_RDMA_CLUSTER_NODES <= 129,
               "D2D cluster budget must contain 2..129 nodes");
_Static_assert(PEER_RDMA_MAX_WORKERS > 0 && PEER_RDMA_MAX_WORKERS <= 16,
               "D2D worker budget must fit the native runtime's 16 workers");

/* Constant-expression power-of-two rounding, clamped to [2, 65536].
 * Guard small inputs so __builtin_clz never receives zero. */
#define PEER_RDMA_QUEUE_DEPTH(n) \
    ((n) <= 2u ? 2u : (n) >= 65536u ? 65536u : \
     (1u << (32u - __builtin_clz((unsigned)(n) - 1u))))
#define PEER_RDMA_QPN_BUCKETS PEER_RDMA_QUEUE_DEPTH(2u * PEER_RDMA_LANES)

/* One descriptor FIFO per ARM/DPA context, shared by its QPs. Match the
 * native Host forward ring (common/ring.h); independent of NIC queue depths. */
#define PEER_RDMA_DESC_RING_CAP 1024u

/* DPA-owned WRITE_WITH_IMM notification receive queue. Payloads land in
 * registered byte rings; only the protected bootstrap uses a receive buffer. */
#define PEER_IMM_RECV_SLOTS 32u
#define PEER_RDMA_CQ_BATCH 32

/* Encoded IB timer/retry values, not durations in milliseconds. RNR retry 7
 * keeps retrying while the receiver is under pressure; upper layers time out. */
#define PEER_RDMA_MIN_RNR_TIMER 1u

/* Native WRITE uses independent contiguous byte rings on ARM and DPA. */
#define PEER_WRITE_RING_DEFAULT (2u * 1024u * 1024u)
#define PEER_WRITE_RING_MAX (16u * 1024u * 1024u)
#define PEER_WRITE_BUDGET 128u
#define PEER_WRITE_BOOTSTRAP_TIMEOUT_NS 5000000000ull

/* ==================== ARM: control and connection limits ==================== */

/* A generation exceeding any bound is refused, never truncated. The byte
 * bound applies to one complete topology document. */
#define DMESH_GEN_POD_MAX 65536u
#define DMESH_GEN_NODE_MAX 1024u
#define DMESH_GEN_SERVICE_MAX 4096u
#define DMESH_GEN_ENDPOINT_MAX 65536u
#define DMESH_TOPOLOGY_MAX_BYTES (16u * 1024u * 1024u)
/* Node-local compact Service IDs ([0, 127]). */
#define DMESH_TOPOLOGY_INTERN_MAX 128
#define DMESH_ASSERT_CLOCK_SKEW_SEC 30u
#define DMESH_ASSERT_MAX_LIFETIME_SEC 300u

/* A channel evicted and reopened between generations pays authentication
 * twice; consider the generation interval when changing idle eviction. */
#define DMESH_CHANNEL_IDLE_NS (60ull * 1000000000ull)
#define DMESH_CHANNEL_MAX 256u
#define DMESH_PEER_CONNECTIONS_MAX 4095u
#define DMESH_PEER_STAGING_MAX (16u * 1024u * 1024u)
#define DMESH_PEER_OPEN_RATE 1000u
#define DMESH_PEER_TX_INFLIGHT_MAX (16u * 1024u * 1024u)
#define DMESH_CONNECTION_ACK_BATCH 64u
/* One un-ACKed slot per extent; the slot bound also limits tiny extents. */
#define DMESH_PEER_TX_SLOTS 8192u
#define DMESH_PEER_SLOT_SET_CAP DMESH_PEER_TX_SLOTS
#define DMESH_LIVE_SLOTS_MAX 256u

#define PEER_ASSOC_LANES_MAX 16u
#define PEER_MANAGER_SESSIONS_MAX 64u
#define PEER_MANAGER_NOTICE_CAP 64u
#define PEER_CONTROL_SESSION_QUEUE_CAP 8u
#define PEER_PAIR_MAILBOX_CAP 64u
#define PEER_CONNECTION_CAP 64u
#define PEER_CONNECTION_INDEX_CAP 256u
#define PEER_CONNECTION_OPEN_TIMEOUT_NS (30ull * 1000000000ull)
/* TX is an admission budget, not a separately allocated staging ring.
 * RX is bounded until the local reader consumes and acknowledges bytes. */
#define PEER_CONNECTION_TX_BYTES (2u * 1024u * 1024u)
#define PEER_CONNECTION_RX_BYTES DMESH_PEER_STAGING_MAX

/* Proxy arena geometry; separate from native WRITE byte rings. */
#define PEER_ARENA_CHUNK_BYTES (64u * 1024u)
#define PEER_ARENA_CHUNKS 1024u
#define PEER_ARENA_ALIGN 128u
#define PEER_ARENA_HEADROOM 64u
#define PEER_ARENA_BYTES ((size_t)PEER_ARENA_CHUNKS * PEER_ARENA_CHUNK_BYTES)

/* Control TCP includes inbound connections still proving their identity. */
#define PEER_TCP_CONN_MAX 512u
#define PEER_TCP_EVENTS 64
#define PEER_CRYPTO_IPC_INFLIGHT_MAX 256u
#define PEER_CRYPTO_IPC_TX_FRAMES 8u
#define PEER_CRYPTO_IPC_RECONNECT_NS 1000000000ull
#define PEER_CRYPTO_OWNER_ASSOC_MAX 256u
#define PEER_CRYPTO_OWNER_PEERS_MAX 64u
#define PEER_CRYPTO_OWNER_SA_DEFAULT 4096u
#define PEER_CRYPTO_OWNER_PROCESS_TIMEOUT_US 10000
#define PEER_CRYPTO_OWNER_RX_FRAMES 4

/* Bounded ARM diagnostic storage. */
#define PEER_PROFILE_TRACE_CAP 256u
#define PEER_PROFILE_SAMPLE_CAP 32u

/* ==================== DPA: resources and scheduling ==================== */

#define PEER_MAX_DPA_EU 64u
#define DPA_RDMA_CONTEXTS 16u
#define PEER_DPA_NOTIFY_CREDITS 128u
#define PEER_DPA_WORK_CAP PEER_RDMA_DESC_RING_CAP
#define PEER_DPA_WORK_LANES PEER_RDMA_LANES

/* Context request budget and per-QP visit bound.
 * A pass can touch at most REQUEST_BATCH QPs; no deferred NIC commit survives
 * the pass. Pending requests live in a context-local bounded work pool. */
#define PEER_DPA_REQUEST_BATCH 32u
#define PEER_DPA_QP_VISIT 32u

/* WRITE queue capacities, separate from frame lifetime. Each fence records
 * a signalled batch. Preserve the per-QP CQ budgets and round for lanes. */
#define WIM_LANES PEER_RDMA_LANES
#define WIM_QPN_BUCKETS PEER_RDMA_QPN_BUCKETS
#define WIM_RING PEER_RDMA_DESC_RING_CAP
#define WIM_SQ 4096u
#define WIM_FENCES PEER_IMM_RECV_SLOTS
#define WIM_IMM_RQ PEER_IMM_RECV_SLOTS
#define WIM_IMM_CQ PEER_RDMA_QUEUE_DEPTH(WIM_LANES * 512u)

#endif
