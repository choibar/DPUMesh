#ifndef DMESH_PEER_RDMA_LIMITS_H
#define DMESH_PEER_RDMA_LIMITS_H
#include <stddef.h>
#include "peer_data_wire.h"
/* D2D resource budget. Rebuild ARM and DPA together after changing these
 * values. Cluster size includes this node; each worker owns one RDMA engine
 * with one QP slot per remote node. These limits do not implement Pod-pair
 * multiplexing: the current authenticated transport admits one pair per peer.
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
 * native DPUMesh Host forward ring (src/transport/common/ring.h). This is
 * submission capacity, independent of NIC SQ/RQ depths or payload bytes. */
#define PEER_RDMA_DESC_RING_CAP 1024u

/* Descriptor-DPA depths are part of the shared host/device layout. */
#define PEER_RDMA_SEND_SLOTS 128u
#define PEER_RDMA_RECV_SLOTS 128u
/* Notification RQ depth shared by ARM and native DPA WRITE_WITH_IMM.
 * Independent of SEND payload receive slots and application concurrency. */
#define PEER_IMM_RECV_SLOTS 32u
#define PEER_VERBS_RECV_SLOTS PEER_IMM_RECV_SLOTS

/* Encoded IB minimum RNR NAK timer, not a retry count or a duration in ms. */
#define PEER_RDMA_MIN_RNR_TIMER 1u

/* Maximum CQEs per ARM poll or per DPA lane visit (subject to work budget). */
#define PEER_RDMA_CQ_BATCH 32
#define PEER_RDMA_CQ_SLOTS 512u /* keep CQ capacity unchanged in the slot-depth comparison */
_Static_assert(PEER_RDMA_CQ_SLOTS >= PEER_RDMA_SEND_SLOTS + PEER_RDMA_RECV_SLOTS,
               "CQ must cover all outstanding SEND and RECV completions per lane");
/* Native SEND has 128 TX and 128 RX slots, each64KiB (8MiB TX /8MiB RX).
 * Both slot sizes include the four-byte dmesh4 header. External arena frames
 * retain their independent 64KiB carrier bound. */
#define PEER_RDMA_TX_SLOT_BYTES (64u * 1024u)
#define PEER_RDMA_RX_SLOT_BYTES (64u * 1024u)
#define PEER_RDMA_RX_BUFFER_BYTES ((size_t)PEER_RDMA_RECV_SLOTS * PEER_RDMA_RX_SLOT_BYTES)
#define PEER_RDMA_TX_BUFFER_BYTES ((size_t)PEER_RDMA_SEND_SLOTS * PEER_RDMA_TX_SLOT_BYTES)
/* Native SEND benchmark borrows payloads from its packed byte ring. Keep only
 * one header of reservation scratch per SQ entry, not a 64KiB payload slot.
 * Production/E2E builds leave this opt-in disabled. Device SEND_FRAME carries
 * its source address explicitly, so the device-side legacy SEND stride stays unchanged. */
#ifndef DMESH_NATIVE_PACKED_SEND_TX
#define DMESH_NATIVE_PACKED_SEND_TX 0
#endif
#define PEER_RDMA_TX_SCRATCH_BYTES (DMESH_NATIVE_PACKED_SEND_TX ? 64u : PEER_RDMA_TX_SLOT_BYTES)
#define PEER_RDMA_TX_SCRATCH_BUFFER_BYTES                                                          \
    ((size_t)PEER_RDMA_SEND_SLOTS * PEER_RDMA_TX_SCRATCH_BYTES)
#define PEER_RDMA_TX_BITMAP_WORDS ((PEER_RDMA_SEND_SLOTS + 63u) / 64u)
#define PEER_RDMA_SLOT_BYTES (64u * 1024u)
/* Controlled DPA experiment: SEND/RECV and WRITE+atomic commit cadence.
 * Production defaults to batching. This does not change the shared ABI. */
#ifndef DMESH_DPA_COMMIT_BATCH
#define DMESH_DPA_COMMIT_BATCH 1
#endif
#if DMESH_DPA_COMMIT_BATCH != 0 && DMESH_DPA_COMMIT_BATCH != 1
#error "DMESH_DPA_COMMIT_BATCH must be 0 or 1"
#endif
#endif
