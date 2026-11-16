#ifndef DMESH_PEER_DPA_ABI_H
#define DMESH_PEER_DPA_ABI_H

#include <stdint.h>
#include <stddef.h>
#include <doca_mmap.h>
#include "peer_rdma_limits.h"

/* Private ARM/RDMA-DPA ABI imported from jukebox. Never use this descriptor
 * for DPUMesh Host DMA: common/ring.h owns that independent wire contract. */
#define PEER_MAX_DPA_EU 64u
#define DPA_RDMA_CONTEXTS 16u
#define DPA_MSG_RDMA_READY 7u
#define DPA_MSG_RDMA_COUNTER 8u

struct peer_dpa_thread_arg {
    uint64_t dpa_producer_comp, dpa_producer;
    uint64_t yield_notification;
    uint32_t dpu_consumer_id, eu_index, producer_deferred, reserved;
    uint64_t rdma_state, rdma_states[DPA_RDMA_CONTEXTS];
    uint32_t rdma_count, rdma_cursor;
} __attribute__((aligned(8)));

#define DPA_RDMA_NOTIFY_VERSION 2u
struct dpa_rdma_notify_msg {
    uint8_t type, version, eu_index, worker;
};
_Static_assert(sizeof(struct dpa_rdma_notify_msg) == 4, "RDMA MsgQ notification ABI");
static inline int dpa_rdma_notify_valid(const struct dpa_rdma_notify_msg *m,
    unsigned bytes, unsigned worker, unsigned workers, unsigned threads)
{
    return bytes == sizeof(*m) && m->type == DPA_MSG_RDMA_READY &&
        m->version == DPA_RDMA_NOTIFY_VERSION && m->worker < workers &&
        workers && worker < workers && m->eu_index < threads &&
        m->eu_index % workers == worker;
}

/* Full counters fit the MsgQ 32-byte immediate limit. Cookie and generation
 * distinguish engines and reused lanes; SEND's 4-byte wake ABI is unchanged. */
struct dpa_rdma_counter_msg {
    uint8_t type, version, eu_index, worker;
    uint32_t cookie, generation;
    uint16_t lane, reserved;
    uint64_t tail, head;
};
_Static_assert(sizeof(struct dpa_rdma_counter_msg)==32, "native RDMA counter notification ABI");
static inline int dpa_rdma_counter_valid(const struct dpa_rdma_counter_msg *m,
    unsigned bytes, unsigned worker, unsigned workers, unsigned threads)
{
    struct dpa_rdma_notify_msg hint={DPA_MSG_RDMA_READY,m->version,m->eu_index,m->worker};
    return bytes==sizeof(*m) && m->type==DPA_MSG_RDMA_COUNTER && !m->reserved &&
        m->cookie && m->generation && m->lane<PEER_RDMA_LANES &&
        dpa_rdma_notify_valid(&hint,sizeof(hint),worker,workers,threads);
}


struct peer_dma_ring_ctrl {
	volatile uint64_t consumer_head;
	uint8_t reserved[56];
} __attribute__((aligned(64)));

/* Exactly 64 bytes = one cache line per descriptor. */
struct peer_dma_desc {
	doca_dpa_dev_mmap_t mmap;      /* 4B */
	uint64_t addr;                 /* 8B byte offset into the host TX buffer */
	uint32_t size;                 /* 4B (fixed width for Host/DPA ABI stability) */
		/* Endpoint tuple; all but src_service are copied into the completion. */
		uint16_t seq;                  /* 2B per-conn sequence */
	uint16_t src_port;             /* 2B sender port */
	uint16_t dst_port;             /* 2B dest port (PORT_BLANK=0 -> accept queue) */
	int8_t   src_service;          /* 1B caller service (SVC_NONE if none) */
	int8_t   dst_service;          /* 1B callee service (routing input when dst_pod==BLANK) */
	int32_t dst_pod_id;            /* 4B routing target; DMESH_POD_BLANK(-1) -> DPU resolves dst_service */
	uint8_t pad0[4];               /* 4B: aligns src_pod_id, and holds the struct at its fixed
	                                * 64B cache line (see above) — widen it, never shrink. */
	int32_t src_pod_id;            /* 4B host sender field; DPA uses ring->pod_id */
	uint8_t reserved[20];          /* fixed descriptor padding */
	volatile uint64_t publish_seq; /* ticket+1; generation-safe MPSC publication */
} __attribute__((__packed__, aligned(8)));

/* Keep Host/DPA descriptor ABI stable across toolchains. */
_Static_assert(sizeof(struct peer_dma_ring_ctrl) == 64, "dma_ring_ctrl must be 64 bytes");
_Static_assert(sizeof(struct peer_dma_desc) == 64, "dma_desc must be 64 bytes");
_Static_assert(offsetof(struct peer_dma_desc, addr) == 4, "dma_desc.addr offset mismatch");
_Static_assert(offsetof(struct peer_dma_desc, size) == 12, "dma_desc.size offset mismatch");
_Static_assert(offsetof(struct peer_dma_desc, seq) == 16, "dma_desc.seq offset mismatch");
_Static_assert(offsetof(struct peer_dma_desc, dst_pod_id) == 24, "dma_desc.dst_pod_id offset mismatch");
_Static_assert(offsetof(struct peer_dma_desc, src_pod_id) == 32, "dma_desc.src_pod_id offset mismatch");
_Static_assert(offsetof(struct peer_dma_desc, publish_seq) == 56, "dma_desc.publish_seq offset mismatch");


#endif
