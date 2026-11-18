#ifndef DMESH_PEER_DPA_RING_H
#define DMESH_PEER_DPA_RING_H

#include "peer_dpa_abi.h"
#include "peer_config.h"
#include "peer_dpa_profile.h"
#include "peer_dpa_work.h"

/* 27 sizes lane tables from the shared D2D cluster budget.
 * Always rebuild ARM and DPA together, including when diagnostics are off. */
#define PEER_DPA_ABI 28u
#define PEER_DPA_NOTIFY_MSGQ 1u

enum peer_dpa_op {
    PEER_DPA_INSTALL = 1,
    PEER_DPA_ENABLE,
    PEER_DPA_SEND,
    PEER_DPA_RECV,
    PEER_DPA_REMOVE,
    PEER_DPA_STOP,
    PEER_DPA_CQ_PAUSE,
    PEER_DPA_CQ_RESUME,
    PEER_DPA_SEND_FRAME
};

enum peer_dpa_error {
    PEER_DPA_BAD_REQUEST = 1,
    PEER_DPA_CQ_ERROR = 2
};

struct peer_dpa_meta {
    uint8_t op, version;
    uint16_t lane_id;
    uint32_t generation, qpn;
    uint64_t token;
} __attribute__((packed));

_Static_assert(sizeof(struct peer_dpa_meta) == 20, "RDMA metadata must fit reserved[]");
_Static_assert(offsetof(struct peer_dma_desc, reserved) == 36, "RDMA descriptor ABI");

/* SEND_FRAME and arena RECV carry the registered ARM frame address/key in bytes 16..27
 * of the same descriptor as its publication ticket. These bytes are Host DMA
 * routing fields on Host rings, but unused on this private RDMA ring. Length
 * is d.size; d.addr remains the TX/RX slot index. Reposts do not read rx_sources.
 * Copy through the enclosing object, without aliasing Host fields as a struct. */
struct peer_dpa_buffer {
    uint64_t addr;
    uint32_t key;
} __attribute__((packed));

#define PEER_DPA_BUFFER_OFFSET 16u
_Static_assert(sizeof(struct peer_dpa_buffer) == 12, "inline buffer address/key ABI");
_Static_assert(offsetof(struct peer_dpa_buffer, key) == 8, "inline buffer key ABI");
_Static_assert(offsetof(struct peer_dma_desc, seq) == PEER_DPA_BUFFER_OFFSET, "inline buffer offset");
_Static_assert(PEER_DPA_BUFFER_OFFSET + sizeof(struct peer_dpa_buffer) <=
                   offsetof(struct peer_dma_desc, reserved),
               "inline buffer must fit unused routing fields");

static inline void peer_dpa_buffer_set(struct peer_dma_desc *d, const struct peer_dpa_buffer *source)
{
    __builtin_memcpy((uint8_t *)d + PEER_DPA_BUFFER_OFFSET, source, sizeof(*source));
}

static inline struct peer_dpa_buffer peer_dpa_buffer_get(const struct peer_dma_desc *d)
{
    struct peer_dpa_buffer source;
    __builtin_memcpy(&source, (const uint8_t *)d + PEER_DPA_BUFFER_OFFSET, sizeof(source));
    return source;
}

/* ARM writes a table entry only before INSTALL, or after REMOVE, successful
 * QP destruction and CQ_RESUME's drain ACK permit lane/QPN reuse.
 * DPA copies it to private memory on INSTALL. Raw handles never come from a
 * Host/Pod descriptor. No table or ring is exported to an untrusted Host. */
struct peer_dpa_binding {
    uint64_t qp, cq, device, tx_addr, rx_addr;
    uint32_t mkey, capacity, qpn, generation;
    uint64_t rx_frame_bytes; /* 65536: arena frames in rx_sources; 0: legacy slots */
} __attribute__((aligned(64)));

/* DPA is the only writer of each completion cache line; ARM only reads it.
 * TX slots remain NIC-owned until their exact request token is published.
 * RX slots remain NIC-owned until a delivery sequence is published, then
 * ARM-owned until it publishes RECV for that slot. RX is consumed/reposted in
 * RC receive order; sequence N uses slot (N-1) % PEER_RDMA_RECV_SLOTS.
 * Sequence/length publication relies on the existing external-window
 * writeback + ARM acquire protocol and coherent NIC access to ARM DRAM. */
struct peer_dpa_slot_status {
    uint64_t sequence;
    uint32_t bytes;
    uint8_t pad[52];
} __attribute__((aligned(64)));

struct peer_dpa_status {
    uint64_t control_ack;
    uint32_t error, generation;
    uint64_t sends, receives, completions, request_consumed;
    uint8_t pad[16];
    struct peer_dpa_slot_status tx[PEER_RDMA_SEND_SLOTS];
    struct peer_dpa_slot_status rx[PEER_RDMA_RECV_SLOTS];
} __attribute__((aligned(64)));

_Static_assert(sizeof(struct peer_dpa_binding) == 64, "QP table ABI");
_Static_assert(sizeof(struct peer_dpa_slot_status) == 64, "slot completion ABI");
_Static_assert(offsetof(struct peer_dpa_status, tx) == 64, "completion ABI");

struct peer_dpa_request_ring {
    struct peer_dma_ring_ctrl consumed;
    struct peer_dma_desc requests[PEER_DPA_RING_CAP];
};

/* Initial RX arena source. Address/key refer to DPU ARM DRAM registered on
 * this lane's SF. Reposts carry address/key inline and length in d.size. */
struct peer_dpa_rx_source {
    uint64_t addr;
    uint32_t key, len;
};

struct peer_dpa_shared {
    struct peer_dma_ring_ctrl consumed;   /* DPA writes; ARM acquire-loads */
    struct peer_dma_ring_ctrl cq_control; /* pause/drain barrier ACK, DPA writes */
    struct peer_dma_ring_ctrl stopped;    /* ACK after all DPA access has ceased */
    /* DPA publishes MSGQ only with live producer/completion handles. ARM
     * acquire-loads this capability; zero keeps standalone probes polling. */
    struct peer_dma_ring_ctrl notification;
    struct peer_dma_desc requests[PEER_DPA_RING_CAP]; /* context control FIFO */
    /* One context-wide SPSC submission FIFO. QPN/generation already occupy
     * reserved[]; REMOVE fences the lane's last context submission ticket. */
    struct peer_dpa_request_ring data;
    struct peer_dpa_binding table[PEER_DPA_LANES];
    /* Initial RQ population only: ARM writes before INSTALL publication and
     * leaves entries unchanged for the lane lifetime. DPA reads only on INSTALL. */
    struct peer_dpa_rx_source rx_sources[PEER_DPA_LANES][PEER_RDMA_RECV_SLOTS];
    struct peer_dpa_status status[PEER_DPA_LANES];
    struct peer_dpa_profile_shared profile;
};

/* Persistent DPA-private data: reschedule restarts the entry function. */
struct peer_dpa_lane_state {
    struct peer_dpa_binding binding;
    uint64_t request_head;
    uint64_t tx_token[PEER_RDMA_SEND_SLOTS], rx_sequence, rx_reposted;
    uint32_t tx_counter[PEER_RDMA_SEND_SLOTS], rx_counter[PEER_RDMA_RECV_SLOTS];
    uint8_t tx_busy[PEER_RDMA_SEND_SLOTS], rx_posted[PEER_RDMA_RECV_SLOTS];
    /* DPA-private WQE-counter hash: slot+1 links, zero is empty. */
    uint16_t tx_heads[PEER_RDMA_SEND_SLOTS], tx_next[PEER_RDMA_SEND_SLOTS];
    uint8_t installed, active, failed;
    uint16_t qpn_next; /* lane index + 1, zero ends a hash bucket chain */
};

struct peer_dpa_device_state {
    uint64_t engine_kind; /* zero: SEND, WSD_ENGINE: WRITE+Sync */
    struct peer_dpa_profile_state profile;
    uint64_t buffers, base_device, head, data_head;
    struct peer_dpa_work pending;
    struct peer_dma_desc pending_request[PEER_DPA_WORK_CAP];
    uint32_t data_count, owner_worker;
    uint16_t data_lanes[PEER_DPA_LANES];
    /* One CQ on one extended SF device for this ARM worker / DPA thread.
     * Immutable handles are initialized before attach; DPA owns all mutable
     * lookup/pause state. QPN has no generation in hardware CQEs: ARM must
     * pause -> destroy QP -> resume/drain barrier before reusing any QPN. */
    uint64_t cq, cq_device, cq_resume_token, stale_completions;
    uint32_t cq_capacity, cq_flush_left;
    uint8_t cq_paused;
    /* DPA-owned, survives reschedule. Retry an unsent hint even with no new
     * CQE/descriptor; there is one producer on this dedicated EU's MsgQ. */
    uint8_t notify_pending;
    /* DPA-private latch, preserved across reschedule. MsgQ handles stay live
     * from capability publication until STOP; ARM must not detach them early. */
    uint8_t notify_published;
    uint16_t qpn_heads[PEER_DPA_QPN_BUCKETS];
    struct peer_dpa_lane_state lanes[PEER_DPA_LANES];
};

static inline struct peer_dpa_meta peer_dpa_metadata(const struct peer_dma_desc *d)
{
    struct peer_dpa_meta m;
    __builtin_memcpy(&m, d->reserved, sizeof(m));
    return m;
}

static inline int peer_dpa_request_valid(const struct peer_dma_desc *d,
                                         const struct peer_dpa_meta *m,
                                         const struct peer_dpa_binding *b)
{
    return m->version == PEER_DPA_ABI && m->lane_id < PEER_DPA_LANES && m->generation &&
           m->generation == b->generation && m->qpn == b->qpn && m->token && d->mmap == b->mkey &&
           (((m->op == PEER_DPA_SEND || (m->op == PEER_DPA_SEND_FRAME && d->size >= PEER_DATA_HEADER_BYTES)) &&
             d->addr < PEER_RDMA_SEND_SLOTS &&
             (m->op == PEER_DPA_SEND_FRAME || d->size <= PEER_DPA_TX_BUFFER_SIZE)) ||
            (m->op == PEER_DPA_RECV && d->addr < PEER_RDMA_RECV_SLOTS &&
             d->size == (b->rx_frame_bytes ? b->rx_frame_bytes : b->capacity))) &&
           d->size &&
           d->size <=
               ((m->op == PEER_DPA_SEND_FRAME || (m->op == PEER_DPA_RECV && b->rx_frame_bytes))
                    ? PEER_RDMA_SLOT_BYTES
                    : b->capacity) &&
           b->capacity == PEER_DPA_BUFFER_SIZE;
}

static inline int peer_dpa_data_op(unsigned op)
{
    return op == PEER_DPA_SEND || op == PEER_DPA_SEND_FRAME || op == PEER_DPA_RECV;
}

#endif
