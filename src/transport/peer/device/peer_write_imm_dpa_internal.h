#ifndef DMESH_DEVICE_PEER_WRITE_IMM_DPA_INTERNAL_H
#define DMESH_DEVICE_PEER_WRITE_IMM_DPA_INTERNAL_H

#include "../peer_write_imm_dpa.h"

/* DPA engine state, separate from the ARM/DPA request and status ABI.
 * ARM setup includes this header only to allocate and initialize device state;
 * runtime progress is owned by the DPA thread. */
_Static_assert(WIM_SQ < 0x8000u, "raw16 CQ prefix must unwrap unambiguously");
_Static_assert(WIM_FENCES > 0, "batch completion queue must be bounded and nonempty");
_Static_assert(WIM_QPN_BUCKETS && !(WIM_QPN_BUCKETS & (WIM_QPN_BUCKETS - 1u)),
               "QPN bucket count must be a power of two");
_Static_assert(WIM_LANES <= UINT16_MAX, "QPN links store lane index plus one");
/* DPA-private scheduling, never shared with ARM. The only shared data FIFO
 * belongs to the context. Copies waiting for SQ space live in this bounded
 * pool; only lanes with pending work enter the ready FIFO. Zero ends a link. */
_Static_assert(PEER_DPA_WORK_CAP > 0 && PEER_DPA_WORK_CAP <= UINT16_MAX,
               "pending links store descriptor index plus one");

struct peer_dpa_work {
    uint16_t next[PEER_DPA_WORK_CAP];
    uint16_t head[PEER_DPA_WORK_LANES], tail[PEER_DPA_WORK_LANES];
    uint16_t ready[PEER_DPA_WORK_LANES];
    unsigned first, count, free, initialized;
};

static inline void peer_dpa_work_init(struct peer_dpa_work *w)
{
    if (w->initialized)
        return;

    for (unsigned i = 0; i < PEER_DPA_WORK_CAP; i++)
        w->next[i] = i + 1u < PEER_DPA_WORK_CAP ? (uint16_t)(i + 2u) : 0;
    w->free = 1;
    w->initialized = 1;
}

static inline void peer_dpa_work_ready(struct peer_dpa_work *w, unsigned lane)
{
    w->ready[(w->first + w->count) % PEER_DPA_WORK_LANES] = (uint16_t)lane;
    w->count++;
}

/* Return a pool index + 1, or zero for backpressure. */
static inline unsigned peer_dpa_work_push(struct peer_dpa_work *w, unsigned lane)
{
    unsigned entry = w->free;
    if (!entry)
        return 0;

    w->free = w->next[entry - 1];
    w->next[entry - 1] = 0;
    if (w->tail[lane])
        w->next[w->tail[lane] - 1] = (uint16_t)entry;
    else {
        w->head[lane] = (uint16_t)entry;
        peer_dpa_work_ready(w, lane);
    }
    w->tail[lane] = (uint16_t)entry;
    return entry;
}

/* Called only for the lane currently removed from the ready FIFO. */
static inline void peer_dpa_work_pop(struct peer_dpa_work *w, unsigned lane)
{
    unsigned entry = w->head[lane];
    w->head[lane] = w->next[entry - 1];
    if (!w->head[lane])
        w->tail[lane] = 0;
    w->next[entry - 1] = (uint16_t)w->free;
    w->free = entry;
}

static inline unsigned peer_dpa_work_take(struct peer_dpa_work *w)
{
    unsigned lane = w->ready[w->first];
    w->first = (w->first + 1) % PEER_DPA_WORK_LANES;
    w->count--;
    return lane;
}

/* DPA-private expanded WRITE range; never published through the shared FIFO. */
struct wim_write {
    uint64_t src, remote;
    uint32_t key, rkey, bytes;
};

static inline void peer_write_fence_stats_sample(struct peer_write_fence_stats *s, unsigned depth)
{
    unsigned bucket = depth <= 1 ? 0 : depth <= 4 ? 1 : depth <= 8 ? 2 : depth <= 16 ? 3 : 4;
    s->samples++;
    s->depth_sum += depth;
    if (s->histogram[bucket] != UINT32_MAX)
        s->histogram[bucket]++;
    if (depth > s->peak)
        s->peak = depth;
}

struct wim_fence {
    uint32_t counter, data_id;
};

_Static_assert(sizeof(struct wim_fence) == 8, "eight bytes per batch fence");

struct wim_lane {
    struct wim_binding binding;
    uint64_t consumed, tx_tail, tx_head, rx_tail, rx_head;
    uint64_t remote_base;
    uint32_t remote_key, remote_capacity;
    uint64_t id[WIM_CONTROLS];
    uint32_t post_index, completion_base, completed_index, data_id;

    struct wim_fence fence[WIM_FENCES];

    unsigned fence_first, fence_count;
    uint32_t control_counter[WIM_CONTROLS];
    struct wim_write last_data;
    uint64_t recv_source;
    uint32_t recv_key, rq_live;
    uint8_t last_data_valid;
    uint8_t completion_base_valid, notify;
    uint8_t busy[WIM_CONTROLS], configured, failed, installed;
    uint16_t qpn_next; /* DPA-private lane index + 1; zero ends the bucket chain. */
    struct peer_write_fence_stats fence_stats; /* Batch fence diagnostics. */
};

_Static_assert(sizeof(struct wim_lane) % 64 == 0, "lane cache line alignment");

struct wim_device {
    uint64_t buffers, base_device, cq, cq_device;
    uint64_t control_head, data_head, resume_id;
    struct peer_dpa_work pending;
    struct wim_request pending_request[PEER_DPA_WORK_CAP];
    uint64_t published_seq, notified_epoch;
    uint32_t cq_capacity, flush_left, count, owner_worker, cookie;
    uint8_t paused, fence_stats_enabled;
    uint16_t active[WIM_LANES];
    uint16_t qpn_heads[WIM_QPN_BUCKETS];
    struct wim_lane lane[WIM_LANES];
};

#endif
