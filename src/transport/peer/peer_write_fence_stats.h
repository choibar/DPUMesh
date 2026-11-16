#ifndef DMESH_PEER_WRITE_FENCE_STATS_H
#define DMESH_PEER_WRITE_FENCE_STATS_H
#include <stdint.h>

/* Batch-weighted observations immediately after adding a fence, before CQ
 * retirement. This is not a time-weighted mean or a hardware queue depth.
 * Histogram ranges: 1, 2-4, 5-8, 9-16, 17+. Snapshot/reset bounds counters. */
struct peer_write_fence_stats {
    uint64_t samples, depth_sum, full_stalls;
    uint32_t histogram[5], peak;
};

_Static_assert(sizeof(struct peer_write_fence_stats) == 48, "fit DPA lane padding");

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
#endif
