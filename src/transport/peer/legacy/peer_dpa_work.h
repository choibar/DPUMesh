#ifndef DMESH_PEER_DPA_WORK_H
#define DMESH_PEER_DPA_WORK_H

#include <stdint.h>
#include "peer_config.h"

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

#endif
