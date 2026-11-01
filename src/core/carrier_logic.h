/* Device-free pieces of the push carrier: forward chunking and the in-order
 * release window. Included by the carrier and by its unit test. */
#ifndef DMESH_CARRIER_PUSH_LOGIC_H
#define DMESH_CARRIER_PUSH_LOGIC_H
#include <stdint.h>
#include <string.h>
#include "channel.h"

/* The DPUMesh forward copy takes a multiple of 128 bytes or one block of at
 * most 128 bytes, each at most 8064 bytes. A descriptor of up to 8192 bytes
 * therefore becomes one or two pieces. Returns the piece count. */
static inline unsigned carrier_chunks(uint32_t len, uint32_t out[2])
{
    if (len <= CHANNEL_DESC_ALIGN) { out[0] = len; return len ? 1u : 0u; }
    uint32_t first = len >= CHANNEL_DESC_MAX ? CHANNEL_DESC_MAX : (len & ~(CHANNEL_DESC_ALIGN - 1u));
    out[0] = first;
    if (first == len) return 1u;
    out[1] = len - first;
    return 2u;
}

/* One connection's receive window: batches arrive in push order and are
 * released by the application in any order; the cursor advances over the
 * released prefix only. */
struct carrier_batch { uint64_t seq; uint32_t pos, len; uint8_t live, released; };
struct carrier_rx_window {
    struct carrier_batch batch[CHANNEL_DESC_N];
    uint64_t next_release;      /* oldest batch not yet released */
    uint64_t consumed_bytes;
};
static inline void carrier_window_init(struct carrier_rx_window *w)
{
    memset(w, 0, sizeof(*w)); w->next_release = 1;
}
static inline int carrier_window_add(struct carrier_rx_window *w, uint64_t seq, uint32_t pos, uint32_t len)
{
    struct carrier_batch *b = &w->batch[seq % CHANNEL_DESC_N];
    if (b->live) return -1;
    *b = (struct carrier_batch){seq, pos, len, 1, 0};
    return 0;
}
/* Marks the batch landing at `pos` released. Returns 0, or -1 when no live
 * batch starts there. Releases come mostly in order, so the search starts at
 * the oldest batch not yet released. */
static inline int carrier_window_release(struct carrier_rx_window *w, uint32_t pos)
{
    for (unsigned i = 0; i < CHANNEL_DESC_N; ++i) {
        struct carrier_batch *b = &w->batch[(w->next_release + i) % CHANNEL_DESC_N];
        if (b->live && !b->released && b->pos == pos) { b->released = 1; return 0; }
    }
    return -1;
}
/* Retires the released prefix. Returns nonzero when the cursor moved and
 * fills the values to publish. */
static inline int carrier_window_advance(struct carrier_rx_window *w, uint64_t *seq, uint64_t *bytes)
{
    int moved = 0;
    for (;;) {
        struct carrier_batch *b = &w->batch[w->next_release % CHANNEL_DESC_N];
        if (!b->live || !b->released || b->seq != w->next_release) break;
        w->consumed_bytes += b->len; b->live = 0; w->next_release++; moved = 1;
    }
    *seq = w->next_release - 1; *bytes = w->consumed_bytes;
    return moved;
}
#endif
