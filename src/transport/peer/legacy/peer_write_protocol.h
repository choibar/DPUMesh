#ifndef DMESH_PEER_WRITE_PROTOCOL_H
#define DMESH_PEER_WRITE_PROTOCOL_H

#include "peer_config.h"
#include <stdint.h>
#include "peer_data_wire.h"

/* Shared by ARM IMM, ARM Sync and native DPA WRITE. No SDK or OS dependencies:
 * device code uses the same bootstrap sizes and cumulative-counter rules. */
#define PEER_WRITE_SYNC_HELLO 4096u
#define PEER_WRITE_IMM_HELLO 64u
#define PEER_WRITE_HELLO_HEADER 40u
#define PEER_WRITE_HELLO_VERSION 4u
/* ARM Sync may reuse its 64B TX slot for two atomic results only after
 * the HELLO SEND completion. No steady-state operation reads the HELLO. */
#define PEER_WRITE_ARM_SYNC_RESULTS 0u

enum peer_write_wr_kind {
    PEER_WRITE_DATA,
    PEER_WRITE_RECV,
    PEER_WRITE_CREDIT,
    PEER_WRITE_BOOT
};

enum peer_write_state {
    PEER_WRITE_UNUSED,
    PEER_WRITE_INIT,
    PEER_WRITE_CONNECTING,
    PEER_WRITE_READY,
    PEER_WRITE_DEAD
};

/* 2 kind bits + 23 epoch bits + 7 connection bits + 32 counter/slot bits. */
#define PEER_WRITE_EPOCH_MAX 0x7fffffu
#define PEER_WRITE_INDEX_MASK 0x7fu
static inline uint64_t peer_write_wr_id(unsigned kind, uint32_t epoch,
                                        uint32_t index, uint32_t low)
{
    return (uint64_t)kind << 62 | (uint64_t)epoch << 39 | (uint64_t)index << 32 | low;
}
static inline unsigned peer_write_wr_index(uint64_t id)
{
    return (unsigned)(id >> 32) & PEER_WRITE_INDEX_MASK;
}
static inline uint32_t peer_write_wr_epoch(uint64_t id)
{
    return (uint32_t)(id >> 39) & PEER_WRITE_EPOCH_MAX;
}
static inline unsigned peer_write_wr_kind(uint64_t id)
{
    return (unsigned)(id >> 62);
}

static inline unsigned peer_write_hello_size(int immediate, int dpa_offload)
{
    return dpa_offload && !immediate ? PEER_WRITE_SYNC_HELLO : PEER_WRITE_IMM_HELLO;
}
static inline unsigned peer_write_scratch_size(int immediate, int dpa_offload)
{
    return 2u * peer_write_hello_size(immediate, dpa_offload);
}

static inline void peer_write_put(uint8_t *p, uint64_t v, unsigned n)
{
    for (unsigned i = 0; i < n; i++)
        p[i] = (uint8_t)(v >> (8u * (n - i - 1u)));
}
static inline uint64_t peer_write_get(const uint8_t *p, unsigned n)
{
    uint64_t v = 0;
    for (unsigned i = 0; i < n; i++)
        v = (v << 8) | p[i];
    return v;
}

/* Host-order view, never sent as a C struct. Each carrier owns the extension
 * after byte 40 (reserved IMM bytes or Sync event metadata). */
struct peer_write_hello {
    uint32_t capacity, key, qpn;
    uint64_t base, generation;
};
static inline int peer_write_capacity_in_range(uint32_t capacity)
{
    return capacity >= PEER_DATA_FRAME_BYTES && capacity <= PEER_WRITE_RING_MAX &&
           !(capacity & (capacity - 1u));
}

static inline void peer_write_hello_encode(uint8_t *p, unsigned bytes,
                                           const char magic[4], unsigned mode,
                                           const struct peer_write_hello *h)
{
    for (unsigned i = 0; i < bytes; i++)
        p[i] = 0;
    for (unsigned i = 0; i < 4; i++)
        p[i] = (uint8_t)magic[i];
    peer_write_put(p + 4, PEER_WRITE_HELLO_VERSION, 2);
    peer_write_put(p + 6, mode, 2);
    peer_write_put(p + 8, h->capacity, 4);
    peer_write_put(p + 12, h->key, 4);
    peer_write_put(p + 16, h->base, 8);
    peer_write_put(p + 24, h->generation, 8);
    peer_write_put(p + 32, h->qpn, 4);
    peer_write_put(p + 36, PEER_DATA_FRAME_BYTES, 4);
}
static inline int peer_write_hello_decode(const uint8_t *p, const char magic[4],
                                          unsigned mode, uint32_t peer_qpn,
                                          struct peer_write_hello *h)
{
    *h = (struct peer_write_hello){
        .capacity = (uint32_t)peer_write_get(p + 8, 4),
        .key = (uint32_t)peer_write_get(p + 12, 4),
        .base = peer_write_get(p + 16, 8),
        .generation = peer_write_get(p + 24, 8),
        .qpn = (uint32_t)peer_write_get(p + 32, 4)};
    /* Page alignment is checked by the ARM adapter before admitting memory. */
    for (unsigned i = 0; i < 4; i++)
        if (p[i] != (uint8_t)magic[i])
            return -1;
    return peer_write_get(p + 4, 2) != PEER_WRITE_HELLO_VERSION ||
           peer_write_get(p + 6, 2) != mode || !peer_write_capacity_in_range(h->capacity) ||
           !h->base || h->base > UINT64_MAX - 2ull * h->capacity || !h->generation ||
           h->qpn != peer_qpn || peer_write_get(p + 36, 4) != PEER_DATA_FRAME_BYTES ? -1 : 0;
}

/* Credit is a cumulative capacity watermark, not an application ACK. Bound
 * unpublished reclaimed space to <C/16. Minimum-size rings return even tiny
 * records immediately: frame_reserve requires room for a full frame. */
static inline uint64_t peer_write_credit_batch_bytes(uint32_t capacity)
{
    return capacity <= PEER_DATA_FRAME_BYTES ? 1u : capacity / 16u;
}

/* All 32 immediate bits are a cumulative byte position, in network order on
 * the wire. RC delivers notifications in order; a credit window <2^31 prevents
 * an unobserved full lap. QP/MR recreation defines a new generation. limit is
 * locally known space/outstanding bytes. Returns 1 advanced, 0 duplicate,
 * -1 stale/impossible/64-bit exhaustion. */
static inline int
peer_write_imm_extend(uint64_t previous, uint32_t low, uint64_t limit, uint64_t *next)
{
    if (!next || limit >= (UINT64_C(1) << 31))
        return -1;
    uint32_t delta = low - (uint32_t)previous;
    if ((uint64_t)delta > limit || UINT64_MAX - previous < delta)
        return -1;
    *next = previous + delta;
    return delta != 0;
}

#endif
