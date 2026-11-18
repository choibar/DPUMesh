#ifndef DMESH_PEER_WRITE_PROTOCOL_H
#define DMESH_PEER_WRITE_PROTOCOL_H

#include "peer_config.h"
#include <stddef.h>
#include <stdint.h>

/* ---- dmesh4 data frames ----------------------------------------------- */
/* dmesh4/1, network byte order: type3 | owner1 | slot12 | body_length16.
 * The authenticated, immutable QP supplies association/lane identity. There
 * is no connection generation or sequence on the wire. RC ordering and bilateral
 * retirement fence slot reuse. TLS/control and internal DMA ABIs are separate. */
#define PEER_DATA_HEADER_BYTES 4u
#define PEER_DATA_OWNER 0x1000u
#define PEER_DATA_SLOT_MASK 0x0fffu
#define PEER_DATA_FRAME_BYTES (64u * 1024u)
#define PEER_DATA_BODY_MAX (PEER_DATA_FRAME_BYTES - PEER_DATA_HEADER_BYTES)
enum peer_sec_data_type {
    PEER_SEC_STREAM_RELEASE_ACK = 0,
    PEER_SEC_STREAM_OPEN = 1,
    PEER_SEC_STREAM_OPEN_ACK,
    PEER_SEC_DATA,
    PEER_SEC_STREAM_ACK,
    PEER_SEC_STREAM_FIN,
    PEER_SEC_STREAM_READY,
    PEER_SEC_STREAM_RELEASE
};

static inline uint16_t peer_data_tag(const uint8_t *p)
{
    return (uint16_t)((uint16_t)p[0] << 8 | p[1]);
}
static inline uint32_t peer_data_body_length(const uint8_t *p)
{
    return (uint32_t)p[2] << 8 | p[3];
}
static inline size_t peer_data_frame_size(const uint8_t *p)
{
    unsigned type = peer_data_tag(p) >> 13;
    unsigned slot = peer_data_tag(p) & PEER_DATA_SLOT_MASK;
    uint32_t bytes = peer_data_body_length(p);
    if (bytes > PEER_DATA_BODY_MAX || (!slot && (peer_data_tag(p) & PEER_DATA_OWNER)) ||
        ((type == PEER_SEC_STREAM_OPEN || type == PEER_SEC_STREAM_ACK)
             ? (peer_data_tag(p) & 0x1fffu) != 0
             : (type == PEER_SEC_STREAM_OPEN_ACK ? 0 : slot == 0)) ||
        (type == PEER_SEC_DATA && !bytes) ||
        ((type == PEER_SEC_STREAM_FIN || type == PEER_SEC_STREAM_READY ||
          type == PEER_SEC_STREAM_RELEASE || type == PEER_SEC_STREAM_RELEASE_ACK) &&
         bytes))
        return 0;
    return PEER_DATA_HEADER_BYTES + bytes;
}

#define PEER_SEC_DATA_MAX PEER_DATA_BODY_MAX

/* Host representation of the compact DATA/control envelope. */
struct peer_sec_data_header {
    uint16_t type;
    uint32_t body_len;
    uint32_t handle;
};

static inline int peer_sec_data_valid(const struct peer_sec_data_header *h)
{
    if (!h || h->type > PEER_SEC_STREAM_RELEASE || h->body_len > PEER_SEC_DATA_MAX ||
        (h->handle & ~(0x80000000u | PEER_DATA_SLOT_MASK)))
        return 0;
    uint8_t p[4];
    unsigned tag = h->type << 13 | (h->handle & PEER_DATA_SLOT_MASK) |
                   ((h->handle & 0x80000000u) ? PEER_DATA_OWNER : 0);
    p[0] = tag >> 8; p[1] = tag;
    p[2] = h->body_len >> 8; p[3] = h->body_len;
    return peer_data_frame_size(p) != 0;
}

static inline int
peer_sec_data_encode(const struct peer_sec_data_header *h, uint8_t out[PEER_DATA_HEADER_BYTES])
{
    if (!out || !peer_sec_data_valid(h))
        return -1;

    unsigned tag = h->type << 13 | (h->handle & PEER_DATA_SLOT_MASK) |
                   ((h->handle & 0x80000000u) ? PEER_DATA_OWNER : 0);
    out[0] = tag >> 8; out[1] = tag;
    out[2] = h->body_len >> 8; out[3] = h->body_len;
    return 0;
}

/* One RDMA message, exact length required. This validates the envelope, not
 * application policy. Caller checks live handle/retirement state, including
 * ACK/FIN; hardware SA->QP enforcement is independently required. */
static inline int
peer_sec_data_decode(const void *buf, size_t len, struct peer_sec_data_header *out)
{
    if (!out)
        return -1;

    /* Keep failure output fully cleared without a host libc dependency. */
    for (size_t i = 0; i < sizeof(*out); i++)
        ((uint8_t *)out)[i] = 0;
    if (!buf || len < PEER_DATA_HEADER_BYTES ||
        len > PEER_DATA_HEADER_BYTES + PEER_SEC_DATA_MAX)
        return -1;

    struct peer_sec_data_header h = {0};
    unsigned tag = peer_data_tag(buf);
    h.type = tag >> 13;
    h.handle = (tag & PEER_DATA_SLOT_MASK) | ((tag & PEER_DATA_OWNER) ? 0x80000000u : 0);
    h.body_len = peer_data_body_length(buf);
    if (!peer_sec_data_valid(&h) || len != PEER_DATA_HEADER_BYTES + h.body_len)
        return -1;

    *out = h;
    return 0;
}

/* ---- WRITE transport protocol ----------------------------------------- */
/* WRITE_WITH_IMM+DPA bootstrap and cumulative counters. No SDK/OS dependencies. */
#define PEER_WRITE_IMM_HELLO 64u
#define PEER_WRITE_HELLO_HEADER 40u
#define PEER_WRITE_HELLO_VERSION 4u
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

/* Host-order view; bytes 40..63 are reserved and must be zero. */
struct peer_write_hello {
    uint32_t capacity, key, qpn;
    uint64_t base, generation;
};
static inline int peer_write_capacity_in_range(uint32_t capacity)
{
    return capacity >= PEER_DATA_FRAME_BYTES && capacity <= PEER_WRITE_RING_MAX &&
           !(capacity & (capacity - 1u));
}

static inline void peer_write_hello_encode(uint8_t *p,
                                           const struct peer_write_hello *h)
{
    for (unsigned i = 0; i < PEER_WRITE_IMM_HELLO; i++)
        p[i] = 0;
    for (unsigned i = 0; i < 4; i++)
        p[i] = (uint8_t)"DMWS"[i];
    peer_write_put(p + 4, PEER_WRITE_HELLO_VERSION, 2);
    peer_write_put(p + 6, 3u, 2);
    peer_write_put(p + 8, h->capacity, 4);
    peer_write_put(p + 12, h->key, 4);
    peer_write_put(p + 16, h->base, 8);
    peer_write_put(p + 24, h->generation, 8);
    peer_write_put(p + 32, h->qpn, 4);
    peer_write_put(p + 36, PEER_DATA_FRAME_BYTES, 4);
}
static inline int peer_write_hello_decode(const uint8_t *p, uint32_t peer_qpn,
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
        if (p[i] != (uint8_t)"DMWS"[i])
            return -1;
    return peer_write_get(p + 4, 2) != PEER_WRITE_HELLO_VERSION ||
           peer_write_get(p + 6, 2) != 3u || !peer_write_capacity_in_range(h->capacity) ||
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
