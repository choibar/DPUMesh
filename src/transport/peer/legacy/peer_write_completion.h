#ifndef DMESH_PEER_WRITE_COMPLETION_H
#define DMESH_PEER_WRITE_COMPLETION_H
#include <stdint.h>
#include <stdlib.h>
#include "peer_data_wire.h"

/* ARM-side WRITE completion and borrowed-source ownership tracking. */

/* ---- TX byte ranges and source ownership ------------------------------ */

/* Logical byte positions never wrap. Physical addresses use position % capacity.
 * A completion certifies the whole submitted prefix [completed, end), not an
 * individual frame. Only borrowed arena sources need ownership records. */
struct peer_write_tx_hold {
    struct peer_write_tx_hold *next;
    uint64_t end;
    void *owner, *cookie;
    void (*retain)(void *, void *);
    void (*release)(void *, void *);
};

struct peer_write_tx_ring {
    uint64_t tail, completed;
    struct peer_write_tx_hold *first, *last;
    unsigned holds;
};

static inline uint64_t peer_write_tx_ring_space(const struct peer_write_tx_ring *r,
                                                uint32_t capacity)
{
    return capacity - (r->tail - r->completed);
}

/* Reserve the source reference BEFORE posting: a partial/failed submission is
 * quarantined until QP destruction. Adjacent uses of the same arena owner share
 * one retained reference, extended through the last byte that needs it. */
static inline int peer_write_tx_ring_hold(struct peer_write_tx_ring *r,
                                          uint64_t end,
                                          void *owner,
                                          void *cookie,
                                          void (*retain)(void *, void *),
                                          void (*release)(void *, void *))
{
    if (end <= r->tail || !retain || !release)
        return -1;

    struct peer_write_tx_hold *h = r->last;
    if (h && h->end == r->tail && h->owner == owner && h->cookie == cookie && h->retain == retain &&
        h->release == release) {
        h->end = end;
        return 0;
    }

    h = malloc(sizeof(*h));
    if (!h)
        return -1;

    *h = (struct peer_write_tx_hold){
        .end = end, .owner = owner, .cookie = cookie, .retain = retain, .release = release};
    retain(owner, cookie);
    if (r->last)
        r->last->next = h;
    else
        r->first = h;
    r->last = h;
    r->holds++;
    return 0;
}

static inline int
peer_write_tx_ring_post(struct peer_write_tx_ring *r, uint32_t bytes, uint32_t capacity)
{
    if (!bytes || r->completed > r->tail || r->tail - r->completed > capacity ||
        bytes > peer_write_tx_ring_space(r, capacity) || UINT64_MAX - r->tail < bytes)
        return -1;

    r->tail += bytes;
    return 0;
}

static inline int peer_write_tx_ring_done(struct peer_write_tx_ring *r, uint64_t end)
{
    if (end < r->completed || end > r->tail)
        return -1;

    r->completed = end;
    while (r->first && r->first->end <= end) {
        struct peer_write_tx_hold *h = r->first;
        r->first = h->next;
        if (!r->first)
            r->last = NULL;
        r->holds--;
        h->release(h->owner, h->cookie);
        free(h);
    }
    return 0;
}

/* Call only AFTER a successful QP destruction fence, including partial posts
 * whose source hold extends past the last successfully submitted tail. */
static inline void peer_write_tx_ring_fenced(struct peer_write_tx_ring *r)
{
    while (r->first) {
        struct peer_write_tx_hold *h = r->first;
        r->first = h->next;
        h->release(h->owner, h->cookie);
        free(h);
    }
    r->last = NULL;
    r->holds = 0;
    /* Destruction authorizes releasing sources, but may have aborted WRs.
     * Preserve the last CQ-certified end for teardown diagnostics. */
}

/* ---- Logical DATA-ID completion lengths ------------------------------- */

/* ARM-only lengths for logical DATA requests, before DPA WRITE coalescing.
 * IDs count DATA only, wrap modulo 2^32, and are implicit in FIFO positions.
 * Store wire bytes (header included) minus one so 65536 fits in two bytes. */
struct peer_write_completion_length {
    uint16_t bytes_minus_one;
};

struct peer_write_completion {
    struct peer_write_completion_length *length;
    uint32_t capacity, limit;
    uint32_t posted_id, completed_id;
    uint64_t posted_bytes, completed_bytes;
};

_Static_assert(sizeof(struct peer_write_completion_length) == 2, "two bytes per DATA request");

/* The local byte window bounds outstanding requests: every wire frame is >=4B.
 * A window below 2^31 requests also makes modulo-ID completion unambiguous. */
static inline int peer_write_completion_init(struct peer_write_completion *q, uint32_t window)
{
    if (window < PEER_DATA_HEADER_BYTES || q->length || q->limit)
        return -1;
    q->limit = window / PEER_DATA_HEADER_BYTES;
    return 0;
}

/* Reserve a length before publishing the descriptor. A failed enqueue must
 * cancel this last entry; no source reference is released by this FIFO. */
static inline int
peer_write_completion_post(struct peer_write_completion *q, uint32_t bytes, uint64_t end)
{
    uint32_t pending = q->posted_id - q->completed_id;
    if (bytes < PEER_DATA_HEADER_BYTES || bytes > 65536 || pending >= q->limit ||
        q->posted_bytes > UINT64_MAX - bytes || end != q->posted_bytes + bytes ||
        q->completed_bytes > q->posted_bytes ||
        q->posted_bytes - q->completed_bytes > (uint64_t)q->limit * PEER_DATA_HEADER_BYTES ||
        bytes > (uint64_t)q->limit * PEER_DATA_HEADER_BYTES - (q->posted_bytes - q->completed_bytes))
        return -1;

    if (pending == q->capacity) {
        uint32_t capacity = q->capacity ? q->capacity * 2 : 32;
        struct peer_write_completion_length *length = malloc((size_t)capacity * sizeof(*length));
        if (!length)
            return -1;
        for (uint32_t n = 0; n < pending; n++) {
            uint32_t id = q->completed_id + n;
            length[id & (capacity - 1)] = q->length[id & (q->capacity - 1)];
        }
        free(q->length);
        q->length = length;
        q->capacity = capacity;
    }

    q->length[q->posted_id & (q->capacity - 1)].bytes_minus_one = (uint16_t)(bytes - 1);
    q->posted_id++;
    q->posted_bytes = end;
    return 0;
}

static inline void peer_write_completion_cancel(struct peer_write_completion *q)
{
    q->posted_id--;
    q->posted_bytes -= (uint32_t)q->length[q->posted_id & (q->capacity - 1)].bytes_minus_one + 1;
}

/* Validate the whole completion prefix before advancing either cursor.
 * A repeated ID is harmless; a stale or future ID must not release sources. */
static inline int
peer_write_completion_done(struct peer_write_completion *q, uint32_t id, uint64_t *end)
{
    uint32_t pending = q->posted_id - q->completed_id;
    uint32_t count = id - q->completed_id;
    if (pending > q->limit || count > pending)
        return -1;

    uint64_t next = q->completed_bytes;
    for (uint32_t n = 0; n < count; n++) {
        uint32_t at = (q->completed_id + n) & (q->capacity - 1);
        uint32_t bytes = (uint32_t)q->length[at].bytes_minus_one + 1;
        if (bytes < PEER_DATA_HEADER_BYTES || next > q->posted_bytes || bytes > q->posted_bytes - next)
            return -1;
        next += bytes;
    }

    q->completed_id = id;
    *end = q->completed_bytes = next;
    return 0;
}

static inline void peer_write_completion_free(struct peer_write_completion *q)
{
    free(q->length);
    *q = (struct peer_write_completion){0};
}
#endif
