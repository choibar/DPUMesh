#ifndef DMESH_PEER_WRITE_TX_RING_H
#define DMESH_PEER_WRITE_TX_RING_H
#include <stdint.h>
#include <stdlib.h>

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
#endif
