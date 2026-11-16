#ifndef DMESH_PEER_SLOT_SET_H
#define DMESH_PEER_SLOT_SET_H

#include <assert.h>
#include <stdint.h>

/* Owner-worker-only slot membership. The two summary words index nonempty
 * bitmap words, so an empty set never walks the 8192 slots (or 128 words).
 * Iteration retains ascending slot order across deletion/reuse. */
#define DMESH_PEER_SLOT_SET_CAP 8192u
#define DMESH_PEER_SLOT_SET_NIL UINT32_MAX
#define DMESH_PEER_SLOT_SET_WORDS (DMESH_PEER_SLOT_SET_CAP / 64u)
#define DMESH_PEER_SLOT_SET_GROUPS (DMESH_PEER_SLOT_SET_WORDS / 64u)

struct dmesh_peer_slot_set {
    uint64_t words[DMESH_PEER_SLOT_SET_WORDS];
    uint64_t nonempty[DMESH_PEER_SLOT_SET_GROUPS];
    uint32_t count;
};

static inline void peer_slot_set_add(struct dmesh_peer_slot_set *set, uint32_t id)
{
    assert(id < DMESH_PEER_SLOT_SET_CAP);

    uint32_t word = id / 64u;
    uint64_t bit = UINT64_C(1) << (id % 64u);
    assert(!(set->words[word] & bit));
    set->words[word] |= bit;
    set->nonempty[word / 64u] |= UINT64_C(1) << (word % 64u);
    set->count++;
}

static inline void peer_slot_set_remove(struct dmesh_peer_slot_set *set, uint32_t id)
{
    assert(id < DMESH_PEER_SLOT_SET_CAP);

    uint32_t word = id / 64u;
    uint64_t bit = UINT64_C(1) << (id % 64u);
    if (!(set->words[word] & bit))
        return;

    set->words[word] &= ~bit;

    if (!set->words[word])
        set->nonempty[word / 64u] &= ~(UINT64_C(1) << (word % 64u));
    set->count--;
}

/* Return the first member >= start, or NIL. At most the current bitmap word
 * and the two summaries are searched before selecting a nonempty word. No
 * shifts by 64 and no ctz(0), including IDs63/4095/8191 and start==capacity. */
static inline uint32_t peer_slot_set_next(const struct dmesh_peer_slot_set *set, uint32_t start)
{
    if (!set->count || start >= DMESH_PEER_SLOT_SET_CAP)
        return DMESH_PEER_SLOT_SET_NIL;

    uint32_t word = start / 64u;
    uint64_t bits = set->words[word] & (UINT64_MAX << (start % 64u));
    if (bits)
        return word * 64u + (uint32_t)__builtin_ctzll(bits);

    uint32_t group = word / 64u;
    unsigned offset = word % 64u;
    uint64_t words = offset == 63u ? 0 : set->nonempty[group] & (UINT64_MAX << (offset + 1u));

    for (;;) {
        if (words) {
            word = group * 64u + (uint32_t)__builtin_ctzll(words);
            return word * 64u + (uint32_t)__builtin_ctzll(set->words[word]);
        }
        if (++group == DMESH_PEER_SLOT_SET_GROUPS)
            return DMESH_PEER_SLOT_SET_NIL;

        words = set->nonempty[group];
    }
}
#endif
