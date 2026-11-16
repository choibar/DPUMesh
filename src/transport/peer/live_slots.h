#ifndef DMESH_LIVE_SLOTS_H
#define DMESH_LIVE_SLOTS_H

#include <assert.h>
#include <stdint.h>

/* Owner-thread-only dense index list. Move IDs, never connection objects:
 * native handles, shared lane IDs and generation checks keep stable addresses.
 * Zero initialization is an empty list; positions encode offset + 1. */
#define DMESH_LIVE_SLOTS_MAX 256u
struct dmesh_live_slots {
    uint16_t ids[DMESH_LIVE_SLOTS_MAX];
    uint16_t positions[DMESH_LIVE_SLOTS_MAX];
    unsigned count;
};
static inline void dmesh_live_add(struct dmesh_live_slots *s, unsigned id)
{
    assert(id < DMESH_LIVE_SLOTS_MAX && !s->positions[id]);
    assert(s->count < DMESH_LIVE_SLOTS_MAX);
    s->ids[s->count] = (uint16_t)id;
    s->positions[id] = (uint16_t)(++s->count);
}
static inline void dmesh_live_remove(struct dmesh_live_slots *s, unsigned id)
{
    assert(id < DMESH_LIVE_SLOTS_MAX);
    if (!s->positions[id]) return; /* partially initialized/closed handle */
    unsigned pos = s->positions[id] - 1;
    assert(pos < s->count && s->ids[pos] == id);
    unsigned last = s->ids[--s->count];
    if (pos < s->count) {
        s->ids[pos] = (uint16_t)last;
        s->positions[last] = (uint16_t)(pos + 1);
    }
    s->positions[id] = 0;
}
#endif
