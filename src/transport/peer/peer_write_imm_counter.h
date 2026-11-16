#ifndef DMESH_PEER_WRITE_IMM_COUNTER_H
#define DMESH_PEER_WRITE_IMM_COUNTER_H
#include <stdint.h>

/* All 32 immediate bits are a cumulative byte position, in network order on
 * the wire. Each RC QP delivers notifications in order, and a credit window
 * smaller than 2^31 prevents an unobserved full lap. Never compare low words
 * with < or reset counters on wrap. QP/MR recreation defines a new generation.
 * limit must come from locally known ring space/outstanding bytes, not a peer.
 * Returns 1 advanced, 0 duplicate, -1 stale/impossible/64-bit exhaustion. */
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
