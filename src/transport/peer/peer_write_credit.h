#ifndef DMESH_PEER_WRITE_CREDIT_H
#define DMESH_PEER_WRITE_CREDIT_H
#include "peer_wire.h"

/* Credit is a cumulative capacity watermark, not an application ACK. Bound
 * unpublished reclaimed space to <C/16. A minimum-size ring must return even
 * tiny records immediately: frame_reserve requires a full frame of free
 * capacity before it knows the actual encoded length. */
static inline uint64_t peer_write_credit_batch_bytes(uint32_t capacity)
{
    return capacity <= PEER_WIRE_FRAME_BYTES ? 1u : capacity / 16u;
}
#endif
