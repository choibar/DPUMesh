#ifndef DMESH_PEER_PAIR_WIRE_H
#define DMESH_PEER_PAIR_WIRE_H
#include "peer_channel.h"

/* Worker-owned dmesh4 stream state. No stream generations, TLS/HW objects or keys.
 * One complete internal host frame is translated into/from the compact wire.
 * Encode exactly once per accepted frame, retaining bytes on would-block.
 * A codec error is terminal: the channel must close, not retry the mutation. */
struct peer_pair_wire;
struct peer_pair_wire *peer_pair_wire_new(const struct dmesh_peer_pair *, uint32_t owner_bit);
void peer_pair_wire_free(struct peer_pair_wire *);
int peer_pair_wire_retiring(const struct peer_pair_wire *);
int peer_pair_wire_local_closed(struct peer_pair_wire *, uint32_t handle);
int peer_pair_wire_released(struct peer_pair_wire *, uint32_t handle);
/* 0 waiting/empty, 1 emit typed control, 2 local allocator release completed.
 * nic_drained must certify the local send CQ prefix, never just consumed work. */
int peer_pair_wire_next_retire(struct peer_pair_wire *, int nic_drained, uint32_t *handle,
                               uint8_t *type);
/* Encode directly into registered header/body slots; no intermediate host
 * frame. Stateful control encoding must happen only AFTER TX reservation.
 * DATA headers advance an implicit ordered sequence but never copy payload. */
long peer_pair_wire_encode_parts(struct peer_pair_wire *, uint8_t type, uint32_t handle,
                                 const void *payload, uint32_t payload_len,
                                 uint8_t header[PEER_DATA_HEADER_BYTES], void *body,
                                 size_t body_cap);
int peer_pair_wire_data_header(struct peer_pair_wire *, uint32_t handle, uint32_t seq, uint32_t len,
                               uint8_t header[PEER_DATA_HEADER_BYTES]);
long peer_pair_wire_encode(struct peer_pair_wire *, const void *host_frame, size_t host_len,
                           void *wire_frame, size_t capacity);
/* Returns complete wire length, 0 incomplete, -1 malformed/stale. The output
 * header/payload use the existing channel's host representation and custody. */
long peer_pair_wire_decode(struct peer_pair_wire *, const void *wire_frame, size_t wire_len,
                           uint32_t incarnation, struct dmesh_peer_msg_header *, void *payload,
                           size_t capacity);
/* Exact complete frame exposed as header/body views. DATA returns its body length and sequence
 * without copying or fabricating the internal prefix; control uses scratch only. */
long peer_pair_wire_decode_parts(struct peer_pair_wire *,
                                 const uint8_t header[PEER_DATA_HEADER_BYTES], const void *body,
                                 size_t body_len, uint32_t incarnation,
                                 struct dmesh_peer_msg_header *, void *scratch, size_t capacity,
                                 uint32_t *data_seq);
#endif
