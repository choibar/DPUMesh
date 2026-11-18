#ifndef DMESH_PEER_DATA_WIRE_H
#define DMESH_PEER_DATA_WIRE_H
#include <stddef.h>
#include <stdint.h>

/* dmesh4/1, network byte order: type3 | owner1 | slot12 | body_length16.
 * The authenticated, immutable QP supplies association/lane identity. There
 * is no stream generation or sequence on the wire. RC ordering and bilateral
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
#endif
