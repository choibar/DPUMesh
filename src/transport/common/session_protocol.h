#ifndef DMESH_SESSION_PROTOCOL_H
#define DMESH_SESSION_PROTOCOL_H

/* Host/DPU control protocol. The public native ABI and DMA descriptor format
 * are independent of this version. Integers in the envelope are little endian;
 * DMA metadata payloads keep the LP64 host/DPU layout. The control channel
 * carries no data-path wake: the DPU's reverse DPA thread wakes a host flow
 * through the flow slot's sync event (dpa_common.h, DMESH_EC_SE_OFF). */
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define DMESH_SESSION_MAGIC UINT32_C(0x444d5348)
#define DMESH_SESSION_VERSION 10u
#define DMESH_SESSION_HEADER_SIZE 24u
#define DMESH_SESSION_MAX_PAYLOAD 2048u
#define DMESH_SESSION_MAX_FRAME (DMESH_SESSION_HEADER_SIZE + DMESH_SESSION_MAX_PAYLOAD)
#define DMESH_SESSION_MAX_FLOWS 32u
#define DMESH_SESSION_MAX_WORKERS 128u
#define DMESH_SESSION_TAILS_MAX 512u /* export descriptor of the TAILS table */

enum dmesh_session_message_type {
    DMESH_SESSION_HELLO = 1,
    DMESH_SESSION_HELLO_ACK = 2,
    DMESH_SESSION_OPEN = 3,
    DMESH_SESSION_READY = 4,
    DMESH_SESSION_CLOSE = 6,
    DMESH_SESSION_CLOSED = 7,
    DMESH_SESSION_ERROR = 8,
    DMESH_SESSION_LISTEN = 9,       /* flow=0, generation=1, endpoint (ip, port) */
    DMESH_SESSION_LISTEN_ACK = 10,
    DMESH_SESSION_BACKEND_REQUEST = 11, /* flow=0, generation=request token, worker */
    DMESH_SESSION_BACKEND_OPEN = 12, /* worker + request token + DMA metadata */
    DMESH_SESSION_BACKEND_REJECT = 13, /* flow=0, generation=request token, worker */
    /* PING: the host sends it on an idle session. The DPU ignores it; a dead
     * DPU proxy fails the send, and only a send notices one: the Comch
     * client gets no event when its server process exits. */
    DMESH_SESSION_PING = 16,
    /* Migration with a spare connection. PREPARE (DPU -> host, on the client
     * flow to be moved, no payload): open a spare stream to that flow's
     * destination now; the dispatcher steers its OPEN to the target worker.
     * SPARE_READY (host -> DPU, on the spare flow): the spare has its server's
     * first bytes; drain the flow it replaces. */
    DMESH_SESSION_PREPARE = 17,
    DMESH_SESSION_SPARE_READY = 18,
    /* Flow scaling. SCALE (DPU -> host, on a client flow; payload u32 count
     * >= 1, u32 token): each client of that flow's destination should keep
     * count streams to it. SCALE_ACK (host -> DPU, on the same flow; payload
     * u32 token, u32 applied): how many clients of it took the count. */
    DMESH_SESSION_SCALE = 19,
    DMESH_SESSION_SCALE_ACK = 20,
    /* SHUTDOWN (host -> DPU, flow): the host closed its sending half. The
     * u64 payload counts the descriptors it posted on the flow's forward ring;
     * the proxy reads end of stream after all of them, and the flow stays
     * open until CLOSE. */
    DMESH_SESSION_SHUTDOWN = 22,
    /* TAILS (host -> DPU, session level, sent once right after HELLO_ACK and
     * so before any OPEN): the PCI export descriptor of the session's tail
     * table, one u32 per flow slot holding the low 32 bits of that flow's
     * forward producer_tail (dpa_common.h, DMESH_TAILS_BYTES). The DPA thread
     * that serves the session's rings polls the table instead of each ring.
     * The same page holds each flow slot's sync event word, which the
     * reverse DPA thread writes (DMESH_EC_SE_OFF). */
    DMESH_SESSION_TAILS = 23,
};

struct dmesh_session_header {
    uint32_t magic;
    uint16_t version;
    uint16_t type;
    uint32_t payload_len;
    uint32_t flow_id;
    uint32_t generation;
    int32_t status; /* zero or a positive errno; independent of DOCA enum values */
};

static inline uint32_t dmesh_session_get_u32(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 |
           (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}
static inline void dmesh_session_put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}
static inline uint64_t dmesh_session_get_u64(const uint8_t *p)
{
    return (uint64_t)dmesh_session_get_u32(p) | (uint64_t)dmesh_session_get_u32(p + 4) << 32;
}
static inline void dmesh_session_put_u64(uint8_t *p, uint64_t v)
{
    dmesh_session_put_u32(p, (uint32_t)v);
    dmesh_session_put_u32(p + 4, (uint32_t)(v >> 32));
}
static inline int dmesh_session_header_valid(const struct dmesh_session_header *h)
{
    if (h->magic != DMESH_SESSION_MAGIC || h->version != DMESH_SESSION_VERSION ||
        h->payload_len > DMESH_SESSION_MAX_PAYLOAD || h->status < 0)
        return 0;
    if (h->type == DMESH_SESSION_TAILS)
        return h->flow_id == 0 && h->generation == 0 && h->status == 0 && h->payload_len != 0 &&
               h->payload_len <= DMESH_SESSION_TAILS_MAX;
    if (h->type == DMESH_SESSION_PING)
        return h->flow_id == 0 && h->generation == 0 && h->payload_len == 0 && h->status == 0;
    if (h->type == DMESH_SESSION_HELLO || h->type == DMESH_SESSION_HELLO_ACK)
        return h->flow_id == 0 && h->generation == 0 && h->payload_len == 0 &&
               (h->type == DMESH_SESSION_HELLO_ACK || h->status == 0);
    if (h->type == DMESH_SESSION_LISTEN)
        return h->flow_id == 0 && h->generation == 1 && h->payload_len == 8 && h->status == 0;
    if (h->type == DMESH_SESSION_LISTEN_ACK)
        return h->flow_id == 0 && h->generation == 1 && h->payload_len == 0;
    if (h->type == DMESH_SESSION_BACKEND_REQUEST || h->type == DMESH_SESSION_BACKEND_REJECT)
        return h->flow_id == 0 && h->generation != 0 && h->payload_len == 4 &&
            (h->type == DMESH_SESSION_BACKEND_REQUEST ? h->status == 0 : h->status != 0);
    if (h->flow_id == 0 || h->flow_id > DMESH_SESSION_MAX_FLOWS || h->generation == 0)
        return 0;
    switch (h->type) {
    case DMESH_SESSION_OPEN:
    case DMESH_SESSION_BACKEND_OPEN:
        return h->payload_len != 0 && h->status == 0;
    case DMESH_SESSION_READY:
    case DMESH_SESSION_CLOSE:
    case DMESH_SESSION_PREPARE:
    case DMESH_SESSION_SPARE_READY:
        return h->payload_len == 0 && h->status == 0;
    case DMESH_SESSION_SHUTDOWN:
    case DMESH_SESSION_SCALE:
    case DMESH_SESSION_SCALE_ACK:
        return h->payload_len == 8 && h->status == 0;
    case DMESH_SESSION_CLOSED:
        return h->payload_len == 0;
    case DMESH_SESSION_ERROR:
        return h->payload_len == 0 && h->status != 0;
    default:
        return 0;
    }
}

/* Decode into aligned caller storage. No callback may cast untrusted Comch
 * bytes to a metadata struct; memcpy the validated payload to aligned storage. */
static inline int dmesh_session_decode(const void *data, size_t len,
                                      struct dmesh_session_header *header,
                                      const uint8_t **payload)
{
    struct dmesh_session_header h;
    const uint8_t *p = (const uint8_t *)data;
    if (!p || !header || !payload || len < DMESH_SESSION_HEADER_SIZE)
        return -1;
    h.magic = dmesh_session_get_u32(p);
    h.version = (uint16_t)((uint16_t)p[4] | (uint16_t)p[5] << 8);
    h.type = (uint16_t)((uint16_t)p[6] | (uint16_t)p[7] << 8);
    h.payload_len = dmesh_session_get_u32(p + 8);
    h.flow_id = dmesh_session_get_u32(p + 12);
    h.generation = dmesh_session_get_u32(p + 16);
    uint32_t status = dmesh_session_get_u32(p + 20);
    if (status > INT32_MAX)
        return -1;
    h.status = (int32_t)status;
    if (!dmesh_session_header_valid(&h) || len != DMESH_SESSION_HEADER_SIZE + h.payload_len)
        return -1;
    *header = h;
    *payload = p + DMESH_SESSION_HEADER_SIZE;
    return 0;
}

static inline size_t dmesh_session_encode(void *output, size_t capacity,
                                         uint16_t type, uint32_t flow_id,
                                         uint32_t generation, int32_t status,
                                         const void *payload, size_t payload_len)
{
    if (!output || payload_len > DMESH_SESSION_MAX_PAYLOAD ||
        (payload_len != 0 && payload == NULL))
        return 0;
    struct dmesh_session_header h = {
        DMESH_SESSION_MAGIC, DMESH_SESSION_VERSION, type,
        (uint32_t)payload_len, flow_id, generation, status
    };
    size_t len = DMESH_SESSION_HEADER_SIZE + payload_len;
    if (!dmesh_session_header_valid(&h) || capacity < len)
        return 0;
    uint8_t *p = (uint8_t *)output;
    dmesh_session_put_u32(p, h.magic);
    p[4] = (uint8_t)h.version; p[5] = (uint8_t)(h.version >> 8);
    p[6] = (uint8_t)h.type; p[7] = (uint8_t)(h.type >> 8);
    dmesh_session_put_u32(p + 8, h.payload_len);
    dmesh_session_put_u32(p + 12, h.flow_id);
    dmesh_session_put_u32(p + 16, h.generation);
    dmesh_session_put_u32(p + 20, (uint32_t)h.status);
    if (payload_len)
        memcpy(p + DMESH_SESSION_HEADER_SIZE, payload, payload_len);
    return len;
}

#endif
