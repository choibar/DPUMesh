#ifndef DMESH_SESSION_PROTOCOL_H
#define DMESH_SESSION_PROTOCOL_H

/* Host/DPU control protocol. The public native ABI and DMA descriptor format
 * are independent of this version. Integers in the envelope are little endian;
 * v1 metadata payloads retain the existing LP64 host/DPU layout. */
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define DMESH_SESSION_MAGIC UINT32_C(0x444d5348)
#define DMESH_SESSION_VERSION 2u
#define DMESH_SESSION_HEADER_SIZE 24u
#define DMESH_SESSION_MAX_PAYLOAD 2048u
#define DMESH_SESSION_MAX_FRAME (DMESH_SESSION_HEADER_SIZE + DMESH_SESSION_MAX_PAYLOAD)
#define DMESH_SESSION_MAX_FLOWS 32u

enum dmesh_session_message_type {
    DMESH_SESSION_HELLO = 1,
    DMESH_SESSION_HELLO_ACK = 2,
    DMESH_SESSION_OPEN = 3,
    DMESH_SESSION_READY = 4,
    DMESH_SESSION_REVERSE_EXPORT = 5,
    DMESH_SESSION_CLOSE = 6,
    DMESH_SESSION_CLOSED = 7,
    DMESH_SESSION_ERROR = 8,
    /* Session messages (flow_id 0, generation 0) that let an idle host sleep.
     * ARM: the host is about to block; it lists, per push flow, the next
     * descriptor sequence it has not seen. DOORBELL: the DPU published a
     * descriptor the ARM did not account for, or one after the ARM. */
    DMESH_SESSION_ARM = 9,
    DMESH_SESSION_DOORBELL = 10,
};

/* ARM payload: u64 epoch, u32 count, u32 zero, then count entries of
 * u32 flow_id, u32 generation, u64 expected_seq. DOORBELL payload: u64 epoch. */
#define DMESH_SESSION_ARM_HEADER_SIZE 16u
#define DMESH_SESSION_ARM_FLOW_SIZE 16u
#define DMESH_SESSION_DOORBELL_SIZE 8u

struct dmesh_session_arm_flow {
    uint32_t flow_id;
    uint32_t generation;
    uint64_t expected_seq;
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
    if (h->type == DMESH_SESSION_ARM || h->type == DMESH_SESSION_DOORBELL) {
        if (h->flow_id != 0 || h->generation != 0 || h->status != 0)
            return 0;
        if (h->type == DMESH_SESSION_DOORBELL)
            return h->payload_len == DMESH_SESSION_DOORBELL_SIZE;
        return h->payload_len >= DMESH_SESSION_ARM_HEADER_SIZE &&
               h->payload_len <= DMESH_SESSION_ARM_HEADER_SIZE +
                                     DMESH_SESSION_MAX_FLOWS * DMESH_SESSION_ARM_FLOW_SIZE &&
               (h->payload_len - DMESH_SESSION_ARM_HEADER_SIZE) % DMESH_SESSION_ARM_FLOW_SIZE == 0;
    }
    if (h->type == DMESH_SESSION_HELLO || h->type == DMESH_SESSION_HELLO_ACK)
        return h->flow_id == 0 && h->generation == 0 && h->payload_len == 0 &&
               (h->type == DMESH_SESSION_HELLO_ACK || h->status == 0);
    if (h->flow_id == 0 || h->flow_id > DMESH_SESSION_MAX_FLOWS || h->generation == 0)
        return 0;
    switch (h->type) {
    case DMESH_SESSION_OPEN:
    case DMESH_SESSION_REVERSE_EXPORT:
        return h->payload_len != 0 && h->status == 0;
    case DMESH_SESSION_READY:
    case DMESH_SESSION_CLOSE:
        return h->payload_len == 0 && h->status == 0;
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

/* Returns the ARM payload length, or 0 when count exceeds the flow limit or
 * the output is too small. */
static inline size_t dmesh_session_arm_encode(uint8_t *out, size_t capacity, uint64_t epoch,
                                             const struct dmesh_session_arm_flow *flows,
                                             uint32_t count)
{
    size_t len = DMESH_SESSION_ARM_HEADER_SIZE + (size_t)count * DMESH_SESSION_ARM_FLOW_SIZE;
    if (!out || count > DMESH_SESSION_MAX_FLOWS || (count && !flows) || capacity < len)
        return 0;
    dmesh_session_put_u64(out, epoch);
    dmesh_session_put_u32(out + 8, count);
    dmesh_session_put_u32(out + 12, 0);
    for (uint32_t i = 0; i < count; ++i) {
        uint8_t *e = out + DMESH_SESSION_ARM_HEADER_SIZE + (size_t)i * DMESH_SESSION_ARM_FLOW_SIZE;
        dmesh_session_put_u32(e, flows[i].flow_id);
        dmesh_session_put_u32(e + 4, flows[i].generation);
        dmesh_session_put_u64(e + 8, flows[i].expected_seq);
    }
    return len;
}

/* Decode a validated ARM payload into flows[DMESH_SESSION_MAX_FLOWS]. */
static inline int dmesh_session_arm_decode(const uint8_t *payload, uint32_t len, uint64_t *epoch,
                                          struct dmesh_session_arm_flow *flows, uint32_t *count)
{
    if (!payload || !epoch || !flows || !count || len < DMESH_SESSION_ARM_HEADER_SIZE)
        return -1;
    uint32_t n = dmesh_session_get_u32(payload + 8);
    if (n > DMESH_SESSION_MAX_FLOWS || dmesh_session_get_u32(payload + 12) != 0 ||
        len != DMESH_SESSION_ARM_HEADER_SIZE + n * DMESH_SESSION_ARM_FLOW_SIZE)
        return -1;
    for (uint32_t i = 0; i < n; ++i) {
        const uint8_t *e = payload + DMESH_SESSION_ARM_HEADER_SIZE + (size_t)i * DMESH_SESSION_ARM_FLOW_SIZE;
        flows[i].flow_id = dmesh_session_get_u32(e);
        flows[i].generation = dmesh_session_get_u32(e + 4);
        flows[i].expected_seq = dmesh_session_get_u64(e + 8);
        if (flows[i].flow_id == 0 || flows[i].flow_id > DMESH_SESSION_MAX_FLOWS ||
            flows[i].generation == 0)
            return -1;
    }
    *epoch = dmesh_session_get_u64(payload);
    *count = n;
    return 0;
}

#endif
