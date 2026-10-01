#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "src/transport/common/session_protocol.h"

/* Check the public wire format independently of the encoder: a decoder and
 * encoder with the same endian/layout mistake must not pass a round trip. */
static void test_wire_layout(void)
{
    static const uint8_t golden[] = {
        0x48, 0x53, 0x4d, 0x44, 0x02, 0x00, 0x03, 0x00,
        0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x78, 0x56, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00,
        0xa5, 0x00, 0x7e,
    };
    uint8_t storage[sizeof(golden) + 2];
    uint8_t *frame = storage + 1; /* Comch callback bytes need not be aligned. */
    struct dmesh_session_header h;
    const uint8_t *payload;

    memset(storage, 0xcc, sizeof(storage));
    assert(dmesh_session_encode(frame, sizeof(golden), DMESH_SESSION_OPEN,
                                2, UINT32_C(0x12345678), 0,
                                golden + 24, 3) == sizeof(golden));
    assert(memcmp(frame, golden, sizeof(golden)) == 0);
    assert(storage[0] == 0xcc && storage[sizeof(storage) - 1] == 0xcc);
    assert(dmesh_session_decode(frame, sizeof(golden), &h, &payload) == 0);
    assert(h.magic == DMESH_SESSION_MAGIC && h.version == 2);
    assert(h.type == DMESH_SESSION_OPEN && h.flow_id == 2);
    assert(h.generation == UINT32_C(0x12345678) && h.status == 0);
    assert(h.payload_len == 3 && payload == frame + 24);
    assert(memcmp(payload, golden + 24, 3) == 0);

    for (size_t len = 0; len < sizeof(golden); ++len)
        assert(dmesh_session_decode(frame, len, &h, &payload) == -1);
    assert(dmesh_session_decode(frame, sizeof(golden) + 1, &h, &payload) == -1);
    assert(dmesh_session_decode(NULL, sizeof(golden), &h, &payload) == -1);
    assert(dmesh_session_decode(frame, sizeof(golden), NULL, &payload) == -1);
    assert(dmesh_session_decode(frame, sizeof(golden), &h, NULL) == -1);
}

struct message_case {
    uint16_t type;
    uint32_t flow_id, generation;
    int32_t status;
    uint32_t payload_len;
    int valid;
};

static void test_message_shapes(void)
{
    static const struct message_case cases[] = {
        {DMESH_SESSION_HELLO,          0, 0, 0,      0, 1},
        {DMESH_SESSION_HELLO_ACK,      0, 0, 0,      0, 1},
        {DMESH_SESSION_HELLO_ACK,      0, 0, EPROTO, 0, 1},
        {DMESH_SESSION_OPEN,           1, 1, 0,      1, 1},
        {DMESH_SESSION_REVERSE_EXPORT, 2, 9, 0,      1, 1},
        {DMESH_SESSION_READY,          1, 1, 0,      0, 1},
        {DMESH_SESSION_CLOSE,          1, 1, 0,      0, 1},
        {DMESH_SESSION_CLOSED,         1, 1, 0,      0, 1},
        {DMESH_SESSION_CLOSED,         1, 1, EIO,    0, 1},
        {DMESH_SESSION_ERROR,          1, 1, EIO,    0, 1},
        {DMESH_SESSION_OPEN,          32, UINT32_MAX, 0, 1, 1},
        {DMESH_SESSION_HELLO,          1, 0, 0,      0, 0},
        {DMESH_SESSION_HELLO_ACK,      0, 1, 0,      0, 0},
        {DMESH_SESSION_HELLO,          0, 0, 0,      1, 0},
        {DMESH_SESSION_HELLO_ACK,      0, 0, 0,      1, 0},
        {DMESH_SESSION_HELLO,          0, 0, EIO,    0, 0},
        {DMESH_SESSION_OPEN,           0, 1, 0,      1, 0},
        {DMESH_SESSION_OPEN,          33, 1, 0,      1, 0},
        {DMESH_SESSION_OPEN,           1, 0, 0,      1, 0},
        {DMESH_SESSION_OPEN,           1, 1, 0,      0, 0},
        {DMESH_SESSION_OPEN,           1, 1, EIO,    1, 0},
        {DMESH_SESSION_REVERSE_EXPORT, 1, 1, 0,      0, 0},
        {DMESH_SESSION_REVERSE_EXPORT, 1, 1, EIO,    1, 0},
        {DMESH_SESSION_READY,          1, 1, 0,      1, 0},
        {DMESH_SESSION_READY,          1, 1, EIO,    0, 0},
        {DMESH_SESSION_CLOSE,          1, 1, 0,      1, 0},
        {DMESH_SESSION_CLOSE,          1, 1, EIO,    0, 0},
        {DMESH_SESSION_CLOSED,         1, 1, 0,      1, 0},
        {DMESH_SESSION_ERROR,          1, 1, 0,      0, 0},
        {DMESH_SESSION_ERROR,          1, 1, EIO,    1, 0},
        {DMESH_SESSION_ERROR,          1, 1, -1,     0, 0},
        {DMESH_SESSION_ARM,            0, 0, 0,     16, 1},
        {DMESH_SESSION_ARM,            0, 0, 0,     32, 1},
        {DMESH_SESSION_ARM,            0, 0, 0,    528, 1},
        {DMESH_SESSION_ARM,            0, 0, 0,    544, 0},
        {DMESH_SESSION_ARM,            0, 0, 0,     15, 0},
        {DMESH_SESSION_ARM,            0, 0, 0,     24, 0},
        {DMESH_SESSION_ARM,            1, 1, 0,     16, 0},
        {DMESH_SESSION_ARM,            0, 1, 0,     16, 0},
        {DMESH_SESSION_ARM,            0, 0, EIO,   16, 0},
        {DMESH_SESSION_DOORBELL,       0, 0, 0,      8, 1},
        {DMESH_SESSION_DOORBELL,       0, 0, 0,      0, 0},
        {DMESH_SESSION_DOORBELL,       0, 0, 0,      9, 0},
        {DMESH_SESSION_DOORBELL,       1, 1, 0,      8, 0},
        {DMESH_SESSION_DOORBELL,       0, 0, EIO,    8, 0},
        {0,                           1, 1, 0,      0, 0},
        {UINT16_MAX,                  1, 1, 0,      0, 0},
    };
    uint8_t frame[DMESH_SESSION_HEADER_SIZE + 1024];
    uint8_t body[1024];
    struct dmesh_session_header h;
    const uint8_t *payload;
    memset(body, 0x5a, sizeof(body));

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        const struct message_case *c = &cases[i];
        size_t len = dmesh_session_encode(frame, sizeof(frame), c->type,
                                          c->flow_id, c->generation, c->status,
                                          body, c->payload_len);
        assert((len != 0) == c->valid);

        /* Construct even rejected frames without passing through encode, so
         * decode is independently exercised against malformed peer input. */
        const uint8_t prefix[8] = {0x48, 0x53, 0x4d, 0x44, DMESH_SESSION_VERSION, 0, 0, 0};
        memcpy(frame, prefix, sizeof(prefix));
        frame[6] = (uint8_t)c->type;
        frame[7] = (uint8_t)(c->type >> 8);
        dmesh_session_put_u32(frame + 8, c->payload_len);
        dmesh_session_put_u32(frame + 12, c->flow_id);
        dmesh_session_put_u32(frame + 16, c->generation);
        dmesh_session_put_u32(frame + 20, (uint32_t)c->status);
        memcpy(frame + 24, body, c->payload_len);
        int rc = dmesh_session_decode(frame, 24 + c->payload_len, &h, &payload);
        assert((rc == 0) == c->valid);
        if (rc == 0) {
            assert(h.type == c->type && h.flow_id == c->flow_id);
            assert(h.generation == c->generation && h.status == c->status);
            assert(h.payload_len == c->payload_len);
            if (h.payload_len) assert(*payload == body[0]);
        }
    }
}

static void test_frame_limits(void)
{
    uint8_t body[DMESH_SESSION_MAX_PAYLOAD];
    uint8_t frame[DMESH_SESSION_MAX_FRAME + 1];
    struct dmesh_session_header h;
    const uint8_t *payload;
    for (size_t i = 0; i < sizeof(body); ++i) body[i] = (uint8_t)(i * 37u);
    size_t len = dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_OPEN,
                                      1, 1, 0, body, sizeof(body));
    assert(len == DMESH_SESSION_MAX_FRAME);
    assert(dmesh_session_decode(frame, len, &h, &payload) == 0);
    assert(h.payload_len == sizeof(body) && memcmp(payload, body, sizeof(body)) == 0);
    assert(dmesh_session_encode(frame, len - 1, DMESH_SESSION_OPEN,
                                1, 1, 0, body, sizeof(body)) == 0);
    assert(dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_OPEN,
                                1, 1, 0, body, sizeof(body) + 1) == 0);
    assert(dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_OPEN,
                                1, 1, 0, body, SIZE_MAX) == 0);
    assert(dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_OPEN,
                                1, 1, 0, NULL, 1) == 0);
    assert(dmesh_session_encode(NULL, sizeof(frame), DMESH_SESSION_OPEN,
                                1, 1, 0, body, 1) == 0);

    /* A peer cannot smuggle extra bytes, unsupported versions, or a length
     * outside the protocol maximum into a callback's metadata parser. */
    assert(dmesh_session_decode(frame, len + 1, &h, &payload) == -1);
    frame[0] ^= 1;
    assert(dmesh_session_decode(frame, len, &h, &payload) == -1);
    frame[0] ^= 1;
    frame[4] = 1;   /* a v1 peer has no ARM/DOORBELL and must fail at HELLO */
    assert(dmesh_session_decode(frame, len, &h, &payload) == -1);
    frame[4] = 3;
    assert(dmesh_session_decode(frame, len, &h, &payload) == -1);
    frame[4] = DMESH_SESSION_VERSION;
    dmesh_session_put_u32(frame + 8, DMESH_SESSION_MAX_PAYLOAD + 1);
    assert(dmesh_session_decode(frame, sizeof(frame), &h, &payload) == -1);
    dmesh_session_put_u32(frame + 8, UINT32_MAX);
    assert(dmesh_session_decode(frame, len, &h, &payload) == -1);
}

static void test_interleaved_flow_tags(void)
{
    /* The single Comch connection may interleave opposite-direction metadata
     * and generations. Tags must survive independently of payload contents. */
    static const struct {
        uint16_t type;
        uint32_t flow, generation;
        uint8_t body[4];
    } messages[] = {
        {DMESH_SESSION_OPEN,           1, 7, {0x11, 0, 0, 1}},
        {DMESH_SESSION_OPEN,           2, 3, {0x22, 0, 0, 2}},
        {DMESH_SESSION_REVERSE_EXPORT, 2, 3, {0xb2, 0, 0, 2}},
        {DMESH_SESSION_REVERSE_EXPORT, 1, 7, {0xa1, 0, 0, 1}},
        {DMESH_SESSION_OPEN,           1, 8, {0x31, 0, 0, 1}},
        {DMESH_SESSION_REVERSE_EXPORT, 1, 7, {0xa1, 0, 0, 1}},
    };
    uint8_t frames[sizeof(messages) / sizeof(messages[0])][28];
    for (size_t i = 0; i < sizeof(messages) / sizeof(messages[0]); ++i) {
        assert(dmesh_session_encode(frames[i], sizeof(frames[i]), messages[i].type,
                                    messages[i].flow, messages[i].generation, 0,
                                    messages[i].body, sizeof(messages[i].body)) == 28);
    }
    for (size_t i = 0; i < sizeof(messages) / sizeof(messages[0]); ++i) {
        struct dmesh_session_header h;
        const uint8_t *payload;
        assert(dmesh_session_decode(frames[i], sizeof(frames[i]), &h, &payload) == 0);
        assert(h.type == messages[i].type && h.flow_id == messages[i].flow);
        assert(h.generation == messages[i].generation);
        assert(h.payload_len == 4 && memcmp(payload, messages[i].body, 4) == 0);
    }
}

static void test_arm_payload(void)
{
    struct dmesh_session_arm_flow in[DMESH_SESSION_MAX_FLOWS], out[DMESH_SESSION_MAX_FLOWS];
    uint8_t payload[DMESH_SESSION_ARM_HEADER_SIZE + DMESH_SESSION_MAX_FLOWS * DMESH_SESSION_ARM_FLOW_SIZE];
    uint64_t epoch;
    uint32_t count;
    for (uint32_t i = 0; i < DMESH_SESSION_MAX_FLOWS; ++i)
        in[i] = (struct dmesh_session_arm_flow){i + 1, 100 + i, UINT64_C(0x0102030405060708) + i};

    /* Golden bytes: little endian, independent of the host layout. */
    size_t len = dmesh_session_arm_encode(payload, sizeof(payload), UINT64_C(0x1122334455667788), in, 1);
    static const uint8_t golden[] = {
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 1, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 100, 0, 0, 0, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
    };
    assert(len == sizeof(golden) && memcmp(payload, golden, len) == 0);

    len = dmesh_session_arm_encode(payload, sizeof(payload), 7, in, DMESH_SESSION_MAX_FLOWS);
    assert(len == sizeof(payload));
    assert(dmesh_session_arm_decode(payload, (uint32_t)len, &epoch, out, &count) == 0);
    assert(epoch == 7 && count == DMESH_SESSION_MAX_FLOWS && memcmp(in, out, sizeof(in)) == 0);
    uint8_t frame[DMESH_SESSION_MAX_FRAME];
    assert(dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_ARM, 0, 0, 0,
                                payload, len) == DMESH_SESSION_HEADER_SIZE + len);

    assert(dmesh_session_arm_encode(payload, sizeof(payload), 7, in, DMESH_SESSION_MAX_FLOWS + 1) == 0);
    assert(dmesh_session_arm_encode(payload, len - 1, 7, in, DMESH_SESSION_MAX_FLOWS) == 0);
    assert(dmesh_session_arm_encode(payload, sizeof(payload), 7, NULL, 1) == 0);
    assert(dmesh_session_arm_encode(payload, sizeof(payload), 7, NULL, 0) == DMESH_SESSION_ARM_HEADER_SIZE);

    /* The count must agree with the length, the reserved word must be zero
     * and every entry must name a real flow. */
    len = dmesh_session_arm_encode(payload, sizeof(payload), 7, in, 2);
    assert(dmesh_session_arm_decode(payload, (uint32_t)len - 16, &epoch, out, &count) == -1);
    dmesh_session_put_u32(payload + 8, 3);
    assert(dmesh_session_arm_decode(payload, (uint32_t)len, &epoch, out, &count) == -1);
    dmesh_session_put_u32(payload + 8, 2);
    dmesh_session_put_u32(payload + 12, 1);
    assert(dmesh_session_arm_decode(payload, (uint32_t)len, &epoch, out, &count) == -1);
    dmesh_session_put_u32(payload + 12, 0);
    dmesh_session_put_u32(payload + 16, 0);
    assert(dmesh_session_arm_decode(payload, (uint32_t)len, &epoch, out, &count) == -1);
    dmesh_session_put_u32(payload + 16, DMESH_SESSION_MAX_FLOWS + 1);
    assert(dmesh_session_arm_decode(payload, (uint32_t)len, &epoch, out, &count) == -1);
    dmesh_session_put_u32(payload + 16, 1);
    dmesh_session_put_u32(payload + 20, 0);
    assert(dmesh_session_arm_decode(payload, (uint32_t)len, &epoch, out, &count) == -1);
}

int main(void)
{
    test_wire_layout();
    test_arm_payload();
    test_message_shapes();
    test_frame_limits();
    test_interleaved_flow_tags();
    puts("session_protocol_test: PASS");
    return 0;
}
