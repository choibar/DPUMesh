/* Run the production codec and handle-return gate without any device. */
#include "src/transport/peer/peer_channel.c"
#include "src/transport/peer/peer_security_wire.h"
#include <assert.h>
#include <stdio.h>

static uint8_t wire[65536], decoded[DMESH_PEER_FRAME_MAX];
static struct dmesh_peer_msg_header message;
static int blocked;
static unsigned commits;

static int reserve(void *conn, struct peer_wire_frame_lease *lease)
{
    (void)conn;
    if (blocked)
        return 0;
    *lease = (struct peer_wire_frame_lease){
        .header = wire, .body = wire + 4, .body_cap = sizeof(wire) - 4};
    return 1;
}
static int commit(void *conn, struct peer_wire_frame_lease *lease, size_t body,
                  const struct peer_wire_payload *payload)
{
    (void)conn;
    (void)lease;
    (void)payload;
    assert(peer_data_frame_size(wire) == body + 4);
    commits++;
    return 1;
}
static int cancel(void *conn, struct peer_wire_frame_lease *lease)
{
    (void)conn;
    (void)lease;
    return 0;
}

static size_t encode(struct peer_pair_wire *from, unsigned type, uint32_t handle, const void *body,
                     uint32_t bytes)
{
    long n = peer_pair_wire_encode_parts(from, type, handle, body, bytes, wire,
                                         wire + PEER_DATA_HEADER_BYTES,
                                         sizeof(wire) - PEER_DATA_HEADER_BYTES);
    assert(n >= PEER_DATA_HEADER_BYTES);
    assert(peer_data_frame_size(wire) == (size_t)n);
    return (size_t)n;
}
static void receive(struct peer_pair_wire *to, size_t n)
{
    assert(peer_pair_wire_decode(to, wire, n, 1, &message, decoded, sizeof(decoded)) == (long)n);
}
static void transfer(struct peer_pair_wire *from, struct peer_pair_wire *to, unsigned type,
                     uint32_t handle, const void *body, uint32_t bytes)
{
    receive(to, encode(from, type, handle, body, bytes));
}

static void open_stream(struct peer_pair_wire *a, struct peer_pair_wire *b, uint32_t token,
                        uint32_t handle)
{
    struct dmesh_peer_stream_open o = {.source_token = token,
                                       .dst_port = 8080,
                                       .src_generation = 1,
                                       .src_pod_uid = "src",
                                       .dst_pod_uid = "dst",
                                       .src_service_key = "ns/service"};
    transfer(a, b, DMESH_PEER_MSG_STREAM_OPEN, 0, &o, sizeof(o));
    assert(message.type == DMESH_PEER_MSG_STREAM_OPEN);
    assert(((struct dmesh_peer_stream_open *)decoded)->source_token == token);
    struct dmesh_peer_stream_open_ack ack = {.source_token = token, .handle = handle};
    transfer(b, a, DMESH_PEER_MSG_STREAM_OPEN_ACK, 0, &ack, sizeof(ack));
    assert(((struct dmesh_peer_stream_open_ack *)decoded)->handle == handle);
}

static void close_stream(struct peer_pair_wire *a, struct peer_pair_wire *b, uint32_t h,
                         struct dmesh_peer_channel *channel)
{
    uint32_t candidate = 0;
    uint8_t type = 0;
    transfer(a, b, DMESH_PEER_MSG_STREAM_FIN, h, NULL, 0);
    assert(peer_pair_wire_next_retire(b, 1, &candidate, &type) == 0); /* Half-close. */
    transfer(b, a, DMESH_PEER_MSG_STREAM_FIN, h, NULL, 0);
    assert(peer_pair_wire_next_retire(a, 1, &candidate, &type) == 0); /* Reader still holds it. */
    assert(!peer_pair_wire_local_closed(a, h));
    assert(peer_pair_wire_next_retire(a, 0, &candidate, &type) == 0); /* Delayed NIC CQ. */
    assert(peer_pair_wire_next_retire(a, 1, &candidate, &type) == 1);
    assert(candidate == h && type == DMESH_PEER_MSG_STREAM_READY);
    /* Would-block before encoding neither loses nor duplicates the action. */
    assert(peer_pair_wire_next_retire(a, 1, &candidate, &type) == 1);
    transfer(a, b, type, h, NULL, 0);
    assert(peer_pair_wire_next_retire(b, 1, &candidate, &type) ==
           0); /* Peer reader still holds it. */
    assert(!peer_pair_wire_local_closed(b, h));
    assert(peer_pair_wire_next_retire(b, 1, &candidate, &type) == 1);
    assert(type == DMESH_PEER_MSG_STREAM_READY);
    transfer(b, a, type, h, NULL, 0);
    if (channel) {
        peer_handle_release(channel, &channel->handles[0]);
        assert(channel->handles[0].in_use); /* Both FINs/READYs still do not permit reuse. */
    }
    assert(peer_pair_wire_next_retire(b, 0, &candidate, &type) == 0); /* READY send CQ delayed. */
    assert(peer_pair_wire_next_retire(b, 1, &candidate, &type) == 1);
    assert(type == DMESH_PEER_MSG_STREAM_RELEASE);
    transfer(b, a, type, h, NULL, 0);
    assert(peer_pair_wire_next_retire(a, 1, &candidate, &type) == 1);
    assert(type == DMESH_PEER_MSG_STREAM_RELEASE_ACK);
    size_t n = encode(a, type, h, NULL, 0);
    assert(!peer_pair_wire_released(b, h)); /* RELEASE_ACK delayed in transport. */
    receive(b, n);
    assert(!peer_pair_wire_released(b, h)); /* Final local send CQ must also settle. */
    assert(peer_pair_wire_next_retire(b, 0, &candidate, &type) == 0);
    assert(peer_pair_wire_next_retire(b, 1, &candidate, &type) == 2);
    assert(peer_pair_wire_released(a, h) && peer_pair_wire_released(b, h));
    if (channel) {
        peer_handle_release(channel, &channel->handles[0]);
        assert(!channel->handles[0].in_use && !channel->handle_count);
    }
    assert(peer_pair_wire_next_retire(a, 1, &candidate, &type) == 0);
    assert(peer_pair_wire_next_retire(b, 1, &candidate, &type) == 0);
}

static void control_header_versions(void)
{
    struct peer_sec_header headers[] = {
        {.type = PEER_SEC_HELLO, .operation = 1},
        {.type = PEER_SEC_PROPOSE, .operation = 2, .session = {1},
         .association = {2}, .epoch = 1},
    };
    for (unsigned i = 0; i < sizeof(headers) / sizeof(headers[0]); i++) {
        uint8_t encoded[PEER_SEC_HEADER_LEN];
        struct peer_sec_header out;
        assert(!peer_sec_header_encode(&headers[i], encoded));
        assert(encoded[4] == 0 && encoded[5] == PEER_SEC_VERSION);
        assert(!peer_sec_header_decode(encoded, sizeof(encoded), &out));
        assert(out.type == headers[i].type && out.operation == headers[i].operation);
        assert(out.epoch == headers[i].epoch && out.body_len == headers[i].body_len);
        assert(!memcmp(out.session, headers[i].session, sizeof(out.session)));
        assert(!memcmp(out.association, headers[i].association, sizeof(out.association)));

        const unsigned rejected[] = {2, PEER_SEC_VERSION + 1};
        for (unsigned j = 0; j < sizeof(rejected) / sizeof(rejected[0]); j++) {
            encoded[5] = rejected[j];
            assert(peer_sec_header_decode(encoded, sizeof(encoded), &out) == -1);
            assert(!out.type && !out.operation && !out.epoch);
        }
    }
}

static void wire_boundaries(void)
{
    _Static_assert(PEER_WIRE_HEADER_BYTES == 4, "four actual wire bytes");
    struct peer_sec_data_header h = {
        .type = PEER_SEC_DATA, .handle = DMESH_PEER_HANDLE_OWNER_BIT | 4095, .body_len = 65532};
    assert(!peer_sec_data_encode(&h, wire));
    const uint8_t expected[] = {0x7f, 0xff, 0xff, 0xfc};
    assert(!memcmp(wire, expected, 4));
    struct peer_sec_data_header out;
    assert(!peer_sec_data_decode(wire, 65536, &out) && out.handle == h.handle);
    assert(peer_sec_data_decode(wire, 65535, &out) == -1);
    h.body_len = 65533;
    assert(peer_sec_data_encode(&h, wire) == -1);
    h.body_len = 0;
    assert(peer_sec_data_encode(&h, wire) == -1);
    h.body_len = 1;
    h.handle = 4096;
    assert(peer_sec_data_encode(&h, wire) == -1);
    h.type = PEER_SEC_STREAM_OPEN_ACK;
    h.body_len = 8;
    h.handle = DMESH_PEER_HANDLE_OWNER_BIT;
    assert(peer_sec_data_encode(&h, wire) == -1);
    uint8_t legacy[64] = {0};
    memcpy(legacy, "DMSD", 4);
    assert(peer_sec_data_decode(legacy, sizeof(legacy), &out) == -1);
}

static void control_backpressure(const struct dmesh_peer_pair *pair)
{
    struct peer_pair_wire *a = peer_pair_wire_new(pair, 0);
    struct peer_pair_wire *b = peer_pair_wire_new(pair, DMESH_PEER_HANDLE_OWNER_BIT);
    uint32_t handle = DMESH_PEER_HANDLE_OWNER_BIT | 1;
    open_stream(a, b, 1, handle);
    const struct dmesh_peer_transport transport = {
        .frame_reserve = reserve, .frame_commit = commit, .frame_cancel = cancel};
    struct dmesh_peer_table table = {.transport = &transport};
    struct dmesh_peer_channel channel = {
        .pair_scoped = 1, .pair_wire = a, .conn = a, .state = DMESH_PEER_OPEN, .incarnation = 1};
    channel.tx_frame = calloc(1, sizeof(struct dmesh_peer_msg_header) + DMESH_PEER_FRAME_MAX);
    assert(channel.tx_frame);
    uint8_t previous[4];
    memcpy(previous, wire, sizeof(previous));
    blocked = 1;
    commits = 0;
    assert(peer_wire_send(&table, &channel, DMESH_PEER_MSG_STREAM_FIN, handle, NULL, 0) == 0);
    assert(channel.tx_len && channel.tx_parts_type == DMESH_PEER_MSG_STREAM_FIN);
    assert(peer_send_parts(&table, &channel, channel.tx_parts_type, handle, NULL, 0) == 1);
    assert(!commits && !memcmp(previous, wire, sizeof(previous)));
    blocked = 0;
    assert(peer_send_parts(&table, &channel, channel.tx_parts_type, handle, NULL, 0) == 0);
    assert(commits == 1);
    receive(b, 4);
    assert(message.type == DMESH_PEER_MSG_STREAM_FIN);
    free(channel.tx_frame);
    peer_pair_wire_free(a);
    peer_pair_wire_free(b);
}

int main(void)
{
    control_header_versions();
    wire_boundaries();
    struct dmesh_peer_pair pair = {.lane_id = 1, .lane_generation = 1, .association = {1}};
    control_backpressure(&pair);
    struct peer_pair_wire *a = peer_pair_wire_new(&pair, 0);
    struct peer_pair_wire *b = peer_pair_wire_new(&pair, DMESH_PEER_HANDLE_OWNER_BIT);
    assert(a && b);
    uint32_t handle = DMESH_PEER_HANDLE_OWNER_BIT | 1;
    struct dmesh_peer_channel channel = {.pair_scoped = 1, .pair_wire = b, .handle_count = 1};
    channel.handles = calloc(1, sizeof(*channel.handles));
    assert(channel.handles);
    channel.handles[0] = (struct dmesh_peer_handle){.in_use = 1, .wire_handle = handle};
    for (uint32_t token = 1; token <= 65537; token++) {
        open_stream(a, b, token, handle);
        struct {
            struct dmesh_peer_data_prefix prefix;
            uint8_t value;
        } data = {.value = 42};
        transfer(a, b, DMESH_PEER_MSG_DATA, handle, &data, sizeof(data.prefix) + 1);
        assert(message.type == DMESH_PEER_MSG_DATA && message.length == sizeof(data.prefix) + 1);
        assert(((struct dmesh_peer_data_prefix *)decoded)->seq == 0 &&
               decoded[sizeof(data.prefix)] == 42);
        assert(peer_data_body_length(wire) == 1 && peer_data_frame_size(wire) == 5);
        if (token == 1) {
            uint8_t *maximum = calloc(1, sizeof(data.prefix) + PEER_DATA_BODY_MAX);
            assert(maximum);
            struct dmesh_peer_data_prefix prefix = {.seq = 1};
            memcpy(maximum, &prefix, sizeof(prefix));
            transfer(a, b, DMESH_PEER_MSG_DATA, handle, maximum,
                     sizeof(prefix) + PEER_DATA_BODY_MAX);
            assert(message.length == sizeof(prefix) + PEER_DATA_BODY_MAX);
            assert(peer_data_frame_size(wire) == 65536);
            free(maximum);
        }
        struct dmesh_peer_ack_entry ack = {
            .handle = handle, .seq_first = 0, .seq_count = token == 1 ? 2 : 1};
        transfer(b, a, DMESH_PEER_MSG_STREAM_ACK, 0, &ack, sizeof(ack));
        assert(message.type == DMESH_PEER_MSG_STREAM_ACK && message.length == sizeof(ack));
        close_stream(a, b, handle, token == 1 ? &channel : NULL);
    }
    /* Opposite owner namespaces use the same slot without aliasing. */
    open_stream(a, b, 65538, handle);
    open_stream(b, a, 1, 1);
    uint8_t hdr[4];
    assert(!peer_pair_wire_data_header(a, handle, 0, 1, hdr));
    assert(!peer_pair_wire_data_header(b, 1, 0, 1, hdr));
    assert(peer_pair_wire_data_header(a, 1, 1, 1, hdr) == -1);
    free(channel.handles);
    peer_pair_wire_free(a);
    peer_pair_wire_free(b);
    puts("dmesh4: exact wire, small/max frames, namespace isolation, bilateral/CQ/reader barriers, "
         "65537 slot reuses: PASS");
    return 0;
}
