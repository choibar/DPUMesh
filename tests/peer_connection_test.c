#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
/* Exercise ownership boundaries with delayed peer ACK/DMA completion. The
 * hardware-independent adapter is included to inject transport completions. */
#include "src/transport/peer/peer_connection_io.c"
#include "src/transport/peer/peer_carrier_write_imm.c"
#include <assert.h>

static unsigned acknowledgements, releases, resets;
static uint32_t last_bytes, last_seq;
static const uint8_t *last_source;
static struct peer_carrier_payload last_payload;
static struct peer_write_tx_ring nic;
static enum dmesh_peer_refusal send_result = DMESH_PEER_OK;
void dmesh_peer_delivered(struct dmesh_peer_table *t, struct dmesh_peer_channel *c, uint32_t h,
                          uint32_t seq, uint32_t bytes)
{
    (void)t;
    (void)c;
    (void)h;
    ++acknowledgements;
    last_seq = seq;
    last_bytes = bytes;
}
void dmesh_peer_source_delivered(struct dmesh_peer_table *t, struct dmesh_peer_channel *c,
                                 uint32_t h, uint32_t seq, uint32_t bytes)
{
    dmesh_peer_delivered(t, c, h, seq, bytes);
}
int dmesh_peer_channel_release(struct dmesh_peer_channel *c, uint32_t incarnation)
{
    (void)c;
    (void)incarnation;
    ++releases;
    return 0;
}
void dmesh_peer_reset(struct dmesh_peer_table *t, struct dmesh_peer_channel *c, const char *why)
{
    (void)t;
    (void)why;
    ++resets;
    c->state = DMESH_PEER_CLOSED;
}
enum dmesh_peer_refusal dmesh_peer_connection_data_send(struct dmesh_peer_table *t,
                                                    struct dmesh_peer_channel *c, uint32_t h,
                                                    uint32_t seq, const uint8_t *data,
                                                    uint32_t bytes, uint8_t kind, void *cookie)
{
    (void)t;
    (void)c;
    (void)h;
    (void)seq;
    if (send_result == DMESH_PEER_REFUSE_INFLIGHT) return send_result;
    assert(t->ops->tx_payload(t->ops_ctx, kind, cookie, data, bytes, &last_payload) == 1);
    assert(peer_carrier_frame_payload_valid(&last_payload, bytes));
    assert(last_payload.data == data);
    memset(last_payload.tx_header, 0x7e, PEER_CARRIER_HEADER_BYTES);
    uint32_t wire_bytes = bytes + PEER_CARRIER_HEADER_BYTES;
    assert(!peer_write_tx_ring_hold(&nic, nic.tail + wire_bytes, last_payload.owner,
        last_payload.cookie, last_payload.retain, last_payload.release));
    assert(!peer_write_tx_ring_post(&nic, wire_bytes, UINT32_MAX));
    last_source = data;
    last_bytes = bytes;
    return send_result;
}

static void arena_lifetime(void)
{
    struct peer_connection_worker *w = calloc(1, sizeof(*w));
    static const struct dmesh_peer_ops ops = {.tx_payload = tx_payload};
    w->table.ops = &ops;
    struct peer_connection *s = allocate(w, 0);
    struct dmesh_peer_channel c = {.state = DMESH_PEER_OPEN};
    s->channel = &c;
    s->info.state = 2;
    uint32_t token = s->info.token, id;
    void *data;
    size_t cap;
    assert(peer_connection_tx_acquire(w, token, &id, &data, &cap) == 1);
    memset(data, 0x42, 7);
    assert(peer_connection_send(w, token, id, 7) == 7);
    release(NULL, DMESH_PEER_CUSTODY_L7, s, 7); /* peer ACK beats local NIC CQ */
    assert(w->tx_arena.busy == 1);
    assert(peer_connection_detach(w, token) == 1); /* connection may retire independently */
    struct peer_connection *replacement = allocate(w, 0);
    replacement->info.state = 2;
    uint32_t other;
    void *other_data;
    assert(peer_connection_tx_acquire(w, replacement->info.token, &other, &other_data, &cap) == 1);
    assert(other != id && other_data != data && !memcmp(data, "BBBBBBB", 7));
    assert(!peer_write_tx_ring_done(&nic, nic.tail));
    assert(peer_connection_tx_acquire(w, replacement->info.token, &other, &other_data, &cap) == 1);
    assert(other == id && other_data == data);
    assert(peer_connection_send(w, token, other, 7) == -ESTALE); /* old token cannot publish it */

    /* A partially accepted failed post can still read its source. Clear Rust's
     * reservation on detach, but keep the carrier hold until a destroy fence. */
    send_result = DMESH_PEER_REFUSE_TRANSPORT;
    assert(peer_connection_send(w, replacement->info.token, other, 7) == -EIO);
    assert(peer_connection_detach(w, replacement->info.token) == 1);
    assert(w->tx_arena.busy == 1 && !memcmp(other_data, "BBBBBBB", 7));
    peer_write_tx_ring_fenced(&nic);
    assert(w->tx_arena.busy == 0);
    nic = (struct peer_write_tx_ring){0};
    send_result = DMESH_PEER_OK;
    free(w->tx_arena.data);
    free(w);
}

static void arena_limits(void)
{
    struct peer_connection_worker *w = calloc(1, sizeof(*w));
    uint32_t tokens[PEER_ARENA_CHUNKS / TX_CHUNKS_PER_CONNECTION + 1];
    for (unsigned i = 0; i < sizeof(tokens) / sizeof(tokens[0]); ++i) {
        struct peer_connection *s = allocate(w, 0);
        assert(s);
        s->info.state = 2;
        tokens[i] = s->info.token;
        for (unsigned j = 0; j < TX_CHUNKS_PER_CONNECTION; ++j) {
            uint32_t id;
            void *data;
            size_t cap;
            int rc = peer_connection_tx_acquire(w, tokens[i], &id, &data, &cap);
            if (i == PEER_ARENA_CHUNKS / TX_CHUNKS_PER_CONNECTION) {
                assert(rc == 0 && !id && !data && !cap);
                break;
            }
            assert(rc == 1 && cap == TX_CHUNK_BODY);
        }
        uint32_t id;
        void *data;
        size_t cap;
        assert(!peer_connection_tx_acquire(w, tokens[i], &id, &data, &cap));
    }
    assert(w->tx_arena.busy == PEER_ARENA_CHUNKS);
    for (unsigned i = 0; i < sizeof(tokens) / sizeof(tokens[0]); ++i)
        assert(peer_connection_detach(w, tokens[i]) == 1);
    assert(!w->tx_arena.busy);
    free(w->tx_arena.data);
    free(w);
}

int main(void)
{
    struct peer_connection_worker *w = calloc(1, sizeof(*w));
    assert(w);
    struct peer_connection *s = allocate(w, 0), *second = allocate(w, 0);
    assert(s && second);
    uint32_t token = s->info.token, second_token = second->info.token;
    struct dmesh_peer_channel channel = {.state = DMESH_PEER_OPEN};
    s->channel = &channel;
    s->held = 1;
    s->info.state = 2;
    s->info.incoming = 1;
    static const struct dmesh_peer_ops test_ops = {.tx_payload = tx_payload};
    w->table.ops = &test_ops;
    w->table.ops_ctx = w;

    /* The H2 writer receives the real NIC source, including exclusive header
     * headroom. Pressure retries must preserve both pointer and payload. */
    uint32_t chunk;
    void *tx;
    size_t cap;
    assert(peer_connection_tx_acquire(w, token, &chunk, &tx, &cap) == 1 && cap == TX_CHUNK_BODY);
    memset(tx, 0x5a, cap);
    send_result = DMESH_PEER_REFUSE_INFLIGHT;
    assert(!peer_connection_send(w, token, chunk, cap) && w->tx_arena.busy == 1);
    send_result = DMESH_PEER_OK;
    assert(peer_connection_send(w, token, chunk, cap + 1) == -EINVAL);
    assert(peer_connection_send(w, token, chunk, cap) == (int)cap);
    assert(last_bytes == cap && last_source == tx && w->tx_arena.busy == 1);
    assert(((uint8_t *)tx)[0] == 0x5a && ((uint8_t *)tx)[cap - 1] == 0x5a);
    assert(peer_connection_send(w, token, chunk, 1) == -EINVAL); /* submitted chunk is immutable */

    uint32_t next_chunk;
    void *next_tx;
    assert(peer_connection_tx_acquire(w, token, &next_chunk, &next_tx, &cap) == 1);
    assert(next_chunk != chunk && next_tx != tx);
    assert(!peer_write_tx_ring_done(&nic, nic.tail) && w->tx_arena.busy == 1);
    /* NIC completion returns only its own chunk, not the next writable chunk. */
    assert(peer_connection_tx_acquire(w, token, &chunk, &tx, &cap) == 1);
    assert(tx == last_source);

    uint8_t data[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    uint32_t pos, bytes;
    uint64_t near_end = PEER_CONNECTION_RX_BYTES - 4;
    s->rx_received = s->rx_published = s->rx_consumed = near_end;
    assert(receive_bytes(s, data, 8, 11) == 1);
    assert(peer_connection_receive(w, token, &pos, &bytes) == 1 && pos == near_end && bytes == 4);
    assert(!memcmp(s->rx + pos, data, 4));
    assert(!peer_connection_consumed(w, token, PEER_CONNECTION_RX_BYTES));
    assert(acknowledgements == 0); /* A partial extent is not ACKed. */
    assert(peer_connection_receive(w, token, &pos, &bytes) == 1 && pos == 0 && bytes == 4);
    assert(!memcmp(s->rx, data + 4, 4));
    assert(!peer_connection_consumed(w, token, 4));
    assert(acknowledgements == 1 && last_seq == 11 && last_bytes == 8);
    assert(!peer_connection_consumed(w, token, 4) && acknowledgements == 1);
    assert(peer_connection_consumed(w, token, 5) == -EINVAL);

    /* A full ring has the same modulo position, but is a distinct consumption. */
    uint8_t *full = calloc(1, PEER_CONNECTION_RX_BYTES);
    assert(full && receive_bytes(s, full, PEER_CONNECTION_RX_BYTES, 12) == 1);
    assert(receive_bytes(s, data, 1, 13) == -1);
    assert(peer_connection_receive(w, token, &pos, &bytes) == 1);
    assert(peer_connection_receive(w, token, &pos, &bytes) == 1);
    assert(!peer_connection_consumed(w, token, 4) && acknowledgements == 2);
    free(full);

    /* Dropping Rust IO is insufficient while the transport retains TX custody. */
    assert(peer_connection_detach(w, token) == 0 && resets == 1 && releases == 0);
    assert(lookup(w, token) && w->live.count == 2);
    release(NULL, DMESH_PEER_CUSTODY_L7, s, (uint32_t)s->tx_sent);
    assert(peer_connection_detach(w, token) == 1 && releases == 1);
    assert(!lookup(w, token) && w->live.count == 1);
    unsigned cursor = 0;
    struct peer_connection_info info;
    assert(peer_connection_next(w, &cursor, &info) == 1 && info.token == second_token);
    struct peer_connection *reused = allocate(w, 0);
    assert(reused == s && reused->info.token != token && !lookup(w, token));
    assert(peer_connection_detach(w, reused->info.token) == 1);
    assert(peer_connection_detach(w, second_token) == 1);
    assert(w->live.count == 0);

    /* Clean EOF may race the next C maintenance pass. Reader detach must
     * publish the real local reference fence, without resetting sibling connections. */
    struct dmesh_peer_pair pair = {.lane_id = 1, .lane_generation = 1, .association = {1}};
    struct peer_pair_protocol *a = peer_pair_protocol_new(&pair, 0);
    struct peer_pair_protocol *b = peer_pair_protocol_new(&pair, DMESH_PEER_HANDLE_OWNER_BIT);
    uint8_t frame[2048], body[2048];
    struct dmesh_peer_msg_header h;
    struct dmesh_peer_connection_open open = {.source_token = 1,
                                          .dst_port = 80,
                                          .src_generation = 1,
                                          .src_pod_uid = "src",
                                          .dst_pod_uid = "dst",
                                          .src_service_key = "ns/service"};
    long n = peer_pair_protocol_encode_parts(a, DMESH_PEER_MSG_STREAM_OPEN, 0, &open, sizeof(open),
                                         frame, frame + 4, sizeof(frame) - 4);
    assert(n > 0 && peer_pair_protocol_decode(b, frame, n, 1, &h, body, sizeof(body)) == n);
    uint32_t handle = DMESH_PEER_HANDLE_OWNER_BIT | 1;
    struct dmesh_peer_connection_open_ack ack = {.source_token = 1, .handle = handle};
    n = peer_pair_protocol_encode_parts(b, DMESH_PEER_MSG_STREAM_OPEN_ACK, 0, &ack, sizeof(ack), frame,
                                    frame + 4, sizeof(frame) - 4);
    assert(n > 0 && peer_pair_protocol_decode(a, frame, n, 1, &h, body, sizeof(body)) == n);
    n = peer_pair_protocol_encode_parts(a, DMESH_PEER_MSG_STREAM_FIN, handle, NULL, 0, frame, frame + 4,
                                    sizeof(frame) - 4);
    assert(n == 4 && peer_pair_protocol_decode(b, frame, n, 1, &h, body, sizeof(body)) == n);
    n = peer_pair_protocol_encode_parts(b, DMESH_PEER_MSG_STREAM_FIN, handle, NULL, 0, frame, frame + 4,
                                    sizeof(frame) - 4);
    assert(n == 4 && peer_pair_protocol_decode(a, frame, n, 1, &h, body, sizeof(body)) == n);
    channel = (struct dmesh_peer_channel){
        .state = DMESH_PEER_OPEN, .pair_scoped = 1, .pair_protocol = a, .incarnation = 7};
    struct peer_connection *clean = allocate(w, 0);
    assert(clean);
    clean->channel = &channel;
    clean->incarnation = 7;
    clean->handle = handle;
    clean->info.state = 2;
    clean->tx_fin = clean->rx_fin = 1;
    clean->held = 1;
    assert(!index_add(w, clean));
    uint32_t candidate;
    uint8_t action;
    assert(peer_pair_protocol_next_retire(a, 1, &candidate, &action) == 0);
    unsigned old_resets = resets;
    assert(peer_connection_detach(w, clean->info.token) == 1 && resets == old_resets);
    assert(peer_pair_protocol_next_retire(a, 0, &candidate, &action) == 0);
    assert(peer_pair_protocol_next_retire(a, 1, &candidate, &action) == 1 &&
           action == DMESH_PEER_MSG_STREAM_READY);
    peer_pair_protocol_free(a);
    peer_pair_protocol_free(b);
    assert(w->tx_arena.busy == 0);
    free(w->tx_arena.data);
    free(w);
    arena_lifetime();
    arena_limits();
    puts("peer connection: arena TX, NIC/ACK lifetime, failed posts, caps, RX wrap, token reuse: PASS");
}
