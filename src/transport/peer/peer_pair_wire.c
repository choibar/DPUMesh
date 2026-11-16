#include "peer_pair_wire.h"
#include "peer_security_wire.h"
#include <stdlib.h>
#include <string.h>

#define OPEN_BODY (2u * DMESH_POD_UID_MAX + DMESH_K8S_NAMESPACE_MAX + DMESH_SVC_NAME_MAX + 14u)
#define ACK_ENTRY 10u
#define STREAMS (2u * DMESH_PEER_STREAMS_MAX)
enum {
    LIVE = 1,
    TX_FIN = 2,
    RX_FIN = 4,
    CLOSED = 8,
    READY = 16,
    PEER_READY = 32,
    RELEASE = 64,
    PEER_RELEASE = 128,
    RELEASED = 256,
    ACK_RECEIVED = 512
};
struct stream {
    uint64_t tx_seq, rx_seq;
    uint16_t flags, next;
};
struct peer_pair_wire {
    struct dmesh_peer_pair pair;
    uint32_t pending[DMESH_PEER_STREAMS_MAX], received_token, last_token, owner;
    struct stream streams[STREAMS];
    uint16_t first, last, retiring;
};

static void put(uint8_t *p, uint64_t n, unsigned bytes)
{
    for (unsigned i = 0; i < bytes; i++)
        p[i] = (uint8_t)(n >> (8u * (bytes - i - 1)));
}
static uint64_t get(const uint8_t *p, unsigned bytes)
{
    uint64_t n = 0;
    for (unsigned i = 0; i < bytes; i++)
        n = (n << 8) | p[i];
    return n;
}
static int text_valid(const char *p, size_t len)
{
    size_t n = strnlen(p, len);
    if (!n || n == len)
        return 0;
    for (size_t i = 0; i < n; i++)
        if ((unsigned char)p[i] < 32 || (unsigned char)p[i] > 126)
            return 0;
    for (size_t i = n; i < len; i++)
        if (p[i])
            return 0;
    return 1;
}
static struct stream *stream(struct peer_pair_wire *w, uint32_t handle)
{
    uint32_t index = handle & DMESH_PEER_HANDLE_INDEX_MASK;
    if (!w || !index || index > DMESH_PEER_STREAMS_MAX ||
        (handle & ~(DMESH_PEER_HANDLE_OWNER_BIT | PEER_DATA_SLOT_MASK)))
        return NULL;
    return &w->streams[index - 1 +
                       ((handle & DMESH_PEER_HANDLE_OWNER_BIT) ? DMESH_PEER_STREAMS_MAX : 0)];
}
static int owner(const struct peer_pair_wire *w, uint32_t handle)
{
    return (handle & DMESH_PEER_HANDLE_OWNER_BIT) == w->owner;
}
static void enqueue(struct peer_pair_wire *w, struct stream *s)
{
    unsigned id = (unsigned)(s - w->streams) + 1;
    if (s->next || w->last == id)
        return;
    if (w->last)
        w->streams[w->last - 1].next = id;
    else
        w->first = id;
    w->last = id;
    w->retiring++;
}
static void remove_retire(struct peer_pair_wire *w, struct stream *s)
{
    unsigned id = (unsigned)(s - w->streams) + 1, previous = 0, current = w->first;
    while (current && current != id) {
        previous = current;
        current = w->streams[current - 1].next;
    }
    if (!current)
        return;
    if (previous)
        w->streams[previous - 1].next = s->next;
    else
        w->first = s->next;
    if (w->last == id)
        w->last = previous;
    s->next = 0;
    w->retiring--;
}
static int bind(struct peer_pair_wire *w, uint32_t handle)
{
    struct stream *s = stream(w, handle);
    if (!s || (s->flags && s->flags != RELEASED))
        return -1;
    *s = (struct stream){.flags = LIVE};
    return 0;
}
struct peer_pair_wire *peer_pair_wire_new(const struct dmesh_peer_pair *p, uint32_t local_owner)
{
    if (!p || !peer_sec_nonzero(p->association, 16) || !p->lane_id || !p->lane_generation ||
        (local_owner && local_owner != DMESH_PEER_HANDLE_OWNER_BIT))
        return NULL;
    struct peer_pair_wire *w = calloc(1, sizeof(*w));
    if (w) {
        w->pair = *p;
        w->owner = local_owner;
    }
    return w;
}
void peer_pair_wire_free(struct peer_pair_wire *w)
{
    free(w);
}
int peer_pair_wire_retiring(const struct peer_pair_wire *w)
{
    return w && w->retiring != 0;
}
int peer_pair_wire_local_closed(struct peer_pair_wire *w, uint32_t handle)
{
    struct stream *s = stream(w, handle);
    if (!s || !(s->flags & LIVE))
        return -1;
    s->flags |= CLOSED;
    enqueue(w, s);
    return 0;
}
int peer_pair_wire_released(struct peer_pair_wire *w, uint32_t handle)
{
    struct stream *s = stream(w, handle);
    return s && s->flags == RELEASED;
}

/* Only retiring streams enter this queue. Rotate it; no full stream-table scan.
 * can_ready certifies local NIC/custody/ACK fences, independently of FIN. */
int peer_pair_wire_next_retire(struct peer_pair_wire *w, int can_ready, uint32_t *handle,
                               uint8_t *type)
{
    if (!w || !handle || !type)
        return -1;
    unsigned count = w->retiring;
    while (count--) {
        unsigned id = w->first;
        struct stream *s = &w->streams[id - 1];
        remove_retire(w, s);
        enqueue(w, s);
        unsigned index = id - 1;
        *handle = (index % DMESH_PEER_STREAMS_MAX + 1) |
                  (index >= DMESH_PEER_STREAMS_MAX ? DMESH_PEER_HANDLE_OWNER_BIT : 0);
        if ((s->flags & (LIVE | TX_FIN | RX_FIN | CLOSED)) != (LIVE | TX_FIN | RX_FIN | CLOSED))
            continue;
        if (!can_ready)
            continue;
        if (s->flags & ACK_RECEIVED) {
            remove_retire(w, s);
            s->flags = RELEASED;
            *type = DMESH_PEER_MSG_STREAM_RELEASE_ACK;
            return 2;
        }
        if (!(s->flags & READY)) {
            *type = DMESH_PEER_MSG_STREAM_READY;
            return 1;
        }
        if ((s->flags & (READY | PEER_READY)) != (READY | PEER_READY))
            continue;
        if (owner(w, *handle) && !(s->flags & RELEASE)) {
            *type = DMESH_PEER_MSG_STREAM_RELEASE;
            return 1;
        }
        if (!owner(w, *handle) && (s->flags & PEER_RELEASE)) {
            *type = DMESH_PEER_MSG_STREAM_RELEASE_ACK;
            return 1;
        }
    }
    return 0;
}

int peer_pair_wire_data_header(struct peer_pair_wire *w, uint32_t handle, uint32_t seq,
                               uint32_t len, uint8_t header[PEER_DATA_HEADER_BYTES])
{
    struct stream *s = stream(w, handle);
    if (!s || !(s->flags & LIVE) || (s->flags & (TX_FIN | CLOSED)) || s->tx_seq != seq || !len ||
        len > PEER_SEC_DATA_MAX)
        return -1;
    struct peer_sec_data_header h = {.type = PEER_SEC_DATA, .handle = handle, .body_len = len};
    if (peer_sec_data_encode(&h, header))
        return -1;
    s->tx_seq++;
    return 0;
}

long peer_pair_wire_encode_parts(struct peer_pair_wire *w, uint8_t type, uint32_t handle,
                                 const void *body, uint32_t length,
                                 uint8_t header[PEER_DATA_HEADER_BYTES], void *output,
                                 size_t capacity)
{
    if (!w || !header || !output || (length && !body))
        return -1;
    uint8_t *out = output;
    struct peer_sec_data_header h = {.handle = handle};
    struct stream *s = stream(w, handle);
    switch (type) {
    case DMESH_PEER_MSG_STREAM_OPEN: {
        if (handle || length != sizeof(struct dmesh_peer_stream_open) || capacity < OPEN_BODY)
            return -1;
        struct dmesh_peer_stream_open o;
        memcpy(&o, body, sizeof(o));
        if (!text_valid(o.src_pod_uid, sizeof(o.src_pod_uid)) ||
            !text_valid(o.dst_pod_uid, sizeof(o.dst_pod_uid)) ||
            !text_valid(o.src_service_key, sizeof(o.src_service_key)) || !o.dst_port ||
            o.reserved || !o.src_generation || !o.source_token || o.source_token <= w->last_token)
            return -1;
        unsigned i = 0;
        while (i < DMESH_PEER_STREAMS_MAX && w->pending[i])
            i++;
        if (i == DMESH_PEER_STREAMS_MAX)
            return -1;
        w->pending[i] = w->last_token = o.source_token;
        put(out, o.source_token, 4);
        out += 4;
        memcpy(out, o.src_pod_uid, sizeof(o.src_pod_uid));
        out += sizeof(o.src_pod_uid);
        memcpy(out, o.dst_pod_uid, sizeof(o.dst_pod_uid));
        out += sizeof(o.dst_pod_uid);
        memcpy(out, o.src_service_key, sizeof(o.src_service_key));
        out += sizeof(o.src_service_key);
        put(out, o.dst_port, 2);
        put(out + 2, o.src_generation, 8);
        h.type = PEER_SEC_STREAM_OPEN;
        h.body_len = OPEN_BODY;
        break;
    }
    case DMESH_PEER_MSG_STREAM_OPEN_ACK: {
        if (handle || length != sizeof(struct dmesh_peer_stream_open_ack) || capacity < 8)
            return -1;
        struct dmesh_peer_stream_open_ack a;
        memcpy(&a, body, sizeof(a));
        if (!w->received_token || a.source_token != w->received_token || a.reserved ||
            a.status < 0 || a.status >= DMESH_PEER_REFUSE_MAX ||
            (a.status ? a.handle != 0 : (!owner(w, a.handle) || bind(w, a.handle))))
            return -1;
        h.type = PEER_SEC_STREAM_OPEN_ACK;
        h.handle = a.handle;
        h.body_len = 8;
        put(out, a.source_token, 4);
        put(out + 4, (uint32_t)a.status, 4);
        w->received_token = 0;
        break;
    }
    case DMESH_PEER_MSG_DATA: {
        if (length <= sizeof(struct dmesh_peer_data_prefix))
            return -1;
        struct dmesh_peer_data_prefix p;
        memcpy(&p, body, sizeof(p));
        h.body_len = length - sizeof(p);
        if (p.reserved || h.body_len > capacity ||
            peer_pair_wire_data_header(w, handle, p.seq, h.body_len, header))
            return -1;
        memcpy(out, (const uint8_t *)body + sizeof(p), h.body_len);
        return PEER_DATA_HEADER_BYTES + h.body_len;
    }
    case DMESH_PEER_MSG_STREAM_FIN:
        if (!s || !(s->flags & LIVE) || (s->flags & TX_FIN) || length)
            return -1;
        s->flags |= TX_FIN;
        h.type = PEER_SEC_STREAM_FIN;
        break;
    case DMESH_PEER_MSG_STREAM_ACK: {
        if (handle || !length || length % sizeof(struct dmesh_peer_ack_entry))
            return -1;
        unsigned count = length / sizeof(struct dmesh_peer_ack_entry);
        if (count > DMESH_STREAM_ACK_BATCH || capacity < count * ACK_ENTRY)
            return -1;
        h.type = PEER_SEC_STREAM_ACK;
        h.body_len = count * ACK_ENTRY;
        for (unsigned i = 0; i < count; i++) {
            struct dmesh_peer_ack_entry a;
            memcpy(&a, (const uint8_t *)body + i * sizeof(a), sizeof(a));
            s = stream(w, a.handle);
            if (!s || !(s->flags & LIVE) || (s->flags & READY) || a.reserved || !a.seq_count ||
                a.seq_count > DMESH_PEER_TX_SLOTS)
                return -1;
            put(out,
                (a.handle & PEER_DATA_SLOT_MASK) |
                    ((a.handle & DMESH_PEER_HANDLE_OWNER_BIT) ? PEER_DATA_OWNER : 0),
                2);
            put(out + 2, a.seq_first, 4);
            put(out + 6, a.seq_count, 4);
            out += ACK_ENTRY;
        }
        break;
    }
    case DMESH_PEER_MSG_STREAM_READY:
        if (!s || length ||
            (s->flags & (LIVE | TX_FIN | RX_FIN | CLOSED | READY)) !=
                (LIVE | TX_FIN | RX_FIN | CLOSED))
            return -1;
        s->flags |= READY;
        h.type = PEER_SEC_STREAM_READY;
        break;
    case DMESH_PEER_MSG_STREAM_RELEASE:
        if (!s || length || !owner(w, handle) ||
            (s->flags & (READY | PEER_READY | RELEASE)) != (READY | PEER_READY))
            return -1;
        s->flags |= RELEASE;
        h.type = PEER_SEC_STREAM_RELEASE;
        break;
    case DMESH_PEER_MSG_STREAM_RELEASE_ACK:
        if (!s || length || owner(w, handle) ||
            (s->flags & (READY | PEER_READY | PEER_RELEASE)) != (READY | PEER_READY | PEER_RELEASE))
            return -1;
        remove_retire(w, s);
        s->flags = RELEASED;
        h.type = PEER_SEC_STREAM_RELEASE_ACK;
        break;
    default:
        return -1;
    }
    if (peer_sec_data_encode(&h, header))
        return -1;
    return PEER_DATA_HEADER_BYTES + h.body_len;
}

long peer_pair_wire_encode(struct peer_pair_wire *w, const void *input, size_t len, void *output,
                           size_t capacity)
{
    if (!input || !output || len < sizeof(struct dmesh_peer_msg_header) ||
        capacity < PEER_DATA_HEADER_BYTES)
        return -1;
    struct dmesh_peer_msg_header in;
    memcpy(&in, input, sizeof(in));
    if (in.version != 1 || in.reserved || len != sizeof(in) + in.length)
        return -1;
    return peer_pair_wire_encode_parts(
        w, in.type, in.handle, (const uint8_t *)input + sizeof(in), in.length, output,
        (uint8_t *)output + PEER_DATA_HEADER_BYTES, capacity - PEER_DATA_HEADER_BYTES);
}

static long decode_parts(struct peer_pair_wire *w, const void *input, const uint8_t *body,
                         size_t length, uint32_t incarnation, struct dmesh_peer_msg_header *out,
                         void *payload, size_t capacity, uint32_t *data_seq)
{
    struct peer_sec_data_header h;
    if (!w || !input || !out || !payload || !incarnation || (length && !body) ||
        length > PEER_SEC_DATA_MAX ||
        peer_sec_data_decode(input, PEER_DATA_HEADER_BYTES + length, &h))
        return -1;
    *out = (struct dmesh_peer_msg_header){
        .version = 1, .incarnation = incarnation, .handle = h.handle};
    struct stream *s = stream(w, h.handle);
    if (h.type != PEER_SEC_STREAM_OPEN && h.type != PEER_SEC_STREAM_OPEN_ACK &&
        h.type != PEER_SEC_STREAM_ACK && (!s || !(s->flags & LIVE)))
        return -1;
    switch (h.type) {
    case PEER_SEC_STREAM_OPEN: {
        if (h.handle || length != OPEN_BODY || capacity < sizeof(struct dmesh_peer_stream_open) ||
            w->received_token)
            return -1;
        struct dmesh_peer_stream_open o = {.source_token = (uint32_t)get(body, 4)};
        body += 4;
        memcpy(o.src_pod_uid, body, sizeof(o.src_pod_uid));
        body += sizeof(o.src_pod_uid);
        memcpy(o.dst_pod_uid, body, sizeof(o.dst_pod_uid));
        body += sizeof(o.dst_pod_uid);
        memcpy(o.src_service_key, body, sizeof(o.src_service_key));
        body += sizeof(o.src_service_key);
        o.dst_port = (uint16_t)get(body, 2);
        o.src_generation = get(body + 2, 8);
        if (!o.source_token || !o.dst_port || !o.src_generation ||
            !text_valid(o.src_pod_uid, sizeof(o.src_pod_uid)) ||
            !text_valid(o.dst_pod_uid, sizeof(o.dst_pod_uid)) ||
            !text_valid(o.src_service_key, sizeof(o.src_service_key)))
            return -1;
        w->received_token = o.source_token;
        memcpy(payload, &o, sizeof(o));
        out->type = DMESH_PEER_MSG_STREAM_OPEN;
        out->handle = 0;
        out->length = sizeof(o);
        break;
    }
    case PEER_SEC_STREAM_OPEN_ACK: {
        if (length != 8 || capacity < sizeof(struct dmesh_peer_stream_open_ack))
            return -1;
        struct dmesh_peer_stream_open_ack a = {.source_token = (uint32_t)get(body, 4),
                                               .handle = h.handle};
        uint64_t status = get(body + 4, 4);
        unsigned i = 0;
        while (i < DMESH_PEER_STREAMS_MAX && w->pending[i] != a.source_token)
            i++;
        if (!a.source_token || i == DMESH_PEER_STREAMS_MAX || status >= DMESH_PEER_REFUSE_MAX ||
            (status ? h.handle != 0 : (owner(w, h.handle) || bind(w, h.handle))))
            return -1;
        a.status = status;
        w->pending[i] = 0;
        memcpy(payload, &a, sizeof(a));
        out->type = DMESH_PEER_MSG_STREAM_OPEN_ACK;
        out->handle = 0;
        out->length = sizeof(a);
        break;
    }
    case PEER_SEC_DATA: {
        if ((s->flags & (RX_FIN | CLOSED)) || s->rx_seq > UINT32_MAX ||
            (!data_seq && capacity < sizeof(struct dmesh_peer_data_prefix) + length))
            return -1;
        struct dmesh_peer_data_prefix p = {.seq = (uint32_t)s->rx_seq++};
        if (data_seq) {
            *data_seq = p.seq;
            out->length = length;
        } else {
            memcpy(payload, &p, sizeof(p));
            memcpy((uint8_t *)payload + sizeof(p), body, length);
            out->length = sizeof(p) + length;
        }
        out->type = DMESH_PEER_MSG_DATA;
        break;
    }
    case PEER_SEC_STREAM_FIN:
        if (s->flags & RX_FIN)
            return -1;
        s->flags |= RX_FIN;
        out->type = DMESH_PEER_MSG_STREAM_FIN;
        break;
    case PEER_SEC_STREAM_ACK: {
        if (h.handle || !length || length % ACK_ENTRY)
            return -1;
        unsigned count = length / ACK_ENTRY;
        if (count > DMESH_STREAM_ACK_BATCH ||
            capacity < count * sizeof(struct dmesh_peer_ack_entry))
            return -1;
        for (unsigned i = 0; i < count; i++, body += ACK_ENTRY) {
            unsigned tag = (unsigned)get(body, 2);
            struct dmesh_peer_ack_entry a = {
                .handle = (tag & PEER_DATA_SLOT_MASK) |
                          ((tag & PEER_DATA_OWNER) ? DMESH_PEER_HANDLE_OWNER_BIT : 0),
                .seq_first = (uint32_t)get(body + 2, 4),
                .seq_count = (uint32_t)get(body + 6, 4)};
            s = stream(w, a.handle);
            if ((tag & 0xe000) || !s || !(s->flags & LIVE) || (s->flags & PEER_READY) ||
                !a.seq_count || a.seq_count > DMESH_PEER_TX_SLOTS)
                return -1;
            memcpy((uint8_t *)payload + i * sizeof(a), &a, sizeof(a));
        }
        out->type = DMESH_PEER_MSG_STREAM_ACK;
        out->handle = 0;
        out->length = count * sizeof(struct dmesh_peer_ack_entry);
        break;
    }
    case PEER_SEC_STREAM_READY:
        if ((s->flags & (TX_FIN | RX_FIN | PEER_READY)) != (TX_FIN | RX_FIN))
            return -1;
        s->flags |= PEER_READY;
        enqueue(w, s);
        out->type = DMESH_PEER_MSG_STREAM_READY;
        break;
    case PEER_SEC_STREAM_RELEASE:
        if (owner(w, h.handle) ||
            (s->flags & (READY | PEER_READY | PEER_RELEASE)) != (READY | PEER_READY))
            return -1;
        s->flags |= PEER_RELEASE;
        out->type = DMESH_PEER_MSG_STREAM_RELEASE;
        break;
    case PEER_SEC_STREAM_RELEASE_ACK:
        if (!owner(w, h.handle) || !(s->flags & RELEASE))
            return -1;
        if (s->flags & ACK_RECEIVED)
            return -1;
        s->flags |= ACK_RECEIVED;
        out->type = DMESH_PEER_MSG_STREAM_RELEASE_ACK;
        break;
    default:
        return -1;
    }
    return PEER_DATA_HEADER_BYTES + length;
}
long peer_pair_wire_decode_parts(struct peer_pair_wire *w,
                                 const uint8_t header[PEER_DATA_HEADER_BYTES], const void *body,
                                 size_t length, uint32_t incarnation,
                                 struct dmesh_peer_msg_header *out, void *scratch, size_t capacity,
                                 uint32_t *seq)
{
    if (!seq)
        return -1;
    return decode_parts(w, header, body, length, incarnation, out, scratch, capacity, seq);
}
long peer_pair_wire_decode(struct peer_pair_wire *w, const void *input, size_t len,
                           uint32_t incarnation, struct dmesh_peer_msg_header *out, void *payload,
                           size_t capacity)
{
    if (!w || !input || !out || !payload || !incarnation)
        return -1;
    if (len < PEER_DATA_HEADER_BYTES)
        return 0;
    size_t frame = peer_data_frame_size(input);
    if (!frame)
        return -1;
    if (len < frame)
        return 0;
    return decode_parts(w, input, (const uint8_t *)input + PEER_DATA_HEADER_BYTES,
                        frame - PEER_DATA_HEADER_BYTES, incarnation, out, payload, capacity, NULL);
}
