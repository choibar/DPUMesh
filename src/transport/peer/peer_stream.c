#include "peer_stream.h"
#include "peer_pair_wire.h"
#include "peer_arena_config.h"
#include "live_slots.h"

#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LEASES DMESH_CHANNEL_MAX
#define INDEX_CAP 256u
#define INDEX_DELETED UINT32_MAX
#define OPEN_TIMEOUT_NS (30ull * 1000000000ull)
#define TX_CHUNK_BODY (PEER_ARENA_CHUNK_BYTES - PEER_ARENA_HEADROOM)
#define TX_CHUNKS_PER_STREAM (PEER_STREAM_TX_BYTES / PEER_ARENA_CHUNK_BYTES)
_Static_assert(PEER_ARENA_HEADROOM >= PEER_WIRE_HEADER_BYTES, "TX header headroom");
_Static_assert(TX_CHUNK_BODY <= PEER_WIRE_TX_BODY_BYTES, "TX chunk fits one frame");

struct stream;
struct tx_arena;
struct tx_chunk {
    struct tx_arena *arena;
    struct stream *writer; /* NULL after submission; NIC holds never reference a stream */
    unsigned index, refs, next;
    uint32_t capacity;
};
struct tx_arena {
    uint8_t *data;
    unsigned free, busy;
    struct tx_chunk chunks[PEER_ARENA_CHUNKS];
};

struct registration_lease {
    struct peer_local_registration registration;
    const _Atomic unsigned *active;
    uint32_t incarnation;
    int used;
};
struct rx_extent { uint64_t end; uint32_t seq, bytes; };
struct stream {
    struct peer_stream_info info;
    struct dmesh_peer_stream_open intent;
    struct dmesh_peer_channel *channel;
    uint32_t incarnation, handle, seq;
    unsigned lease;
    uint8_t *rx;
    struct tx_arena *arena;
    struct tx_chunk *submitting;
    unsigned tx_reserved;
    uint32_t tx_reserved_bytes;
    struct rx_extent *extents;
    uint64_t rx_received, rx_published, rx_consumed, tx_sent, tx_completed;
    uint32_t rx_put, rx_get;
    uint64_t started;
    int requested, held, detached, tx_fin, rx_fin;
};
struct peer_stream_worker {
    struct peer_authority *authority;
    struct peer_authority_view *view;
    struct peer_pair_transport *transport;
    struct dmesh_peer_table table;
    struct registration_lease leases[LEASES];
    struct stream streams[PEER_STREAM_CAP];
    struct tx_arena tx_arena;
    struct dmesh_live_slots live;
    uint32_t index[INDEX_CAP], next_token;
    uint32_t next_incarnation;
    int (*listener)(uint32_t, uint16_t);
    int stopping, quiesced;
    uint64_t view_generation, maintenance_ns;
};

static int tx_arena_init(struct tx_arena *a)
{
    if (a->data) return 0;
    a->data = aligned_alloc(PEER_ARENA_ALIGN, PEER_ARENA_BYTES);
    if (!a->data) return -ENOMEM;
    for (unsigned i = 0; i < PEER_ARENA_CHUNKS; ++i)
        a->chunks[i] = (struct tx_chunk){.arena = a, .index = i, .next = i + 1};
    a->free = 0;
    return 0;
}
static void tx_retain(void *owner, void *cookie)
{
    (void)owner;
    ++((struct tx_chunk *)cookie)->refs;
}
static void tx_release(void *owner, void *cookie)
{
    struct tx_arena *a = owner;
    struct tx_chunk *c = cookie;
    if (--c->refs) return;
    c->next = a->free;
    a->free = c->index;
    --a->busy;
}
/* Rust must first clear its chunk pointers. Submitted chunks are independent
 * NIC references and remain in the arena even after this stream is reaped. */
static void tx_discard_writable(struct stream *s)
{
    if (!s->tx_reserved) return;
    for (unsigned i = 0; i < PEER_ARENA_CHUNKS; ++i) {
        struct tx_chunk *c = &s->arena->chunks[i];
        if (c->writer != s) continue;
        c->writer = NULL;
        tx_release(s->arena, c);
    }
    s->tx_reserved = s->tx_reserved_bytes = 0;
}
static int tx_payload(void *ctx, uint8_t kind, void *cookie, const uint8_t *bytes,
                       uint32_t len, struct peer_wire_payload *out)
{
    (void)ctx;
    struct stream *s = cookie;
    struct tx_chunk *c = s->submitting;
    if (kind != DMESH_PEER_CUSTODY_L7 || !c || c->writer != s || !len || len > c->capacity)
        return -1;
    uint8_t *data = s->arena->data + c->index * PEER_ARENA_CHUNK_BYTES + PEER_ARENA_HEADROOM;
    if (bytes != data) return -1;
    *out = (struct peer_wire_payload){.data = data, .tx_header = data - PEER_WIRE_HEADER_BYTES,
        .len = len, .region = s->arena->data, .region_len = PEER_ARENA_BYTES,
        .owner = s->arena, .cookie = c, .retain = tx_retain, .release = tx_release};
    return 1;
}

static uint64_t now_ns(void *unused)
{
    (void)unused;
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + t.tv_nsec;
}
static int snapshot(struct peer_stream_worker *w)
{
    if (w->view && w->view_generation == peer_authority_generation(w->authority))
        return 0;
    struct peer_authority_view *next = peer_authority_acquire(w->authority);
    peer_authority_release(w->view);
    w->view = next;
    const struct dmesh_topology_tables *t = peer_authority_tables(next);
    w->view_generation = t ? t->version : 0;
    return 1;
}
static struct stream *lookup(struct peer_stream_worker *w, uint32_t token)
{
    if (!w || !token)
        return NULL;
    struct stream *s = &w->streams[(token - 1u) % PEER_STREAM_CAP];
    return s->info.token == token ? s : NULL;
}
static unsigned hash(struct dmesh_peer_channel *c, uint32_t handle)
{
    uintptr_t x = (uintptr_t)c >> 6;
    return (unsigned)((x ^ (handle * 2654435761u)) & (INDEX_CAP - 1u));
}
static struct stream *source(struct peer_stream_worker *w, struct dmesh_peer_channel *c,
                              uint32_t handle)
{
    unsigned begin = hash(c, handle);
    for (unsigned i = 0; i < INDEX_CAP; ++i) {
        uint32_t token = w->index[(begin + i) & (INDEX_CAP - 1u)];
        if (!token)
            return NULL;
        if (token == INDEX_DELETED)
            continue;
        struct stream *s = lookup(w, token);
        if (s && s->channel == c && s->incarnation == c->incarnation && s->handle == handle)
            return s;
    }
    return NULL;
}
static int index_add(struct peer_stream_worker *w, struct stream *s)
{
    unsigned begin = hash(s->channel, s->handle);
    for (unsigned i = 0; i < INDEX_CAP; ++i) {
        uint32_t *entry = &w->index[(begin + i) & (INDEX_CAP - 1u)];
        if (!*entry || *entry == INDEX_DELETED) {
            *entry = s->info.token;
            return 0;
        }
    }
    return -1;
}
static void index_remove(struct peer_stream_worker *w, struct stream *s)
{
    unsigned begin = hash(s->channel, s->handle);
    for (unsigned i = 0; i < INDEX_CAP; ++i) {
        uint32_t *entry = &w->index[(begin + i) & (INDEX_CAP - 1u)];
        if (*entry == s->info.token) {
            *entry = INDEX_DELETED;
            return;
        }
        if (!*entry)
            return;
    }
}
static int lease(struct peer_stream_worker *w, const char *uid)
{
    if (w->stopping)
        return -1;

    unsigned available = LEASES;
    for (unsigned i = 0; i < LEASES; ++i) {
        struct registration_lease *l = &w->leases[i];
        if (!l->used) {
            if (available == LEASES) available = i;
            continue;
        }
        if (!strcmp(l->registration.identity.pod_uid, uid))
            return atomic_load_explicit(l->active, memory_order_acquire) ? (int)i : -1;
    }
    struct peer_local_registration r;
    const _Atomic unsigned *active;
    if (available == LEASES || w->next_incarnation == UINT32_MAX ||
        !peer_authority_local(w->authority, uid, &r) ||
        peer_authority_watch(w->authority, r.session_id, r.session_epoch, &active))
        return -1;

    w->leases[available] = (struct registration_lease){.registration = r,
        .incarnation = ++w->next_incarnation, .active = active, .used = 1};
    return (int)available;
}
static int node_binding(void *ctx, const char *node, const uint8_t **key,
                         uint32_t *ip, uint16_t *port)
{
    struct peer_stream_worker *w = ctx;
    return peer_authority_node(w->view, node, key, ip, port);
}
static int pod_on_node(void *ctx, const char *uid, const char *node)
{
    struct peer_stream_worker *w = ctx;
    const struct dmesh_gen_pod *p = peer_authority_pod(w->view, uid);
    return p && !strcmp(p->node_name, node);
}
static int32_t local_pod(void *ctx, const char *uid, uint32_t *generation)
{
    struct peer_stream_worker *w = ctx;
    int slot = lease(w, uid);
    if (slot >= 0 && generation)
        *generation = w->leases[slot].incarnation;
    return slot;
}
static int local_registration(void *ctx, const char *uid, struct dmesh_peer_registration *out)
{
    struct peer_stream_worker *w = ctx;
    int slot = lease(w, uid);
    if (slot < 0)
        return 0;
    *out = w->leases[slot].registration.peer;
    return 1;
}
static uint32_t pod_generation(void *ctx, int32_t slot)
{
    struct peer_stream_worker *w = ctx;
    if (slot < 0 || slot >= LEASES || !w->leases[slot].used)
        return 0;
    const struct registration_lease *l = &w->leases[slot];
    if (!atomic_load_explicit(l->active, memory_order_acquire))
        return 0;
    return l->incarnation;
}
static int authorize(void *ctx, const struct dmesh_peer_pair *pair,
                      const struct dmesh_peer_stream_open *open)
{
    struct peer_stream_worker *w = ctx;
    int slot = lease(w, pair->local_uid);
    if (slot < 0 || !open->dst_port ||
        !dmesh_peer_registration_equal(&pair->local, &w->leases[slot].registration.peer) ||
        !peer_authority_pod_in_service(w->view, open->src_pod_uid, open->src_service_key))
        return 0;

    if (!strcmp(open->src_pod_uid, pair->local_uid))
        return !strcmp(open->dst_pod_uid, pair->remote_uid) &&
               open->src_generation == pair->local.generation;

    if (strcmp(open->src_pod_uid, pair->remote_uid) ||
        strcmp(open->dst_pod_uid, pair->local_uid) ||
        open->src_generation != pair->remote.generation)
        return 0;

    /* Resource admission only. The receiving Linkerd stack must subsequently
     * enforce its inbound HTTP/identity policy before delivering to this Host. */
    return w->listener(htonl(w->leases[slot].registration.pod_ip), open->dst_port) > 0;
}
static struct stream *allocate(struct peer_stream_worker *w, unsigned slot)
{
    if (w->stopping || w->next_token > UINT32_MAX - PEER_STREAM_CAP)
        return NULL;
    for (unsigned i = 0; i < PEER_STREAM_CAP; ++i) {
        struct stream *s = &w->streams[i];
        if (s->info.token)
            continue;
        uint32_t token = w->next_token + i + 1;
        w->next_token += PEER_STREAM_CAP;
        s->rx = calloc(1, PEER_STREAM_RX_BYTES);
        s->extents = calloc(DMESH_PEER_TX_SLOTS, sizeof(*s->extents));
        if (!s->rx || !s->extents) {
            free(s->rx); free(s->extents);
            memset(s, 0, sizeof(*s));
            return NULL;
        }
        s->lease = slot;
        s->arena = &w->tx_arena;
        s->info.token = token;
        s->info.state = 1;
        s->started = now_ns(NULL);
        dmesh_live_add(&w->live, i);
        return s;
    }
    return NULL;
}
static void fail_stream(struct stream *s)
{
    s->info.state = 3;
}
static uint16_t destination_opened(void *ctx, struct dmesh_peer_channel *c, uint32_t handle,
                                    const struct dmesh_peer_stream_open *open, int32_t pod)
{
    struct peer_stream_worker *w = ctx;
    const struct dmesh_gen_pod *src = peer_authority_pod(w->view, open->src_pod_uid);
    if (!src || pod < 0 || pod >= LEASES || !authorize(w, &c->pair, open))
        return 0;
    struct stream *s = allocate(w, (unsigned)pod);
    if (!s)
        return 0;
    s->channel = c;
    s->incarnation = c->incarnation;
    s->handle = handle;
    s->intent = *open;
    s->info.incoming = 1;
    s->info.src_ip = htonl(src->ip_be);
    s->info.dst_ip = htonl(w->leases[pod].registration.pod_ip);
    s->info.dst_port = open->dst_port;
    s->info.origin.generation = open->src_generation;
    strcpy(s->info.origin.pod_uid, src->uid);
    strcpy(s->info.origin.namespace_name, src->namespace_name);
    strcpy(s->info.origin.service_account, src->service_account);
    const char *slash = strchr(open->src_service_key, '/');
    if (slash) snprintf(s->info.origin.service_name, sizeof(s->info.origin.service_name), "%s", slash + 1);
    if (dmesh_peer_channel_hold(c, c->incarnation)) {
        fail_stream(s);
        s->detached = 1;
        return 0;
    }
    s->held = 1;
    s->info.state = 2;
    return (uint16_t)(s - w->streams + 1);
}
static int receive_bytes(struct stream *s, const uint8_t *data, uint32_t bytes, uint32_t seq)
{
    if (!s || s->info.state != 2 || s->rx_fin || !bytes ||
        bytes > PEER_STREAM_RX_BYTES - (s->rx_received - s->rx_consumed) ||
        s->rx_put - s->rx_get == DMESH_PEER_TX_SLOTS)
        return -1;
    uint32_t pos = s->rx_received % PEER_STREAM_RX_BYTES;
    uint32_t first = PEER_STREAM_RX_BYTES - pos;
    if (first > bytes) first = bytes;
    memcpy(s->rx + pos, data, first);
    memcpy(s->rx, data + first, bytes - first);
    s->rx_received += bytes;
    s->extents[s->rx_put++ % DMESH_PEER_TX_SLOTS] =
        (struct rx_extent){s->rx_received, seq, bytes};
    return 1;
}
static struct stream *destination(void *ctx, const struct dmesh_peer_handle *h)
{
    struct peer_stream_worker *w = ctx;
    if (!h->up_port || h->up_port > PEER_STREAM_CAP)
        return NULL;
    struct stream *s = &w->streams[h->up_port - 1u];
    return s->info.token && s->info.incoming && s->handle == h->wire_handle &&
           s->incarnation == h->incarnation ? s : NULL;
}
static int deliver(void *ctx, const struct dmesh_peer_handle *h, const uint8_t *data,
                    uint32_t bytes, uint32_t seq)
{
    return receive_bytes(destination(ctx, h), data, bytes, seq);
}
static int source_deliver(void *ctx, struct dmesh_peer_channel *c, uint32_t handle,
                           const uint8_t *data, uint32_t bytes, uint32_t seq)
{
    return receive_bytes(source(ctx, c, handle), data, bytes, seq);
}
static void release(void *ctx, uint8_t kind, void *cookie, uint32_t bytes)
{
    (void)ctx; (void)kind;
    struct stream *s = cookie;
    s->tx_completed += bytes;
}
static void source_opened(void *ctx, struct dmesh_peer_channel *c, uint32_t token,
                           uint32_t handle, int32_t status)
{
    struct peer_stream_worker *w = ctx;
    struct stream *s = lookup(w, token);
    if (!s || s->channel != c || s->incarnation != c->incarnation)
        return;
    if (status || !handle || s->info.state != 1) {
        fail_stream(s);
        return;
    }
    s->handle = handle;
    if (index_add(w, s)) {
        fail_stream(s);
        return;
    }
    s->info.state = 2;
}
static int source_fin(void *ctx, struct dmesh_peer_channel *c, uint32_t handle)
{
    struct stream *s = source(ctx, c, handle);
    if (!s) return 0;
    s->rx_fin = 1;
    return 1;
}
static int destination_fin(void *ctx, const struct dmesh_peer_handle *h)
{
    struct stream *s = destination(ctx, h);
    if (!s) return -1;
    s->rx_fin = 1;
    return 0;
}
static void source_reset(void *ctx, struct dmesh_peer_channel *c, const char *why)
{
    (void)why;
    struct peer_stream_worker *w = ctx;
    for (unsigned i = 0; i < w->live.count; ++i) {
        struct stream *s = &w->streams[w->live.ids[i]];
        if (s->channel == c && s->incarnation == c->incarnation)
            fail_stream(s);
    }
}
static void poison(void *ctx, const struct dmesh_peer_handle *h, const char *why)
{
    (void)why;
    struct stream *s = destination(ctx, h);
    if (s) fail_stream(s);
}
static const struct dmesh_peer_ops OPS = {
    .node_binding = node_binding, .pod_on_node = pod_on_node,
    .local_pod = local_pod, .local_registration = local_registration,
    .pod_generation = pod_generation, .pair_authorize = authorize,
    .destination_opened = destination_opened, .deliver = deliver,
    .source_deliver = source_deliver, .release = release, .source_opened = source_opened,
    .source_fin = source_fin, .destination_fin = destination_fin,
    .source_reset = source_reset, .poison = poison, .now_ns = now_ns,
    .tx_payload = tx_payload,
};

int peer_stream_worker_new(struct peer_authority *authority, const char *node,
                            const uint8_t key[32], struct peer_pair_transport *transport,
                            int (*listener)(uint32_t, uint16_t), struct peer_stream_worker **out)
{
    if (!authority || !node || !key || !transport || !listener || !out)
        return EINVAL;
    struct peer_stream_worker *w = calloc(1, sizeof(*w));
    if (!w) return ENOMEM;
    w->authority = authority;
    w->transport = transport;
    w->listener = listener;
    snapshot(w);
    dmesh_peer_table_init(&w->table, node, key, peer_pair_transport_ops(), transport, &OPS, w);
    peer_pair_transport_attach(transport, &w->table);
    *out = w;
    return 0;
}

int peer_stream_open(struct peer_stream_worker *w, const struct peer_flow_origin *origin,
                      uint32_t ip, uint16_t port, uint32_t *token)
{
    if (!w || !origin || !token || !port || w->stopping)
        return -EINVAL;
    struct peer_local_registration r;
    if (!peer_authority_session(w->authority, origin->session_id, origin->session_epoch, &r) ||
        r.peer.generation != origin->generation || strcmp(origin->pod_uid, r.identity.pod_uid))
        return -ESTALE;
    snapshot(w);
    const struct dmesh_topology_tables *t = peer_authority_tables(w->view);
    const struct dmesh_gen_pod *dst = NULL;
    for (size_t i = 0; t && i < t->pod_count; ++i)
        if (htonl(t->pods[i].ip_be) == ip) { dst = &t->pods[i]; break; }
    if (!dst || !strcmp(dst->node_name, w->table.node_name))
        return -EXDEV;
    int slot = lease(w, r.identity.pod_uid);
    if (slot < 0) return -ESTALE;
    struct stream *s = allocate(w, (unsigned)slot);
    if (!s) return -EAGAIN;
    s->info.origin = *origin;
    s->info.src_ip = htonl(r.pod_ip);
    s->info.dst_ip = ip;
    s->info.dst_port = port;
    strcpy(s->intent.src_pod_uid, r.identity.pod_uid);
    strcpy(s->intent.dst_pod_uid, dst->uid);
    snprintf(s->intent.src_service_key, sizeof(s->intent.src_service_key), "%s/%s",
             r.identity.namespace_name, r.identity.service_name);
    s->intent.src_generation = r.peer.generation;
    s->intent.dst_port = port;
    s->intent.source_token = s->info.token;
    enum dmesh_peer_refusal reason;
    s->channel = dmesh_peer_open_stream(&w->table, dst->node_name, &s->intent, &reason);
    if (!s->channel) {
        fail_stream(s);
        s->detached = 1;
        return -ECONNREFUSED;
    }
    s->incarnation = s->channel->incarnation;
    *token = s->info.token;
    return 0;
}

static void reap(struct peer_stream_worker *w, struct stream *s)
{
    if (!s->detached || s->info.state != 3 || s->tx_sent != s->tx_completed)
        return;
    struct dmesh_peer_channel *channel = s->channel;
    uint32_t handle = s->handle, incarnation = s->incarnation;
    if (s->held)
        dmesh_peer_channel_release(s->channel, s->incarnation);
    index_remove(w, s);
    dmesh_live_remove(&w->live, (unsigned)(s - w->streams));
    tx_discard_writable(s);
    free(s->rx); free(s->extents);
    memset(s, 0, sizeof(*s));
    /* detach certifies Rust reader/writer release; custody has completed and
     * the old stream/index/buffers no longer exist. Keep its wire slot until
     * both peers also complete READY/RELEASE/ACK. Internal token fencing stays. */
    if (handle && channel && channel->state == DMESH_PEER_OPEN &&
        channel->incarnation == incarnation && channel->pair_wire)
        (void)peer_pair_wire_local_closed(channel->pair_wire, handle);
}
static void retire_leases(struct peer_stream_worker *w)
{
    for (unsigned i = 0; i < LEASES; ++i) {
        struct registration_lease *l = &w->leases[i];
        if (!l->used) continue;
        int busy = peer_pair_transport_registration_busy(w->transport,
                         l->registration.identity.pod_uid, l->registration.peer.generation);
        for (unsigned j = 0; !busy && j < w->live.count; ++j)
            busy = w->streams[w->live.ids[j]].lease == i;
        for (unsigned j = 0; !busy && j < DMESH_CHANNEL_MAX; ++j) {
            const struct dmesh_peer_channel *c = &w->table.channels[j];
            busy = c->in_use && !strcmp(c->pair.local_uid, l->registration.identity.pod_uid) &&
                   c->pair.local.generation == l->registration.peer.generation;
        }
        if (!busy) {
            peer_authority_unhold(w->authority, l->registration.session_id, l->registration.session_epoch);
            l->used = 0;
        }
    }
}
int peer_stream_worker_progress(struct peer_stream_worker *w)
{
    if (!w || w->quiesced) return 0;
    int changed = snapshot(w);
    int progress = peer_pair_transport_ops()->progress(w->transport);
    if (changed) dmesh_peer_table_rebind(&w->table);
    for (unsigned i = 0; i < DMESH_CHANNEL_MAX; ++i) {
        struct dmesh_peer_channel *c = &w->table.channels[i];
        if (c->in_use) progress += dmesh_peer_channel_progress(&w->table, c, 64) > 0;
    }
    uint64_t now = now_ns(NULL);
    for (unsigned i = 0; i < w->live.count;) {
        struct stream *s = &w->streams[w->live.ids[i]];
        if (s->info.state == 1 && now - s->started >= OPEN_TIMEOUT_NS) {
            if (s->channel) dmesh_peer_reset(&w->table, s->channel, "stream open timeout");
            fail_stream(s);
        }
        if (s->info.state == 1 && !s->requested && s->channel->state == DMESH_PEER_OPEN) {
            if (!s->held) {
                if (dmesh_peer_channel_hold(s->channel, s->incarnation)) {
                    fail_stream(s);
                    continue;
                }
                s->held = 1;
            }
            enum dmesh_peer_refusal result = dmesh_peer_stream_request(&w->table, s->channel, &s->intent);
            if (result == DMESH_PEER_OK) s->requested = 1;
            else if (result != DMESH_PEER_REFUSE_INFLIGHT) fail_stream(s);
        }
        if (s->info.state == 2 && s->tx_fin && s->rx_fin &&
            s->tx_sent == s->tx_completed && s->rx_received == s->rx_consumed)
            fail_stream(s);
        unsigned before = w->live.count;
        reap(w, s);
        if (before == w->live.count) ++i;
    }
    if (now >= w->maintenance_ns) {
        w->maintenance_ns = now + 1000000ull;
        dmesh_peer_evict_idle(&w->table);
        dmesh_peer_reap_closed(&w->table);
        retire_leases(w);
    }
    return progress;
}
int peer_stream_next(struct peer_stream_worker *w, unsigned *cursor, struct peer_stream_info *info)
{
    if (!w || !cursor || !info) return 0;
    while (*cursor < w->live.count) {
        struct stream *s = &w->streams[w->live.ids[(*cursor)++]];
        *info = s->info;
        return 1;
    }
    return 0;
}
int peer_stream_info(struct peer_stream_worker *w, uint32_t token, struct peer_stream_info *out)
{
    struct stream *s = lookup(w, token);
    if (!s || !out) return -ENOENT;
    *out = s->info;
    return 0;
}
int peer_stream_buffers(struct peer_stream_worker *w, uint32_t token,
                         void **tx, size_t *tx_bytes, void **rx, size_t *rx_bytes)
{
    struct stream *s = lookup(w, token);
    if (!s || s->info.state == 3 || !tx || !tx_bytes || !rx || !rx_bytes) return -ESTALE;
    *tx = NULL; *tx_bytes = 0; /* peer TX is acquired from the worker arena */
    *rx = s->rx; *rx_bytes = PEER_STREAM_RX_BYTES;
    return 0;
}
int peer_stream_tx_acquire(struct peer_stream_worker *w, uint32_t token,
                           uint32_t *chunk, void **data, size_t *capacity)
{
    struct stream *s = lookup(w, token);
    if (!s || s->info.state != 2 || s->tx_fin || s->detached || w->stopping) return -ESTALE;
    if (!chunk || !data || !capacity) return -EINVAL;
    *chunk = 0; *data = NULL; *capacity = 0;
    uint64_t retained = s->tx_sent - s->tx_completed + s->tx_reserved_bytes;
    if (s->tx_reserved == TX_CHUNKS_PER_STREAM || retained >= PEER_STREAM_TX_BYTES) return 0;
    struct tx_arena *a = s->arena;
    int rc = tx_arena_init(a);
    if (rc) return rc;
    if (a->free == PEER_ARENA_CHUNKS) return 0;
    struct tx_chunk *c = &a->chunks[a->free];
    a->free = c->next;
    c->writer = s;
    c->refs = 1;
    c->capacity = (uint32_t)(PEER_STREAM_TX_BYTES - retained);
    if (c->capacity > TX_CHUNK_BODY) c->capacity = TX_CHUNK_BODY;
    ++a->busy;
    ++s->tx_reserved;
    s->tx_reserved_bytes += c->capacity;
    *chunk = c->index + 1;
    *data = a->data + c->index * PEER_ARENA_CHUNK_BYTES + PEER_ARENA_HEADROOM;
    *capacity = c->capacity;
    return 1;
}
int peer_stream_receive(struct peer_stream_worker *w, uint32_t token, uint32_t *pos, uint32_t *bytes)
{
    struct stream *s = lookup(w, token);
    if (!s || s->info.state == 3) return -ESTALE;
    if (s->rx_received == s->rx_published) return s->rx_fin ? -ENODATA : 0;
    *pos = s->rx_published % PEER_STREAM_RX_BYTES;
    uint64_t count = s->rx_received - s->rx_published;
    if (count > PEER_STREAM_RX_BYTES - *pos) count = PEER_STREAM_RX_BYTES - *pos;
    *bytes = (uint32_t)count;
    s->rx_published += count;
    return 1;
}
int peer_stream_consumed(struct peer_stream_worker *w, uint32_t token, uint32_t pos)
{
    struct stream *s = lookup(w, token);
    if (!s || s->info.state == 3 || pos > PEER_STREAM_RX_BYTES) return -ESTALE;
    uint64_t delta = (pos + PEER_STREAM_RX_BYTES - s->rx_consumed % PEER_STREAM_RX_BYTES) % PEER_STREAM_RX_BYTES;
    if (!delta && s->rx_published - s->rx_consumed == PEER_STREAM_RX_BYTES) delta = PEER_STREAM_RX_BYTES;
    if (delta > s->rx_published - s->rx_consumed) return -EINVAL;
    s->rx_consumed += delta;
    while (s->rx_get != s->rx_put) {
        const struct rx_extent *e = &s->extents[s->rx_get % DMESH_PEER_TX_SLOTS];
        if (e->end > s->rx_consumed) break;
        if (s->info.incoming) dmesh_peer_delivered(&w->table, s->channel, s->handle, e->seq, e->bytes);
        else dmesh_peer_source_delivered(&w->table, s->channel, s->handle, e->seq, e->bytes);
        ++s->rx_get;
    }
    return 0;
}
int peer_stream_send(struct peer_stream_worker *w, uint32_t token, uint32_t chunk, uint32_t bytes)
{
    struct stream *s = lookup(w, token);
    if (!s || s->info.state != 2 || s->tx_fin) return -ESTALE;
    if (!chunk || chunk > PEER_ARENA_CHUNKS || !bytes) return -EINVAL;
    struct tx_chunk *c = &s->arena->chunks[chunk - 1];
    if (c->writer != s || bytes > c->capacity) return -EINVAL;
    const uint8_t *data = s->arena->data + c->index * PEER_ARENA_CHUNK_BYTES + PEER_ARENA_HEADROOM;
    s->submitting = c;
    enum dmesh_peer_refusal result = dmesh_peer_stream_data_send(&w->table, s->channel, s->handle,
        s->seq, data, bytes, DMESH_PEER_CUSTODY_L7, s);
    s->submitting = NULL;
    if (result == DMESH_PEER_REFUSE_INFLIGHT) return 0;
    if (result != DMESH_PEER_OK) return -EIO;
    ++s->seq;
    s->tx_sent += bytes;
    --s->tx_reserved;
    s->tx_reserved_bytes -= c->capacity;
    c->writer = NULL;
    tx_release(s->arena, c); /* carrier hold owns the NIC source from here */
    return (int)bytes;
}
int peer_stream_completed(struct peer_stream_worker *w, uint32_t token, uint64_t *bytes)
{
    struct stream *s = lookup(w, token);
    if (!s || s->info.state == 3) return -ESTALE;
    *bytes = s->tx_completed;
    return 0;
}
int peer_stream_fin(struct peer_stream_worker *w, uint32_t token)
{
    struct stream *s = lookup(w, token);
    if (!s || s->info.state != 2) return -ESTALE;
    if (s->tx_fin) return 0;
    enum dmesh_peer_refusal result = dmesh_peer_stream_fin_send(&w->table, s->channel, s->handle);
    if (result == DMESH_PEER_REFUSE_INFLIGHT) return -EAGAIN;
    if (result != DMESH_PEER_OK) return -EIO;
    s->tx_fin = 1;
    return 0;
}
int peer_stream_detach(struct peer_stream_worker *w, uint32_t token)
{
    struct stream *s = lookup(w, token);
    if (!s) return -ENOENT;
    s->detached = 1;
    tx_discard_writable(s);
    /* Drop can race the maintenance pass that recognizes a clean EOF. */
    if (s->info.state == 2 && s->tx_fin && s->rx_fin &&
        s->tx_sent == s->tx_completed && s->rx_received == s->rx_consumed)
        fail_stream(s);
    /* Abandoning unread data cannot silently ACK delivery. Reset the pair;
     * this also releases transport accounting before buffers can be freed. */
    if (s->channel && s->channel->state != DMESH_PEER_CLOSED &&
        (s->info.state != 3 || s->tx_sent != s->tx_completed || s->rx_received != s->rx_consumed))
        dmesh_peer_reset(&w->table, s->channel, "local stream reader detached");
    fail_stream(s);
    reap(w, s);
    return lookup(w, token) ? 0 : 1;
}
void peer_stream_worker_stop(struct peer_stream_worker *w)
{
    if (!w || w->quiesced) return;
    w->stopping = 1;
    for (unsigned i = 0; i < DMESH_CHANNEL_MAX; ++i)
        if (w->table.channels[i].in_use)
            dmesh_peer_reset(&w->table, &w->table.channels[i], "worker stopping");
}
int peer_stream_worker_quiesce(struct peer_stream_worker *w)
{
    if (!w || w->quiesced) return 0;
    if (w->live.count || dmesh_peer_table_fini(&w->table)) return EBUSY;
    if (w->tx_arena.busy) return EBUSY;
    retire_leases(w);
    for (unsigned i = 0; i < LEASES; ++i) if (w->leases[i].used) return EBUSY;
    peer_pair_transport_attach(w->transport, NULL);
    peer_authority_release(w->view);
    w->view = NULL;
    w->quiesced = 1;
    return 0;
}
int peer_stream_worker_free(struct peer_stream_worker **ptr)
{
    if (!ptr || !*ptr) return 0;
    struct peer_stream_worker *w = *ptr;
    int rc = peer_stream_worker_quiesce(w);
    if (rc) return rc;
    /* The caller must destroy the transport's arena registrations first. */
    free(w->tx_arena.data);
    free(w);
    *ptr = NULL;
    return 0;
}
