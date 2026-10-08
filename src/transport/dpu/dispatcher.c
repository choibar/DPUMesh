#define _GNU_SOURCE
#include "dispatcher.h"
#include "placement.h"
#include "object.h"
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

/* Only control messages cross these queues. In particular no DMA descriptor,
 * payload, data completion or credit update visits the dispatcher. */
#define QUEUE_SIZE 128u
enum command { OPEN = 100, CLOSE, LOST, RETIRED, BACKEND_DEMAND, ARM };
struct message {
    struct dmesh_flow_key key;
    struct dmesh_flow_location location;
    uint16_t type;
    int32_t status;
    size_t length;
    unsigned char payload[DMESH_SESSION_MAX_PAYLOAD];
};
struct queue {
    pthread_mutex_t lock;
    _Atomic unsigned count;
    unsigned head;
    struct message messages[QUEUE_SIZE];
};
struct dmesh_worker_mailbox {
    struct dmesh_dispatcher *dispatcher;
    struct queue requests, replies;
    int fd;
    _Atomic bool ready, departed;
};
struct assignment {
    bool used, opening, ready, close_pending, lost_pending;
    struct objects *control;
    struct dmesh_session *session;
    struct dmesh_flow_key key;
    struct dmesh_flow_location location;
    uint32_t backend_token;
};
struct dmesh_dispatcher {
    size_t count, cursor;
    uint32_t next_backend_token;
    struct objects **controls;
    struct dmesh_worker_mailbox *workers;
    struct dmesh_worker_load *loads;
    struct assignment *flows; /* worker-local slot reservation table */
    struct pollfd *pollfds;
    struct dmesh_placement_policy policy;
    int fd;
    pthread_t thread;
    _Atomic unsigned remaining;
};

/* Read-only membership snapshots for Rust routing before any backend exists.
 * Retired endpoints remain known until dispatcher shutdown, preventing a
 * registered DMA destination from falling through to TCP after disconnect. */
struct listener_membership {
    struct dmesh_dispatcher *dispatcher;
    uint32_t ip;
    uint16_t port;
    struct listener_membership *next;
};
static pthread_mutex_t membership_lock = PTHREAD_MUTEX_INITIALIZER;
static struct listener_membership *memberships;
int dmesh_dispatch_listener_known(uint32_t ip, uint16_t port)
{
    pthread_mutex_lock(&membership_lock);
    int known = 0;
    for (struct listener_membership *m = memberships; m; m = m->next)
        if (m->ip == ip && m->port == port) { known = 1; break; }
    pthread_mutex_unlock(&membership_lock);
    return known;
}
static int remember_listener(struct dmesh_dispatcher *d, uint32_t ip, uint16_t port)
{
    pthread_mutex_lock(&membership_lock);
    for (struct listener_membership *m = memberships; m; m = m->next)
        if (m->dispatcher == d && m->ip == ip && m->port == port) {
            pthread_mutex_unlock(&membership_lock); return 0;
        }
    struct listener_membership *m = malloc(sizeof(*m));
    if (m) {
        *m = (struct listener_membership){d, ip, port, memberships};
        memberships = m;
    }
    pthread_mutex_unlock(&membership_lock);
    return m ? 0 : ENOMEM;
}
static void forget_listeners(struct dmesh_dispatcher *d)
{
    pthread_mutex_lock(&membership_lock);
    struct listener_membership **p = &memberships;
    while (*p) {
        struct listener_membership *m = *p;
        if (m->dispatcher == d) { *p = m->next; free(m); }
        else p = &m->next;
    }
    pthread_mutex_unlock(&membership_lock);
}

static void wake(int fd)
{
    uint64_t one = 1;
    ssize_t n;
    do { n = write(fd, &one, sizeof(one)); } while (n < 0 && errno == EINTR);
    /* EAGAIN means a wake is already pending. */
}
static void clear(int fd) { uint64_t n; while (read(fd, &n, sizeof(n)) > 0) {} }
static bool push(struct queue *q, const struct message *m)
{
    pthread_mutex_lock(&q->lock);
    unsigned count = atomic_load_explicit(&q->count, memory_order_relaxed);
    bool ok = count < QUEUE_SIZE;
    if (ok) {
        q->messages[(q->head + count) % QUEUE_SIZE] = *m;
        atomic_fetch_add_explicit(&q->count, 1, memory_order_release);
    }
    pthread_mutex_unlock(&q->lock);
    return ok;
}
static bool admit(struct dmesh_worker_mailbox *b, const struct message *m)
{
    struct queue *q = &b->requests;
    pthread_mutex_lock(&q->lock);
    unsigned n = atomic_load_explicit(&q->count, memory_order_relaxed);
    bool ok = !atomic_load(&b->departed) && n < QUEUE_SIZE;
    if (ok) {
        q->messages[(q->head + n) % QUEUE_SIZE] = *m;
        atomic_fetch_add_explicit(&q->count, 1, memory_order_release);
    }
    pthread_mutex_unlock(&q->lock);
    return ok;
}
static bool peek(struct queue *q, struct message *m, bool consume)
{
    if (!atomic_load_explicit(&q->count, memory_order_acquire)) return false;
    pthread_mutex_lock(&q->lock);
    bool ok = atomic_load_explicit(&q->count, memory_order_relaxed) != 0;
    if (ok) {
        *m = q->messages[q->head];
        if (consume) {
            q->head = (q->head + 1) % QUEUE_SIZE;
            atomic_fetch_sub_explicit(&q->count, 1, memory_order_release);
        }
    }
    pthread_mutex_unlock(&q->lock);
    return ok;
}
static bool same_key(const struct dmesh_flow_key *a, const struct dmesh_flow_key *b)
{
    return a->session_id == b->session_id && a->session_epoch == b->session_epoch &&
           a->flow_id == b->flow_id && a->generation == b->generation;
}
static struct assignment *find(struct dmesh_dispatcher *d, struct dmesh_session *s, uint32_t id)
{
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i)
        if (d->flows[i].used && d->flows[i].session == s && d->flows[i].key.flow_id == id)
            return &d->flows[i];
    return NULL;
}
static uint64_t session_id(struct dmesh_dispatcher *d, struct objects *o, struct dmesh_session *s)
{
    for (size_t i = 0; i < d->count; ++i)
        if (d->controls[i] == o)
            return 1 + i * DMESH_MAX_SESSIONS + (size_t)(s - o->sessions);
    abort();
}
static void reply_error(struct objects *o, struct dmesh_session *s,
                        const struct dmesh_session_header *h, int error)
{
    /* If the SDK send queue is full the caller can retry OPEN. No resource has
     * been admitted on this path. Lifecycle replies use the retained queues. */
    (void)dmesh_session_send(o, s, DMESH_SESSION_ERROR, h->flow_id, h->generation, NULL, 0, error);
}

/* All listener state is accessed by dispatch_main only. */
static struct dmesh_session *listener(struct dmesh_dispatcher *d, uint32_t ip, uint16_t port)
{
    for (size_t w = 0; w < d->count; ++w)
        for (unsigned i = 0; i < DMESH_MAX_SESSIONS; ++i) {
            struct dmesh_session *s = &d->controls[w]->sessions[i];
            if (s->occupied && s->negotiated && !s->closing &&
                s->listen_ip == ip && s->listen_port == port) return s;
        }
    return NULL;
}

static void backend_demand(struct dmesh_dispatcher *d, size_t worker, const struct message *m)
{
    uint32_t ip = dmesh_session_get_u32(m->payload);
    uint16_t port = (uint16_t)dmesh_session_get_u32(m->payload + 4);
    struct dmesh_session *s = listener(d, ip, port);
    if (!s || s->backend[worker].token || !atomic_load(&d->workers[worker].ready) ||
        atomic_load(&d->workers[worker].departed) ||
        d->loads[worker].assigned >= DMESH_MAX_CONNECTIONS) return;
    /* Never wrap a token: delayed requests must not alias a new operation. */
    if (d->next_backend_token == UINT32_MAX) return;
    s->backend[worker].token = ++d->next_backend_token;
    s->backend[worker].sent = false;
}

int dmesh_dispatch_backend_request(struct objects *o, uint32_t ip, uint16_t port)
{
    if (!o || !o->mailbox || !ip || !port) return EINVAL;
    struct message m = {.type = BACKEND_DEMAND, .length = 8};
    dmesh_session_put_u32(m.payload, ip);
    dmesh_session_put_u32(m.payload + 4, port);
    if (!push(&o->mailbox->replies, &m)) return EAGAIN;
    wake(o->mailbox->dispatcher->fd);
    return 0;
}

static void flush_listeners(struct dmesh_dispatcher *d)
{
    for (size_t w = 0; w < d->count; ++w) {
        struct objects *o = d->controls[w];
        for (unsigned i = 0; i < DMESH_MAX_SESSIONS; ++i) {
            struct dmesh_session *s = &o->sessions[i];
            if (!s->occupied || s->closing) continue;
            if (s->listen_pending && dmesh_session_send(o, s, DMESH_SESSION_LISTEN_ACK,
                    0, 1, NULL, 0, s->listen_status) == DOCA_SUCCESS) s->listen_pending = false;
            for (size_t owner = 0; owner < d->count; ++owner) {
                if (!s->backend[owner].token || s->backend[owner].sent) continue;
                uint8_t payload[4]; dmesh_session_put_u32(payload, (uint32_t)owner);
                if (dmesh_session_send(o, s, DMESH_SESSION_BACKEND_REQUEST, 0,
                        s->backend[owner].token, payload, sizeof(payload), 0) == DOCA_SUCCESS)
                    s->backend[owner].sent = true;
            }
        }
    }
}

/* The host is about to sleep. A listed flow's descriptor progress lives on its
 * worker: hand the owner the sequence the host expects, and the owner rings
 * through its reply queue (DOORBELL). A flow whose owner cannot take the
 * command rings at once; the host then re-polls and arms again. */
static void arm_flows(struct dmesh_dispatcher *d, struct objects *o, struct dmesh_session *s,
                      const void *payload, uint32_t len)
{
    struct dmesh_session_arm_flow flows[DMESH_SESSION_MAX_FLOWS];
    uint64_t epoch;
    uint32_t count;
    if (dmesh_session_arm_decode(payload, len, &epoch, flows, &count) != 0) {
        s->closing = true;
        dmesh_dispatcher_session_lost(o, s);
        return;
    }
    s->armed_epoch = epoch;
    s->armed = true;
    for (uint32_t i = 0; i < count; ++i) {
        struct assignment *a = find(d, s, flows[i].flow_id);
        /* Stale generations and unknown flows are ignored, not trusted. */
        if (!a || a->key.generation != flows[i].generation || !a->ready) continue;
        struct message m = {.key = a->key, .location = a->location, .type = ARM, .length = 16};
        dmesh_session_put_u64(m.payload, epoch);
        dmesh_session_put_u64(m.payload + 8, flows[i].expected_seq);
        if (push(&d->workers[a->location.worker].requests, &m)) {
            wake(d->workers[a->location.worker].fd);
        } else {
            s->armed = false;
            s->doorbell_pending_epoch = epoch;
        }
    }
}

void dmesh_dispatcher_request(struct objects *o, struct dmesh_session *s,
                              const struct dmesh_session_header *h, const void *payload)
{
    struct dmesh_dispatcher *d = o->dispatcher;
    if (h->type == DMESH_SESSION_ARM) {
        arm_flows(d, o, s, payload, h->payload_len);
        return;
    }
    if (h->type == DMESH_SESSION_LISTEN) {
        const uint8_t *p = payload;
        uint32_t ip = dmesh_session_get_u32(p), port = dmesh_session_get_u32(p + 4);
        struct dmesh_session *existing = listener(d, ip, (uint16_t)port);
        s->listen_status = (!ip || !port || port > UINT16_MAX) ? EINVAL :
            ((existing && existing != s) || (s->listen_port &&
                (s->listen_ip != ip || s->listen_port != port))) ? EADDRINUSE : 0;
        if (!s->listen_status) s->listen_status = remember_listener(d, ip, (uint16_t)port);
        if (!s->listen_status) { s->listen_ip = ip; s->listen_port = (uint16_t)port; }
        s->listen_pending = true;
        return;
    }
    if (h->type == DMESH_SESSION_BACKEND_REJECT) {
        uint32_t worker = dmesh_session_get_u32(payload);
        if (worker < d->count && s->backend[worker].token == h->generation) {
            /* Only a request without an admitted flow may be rejected. */
            bool admitted = false;
            for (unsigned slot = 0; slot < DMESH_MAX_CONNECTIONS; ++slot) {
                struct assignment *a = &d->flows[worker * DMESH_MAX_CONNECTIONS + slot];
                admitted |= a->used && a->session == s && a->backend_token == h->generation;
            }
            if (!admitted) s->backend[worker].token = 0;
        }
        return;
    }
    if (!h->flow_id || h->flow_id > DMESH_MAX_CONNECTIONS) return;
    unsigned idx = h->flow_id - 1;
    struct assignment *a = find(d, s, h->flow_id);
    if (h->type == DMESH_SESSION_CLOSE) {
        if (a && a->key.generation == h->generation) {
            a->close_pending = true; /* durable, including queue saturation */
        } else if (!a && h->generation >= s->generation[idx]) {
            s->generation[idx] = h->generation;
            s->closed[idx] = true;
            s->close_status[idx] = 0;
            s->close_pending[idx] = true;
        } else {
            (void)dmesh_session_send(o, s, DMESH_SESSION_CLOSED, h->flow_id,
                                     h->generation, NULL, 0, 0);
        }
        return;
    }
    bool backend = h->type == DMESH_SESSION_BACKEND_OPEN;
    uint32_t requested_worker = 0, request_token = 0;
    if ((h->type != DMESH_SESSION_OPEN && !backend) ||
        h->payload_len != sizeof(struct dmesh_export_metadata_msg) + (backend ? 8 : 0)) {
        reply_error(o, s, h, EINVAL); return;
    }
    if (h->generation < s->generation[idx] ||
        (h->generation == s->generation[idx] && s->closed[idx])) {
        reply_error(o, s, h, ESTALE); return;
    }
    if (a) {
        if (a->key.generation != h->generation) reply_error(o, s, h, EBUSY);
        else if (a->ready)
            (void)dmesh_session_send(o, s, DMESH_SESSION_READY, h->flow_id, h->generation, NULL, 0, 0);
        return;
    }
    if (s->close_pending[idx]) { reply_error(o, s, h, EBUSY); return; }
    if (backend) {
        requested_worker = dmesh_session_get_u32(payload);
        request_token = dmesh_session_get_u32((const uint8_t *)payload + 4);
        if (requested_worker >= d->count || !request_token ||
            s->backend[requested_worker].token != request_token) {
            reply_error(o, s, h, ESTALE); return;
        }
        /* A malicious/duplicated reply may not admit a second flow. */
        for (unsigned slot = 0; slot < DMESH_MAX_CONNECTIONS; ++slot) {
            struct assignment *b = &d->flows[requested_worker * DMESH_MAX_CONNECTIONS + slot];
            if (b->used && b->session == s && b->backend_token == request_token) {
                reply_error(o, s, h, EBUSY); return;
            }
        }
        payload = (const uint8_t *)payload + 8;
    }
    struct dmesh_export_metadata_msg metadata;
    memcpy(&metadata, payload, sizeof(metadata));
    bool backend_mode = metadata.flow.mode == DMESH_FLOW_MODE_BACKEND ||
                        metadata.flow.mode == DMESH_FLOW_MODE_BACKEND_PULL;
    if (backend != backend_mode || (backend && (metadata.flow.dst_ip != s->listen_ip ||
            metadata.flow.dst_port != s->listen_port))) {
        reply_error(o, s, h, EINVAL); return;
    }
    struct dmesh_flow_spec spec = {
        .session_id = session_id(d, o, s), .session_epoch = s->epoch,
        .flow_id = h->flow_id, .generation = h->generation, .mode = metadata.flow.mode,
        .service_ip = metadata.flow.dst_ip, .service_port = metadata.flow.dst_port,
    };
    for (size_t i = 0; i < d->count; ++i)
        d->loads[i].eligible = atomic_load(&d->workers[i].ready) && !atomic_load(&d->workers[i].departed);
    int owner = backend ? (int)requested_worker :
        d->policy.select(&spec, d->loads, d->count, d->cursor, d->policy.context);
    if (owner < 0 || (size_t)owner >= d->count || !d->loads[owner].eligible ||
        d->loads[owner].assigned >= DMESH_MAX_CONNECTIONS) {
        reply_error(o, s, h, ENOSPC); return;
    }
    unsigned slot;
    for (slot = 0; slot < DMESH_MAX_CONNECTIONS; ++slot)
        if (!d->flows[owner * DMESH_MAX_CONNECTIONS + slot].used) break;
    if (slot == DMESH_MAX_CONNECTIONS) { reply_error(o, s, h, ENOSPC); return; }
    a = &d->flows[owner * DMESH_MAX_CONNECTIONS + slot];
    *a = (struct assignment){.used = true, .opening = true, .control = o, .session = s,
        .key = {spec.session_id, spec.session_epoch, h->flow_id, h->generation},
        .location = {.worker = (uint32_t)owner, .slot = slot, .ownership_epoch = 1},
        .backend_token = request_token};
    struct message m = {.key = a->key, .location = a->location, .type = OPEN, .length = sizeof(metadata)};
    memcpy(m.payload, &metadata, sizeof(metadata));
    if (!admit(&d->workers[owner], &m)) {
        memset(a, 0, sizeof(*a)); reply_error(o, s, h, EAGAIN); return;
    }
    ++d->loads[owner].assigned;
    ++d->loads[owner].opening;
    if (!backend) d->cursor = ((size_t)owner + 1) % d->count;
    s->generation[idx] = h->generation;
    s->closed[idx] = false;
    s->close_status[idx] = 0;
    wake(d->workers[owner].fd);
    fprintf(stderr, "[dispatcher] session=%lu/%lu flow=%u/%u owner=%d slot=%u epoch=1 backend=%d\n",
            a->key.session_id, a->key.session_epoch, h->flow_id, h->generation, owner, slot, backend);
}

void dmesh_dispatcher_session_lost(struct objects *o, struct dmesh_session *s)
{
    struct dmesh_dispatcher *d = o->dispatcher;
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i)
        if (d->flows[i].used && d->flows[i].session == s) d->flows[i].lost_pending = true;
}
bool dmesh_dispatcher_session_busy(struct objects *o, struct dmesh_session *s)
{
    struct dmesh_dispatcher *d = o->dispatcher;
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i)
        if (d->flows[i].used && d->flows[i].session == s) return true;
    return false;
}
bool dmesh_dispatcher_accepting(struct objects *o)
{
    struct dmesh_dispatcher *d = o->dispatcher;
    for (size_t i = 0; i < d->count; ++i)
        if (atomic_load(&d->workers[i].ready) && !atomic_load(&d->workers[i].departed)) return true;
    return false;
}
static void flush_commands(struct dmesh_dispatcher *d)
{
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i) {
        struct assignment *a = &d->flows[i];
        if (!a->used || (!a->lost_pending && !a->close_pending)) continue;
        struct message m = {.key = a->key, .location = a->location,
                            .type = a->lost_pending ? LOST : CLOSE};
        if (push(&d->workers[a->location.worker].requests, &m)) {
            if (m.type == LOST) a->lost_pending = false;
            else a->close_pending = false;
            wake(d->workers[a->location.worker].fd);
        }
    }
}
static void flush_replies(struct dmesh_dispatcher *d)
{
    for (size_t w = 0; w < d->count; ++w) {
        struct message m;
        while (peek(&d->workers[w].replies, &m, false)) {
            if (m.type == BACKEND_DEMAND) {
                backend_demand(d, w, &m);
                (void)peek(&d->workers[w].replies, &m, true);
                continue;
            }
            struct assignment *a = m.location.slot < DMESH_MAX_CONNECTIONS ?
                &d->flows[w * DMESH_MAX_CONNECTIONS + m.location.slot] : NULL;
            if (a && a->used && same_key(&a->key, &m.key) &&
                a->location.ownership_epoch == m.location.ownership_epoch) {
                struct dmesh_session *s = a->session;
                unsigned idx = m.key.flow_id - 1;
                if (m.type == RETIRED) {
                    s->closed[idx] = true;
                    s->close_status[idx] = 0;
                    s->close_pending[idx] = !s->closing;
                    if (a->backend_token && s->backend[w].token == a->backend_token)
                        s->backend[w].token = 0;
                    --d->loads[w].assigned;
                    if (a->opening) --d->loads[w].opening;
                    memset(a, 0, sizeof(*a));
                } else if (m.type == DMESH_SESSION_DOORBELL) {
                    /* One DOORBELL per ARM, from whichever worker rings first;
                     * dmesh_sessions_advance sends it and retries until accepted. */
                    uint64_t epoch = dmesh_session_get_u64(m.payload);
                    if (s->armed && s->armed_epoch == epoch) {
                        s->armed = false;
                        s->doorbell_pending_epoch = epoch;
                    }
                } else if (!s->closing) {
                    doca_error_t r = dmesh_session_send(a->control, s, m.type, m.key.flow_id,
                        m.key.generation, m.payload, m.length, m.status);
                    if (r == DOCA_ERROR_AGAIN || r == DOCA_ERROR_NO_MEMORY || r == DOCA_ERROR_IN_PROGRESS)
                        break; /* retain until accepted/disconnected */
                    if (r != DOCA_SUCCESS) {
                        s->closing = true;
                        dmesh_dispatcher_session_lost(a->control, s);
                    }
                    if (r == DOCA_SUCCESS && m.type == DMESH_SESSION_READY) {
                        a->ready = true;
                        if (a->opening) { a->opening = false; --d->loads[w].opening; }
                    }
                }
            }
            (void)peek(&d->workers[w].replies, &m, true);
        }
    }
}

/* Worker side of ARM, on the flow's worker-local session: ring at once if the
 * flow already published the descriptor the host expects, otherwise on its
 * next publication (dmesh_session_push_published). */
static void arm_flow(struct dmesh_conn *c, const struct message *m)
{
    if (!DMESH_FLOW_USES_PUSH(c->flow.mode)) return;
    uint64_t epoch = dmesh_session_get_u64(m->payload);
    struct dmesh_session *s = c->session;
    if (c->push_seq >= dmesh_session_get_u64(m->payload + 8)) {
        s->armed = false;
        s->doorbell_pending_epoch = epoch;
    } else {
        s->armed = true;
        s->armed_epoch = epoch;
    }
}

int dmesh_dispatch_worker_fd(struct objects *o) { return o->mailbox->fd; }
void dmesh_dispatch_worker_arm(struct objects *o) { clear(o->mailbox->fd); }
void dmesh_dispatch_worker_ready(struct objects *o)
{
    atomic_store(&o->mailbox->ready, true);
    wake(o->mailbox->dispatcher->fd);
}
void dmesh_dispatch_worker_drain(struct objects *o)
{
    struct dmesh_worker_mailbox *b = o->mailbox;
    if (!atomic_load_explicit(&b->requests.count, memory_order_acquire)) return;
    clear(b->fd);
    struct message m;
    while (peek(&b->requests, &m, true)) {
        if (m.location.worker != (uint32_t)o->worker_idx || m.location.slot >= DMESH_MAX_CONNECTIONS) continue;
        struct dmesh_conn *c = &o->conns[m.location.slot];
        if (m.type == OPEN) {
            /* Dispatcher reserves this exact slot until RETIRED. */
            if (c->state != DMESH_CONN_FREE || c->key.session_id) abort();
            memset(c, 0, sizeof(*c));
            c->objs = o; c->key = m.key; c->location = m.location;
            c->flow_id = m.key.flow_id; c->generation = m.key.generation;
            c->multiplexed = true;
            /* Worker-local lifecycle state, never the dispatcher's session.
             * Native multiplexed flows use MsgQ, not a peer Comch consumer;
             * no SDK connection pointer crosses the thread boundary. */
            c->session = &o->sessions[m.location.slot];
            memset(c->session, 0, sizeof(*c->session));
            c->pending_metadata = malloc(sizeof(*c->pending_metadata));
            if (c->pending_metadata) {
                memcpy(c->pending_metadata, m.payload, sizeof(*c->pending_metadata));
                c->state = DMESH_CONN_NEW;
            } else { c->error_status = ENOMEM; c->state = DMESH_CONN_ERROR; }
        } else if (same_key(&c->key, &m.key) &&
                   c->location.ownership_epoch == m.location.ownership_epoch && c->state != DMESH_CONN_FREE) {
            if (m.type == ARM) { arm_flow(c, &m); continue; }
            if (m.type == LOST) c->session->closing = true;
            if (m.type == CLOSE) c->close_requested = true;
            c->state = DMESH_CONN_CLOSING;
        }
    }
}
doca_error_t dmesh_dispatch_reply(struct dmesh_conn *c, uint16_t type,
                                  const void *payload, size_t length, int32_t status)
{
    if (length > DMESH_SESSION_MAX_PAYLOAD) return DOCA_ERROR_INVALID_VALUE;
    struct message m = {.key = c->key, .location = c->location,
                        .type = type, .status = status, .length = length};
    if (length) memcpy(m.payload, payload, length);
    if (!push(&c->objs->mailbox->replies, &m)) return DOCA_ERROR_AGAIN;
    wake(c->objs->mailbox->dispatcher->fd);
    return DOCA_SUCCESS;
}
void dmesh_dispatch_worker_flush(struct objects *o)
{
    for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        struct dmesh_conn *c = &o->conns[i];
        if (!c->key.session_id) continue;
        if (c->state == DMESH_CONN_FREE) {
            if (dmesh_dispatch_reply(c, RETIRED, NULL, 0, 0) == DOCA_SUCCESS) {
                memset(c->session, 0, sizeof(*c->session));
                memset(&c->key, 0, sizeof(c->key));
            }
        } else if (c->session->close_pending[c->flow_id - 1]) {
            if (dmesh_dispatch_reply(c, DMESH_SESSION_CLOSED, NULL, 0,
                                     c->session->close_status[c->flow_id - 1]) == DOCA_SUCCESS)
                c->session->close_pending[c->flow_id - 1] = false;
        }
        if (c->state != DMESH_CONN_FREE &&
            c->session->doorbell_pending_epoch != c->session->doorbell_sent_epoch) {
            uint8_t epoch[DMESH_SESSION_DOORBELL_SIZE];
            dmesh_session_put_u64(epoch, c->session->doorbell_pending_epoch);
            if (dmesh_dispatch_reply(c, DMESH_SESSION_DOORBELL, epoch, sizeof(epoch), 0) == DOCA_SUCCESS)
                c->session->doorbell_sent_epoch = c->session->doorbell_pending_epoch;
        }
    }
}
void dmesh_dispatch_worker_depart(struct objects *o)
{
    struct dmesh_worker_mailbox *b = o->mailbox;
    if (!b) return;
    atomic_store(&b->ready, false);
    atomic_store(&b->departed, true);
    struct dmesh_dispatcher *d = b->dispatcher;
    o->mailbox = NULL;
    wake(d->fd);
    /* Last reference release: the dispatcher may free d immediately. */
    atomic_fetch_sub(&d->remaining, 1);
}
bool dmesh_dispatch_worker_stop_admission(struct objects *o)
{
    struct dmesh_worker_mailbox *b = o->mailbox;
    if (!b) return true;
    pthread_mutex_lock(&b->requests.lock);
    atomic_store(&b->departed, true);
    atomic_store(&b->ready, false);
    bool empty = atomic_load(&b->requests.count) == 0;
    pthread_mutex_unlock(&b->requests.lock);
    wake(b->dispatcher->fd);
    return empty;
}

static void *dispatch_main(void *arg)
{
    struct dmesh_dispatcher *d = arg;
    pthread_setname_np(pthread_self(), "dmesh-control");
    struct pollfd *fds = d->pollfds;
    while (atomic_load(&d->remaining)) {
        clear(d->fd);
        for (size_t i = 0; i < d->count; ++i) {
            (void)dmesh_doca_ctrl_arm(d->controls[i]);
            (void)dmesh_doca_ctrl_drain(d->controls[i]);
        }
        flush_replies(d);
        flush_commands(d);
        flush_listeners(d);
        for (size_t i = 0; i < d->count; ++i) dmesh_sessions_advance(d->controls[i]);
        /* Retry pending SDK sends/HELLO deadlines without busy polling. */
        (void)poll(fds, d->count + 1, 10);
    }
    forget_listeners(d);
    /* No worker can retain exported buffers here: failed cleanup deliberately
     * retains its mailbox reference. Retire idle physical sessions last. */
    for (size_t i = 0; i < d->count; ++i)
        for (unsigned s = 0; s < DMESH_MAX_SESSIONS; ++s)
            d->controls[i]->sessions[s].closing = true;
    flush_replies(d);
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i) {
        if (d->flows[i].used) {
            fprintf(stderr, "[dispatcher] retaining unacknowledged flow reservation at shutdown\n");
            free(fds); return NULL;
        }
    }
    for (size_t i = 0; i < d->count; ++i) {
        struct objects *o = d->controls[i];
        for (unsigned pass = 0; pass < 100; ++pass) {
            (void)dmesh_doca_ctrl_drain(o);
            dmesh_sessions_advance(o);
        }
        if (cleanup_objects(o) != DOCA_SUCCESS) {
            fprintf(stderr, "[dispatcher] retaining control resources after cleanup failure\n");
            free(fds); return NULL;
        }
        free(o);
    }
    free(fds);
    for (size_t i = 0; i < d->count; ++i) {
        close(d->workers[i].fd);
        pthread_mutex_destroy(&d->workers[i].requests.lock);
        pthread_mutex_destroy(&d->workers[i].replies.lock);
    }
    close(d->fd); free(d->controls); free(d->workers); free(d->loads); free(d->flows); free(d);
    return NULL;
}

doca_error_t dmesh_dispatcher_start(struct objects **controls, struct objects **workers,
                                  size_t count, struct dmesh_dispatcher **out)
{
    if (!count || count > 128 || !controls || !workers || !out) return DOCA_ERROR_INVALID_VALUE;
    struct dmesh_dispatcher *d = calloc(1, sizeof(*d));
    if (!d) return DOCA_ERROR_NO_MEMORY;
    d->count = count; d->fd = -1;
    d->controls = calloc(count, sizeof(*d->controls));
    d->workers = calloc(count, sizeof(*d->workers));
    d->loads = calloc(count, sizeof(*d->loads));
    d->flows = calloc(count * DMESH_MAX_CONNECTIONS, sizeof(*d->flows));
    d->pollfds = calloc(count + 1, sizeof(*d->pollfds));
    if (!d->controls || !d->workers || !d->loads || !d->flows || !d->pollfds) goto fail;
    size_t initialized = 0;
    d->policy = dmesh_placement_least_flows;
    const char *policy = getenv("DMESH_FLOW_PLACEMENT");
    if (policy && strcmp(policy, "round-robin") == 0) d->policy = dmesh_placement_round_robin;
    else if (policy && strcmp(policy, "least-flows") != 0) goto fail;
    d->fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (d->fd < 0) goto fail;
    d->pollfds[0] = (struct pollfd){.fd = d->fd, .events = POLLIN};
    for (size_t i = 0; i < count; ++i) d->workers[i].fd = -1;
    for (size_t i = 0; i < count; ++i) {
        struct dmesh_worker_mailbox *b = &d->workers[i];
        b->dispatcher = d;
        atomic_init(&b->ready, false);
        atomic_init(&b->departed, false);
        atomic_init(&b->requests.count, 0);
        atomic_init(&b->replies.count, 0);
        pthread_mutex_init(&b->requests.lock, NULL);
        pthread_mutex_init(&b->replies.lock, NULL);
        ++initialized;
        b->fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (b->fd < 0) goto fail_initialized;
        int fd = -1;
        if (dmesh_doca_ctrl_get_fd(controls[i], &fd) != DOCA_SUCCESS) goto fail_initialized;
        d->pollfds[i + 1] = (struct pollfd){.fd = fd, .events = POLLIN};
        d->controls[i] = controls[i];
        d->loads[i].capacity = DMESH_MAX_CONNECTIONS;
    }
    atomic_init(&d->remaining, (unsigned)count);
    for (size_t i = 0; i < count; ++i) {
        controls[i]->dispatcher = d;
        workers[i]->mailbox = &d->workers[i];
        workers[i]->worker_idx = (int)i;
    }
    if (pthread_create(&d->thread, NULL, dispatch_main, d) != 0) {
        for (size_t i = 0; i < count; ++i) { controls[i]->dispatcher = NULL; workers[i]->mailbox = NULL; }
        goto fail_initialized;
    }
    pthread_detach(d->thread);
    *out = d;
    return DOCA_SUCCESS;
fail_initialized:
    for (size_t i = 0; i < initialized; ++i) {
        if (d->workers[i].fd >= 0) close(d->workers[i].fd);
        pthread_mutex_destroy(&d->workers[i].requests.lock);
        pthread_mutex_destroy(&d->workers[i].replies.lock);
    }
fail:
    if (d->fd >= 0) close(d->fd);
    free(d->pollfds); free(d->controls); free(d->workers); free(d->loads); free(d->flows); free(d);
    return DOCA_ERROR_INITIALIZATION;
}
