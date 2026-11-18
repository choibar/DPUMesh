#define _GNU_SOURCE
#include "dispatcher.h"
#include "placement.h"
#include "object.h"
#include "comch_wake.h"
#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <linux/perf_event.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

/* Only control messages cross these queues. In particular no DMA descriptor,
 * payload, data completion or credit update visits the dispatcher. */
#define QUEUE_SIZE 128u
enum command { OPEN = 100, CLOSE, LOST, RETIRED, BACKEND_DEMAND, ARM, DRAIN, SHUTDOWN };
struct message {
    struct dmesh_flow_key key;
    struct dmesh_flow_location location;
    uint16_t type;
    int32_t status;
    size_t length;
    /* OPEN of a session with a fast-path consumer: the worker creates its
     * producer on this connection (under the SDK lock). */
    struct doca_comch_connection *wake_connection;
    uint32_t wake_consumer;
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
    struct objects *objs;       /* the worker's objects: its slots' flow_stats */
    _Atomic int tid;            /* the worker thread, for its busy time */
    uint64_t busy_ticks;        /* its CPU ticks at the last flow-stat sample */
    /* DMESH_WORKER_INSTR=1: a counter of the thread's retired instructions
     * (instr_tid's), and its value at the last sample. */
    int instr_fd, instr_tid;
    uint64_t instr_prev;
};
struct assignment {
    bool used, opening, ready, close_pending, lost_pending, shut_pending;
    uint64_t shut_descs;           /* SHUTDOWN's descriptor count */
    struct objects *control;
    struct dmesh_session *session;
    struct dmesh_flow_key key;
    struct dmesh_flow_location location;
    uint32_t backend_token;
    /* Migration: a draining flow was told to GOAWAY; its host reconnects and
     * the reconnect is steered (struct steer) to another worker. */
    bool draining, drain_pending;
    uint32_t mode, dst_ip;
    uint16_t dst_port;
    uint64_t pair;              /* client flows: pair_key(); 0 for backends */
    uint64_t ready_ms;
    /* Flow statistics: the proxy's counters at the last sample, and the
     * rates over the last DMESH_FLOWSTAT_MS. */
    uint64_t stat_ns, stat_prev[DMESH_FLOW_STATS];
    unsigned rps, cpu_permille, cpu_ns_per_req, residence_us, backend_us;
    /* Retired instructions per request over the last period that had
     * requests (kept through idle periods); 0 while the proxy counts none.
     * stat_valid: rps and instr_per_req hold a measured period. */
    unsigned instr_per_req;
    bool stat_valid;
    /* Spare mode: the old flow waits (until spare_deadline_ms) for the spare
     * that replaces it; the spare remembers the flow it replaces. */
    bool awaiting_spare, replaces;
    uint64_t spare_deadline_ms;
    size_t replaced;
    struct dmesh_flow_key replaced_key;
};
/* The next client OPEN of `session` to dst goes to `worker`, until expiry. */
struct steer {
    struct dmesh_session *session;
    uint64_t session_epoch, expires_ms;
    uint32_t dst_ip, worker;
    uint16_t dst_port;
    bool replaces;              /* spare mode: the flow at `replaced` waits */
    size_t replaced;
    struct dmesh_flow_key replaced_key;
    uint64_t instr_ps;          /* the migrating flow's measured cost */
};
/* Flow scaling: the stream count asked of one session's clients of a
 * destination. The entry stays after its ACK, so a count is asked once. It
 * travels on a client flow to that destination (flow_id/generation). */
struct scale {
    struct objects *control;
    struct dmesh_session *session;
    uint64_t session_epoch, sent_ms;
    uint32_t dst_ip, count, token, applied, flow_id, generation;
    uint16_t dst_port;
    bool pending, acked, scaled_in;
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
    /* DMESH_MIGRATE_PERIOD_MS: drain every client flow that has been ready on
     * its worker this long and steer its reconnect to the next worker (a
     * mechanism test; the policy deciding when/where comes later).
     * DMESH_MIGRATE_MAX caps the total (0 = unlimited). */
    uint64_t migrate_period_ms, steer_ms;
    unsigned long migrate_max, migrations;
    bool migrate_spare;         /* DMESH_MIGRATE_SPARE=1: PREPARE first */
    struct steer steers[DMESH_MAX_CONNECTIONS];
    /* DMESH_SCALE_AFTER_MS: once a client flow has been ready this long, ask
     * its session's clients of that destination for DMESH_SCALE_COUNT
     * streams (default 2); DMESH_SCALE_IN_AFTER_MS later, for one again (0 =
     * never). A mechanism test; the policy deciding when comes later. */
    uint64_t scale_after_ms, scale_in_after_ms;
    unsigned scale_count;
    uint32_t next_scale_token;
    struct scale scales[DMESH_MAX_CONNECTIONS];
    /* DMESH_FLOWSTAT_MS (default 1000, 0 = off): sample every client flow's
     * statistics and each worker's busy time this often; DMESH_FLOWSTAT_LOG=1
     * also logs them. */
    uint64_t flowstat_ms, flowstat_next_ms, flowstat_ns;
    bool flowstat_log, worker_instr;
    /* Mean instructions per second of the client flows measured last period:
     * the cost placement assumes for a flow not yet measured. */
    uint64_t flow_instr_mean;
    int fd;
    pthread_t thread;
    _Atomic unsigned remaining;
    pthread_mutex_t sdk_lock;   /* see dmesh_dispatch_sdk_lock */
};

/* Read-only membership snapshots for Rust routing before any backend exists.
 * A retired endpoint stays known for DMESH_LISTENER_GRACE_MS, so a restarting
 * DMA server is refused rather than dialled over TCP, then is forgotten: its
 * address may belong to a different, TCP workload by then (Pod IPs are
 * reused). */
#define LISTENER_GRACE_MS_DEFAULT 30000u
struct listener_membership {
    struct dmesh_dispatcher *dispatcher;
    uint32_t ip;
    uint16_t port;
    uint64_t retired_ms;      /* 0 while a session listens */
    struct listener_membership *next;
};
static pthread_mutex_t membership_lock = PTHREAD_MUTEX_INITIALIZER;
static struct listener_membership *memberships;
static uint64_t membership_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}
static uint64_t listener_grace_ms(void)
{
    static uint64_t grace = UINT64_MAX;
    if (grace == UINT64_MAX) {
        const char *env = getenv("DMESH_LISTENER_GRACE_MS");
        char *end = NULL;
        unsigned long long v = env && *env ? strtoull(env, &end, 10) : 0;
        grace = env && *env && end && !*end ? (uint64_t)v : LISTENER_GRACE_MS_DEFAULT;
    }
    return grace;
}
int dmesh_dispatch_listener_known(uint32_t ip, uint16_t port)
{
    uint64_t now = membership_now_ms(), grace = listener_grace_ms();
    pthread_mutex_lock(&membership_lock);
    int known = 0;
    for (struct listener_membership **p = &memberships; *p;) {
        struct listener_membership *m = *p;
        if (m->retired_ms && now - m->retired_ms >= grace) { *p = m->next; free(m); continue; }
        if (m->ip == ip && m->port == port) known = 1;
        p = &m->next;
    }
    pthread_mutex_unlock(&membership_lock);
    return known;
}
static int remember_listener(struct dmesh_dispatcher *d, uint32_t ip, uint16_t port)
{
    pthread_mutex_lock(&membership_lock);
    for (struct listener_membership *m = memberships; m; m = m->next)
        if (m->dispatcher == d && m->ip == ip && m->port == port) {
            m->retired_ms = 0;
            pthread_mutex_unlock(&membership_lock); return 0;
        }
    struct listener_membership *m = malloc(sizeof(*m));
    if (m) {
        *m = (struct listener_membership){d, ip, port, 0, memberships};
        memberships = m;
    }
    pthread_mutex_unlock(&membership_lock);
    return m ? 0 : ENOMEM;
}
static void retire_listener(struct dmesh_dispatcher *d, uint32_t ip, uint16_t port)
{
    pthread_mutex_lock(&membership_lock);
    for (struct listener_membership *m = memberships; m; m = m->next)
        if (m->dispatcher == d && m->ip == ip && m->port == port && !m->retired_ms)
            m->retired_ms = membership_now_ms();
    pthread_mutex_unlock(&membership_lock);
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

static uint64_t realtime_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000u + (uint64_t)ts.tv_nsec;
}
static uint64_t env_u64(const char *name, uint64_t fallback)
{
    const char *env = getenv(name);
    char *end = NULL;
    unsigned long long v = env && *env ? strtoull(env, &end, 10) : 0;
    return env && *env && end && !*end ? (uint64_t)v : fallback;
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

/* A listening session is closing: its address starts its grace period unless
 * another session already listens there. Idempotent. */
static void retire_session_listener(struct dmesh_dispatcher *d, struct dmesh_session *s)
{
    if (s->listen_port && !listener(d, s->listen_ip, s->listen_port))
        retire_listener(d, s->listen_ip, s->listen_port);
}

/* Ask listener s for a backend flow on worker, unless one is requested or
 * open there. Returns whether a request was queued. */
static bool backend_demand_session(struct dmesh_dispatcher *d, size_t worker, struct dmesh_session *s)
{
    if (!s || s->backend[worker].token || !atomic_load(&d->workers[worker].ready) ||
        atomic_load(&d->workers[worker].departed) ||
        d->loads[worker].assigned >= DMESH_MAX_CONNECTIONS) return false;
    /* Never wrap a token: delayed requests must not alias a new operation. */
    if (d->next_backend_token == UINT32_MAX) return false;
    s->backend[worker].token = ++d->next_backend_token;
    s->backend[worker].sent = false;
    return true;
}

static void backend_demand(struct dmesh_dispatcher *d, size_t worker, const struct message *m)
{
    uint32_t ip = dmesh_session_get_u32(m->payload);
    uint16_t port = (uint16_t)dmesh_session_get_u32(m->payload + 4);
    (void)backend_demand_session(d, worker, listener(d, ip, port));
}

/* A client flow of a pair that other workers already serve was placed on
 * owner: open there the backends those workers hold, so its first requests
 * do not wait for backend setup. The other workers' backends stand for what
 * the pair uses; one never used waits in the proxy's registry. Reads the
 * loads[].pair_flows that pair_restrict() filled for this placement. */
static void backend_follow(struct dmesh_dispatcher *d, size_t owner)
{
    unsigned queued = 0;
    for (size_t c = 0; c < d->count; ++c)
        for (unsigned i = 0; i < DMESH_MAX_SESSIONS; ++i) {
            struct dmesh_session *s = &d->controls[c]->sessions[i];
            if (!s->occupied || !s->negotiated || s->closing || !s->listen_port) continue;
            for (size_t w = 0; w < d->count; ++w) {
                if (w == owner || !d->loads[w].pair_flows || !s->backend[w].token) continue;
                queued += backend_demand_session(d, owner, s);
                break;
            }
        }
    if (queued)
        fprintf(stderr, "[dispatcher] backend-follow worker=%zu listeners=%u unix_ns=%llu\n",
                owner, queued, (unsigned long long)realtime_ns());
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
            if (s->occupied && s->closing) retire_session_listener(d, s);
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

/* A svc-to-svc pair: the source workload and the destination service. Client
 * flows of one pair are spread over workers, so a scaled-out pair gets the
 * extra cores it was scaled for. */
static uint64_t pair_key(const struct dmesh_flow_id *flow)
{
    uint64_t h = 1469598103934665603ull;            /* FNV-1a */
    for (size_t i = 0; i < sizeof(flow->src_workload) && flow->src_workload[i]; ++i)
        h = (h ^ (uint8_t)flow->src_workload[i]) * 1099511628211ull;
    const uint8_t dst[6] = {(uint8_t)(flow->dst_ip >> 24), (uint8_t)(flow->dst_ip >> 16),
                            (uint8_t)(flow->dst_ip >> 8), (uint8_t)flow->dst_ip,
                            (uint8_t)(flow->dst_port >> 8), (uint8_t)flow->dst_port};
    for (size_t i = 0; i < sizeof(dst); ++i)
        h = (h ^ dst[i]) * 1099511628211ull;
    return h ? h : 1;
}

/* Fills loads[].pending_instr_ps: each client flow placed but not yet
 * measured (opening, or ready less than two statistics periods) is assumed to
 * cost the mean measured flow, and a migration steered to a worker but not yet
 * reconnected costs what the migrating flow measured, so placements between
 * two samples spread instead of all landing on the worker that looked least
 * loaded. */
static void pending_instr(struct dmesh_dispatcher *d)
{
    for (size_t w = 0; w < d->count; ++w) {
        unsigned pending = 0;
        for (size_t s = 0; s < DMESH_MAX_CONNECTIONS; ++s) {
            const struct assignment *a = &d->flows[w * DMESH_MAX_CONNECTIONS + s];
            if (a->used && !a->backend_token && !a->stat_valid &&
                (a->mode == DMESH_FLOW_MODE_CLIENT || a->mode == DMESH_FLOW_MODE_INGRESS_PUSH))
                ++pending;
        }
        d->loads[w].pending_instr_ps = pending * d->flow_instr_mean;
    }
    uint64_t now = membership_now_ms();
    for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        const struct steer *st = &d->steers[i];
        if (st->session && now < st->expires_ms && st->worker < d->count)
            d->loads[st->worker].pending_instr_ps += st->instr_ps;
    }
}

/* Fills loads[].pair_flows with the pair's client flows per worker, opening
 * ones included, and leaves eligible only the workers holding the fewest: a
 * pair never doubles up on a worker while another has room and none of it.
 * Returns that fewest count. */
static unsigned pair_restrict(struct dmesh_dispatcher *d, uint64_t pair)
{
    unsigned fewest = UINT_MAX;
    for (size_t w = 0; w < d->count; ++w) {
        struct dmesh_worker_load *l = &d->loads[w];
        l->pair_flows = 0;
        for (unsigned slot = 0; slot < DMESH_MAX_CONNECTIONS; ++slot) {
            const struct assignment *a = &d->flows[w * DMESH_MAX_CONNECTIONS + slot];
            if (a->used && a->pair == pair) ++l->pair_flows;
        }
        if (l->eligible && l->assigned < l->capacity && l->pair_flows < fewest)
            fewest = l->pair_flows;
    }
    for (size_t w = 0; w < d->count; ++w)
        if (d->loads[w].pair_flows > fewest) d->loads[w].eligible = false;
    return fewest;
}

static bool take_steer(struct dmesh_dispatcher *d, struct dmesh_session *s,
                       uint32_t dst_ip, uint16_t dst_port, struct steer *out)
{
    uint64_t now = membership_now_ms();
    for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        struct steer *st = &d->steers[i];
        if (!st->session) continue;
        if (now >= st->expires_ms) { memset(st, 0, sizeof(*st)); continue; }
        if (st->session == s && st->session_epoch == s->epoch &&
            st->dst_ip == dst_ip && st->dst_port == dst_port) {
            *out = *st;
            memset(st, 0, sizeof(*st));
            return true;
        }
    }
    return false;
}
static struct scale *find_scale(struct dmesh_dispatcher *d, const struct dmesh_session *s,
                                uint32_t ip, uint16_t port)
{
    for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        struct scale *e = &d->scales[i];
        if (e->session == s && e->session_epoch == s->epoch &&
            e->dst_ip == ip && e->dst_port == port) return e;
    }
    return NULL;
}

/* The flow-scaling hook: ask session s's clients of ip:port to keep count
 * streams to it, on the client flow `via` (NULL: the flow the entry used
 * before). count is clamped to the eligible workers, the most streams the
 * pair placement can put on distinct workers. Returns the count asked, or
 * -1 when no entry is free. */
static int dmesh_scale(struct dmesh_dispatcher *d, struct objects *o, struct dmesh_session *s,
                       uint32_t ip, uint16_t port, unsigned count, const struct dmesh_flow_key *via)
{
    unsigned eligible = 0;
    for (size_t w = 0; w < d->count; ++w)
        eligible += atomic_load(&d->workers[w].ready) && !atomic_load(&d->workers[w].departed);
    if (count > eligible) count = eligible;
    if (count < 1) count = 1;
    struct scale *e = find_scale(d, s, ip, port);
    for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS && !e; ++i) {
        struct scale *f = &d->scales[i];
        if (!f->session || !f->session->occupied || f->session->closing ||
            f->session->epoch != f->session_epoch) e = f;
    }
    if (!e) return -1;
    if (!++d->next_scale_token) ++d->next_scale_token;
    bool same = e->session == s && e->session_epoch == s->epoch;
    bool scaled_in = same && e->scaled_in;
    uint32_t flow_id = via ? via->flow_id : same ? e->flow_id : 0;
    uint32_t generation = via ? via->generation : same ? e->generation : 0;
    *e = (struct scale){.control = o, .session = s, .session_epoch = s->epoch,
        .dst_ip = ip, .dst_port = port, .count = count, .token = d->next_scale_token,
        .flow_id = flow_id, .generation = generation, .pending = true, .scaled_in = scaled_in};
    return (int)count;
}

static bool scale_rides(const struct assignment *a, const struct scale *e)
{
    return a->used && a->session == e->session && a->ready && !a->draining && !a->backend_token &&
           !a->close_pending && !a->lost_pending &&
           (a->mode == DMESH_FLOW_MODE_CLIENT || a->mode == DMESH_FLOW_MODE_INGRESS_PUSH) &&
           a->dst_ip == e->dst_ip && a->dst_port == e->dst_port;
}

/* The client flow a SCALE travels on: the one it was asked on or, once that
 * one is gone, another of the session's ready client flows to the
 * destination. NULL while the session has none: the SCALE waits for one. */
static struct assignment *scale_flow(struct dmesh_dispatcher *d, struct scale *e)
{
    struct assignment *a = find(d, e->session, e->flow_id);
    if (a && a->key.generation == e->generation && scale_rides(a, e)) return a;
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i) {
        a = &d->flows[i];
        if (scale_rides(a, e)) {
            e->flow_id = a->key.flow_id;
            e->generation = a->key.generation;
            return a;
        }
    }
    return NULL;
}

/* A SCALE unacknowledged this long is sent again: the flow it went on may
 * have closed before its host took it. A host applies a count idempotently. */
#define SCALE_RESEND_MS 1000

static void flush_scales(struct dmesh_dispatcher *d)
{
    uint64_t now = membership_now_ms();
    for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        struct scale *e = &d->scales[i];
        if (e->session && !e->pending && !e->acked && e->sent_ms && now - e->sent_ms >= SCALE_RESEND_MS)
            e->pending = true;
        if (!e->pending) continue;
        if (!e->session->occupied || e->session->closing || e->session->epoch != e->session_epoch) {
            e->pending = false;
            continue;
        }
        struct assignment *a = scale_flow(d, e);
        if (!a) continue;
        uint8_t payload[8];
        dmesh_session_put_u32(payload, e->count);
        dmesh_session_put_u32(payload + 4, e->token);
        if (dmesh_session_send(e->control, e->session, DMESH_SESSION_SCALE, a->key.flow_id,
                               a->key.generation, payload, sizeof(payload), 0) != DOCA_SUCCESS) continue;
        e->pending = false;
        e->sent_ms = now;
        fprintf(stderr, "[dispatcher] scale session=%lu/%lu flow=%u/%u dst=%08x:%u count=%u token=%u unix_ns=%llu\n",
                session_id(d, e->control, e->session), e->session_epoch, a->key.flow_id,
                a->key.generation, e->dst_ip, e->dst_port, e->count, e->token,
                (unsigned long long)realtime_ns());
    }
}

static void scale_due(struct dmesh_dispatcher *d)
{
    if (!d->scale_after_ms) return;
    uint64_t now = membership_now_ms();
    for (unsigned i = 0; d->scale_in_after_ms && i < DMESH_MAX_CONNECTIONS; ++i) {
        struct scale *e = &d->scales[i];
        if (!e->session || e->pending || e->scaled_in || e->count < 2 || !e->sent_ms ||
            now - e->sent_ms < d->scale_in_after_ms) continue;
        if (dmesh_scale(d, e->control, e->session, e->dst_ip, e->dst_port, 1, NULL) > 0)
            e->scaled_in = true;
    }
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i) {
        struct assignment *a = &d->flows[i];
        if (!a->used || !a->ready || a->backend_token || a->close_pending || a->lost_pending ||
            a->session->closing ||
            (a->mode != DMESH_FLOW_MODE_CLIENT && a->mode != DMESH_FLOW_MODE_INGRESS_PUSH) ||
            now - a->ready_ms < d->scale_after_ms ||
            find_scale(d, a->session, a->dst_ip, a->dst_port))
            continue;
        (void)dmesh_scale(d, a->control, a->session, a->dst_ip, a->dst_port, d->scale_count, &a->key);
    }
}

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* A thread's user + system CPU in clock ticks, or 0 when unreadable. */
static uint64_t thread_ticks(int tid)
{
    char path[64], line[512];
    snprintf(path, sizeof(path), "/proc/self/task/%d/stat", tid);
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char *ok = fgets(line, sizeof(line), f);
    fclose(f);
    char *p = ok ? strrchr(line, ')') : NULL;
    unsigned long utime = 0, stime = 0;
    /* After "pid (comm)": state, then fields 4..13, then utime and stime. */
    if (!p || sscanf(p + 1, " %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu",
                     &utime, &stime) != 2) return 0;
    return (uint64_t)utime + stime;
}

/* DMESH_WORKER_INSTR: the instructions per second worker w's thread retired
 * over the last period, all of its work counted (flows, backend connections,
 * the DMA driver), from a counter opened on the thread when its tid is known.
 * The event matches the proxy's per-flow counter (INST_RETIRED, user and
 * kernel), so the two compare. */
static void worker_instr_sample(struct dmesh_dispatcher *d, size_t w, int tid, uint64_t period)
{
    struct dmesh_worker_mailbox *m = &d->workers[w];
    d->loads[w].worker_instr_ps = 0;
    if (tid != m->instr_tid) {
        if (m->instr_fd >= 0) close(m->instr_fd);
        m->instr_fd = -1;
        m->instr_tid = tid;
        if (!tid) return;
        struct perf_event_attr attr = {.type = PERF_TYPE_RAW, .size = sizeof(attr),
                                       .config = 0x08 /* INST_RETIRED */, .exclude_hv = 1};
        m->instr_fd = (int)syscall(SYS_perf_event_open, &attr, tid, -1, -1, PERF_FLAG_FD_CLOEXEC);
        if (m->instr_fd < 0)
            fprintf(stderr, "[dispatcher] worker=%zu instruction counter: %s\n", w, strerror(errno));
        m->instr_prev = 0;
    }
    uint64_t v;
    if (m->instr_fd < 0 || read(m->instr_fd, &v, sizeof(v)) != (ssize_t)sizeof(v)) return;
    if (period && m->instr_prev && v >= m->instr_prev)
        d->loads[w].worker_instr_ps = (v - m->instr_prev) * 1000000000ull / period;
    m->instr_prev = v;
}

/* Samples every ready client flow's statistics and each worker's busy time,
 * into the assignments and loads[] that placement sees. */
static void flowstat_due(struct dmesh_dispatcher *d)
{
    if (!d->flowstat_ms) return;
    uint64_t now_ms = membership_now_ms();
    if (now_ms < d->flowstat_next_ms) return;
    d->flowstat_next_ms = now_ms + d->flowstat_ms;
    uint64_t now = monotonic_ns(), period = d->flowstat_ns ? now - d->flowstat_ns : 0;
    d->flowstat_ns = now;
    unsigned flows[d->count];
    uint64_t instr_total = 0, measured = 0;
    for (size_t w = 0; w < d->count; ++w) {
        d->loads[w].rps = d->loads[w].flow_cpu_permille = 0;
        d->loads[w].flow_instr_ps = 0;
        flows[w] = 0;
    }
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i) {
        struct assignment *a = &d->flows[i];
        if (!a->used || !a->ready || a->backend_token ||
            (a->mode != DMESH_FLOW_MODE_CLIENT && a->mode != DMESH_FLOW_MODE_INGRESS_PUSH))
            continue;
        size_t w = a->location.worker;
        const struct dmesh_conn *c = &d->workers[w].objs->conns[a->location.slot];
        uint64_t v[DMESH_FLOW_STATS];
        for (int k = 0; k < DMESH_FLOW_STATS; ++k)
            v[k] = __atomic_load_n(&c->flow_stats[k], __ATOMIC_RELAXED);
        /* The first sample, or counters that restarted, only set a baseline. */
        bool baseline = !a->stat_ns || v[0] < a->stat_prev[0] || v[1] < a->stat_prev[1];
        uint64_t dt = now - a->stat_ns;
        if (!baseline && dt) {
            uint64_t req = v[0] - a->stat_prev[0], cpu = v[1] - a->stat_prev[1];
            a->rps = (unsigned)(req * 1000000000ull / dt);
            a->cpu_permille = (unsigned)(cpu * 1000 / dt);
            a->cpu_ns_per_req = req ? (unsigned)(cpu / req) : 0;
            a->residence_us = req ? (unsigned)((v[2] - a->stat_prev[2]) / req / 1000) : 0;
            a->backend_us = req ? (unsigned)((v[3] - a->stat_prev[3]) / req / 1000) : 0;
            if (req) a->instr_per_req = (unsigned)((v[4] - a->stat_prev[4]) / req);
            a->stat_valid = true;
            d->loads[w].rps += a->rps;
            d->loads[w].flow_cpu_permille += a->cpu_permille;
            d->loads[w].flow_instr_ps += (uint64_t)a->rps * a->instr_per_req;
            instr_total += (uint64_t)a->rps * a->instr_per_req;
            ++measured;
            ++flows[w];
            if (d->flowstat_log)
                fprintf(stderr, "[dispatcher] flowstat worker=%zu slot=%u flow=%u/%u session=%lu/%lu pair=%016llx rps=%u cpu_permille=%u cpu_ns_per_req=%u residence_us=%u backend_us=%u instr_per_req=%u unix_ns=%llu\n",
                        w, a->location.slot, a->key.flow_id, a->key.generation,
                        a->key.session_id, a->key.session_epoch, (unsigned long long)a->pair,
                        a->rps, a->cpu_permille, a->cpu_ns_per_req, a->residence_us,
                        a->backend_us, a->instr_per_req, (unsigned long long)realtime_ns());
        }
        a->stat_ns = now;
        memcpy(a->stat_prev, v, sizeof(v));
    }
    d->flow_instr_mean = measured ? instr_total / measured : 0;
    long hz = sysconf(_SC_CLK_TCK);
    for (size_t w = 0; w < d->count; ++w) {
        int tid = atomic_load(&d->workers[w].tid);
        uint64_t ticks = tid ? thread_ticks(tid) : 0;
        if (period && hz > 0 && ticks >= d->workers[w].busy_ticks && d->workers[w].busy_ticks)
            d->loads[w].busy_permille = (unsigned)((ticks - d->workers[w].busy_ticks) *
                1000000000000ull / (uint64_t)hz / period);
        d->workers[w].busy_ticks = ticks;
        if (d->worker_instr) worker_instr_sample(d, w, tid, period);
        if (d->flowstat_log && period)
            fprintf(stderr, "[dispatcher] workerstat worker=%zu flows=%u rps=%u flow_cpu_permille=%u busy_permille=%u flow_instr_ps=%llu worker_instr_ps=%llu unix_ns=%llu\n",
                    w, flows[w], d->loads[w].rps, d->loads[w].flow_cpu_permille,
                    d->loads[w].busy_permille, (unsigned long long)d->loads[w].flow_instr_ps,
                    (unsigned long long)d->loads[w].worker_instr_ps,
                    (unsigned long long)realtime_ns());
    }
}

/* Where migrating flow a goes: the placement policy's choice among the
 * workers other than its own, under the same svc-to-svc pair rule an OPEN
 * sees, with flows not yet measured and migrations not yet reconnected
 * counted (pending_instr). -1 when no other worker can take it. */
static int migration_target(struct dmesh_dispatcher *d, const struct assignment *a)
{
    size_t from = a->location.worker;
    for (size_t w = 0; w < d->count; ++w)
        d->loads[w].eligible = w != from && atomic_load(&d->workers[w].ready) &&
                               !atomic_load(&d->workers[w].departed);
    pending_instr(d);
    pair_restrict(d, a->pair);
    struct dmesh_flow_spec spec = {
        .session_id = a->key.session_id, .session_epoch = a->key.session_epoch,
        .flow_id = a->key.flow_id, .generation = a->key.generation, .mode = a->mode,
        .service_ip = a->dst_ip, .service_port = a->dst_port,
    };
    int to = d->policy.select(&spec, d->loads, d->count, (from + 1) % d->count, d->policy.context);
    if (d->flowstat_log) {
        /* Each worker as eligible:measured+pending instructions per second. */
        char loads[1024];
        size_t n = 0;
        for (size_t w = 0; w < d->count && n < sizeof(loads); ++w)
            n += (size_t)snprintf(loads + n, sizeof(loads) - n, "%s%d:%llu+%llu", w ? "," : "",
                                  d->loads[w].eligible, (unsigned long long)d->loads[w].flow_instr_ps,
                                  (unsigned long long)d->loads[w].pending_instr_ps);
        fprintf(stderr, "[dispatcher] migrate-target flow=%u/%u from=%zu to=%d loads=%s unix_ns=%llu\n",
                a->key.flow_id, a->key.generation, from, to, loads, (unsigned long long)realtime_ns());
    }
    for (size_t w = 0; w < d->count; ++w)
        d->loads[w].eligible = atomic_load(&d->workers[w].ready) && !atomic_load(&d->workers[w].departed);
    return to >= 0 && (size_t)to != from ? to : -1;
}

/* Drain each client flow ready for migrate_period_ms; steer its reconnect to
 * migration_target(). The old flow keeps its worker and slot until the host
 * closes it, so no live state ever changes owner. */
static void migrate_due(struct dmesh_dispatcher *d)
{
    if (!d->migrate_period_ms || d->count < 2) return;
    uint64_t now = membership_now_ms();
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i) {
        struct assignment *a = &d->flows[i];
        if (a->used && a->awaiting_spare && now >= a->spare_deadline_ms) {
            /* No SPARE_READY in time: degrade to the GOAWAY-only path. */
            a->awaiting_spare = false;
            a->drain_pending = true;
            fprintf(stderr, "[dispatcher] spare-timeout flow=%u/%u unix_ns=%llu\n",
                    a->key.flow_id, a->key.generation, (unsigned long long)realtime_ns());
        }
    }
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i) {
        if (d->migrate_max && d->migrations >= d->migrate_max) return;
        struct assignment *a = &d->flows[i];
        if (!a->used || !a->ready || a->draining || a->backend_token ||
            a->close_pending || a->lost_pending || a->session->closing ||
            (a->mode != DMESH_FLOW_MODE_CLIENT && a->mode != DMESH_FLOW_MODE_INGRESS_PUSH) ||
            now - a->ready_ms < d->migrate_period_ms)
            continue;
        size_t from = a->location.worker;
        struct steer *st = NULL;
        for (unsigned j = 0; j < DMESH_MAX_CONNECTIONS && !st; ++j)
            if (!d->steers[j].session || now >= d->steers[j].expires_ms) st = &d->steers[j];
        if (!st) continue;
        int target = migration_target(d, a);
        if (target < 0) continue;
        size_t to = (size_t)target;
        uint64_t cost = a->stat_valid ? (uint64_t)a->rps * a->instr_per_req : d->flow_instr_mean;
        *st = (struct steer){.session = a->session, .session_epoch = a->session->epoch,
            .expires_ms = now + d->steer_ms, .dst_ip = a->dst_ip, .dst_port = a->dst_port,
            .worker = (uint32_t)to, .replaces = d->migrate_spare, .replaced = i,
            .replaced_key = a->key, .instr_ps = cost};
        a->draining = true;
        if (d->migrate_spare) {
            if (dmesh_session_send(a->control, a->session, DMESH_SESSION_PREPARE, a->key.flow_id,
                                   a->key.generation, NULL, 0, 0) == DOCA_SUCCESS) {
                a->awaiting_spare = true;
                a->spare_deadline_ms = now + d->steer_ms;
            } else {
                st->replaces = false;
                a->drain_pending = true;
            }
        } else {
            a->drain_pending = true;
        }
        ++d->migrations;
        fprintf(stderr, "[dispatcher] migrate session=%lu/%lu flow=%u/%u from=%zu slot=%u to=%zu n=%lu unix_ns=%llu\n",
                a->key.session_id, a->key.session_epoch, a->key.flow_id, a->key.generation,
                from, a->location.slot, to, d->migrations, (unsigned long long)realtime_ns());
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
    if (h->type == DMESH_SESSION_SCALE_ACK) {
        for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
            struct scale *e = &d->scales[i];
            if (e->session != s || e->session_epoch != s->epoch ||
                e->token != dmesh_session_get_u32(payload))
                continue;
            e->acked = true;
            e->applied = dmesh_session_get_u32(payload + 4);
            fprintf(stderr, "[dispatcher] scale-ack session=%lu/%lu dst=%08x:%u count=%u applied=%u unix_ns=%llu\n",
                    session_id(d, o, s), s->epoch, e->dst_ip, e->dst_port, e->count, e->applied,
                    (unsigned long long)realtime_ns());
        }
        return;
    }
    if (!h->flow_id || h->flow_id > DMESH_MAX_CONNECTIONS) return;
    unsigned idx = h->flow_id - 1;
    struct assignment *a = find(d, s, h->flow_id);
    if (h->type == DMESH_SESSION_SHUTDOWN) {
        if (a && a->key.generation == h->generation) {
            a->shut_descs = dmesh_session_get_u64(payload);
            a->shut_pending = true;  /* durable like CLOSE */
        }
        return;
    }
    if (h->type == DMESH_SESSION_SPARE_READY) {
        if (!a || a->key.generation != h->generation || !a->replaces) return;
        a->replaces = false;
        struct assignment *old = &d->flows[a->replaced];
        if (old->used && same_key(&old->key, &a->replaced_key) && old->awaiting_spare) {
            old->awaiting_spare = false;
            old->drain_pending = true;
            fprintf(stderr, "[dispatcher] spare-ready flow=%u/%u owner=%u replaces=%u/%u unix_ns=%llu\n",
                    h->flow_id, h->generation, a->location.worker, old->key.flow_id,
                    old->key.generation, (unsigned long long)realtime_ns());
        }
        return;
    }
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
    /* A flow-wake host appends the u32 id of the flow's own consumer. */
    size_t base = sizeof(struct dmesh_export_metadata_msg) + (backend ? 8 : 0);
    if ((h->type != DMESH_SESSION_OPEN && !backend) ||
        (h->payload_len != base && h->payload_len != base + 4)) {
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
    uint32_t flow_consumer = h->payload_len == base + 4 ?
        dmesh_session_get_u32((const uint8_t *)payload + sizeof(metadata)) : 0;
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
    uint64_t pair = backend ? 0 : pair_key(&metadata.flow);
    for (size_t i = 0; i < d->count; ++i)
        d->loads[i].eligible = atomic_load(&d->workers[i].ready) && !atomic_load(&d->workers[i].departed);
    if (!backend) pending_instr(d);
    unsigned pair_fewest = backend ? 0 : pair_restrict(d, pair);
    int owner = backend ? (int)requested_worker :
        d->policy.select(&spec, d->loads, d->count, d->cursor, d->policy.context);
    for (size_t i = 0; i < d->count; ++i)   /* the steer below sees every worker */
        d->loads[i].eligible = atomic_load(&d->workers[i].ready) && !atomic_load(&d->workers[i].departed);
    struct steer steer = {0};
    int steered = !backend && take_steer(d, s, metadata.flow.dst_ip, metadata.flow.dst_port, &steer) ?
        (int)steer.worker : -1;
    if (steered >= 0 && d->loads[steered].eligible &&
        d->loads[steered].assigned < DMESH_MAX_CONNECTIONS)
        owner = steered;
    else
        steered = -1;
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
        .backend_token = request_token, .mode = metadata.flow.mode,
        .dst_ip = metadata.flow.dst_ip, .dst_port = metadata.flow.dst_port, .pair = pair,
        .replaces = steered >= 0 && steer.replaces, .replaced = steer.replaced,
        .replaced_key = steer.replaced_key};
    struct message m = {.key = a->key, .location = a->location, .type = OPEN, .length = sizeof(metadata)};
    memcpy(m.payload, &metadata, sizeof(metadata));
    if (s->wake_consumer && s->connection && DMESH_FLOW_USES_PUSH(metadata.flow.mode)) {
        m.wake_connection = s->connection;
        /* A named consumer this session has not seen arrive yet leaves the
         * flow on the session's: the host publishes from either. */
        m.wake_consumer = dmesh_session_flow_consumer(s, flow_consumer) ? flow_consumer : s->wake_consumer;
    }
    if (!admit(&d->workers[owner], &m)) {
        memset(a, 0, sizeof(*a)); reply_error(o, s, h, EAGAIN); return;
    }
    ++d->loads[owner].assigned;
    ++d->loads[owner].opening;
    if (!backend) d->cursor = ((size_t)owner + 1) % d->count;
    if (!backend) backend_follow(d, (size_t)owner);
    s->generation[idx] = h->generation;
    s->closed[idx] = false;
    s->close_status[idx] = 0;
    wake(d->workers[owner].fd);
    fprintf(stderr, "[dispatcher] session=%lu/%lu flow=%u/%u owner=%d slot=%u epoch=1 backend=%d steered=%d pair=%016llx pair_on_owner=%u pair_fewest=%u unix_ns=%llu\n",
            a->key.session_id, a->key.session_epoch, h->flow_id, h->generation, owner, slot, backend,
            steered >= 0, (unsigned long long)pair, backend ? 0 : d->loads[owner].pair_flows,
            pair_fewest, (unsigned long long)realtime_ns());
}

void dmesh_dispatcher_session_lost(struct objects *o, struct dmesh_session *s)
{
    struct dmesh_dispatcher *d = o->dispatcher;
    for (size_t i = 0; i < d->count * DMESH_MAX_CONNECTIONS; ++i)
        if (d->flows[i].used && d->flows[i].session == s) d->flows[i].lost_pending = true;
    retire_session_listener(d, s);
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
        if (!a->used) continue;
        if (a->drain_pending) {
            struct message m = {.key = a->key, .location = a->location, .type = DRAIN};
            if (push(&d->workers[a->location.worker].requests, &m)) {
                a->drain_pending = false;
                wake(d->workers[a->location.worker].fd);
            }
        }
        if (!a->lost_pending && !a->close_pending && !a->shut_pending) continue;
        struct message m = {.key = a->key, .location = a->location,
                            .type = a->lost_pending ? LOST : a->close_pending ? CLOSE : SHUTDOWN};
        if (m.type == SHUTDOWN) { m.length = 8; dmesh_session_put_u64(m.payload, a->shut_descs); }
        if (push(&d->workers[a->location.worker].requests, &m)) {
            if (m.type == LOST) a->lost_pending = false;
            else if (m.type == CLOSE) a->close_pending = false;
            a->shut_pending = false;     /* sent, or moot after a close or loss */
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
                        a->ready_ms = membership_now_ms();
                        /* An outstanding ARM cannot list a flow that was not
                         * ready yet, so nothing would ring for it and the
                         * host, which re-ARMs only after a DOORBELL, sleeps to
                         * its backstop while this flow carries traffic. */
                        if (s->armed && DMESH_FLOW_USES_PUSH(a->mode)) {
                            s->armed = false;
                            s->doorbell_pending_epoch = s->armed_epoch;
                        }
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

pthread_mutex_t *dmesh_dispatch_sdk_lock(struct objects *o)
{
    return o && o->mailbox ? &o->mailbox->dispatcher->sdk_lock : NULL;
}
int dmesh_dispatch_worker_fd(struct objects *o) { return o->mailbox->fd; }
void dmesh_dispatch_worker_arm(struct objects *o) { clear(o->mailbox->fd); }
void dmesh_dispatch_worker_ready(struct objects *o)
{
    atomic_store(&o->mailbox->tid, (int)syscall(SYS_gettid));
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
            /* The host of a fast-path session no longer ARMs: a flow that
             * cannot publish through a producer must fail rather than fall
             * back to descriptors nobody is woken for. */
            if (c->state == DMESH_CONN_NEW && m.wake_connection &&
                dmesh_wake_attach(c, m.wake_connection, m.wake_consumer) != DOCA_SUCCESS) {
                c->error_status = EIO;
                c->state = DMESH_CONN_ERROR;
            }
        } else if (same_key(&c->key, &m.key) &&
                   c->location.ownership_epoch == m.location.ownership_epoch && c->state != DMESH_CONN_FREE) {
            if (m.type == ARM) { arm_flow(c, &m); continue; }
            if (m.type == DRAIN) { c->migrate_requested = o->migrate_pending = true; continue; }
            if (m.type == SHUTDOWN) {
                c->host_shut_descs = dmesh_session_get_u64(m.payload);
                c->host_shut = true;
                continue;
            }
            if (m.type == LOST) c->session->closing = true;
            if (m.type == CLOSE) c->close_requested = true;
            c->state = DMESH_CONN_CLOSING;
        }
    }
}
void dmesh_dispatch_worker_flow_stats(struct objects *o, int slot, const uint64_t *counters)
{
    if (!o || slot < 0 || slot >= DMESH_MAX_CONNECTIONS) return;
    for (int i = 0; i < DMESH_FLOW_STATS; ++i)
        __atomic_store_n(&o->conns[slot].flow_stats[i], counters[i], __ATOMIC_RELAXED);
}

int dmesh_dispatch_worker_take_migrations(struct objects *o, int32_t *slots, int max)
{
    if (!o || !o->migrate_pending) return 0;
    int n = 0;
    bool more = false;
    for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        struct dmesh_conn *c = &o->conns[i];
        if (!c->migrate_requested) continue;
        if (n == max) { more = true; break; }
        c->migrate_requested = false;
        slots[n++] = (int32_t)i;
    }
    o->migrate_pending = more;
    return n;
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
        pthread_mutex_lock(&d->sdk_lock);
        for (size_t i = 0; i < d->count; ++i) {
            (void)dmesh_doca_ctrl_arm(d->controls[i]);
            (void)dmesh_doca_ctrl_drain(d->controls[i]);
        }
        flush_replies(d);
        flowstat_due(d);
        migrate_due(d);
        scale_due(d);
        flush_commands(d);
        flush_listeners(d);
        flush_scales(d);
        for (size_t i = 0; i < d->count; ++i) dmesh_sessions_advance(d->controls[i]);
        pthread_mutex_unlock(&d->sdk_lock);
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
        if (d->workers[i].instr_fd >= 0) close(d->workers[i].instr_fd);
        pthread_mutex_destroy(&d->workers[i].requests.lock);
        pthread_mutex_destroy(&d->workers[i].replies.lock);
    }
    close(d->fd); free(d->controls); free(d->workers); free(d->loads); free(d->flows);
    pthread_mutex_destroy(&d->sdk_lock);
    free(d);
    return NULL;
}

doca_error_t dmesh_dispatcher_start(struct objects **controls, struct objects **workers,
                                  size_t count, struct dmesh_dispatcher **out)
{
    if (!count || count > 128 || !controls || !workers || !out) return DOCA_ERROR_INVALID_VALUE;
    struct dmesh_dispatcher *d = calloc(1, sizeof(*d));
    if (!d) return DOCA_ERROR_NO_MEMORY;
    if (pthread_mutex_init(&d->sdk_lock, NULL) != 0) { free(d); return DOCA_ERROR_NO_MEMORY; }
    d->count = count; d->fd = -1;
    d->controls = calloc(count, sizeof(*d->controls));
    d->workers = calloc(count, sizeof(*d->workers));
    d->loads = calloc(count, sizeof(*d->loads));
    d->flows = calloc(count * DMESH_MAX_CONNECTIONS, sizeof(*d->flows));
    d->pollfds = calloc(count + 1, sizeof(*d->pollfds));
    if (!d->controls || !d->workers || !d->loads || !d->flows || !d->pollfds) goto fail;
    size_t initialized = 0;
    d->policy = dmesh_placement_least_instructions;
    d->migrate_period_ms = env_u64("DMESH_MIGRATE_PERIOD_MS", 0);
    d->migrate_max = (unsigned long)env_u64("DMESH_MIGRATE_MAX", 0);
    d->steer_ms = env_u64("DMESH_MIGRATE_STEER_MS", 5000);
    d->migrate_spare = env_u64("DMESH_MIGRATE_SPARE", 0) != 0;
    if (d->migrate_period_ms)
        fprintf(stderr, "[dispatcher] migration period=%llums max=%lu steer=%llums spare=%d\n",
                (unsigned long long)d->migrate_period_ms, d->migrate_max,
                (unsigned long long)d->steer_ms, d->migrate_spare);
    d->flowstat_ms = env_u64("DMESH_FLOWSTAT_MS", 1000);
    d->flowstat_log = env_u64("DMESH_FLOWSTAT_LOG", 0) != 0;
    d->worker_instr = env_u64("DMESH_WORKER_INSTR", 0) != 0;
    d->scale_after_ms = env_u64("DMESH_SCALE_AFTER_MS", 0);
    d->scale_in_after_ms = env_u64("DMESH_SCALE_IN_AFTER_MS", 0);
    d->scale_count = (unsigned)env_u64("DMESH_SCALE_COUNT", 2);
    if (d->scale_after_ms)
        fprintf(stderr, "[dispatcher] scaling after=%llums count=%u in_after=%llums\n",
                (unsigned long long)d->scale_after_ms, d->scale_count,
                (unsigned long long)d->scale_in_after_ms);
    /* DMESH_FLOW_PLACEMENT: where OPENs (scale-out streams included) and
     * migrations go. least-instr (default) without instruction counting
     * places as least-flows does. */
    const char *policy = getenv("DMESH_FLOW_PLACEMENT");
    if (policy && strcmp(policy, "round-robin") == 0) d->policy = dmesh_placement_round_robin;
    else if (policy && strcmp(policy, "first-eligible") == 0) d->policy = dmesh_placement_first_eligible;
    else if (policy && strcmp(policy, "least-flows") == 0) d->policy = dmesh_placement_least_flows;
    else if (policy && strcmp(policy, "least-instr") != 0) goto fail;
    d->fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (d->fd < 0) goto fail;
    d->pollfds[0] = (struct pollfd){.fd = d->fd, .events = POLLIN};
    for (size_t i = 0; i < count; ++i) d->workers[i].fd = d->workers[i].instr_fd = -1;
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
        d->workers[i].objs = workers[i];
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
    pthread_mutex_destroy(&d->sdk_lock);
    free(d->pollfds); free(d->controls); free(d->workers); free(d->loads); free(d->flows); free(d);
    return DOCA_ERROR_INITIALIZATION;
}
