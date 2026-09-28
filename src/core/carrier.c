/* Host carrier over the DPUMesh push transport: one Comch session per channel; DMA rings remain per QP.
 * A client QP opens an INGRESS_PUSH flow to its service; a server channel
 * keeps a pool of BACKEND flows that the DPU claims one stream at a time.
 * The core keeps its custody, credit and accept semantics: forward
 * consumption is reported as custody ACKs and each push batch as one receive
 * event; releases advance the per-connection cursor in order.
 *
 * Each slot owns an epoll fd holding its connection's doorbells (the channel layer's
 * progress-engine notification fds); the core nests it in the owning EQ's fd.
 * Custody ACKs (the DPU's consumer_head) and push-wire batches have no
 * doorbell, so stripe_arm asks for periodic polling while either is possible. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "native_transport.h"
#include "service_registry.h"
#include "channel.h"
#include "carrier_logic.h"
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>

#define SLOTS 32                       /* DPUMesh connections per worker */
#define TICKETS 1024                   /* forward ring depth */
#define UPORT_BASE 32768u
#define BACKEND_POOL_DEFAULT 8        /* spare BACKEND flows kept open */
#define BACKEND_MAX_DEFAULT 16         /* spare + claimed BACKEND flows */
static int trace_enabled = -1;
#define TRACE(...) do { if (trace_enabled < 0) trace_enabled = getenv("DPUMESH_CARRIER_TRACE") != NULL; \
    if (trace_enabled) { fprintf(stderr, "carrier: " __VA_ARGS__); fputc('\n', stderr); } } while (0)

enum { SLOT_FREE = 0, SLOT_OPEN, SLOT_CLOSED };
struct ticket { uint16_t seq; uint64_t ticket; };
struct slot {
    pthread_mutex_t lock;
    int epfd;                          /* doorbells of the open connection */
    int armed;                         /* doorbells requested, not yet acknowledged */
    int state, backend, claimed, service_id;
    uint16_t port, peer;
    struct channel_conn *conn;
    struct ticket tickets[TICKETS];
    uint32_t t_head, t_tail;
    int fin_pending, peer_gone_reported;
    uint16_t fin_seq, rx_seq;
    struct carrier_rx_window window;
};
struct dmesh_native_transport {
    struct channel_dev *dev;
    struct channel_mem *tx, *rx;
    struct dmesh_service_registry registry;
    char server[64], workload[64];
    uint32_t pod_ip;
    int pod_id, service_id, backend_pool, backend_max;
    uint16_t next_uport;
    pthread_mutex_t lock;
    struct slot slots[SLOTS];
    uint16_t port_slot[65536];         /* port -> slot index + 1 */
};

static int env_int(const char *name, int fallback, int lo, int hi)
{
    const char *s = getenv(name);
    if (!s || !*s) return fallback;
    int v = atoi(s);
    return v < lo || v > hi ? fallback : v;
}
static int slot_index(const struct dmesh_native_transport *t, const struct slot *s) { return (int)(s - t->slots); }

/* Opens one flow on a free slot. Caller holds t->lock. */
static int slot_open(struct dmesh_native_transport *t, struct slot *s, uint32_t mode,
                     uint16_t port, int service_id)
{
    const struct dmesh_service *svc = dmesh_registry_id(&t->registry, service_id);
    if (!svc) { errno = ENOENT; return -1; }
    /* host-dpa reverse path: the same roles on the export flow modes */
    if (channel_dev_host_dpa(t->dev))
        mode = mode == CHANNEL_MODE_BACKEND_DPU_DMA ? CHANNEL_MODE_BACKEND_HOST_DPA : CHANNEL_MODE_CLIENT_HOST_DPA;
    struct channel_conn_config cfg = {
        .flow_id = (uint32_t)slot_index(t, s) + 1, .workload = t->workload,
        .src_ip = t->pod_ip, .dst_ip = svc->ipv4, .src_port = port, .dst_port = svc->port,
        .mode = mode, .tx = t->tx, .rx = t->rx,
        .rx_offset = (size_t)slot_index(t, s) * CHANNEL_WINDOW,
    };
    struct channel_conn *c = NULL;
    if (channel_conn_open(t->dev, &cfg, &c) != 0) return -1;
    int fds[CHANNEL_CONN_FDS], nfd = channel_conn_fds(c, fds, CHANNEL_CONN_FDS);
    for (int i = 0; i < nfd; ++i) {
        struct epoll_event ev = { .events = EPOLLIN, .data.fd = fds[i] };
        if (epoll_ctl(s->epfd, EPOLL_CTL_ADD, fds[i], &ev) != 0) TRACE("slot %d doorbell %d not registered (%s)", slot_index(t, s), fds[i], strerror(errno));
    }
    s->armed = 0;
    s->conn = c; s->port = port; s->service_id = service_id;
    s->backend = mode == CHANNEL_MODE_BACKEND_DPU_DMA || mode == CHANNEL_MODE_BACKEND_HOST_DPA;
    s->peer = s->backend ? (uint16_t)(1 + slot_index(t, s)) : (uint16_t)(UPORT_BASE + slot_index(t, s));
    s->t_head = s->t_tail = 0; s->fin_pending = 0; s->peer_gone_reported = 0; s->claimed = 0;
    s->fin_seq = 0; s->rx_seq = 0;
    carrier_window_init(&s->window);
    s->state = SLOT_OPEN;
    t->port_slot[port] = (uint16_t)(slot_index(t, s) + 1);
    TRACE("slot %d open mode %u port %u peer %u service %d", slot_index(t, s), mode, port, s->peer, service_id);
    return 0;
}
static int slot_close(struct dmesh_native_transport *t, struct slot *s)
{
    (void)t;
    if (s->conn) {
        int fds[CHANNEL_CONN_FDS], nfd = channel_conn_fds(s->conn, fds, CHANNEL_CONN_FDS);
        if (channel_conn_close(s->conn) != 0) return -1;
        s->conn = NULL;
        for (int i = 0; i < nfd; ++i) (void)epoll_ctl(s->epfd, EPOLL_CTL_DEL, fds[i], NULL);
    }
    s->armed = 0;
    s->state = SLOT_CLOSED;
    return 0;
}
static void slot_free(struct dmesh_native_transport *t, struct slot *s)
{
    if (t->port_slot[s->port] == slot_index(t, s) + 1) t->port_slot[s->port] = 0;
    s->state = SLOT_FREE; s->backend = 0; s->port = 0;
}
static uint16_t next_uport(struct dmesh_native_transport *t)
{
    for (unsigned tries = 0; tries < 65536u - UPORT_BASE; ++tries) {
        uint16_t p = t->next_uport;
        t->next_uport = (uint16_t)((unsigned)p + 1u < UPORT_BASE ? UPORT_BASE : (unsigned)p + 1u);
        if (!t->port_slot[p]) return p;
    }
    return 0;
}
/* A backend flow serves one inbound stream. The DPU claims a spare flow when a
 * stream arrives, so the pool keeps `backend_pool` unclaimed flows open, each
 * under a fresh upstream port, up to `backend_max` flows in total. */
static void backend_maintain(struct dmesh_native_transport *t)
{
    pthread_mutex_lock(&t->lock);
    for (;;) {
        int spare = 0, total = 0;
        struct slot *free_slot = NULL;
        for (int i = 0; i < SLOTS; ++i) {
            struct slot *s = &t->slots[i];
            if (s->state == SLOT_FREE) { if (!free_slot) free_slot = s; continue; }
            if (s->backend) { ++total; if (s->state == SLOT_OPEN && !s->claimed) ++spare; }
        }
        if (spare >= t->backend_pool || total >= t->backend_max || !free_slot) break;
        uint16_t up = next_uport(t);
        if (!up || slot_open(t, free_slot, CHANNEL_MODE_BACKEND_DPU_DMA, up, t->service_id) != 0) {
            fprintf(stderr, "dpumesh: backend flow not opened (%s)\n", strerror(errno));
            break;
        }
    }
    pthread_mutex_unlock(&t->lock);
}
static struct slot *slot_of_port(struct dmesh_native_transport *t, uint16_t port)
{
    uint16_t i = t->port_slot[port];
    return i ? &t->slots[i - 1] : NULL;
}

int dmesh_native_open(struct dmesh_native_transport **out, struct dmesh_native_config *cfg)
{
    if (!out || !cfg || !cfg->bytes) { errno = EINVAL; return -1; }
    *out = NULL;
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    if (!t) return -1;
    pthread_mutex_init(&t->lock, NULL);
    for (int i = 0; i < SLOTS; ++i) { pthread_mutex_init(&t->slots[i].lock, NULL); t->slots[i].epfd = -1; }
    for (int i = 0; i < SLOTS; ++i) if ((t->slots[i].epfd = epoll_create1(EPOLL_CLOEXEC)) < 0) goto fail;
    const char *registry = getenv("DPUMESH_CONFIG");
    if (!registry) registry = "/etc/dpumesh/registry";
    if (dmesh_registry_load(&t->registry, registry) != 0) goto fail;
    const char *pci = getenv("DPUMESH_PCI_ADDR"), *server = getenv("DPUMESH_SERVER");
    const char *pod_ip = getenv("DPUMESH_POD_IP"), *workload = getenv("DPUMESH_WORKLOAD");
    if (!pci || !*pci || !pod_ip || inet_pton(AF_INET, pod_ip, &t->pod_ip) != 1) { errno = EINVAL; goto fail; }
    snprintf(t->server, sizeof(t->server), "%s", server && *server ? server : "DPUMesh0");
    snprintf(t->workload, sizeof(t->workload), "%s", workload ? workload : "");
    t->pod_id = env_int("DPUMESH_POD_ID", 0, 0, 126);
    t->backend_pool = env_int("DPUMESH_BACKEND_POOL", BACKEND_POOL_DEFAULT, 1, SLOTS);
    t->backend_max = env_int("DPUMESH_BACKEND_MAX", BACKEND_MAX_DEFAULT, t->backend_pool, SLOTS);
    t->next_uport = UPORT_BASE;
    t->service_id = DMESH_SVC_NONE;
    if (cfg->service_name && *cfg->service_name) {
        const struct dmesh_service *svc = dmesh_registry_name(&t->registry, cfg->service_name);
        if (!svc) { errno = ENOENT; goto fail; }
        t->service_id = svc->id;
    }
    if (channel_dev_open(pci, &t->dev) != 0) goto fail;
    if (channel_session_open(t->dev, t->server) != 0) goto fail;
    if (channel_mem_alloc(t->dev, cfg->bytes, &t->tx) != 0) goto fail;
    if (channel_mem_alloc(t->dev, (size_t)SLOTS * CHANNEL_WINDOW, &t->rx) != 0) goto fail;
    if (t->service_id != DMESH_SVC_NONE) {
        backend_maintain(t);
        int opened = 0;
        for (int i = 0; i < SLOTS; ++i) opened += t->slots[i].state == SLOT_OPEN;
        if (!opened) { errno = EIO; goto fail; }
    }
    cfg->tx = channel_mem_base(t->tx); cfg->rx = channel_mem_base(t->rx);
    cfg->rx_bytes = (size_t)SLOTS * CHANNEL_WINDOW;
    cfg->pod_id = t->pod_id; cfg->service_id = t->service_id; cfg->stripes = SLOTS;
    *out = t;
    return 0;
fail: {
    int saved = errno ? errno : EIO;
    (void)dmesh_native_close(t);
    errno = saved; return -1;
}}
int dmesh_native_close(struct dmesh_native_transport *t)
{
    if (!t) return 0;
    int saved = 0;
    for (int i = 0; i < SLOTS; ++i)
        if (t->slots[i].conn && slot_close(t, &t->slots[i]) != 0 && !saved) saved = errno;
    if (!saved && channel_session_close(t->dev) != 0) saved = errno;
    /* A missing CLOSED/quiescence fence leaves exported regions registered. */
    if (saved) { errno = saved; return -1; }
    for (int i = 0; i < SLOTS; ++i) if (t->slots[i].epfd >= 0) close(t->slots[i].epfd);
    channel_mem_free(t->rx); channel_mem_free(t->tx);
    channel_dev_close(t->dev);
    free(t);
    return 0;
}
int dmesh_native_connect(struct dmesh_native_transport *t, uint16_t port, int service_id)
{
    pthread_mutex_lock(&t->lock);
    if (t->port_slot[port]) { pthread_mutex_unlock(&t->lock); errno = EADDRINUSE; return -1; }
    struct slot *s = NULL;
    for (int i = 0; i < SLOTS; ++i) if (t->slots[i].state == SLOT_FREE) { s = &t->slots[i]; break; }
    if (!s) { pthread_mutex_unlock(&t->lock); errno = ENOSPC; return -1; }
    int rc = slot_open(t, s, CHANNEL_MODE_CLIENT_DPU_DMA, port, service_id);
    pthread_mutex_unlock(&t->lock);
    return rc;
}
int dmesh_native_disconnect(struct dmesh_native_transport *t, uint16_t port)
{
    pthread_mutex_lock(&t->lock);
    struct slot *s = slot_of_port(t, port);
    int rc = 0;
    if (s && !s->backend) {
        pthread_mutex_lock(&s->lock);
        if (s->state == SLOT_OPEN && s->t_head == s->t_tail) { rc = slot_close(t, s); if (rc == 0) slot_free(t, s); }
        pthread_mutex_unlock(&s->lock);
    }
    pthread_mutex_unlock(&t->lock);
    return rc;
}
int dmesh_native_submit(struct dmesh_native_transport *t, const sw_descriptor_t *d)
{
    struct slot *s = slot_of_port(t, d->src_port);
    if (!s) { errno = EPIPE; return -1; }
    pthread_mutex_lock(&s->lock);
    int rc = 0;
    TRACE("submit slot %d port %u seq %u len %u off %d state %d", slot_index(t, s), d->src_port, d->seq, d->body_len, d->body_buf_slot, s->state);
    if (d->body_len == 0) {
        /* FIN or reset: the stream ends with the connection. */
        if (s->state == SLOT_OPEN) rc = slot_close(t, s);
        if (rc == 0) { s->fin_pending = 1; s->fin_seq = d->seq; }
    } else if (s->state != SLOT_OPEN) {
        errno = EPIPE; rc = -1;
    } else {
        uint32_t piece[2];
        unsigned n = carrier_chunks(d->body_len, piece);
        uint32_t queued = (s->t_tail - s->t_head);
        if (channel_conn_ring_free(s->conn) < n || queued >= TICKETS) { errno = EAGAIN; rc = -1; }
        else {
            uint64_t addr = (uint64_t)(uintptr_t)channel_mem_base(t->tx) + (uint64_t)d->body_buf_slot, last = 0;
            for (unsigned i = 0; i < n; ++i) { last = channel_conn_post(s->conn, addr, piece[i]); addr += piece[i]; }
            s->tickets[s->t_tail % TICKETS] = (struct ticket){d->seq, last};
            s->t_tail++;
        }
    }
    pthread_mutex_unlock(&s->lock);
    return rc;
}
static void fill_rx(struct dmesh_native_transport *t, struct slot *s, struct dmesh_native_event *e,
                    uint32_t pos, uint32_t len)
{
    memset(e, 0, sizeof(*e));
    e->kind = DMESH_NATIVE_RX;
    e->desc.body_buf_slot = (int32_t)((size_t)slot_index(t, s) * CHANNEL_WINDOW + CHANNEL_DATA_OFF + pos);
    e->desc.body_len = len;
    e->desc.src_port = s->peer; e->desc.dst_port = s->port;
    e->desc.src_pod = 0; e->desc.dst_pod = t->pod_id;
    e->desc.src_service = s->service_id; e->desc.dst_service = t->service_id;
    e->desc.seq = ++s->rx_seq; e->desc.valid = 1;
    TRACE("rx slot %d port %u seq %u pos %u len %u", slot_index(t, s), s->port, e->desc.seq, pos, len);
}
static void fill_ack(struct slot *s, struct dmesh_native_event *e, uint16_t seq)
{
    memset(e, 0, sizeof(*e));
    e->kind = DMESH_NATIVE_ACK; e->port = s->port; e->seq = seq; e->seq_count = 1;
    TRACE("ack port %u seq %u", s->port, seq);
}
int dmesh_native_poll(struct dmesh_native_transport *t, int stripe, struct dmesh_native_event *e)
{
    if (stripe < 0 || stripe >= SLOTS) return 0;
    struct slot *s = &t->slots[stripe];
    if (s->state == SLOT_FREE) return 0;
    pthread_mutex_lock(&s->lock);
    int n = 0;
    if (s->state == SLOT_OPEN) {
        int gone = channel_conn_progress(s->conn);
        uint64_t consumed = channel_conn_consumed(s->conn);
        if (s->t_head != s->t_tail && s->tickets[s->t_head % TICKETS].ticket <= consumed) {
            fill_ack(s, e, s->tickets[s->t_head % TICKETS].seq); s->t_head++; n = 1;
        } else if (gone && !s->peer_gone_reported) {
            s->peer_gone_reported = 1;
            if (gone < 0) {
                int saved = errno;
                memset(e, 0, sizeof(*e));
                e->kind = DMESH_NATIVE_ERROR; e->port = s->port; e->error = saved ? saved : EIO;
            } else fill_rx(t, s, e, 0, 0);
            n = 1;
        } else {
            uint64_t seq; uint32_t pos, len;
            int r = channel_conn_rx_next(s->conn, &seq, &pos, &len);
            if (r > 0 && carrier_window_add(&s->window, seq, pos, len) == 0) {
                fill_rx(t, s, e, pos, len); n = 1;
                if (s->backend && !s->claimed) {
                    s->claimed = 1;
                    pthread_mutex_unlock(&s->lock);
                    backend_maintain(t);            /* keep spares for the next stream */
                    return n;
                }
            }
            else if (r < 0 && !s->peer_gone_reported) {
                s->peer_gone_reported = 1; fill_rx(t, s, e, 0, 0); n = 1;   /* end of stream or malformed batch */
            }
        }
    } else {
        /* Closed: retire outstanding custody, then the close marker, then recycle. */
        if (s->t_head != s->t_tail) { fill_ack(s, e, s->tickets[s->t_head % TICKETS].seq); s->t_head++; n = 1; }
        else if (s->fin_pending) {
            s->fin_pending = 0; fill_ack(s, e, s->fin_seq); n = 1;
            int backend = s->backend;
            pthread_mutex_unlock(&s->lock);
            pthread_mutex_lock(&t->lock); slot_free(t, s); pthread_mutex_unlock(&t->lock);
            if (backend) backend_maintain(t);
            return n;
        }
    }
    pthread_mutex_unlock(&s->lock);
    return n;
}
void dmesh_native_release(struct dmesh_native_transport *t, int pos)
{
    if (pos < 0) return;
    size_t idx = (size_t)pos / CHANNEL_WINDOW, local = (size_t)pos % CHANNEL_WINDOW;
    if (idx >= SLOTS || local < CHANNEL_DATA_OFF) return;
    struct slot *s = &t->slots[idx];
    pthread_mutex_lock(&s->lock);
    if (carrier_window_release(&s->window, (uint32_t)(local - CHANNEL_DATA_OFF)) == 0) {
        uint64_t seq, bytes;
        if (carrier_window_advance(&s->window, &seq, &bytes) && s->conn) { channel_conn_rx_consumed(s->conn, seq, bytes); TRACE("release slot %zu cursor seq %lu bytes %lu", idx, (unsigned long)seq, (unsigned long)bytes); }
    }
    pthread_mutex_unlock(&s->lock);
}
int dmesh_native_stripe_fd(struct dmesh_native_transport *t, int stripe)
{
    return stripe >= 0 && stripe < SLOTS ? t->slots[stripe].epfd : -1;
}
int dmesh_native_stripe_of(struct dmesh_native_transport *t, uint16_t port)
{
    uint16_t i = t->port_slot[port];
    return i ? (int)i - 1 : -1;
}
/* An armed doorbell is one-shot: it fires on the next completion of any of the
 * connection's engines and stays readable until cleared. Rearmed only after a
 * clear, so a sleeping consumer costs one request per wake. */
int dmesh_native_stripe_arm(struct dmesh_native_transport *t, int stripe)
{
    if (stripe < 0 || stripe >= SLOTS) return 0;
    struct slot *s = &t->slots[stripe];
    if (s->state == SLOT_FREE) return 0;
    /* Still armed from an earlier sleep (the common case of a busy consumer
     * that ran empty): only the polling question remains, answered from a
     * racy read that a concurrent poll can at worst make conservative. */
    if (s->state == SLOT_OPEN && s->armed)
        return 1; /* shared control PE has no stripe fd, including host-DPA */
    pthread_mutex_lock(&s->lock);
    int tick = 0;
    if (s->state == SLOT_OPEN) {
        if (!s->armed && channel_conn_arm(s->conn) == 0) s->armed = 1;
        /* The channel's shared control PE deliberately has no per-EQ fd.
         * Poll it under the session mutex even when reverse DMA has a doorbell. */
        tick = 1;
    } else {
        tick = s->t_head != s->t_tail || s->fin_pending;   /* retired by the next poll */
    }
    pthread_mutex_unlock(&s->lock);
    return tick;
}
void dmesh_native_stripe_clear(struct dmesh_native_transport *t, int stripe)
{
    if (stripe < 0 || stripe >= SLOTS) return;
    struct slot *s = &t->slots[stripe];
    pthread_mutex_lock(&s->lock);
    if (s->conn) {
        struct epoll_event evs[CHANNEL_CONN_FDS];
        int n = epoll_wait(s->epfd, evs, CHANNEL_CONN_FDS, 0);
        for (int i = 0; i < n; ++i) channel_conn_clear(s->conn, evs[i].data.fd);
        if (n > 0) s->armed = 0;
    }
    pthread_mutex_unlock(&s->lock);
}
int dmesh_native_resolve(struct dmesh_native_transport *t, const char *name, uint32_t addr, uint16_t port)
{
    const struct dmesh_service *e = name ? dmesh_registry_name(&t->registry, name)
                                         : dmesh_registry_addr(&t->registry, addr, port);
    if (!e) { errno = ENOENT; return -1; }
    return e->id;
}
