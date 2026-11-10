/* Host carrier over the DPUMesh push transport: one Comch session per channel; DMA rings remain per QP.
 * A client QP opens an INGRESS_PUSH flow to its service; a server channel
 * keeps a pool of BACKEND flows that the DPU claims one stream at a time.
 * The core keeps its custody, credit and accept semantics: forward
 * consumption is reported as custody ACKs and each push batch as one receive
 * event; releases advance the per-connection cursor in order.
 *
 * A slot's flow lives as long as its QP. A half close ends only the forward
 * stream; the QP's own FIN, reset or disconnect closes the flow. A closed
 * slot leaves its port once its custody and FIN are retired, and is reused
 * only after the application has released every batch it holds.
 *
 * Each slot owns an epoll fd holding its connection's doorbells (the channel layer's
 * progress-engine notification fds); the core nests it in the owning EQ's fd.
 * Push batches have no doorbell: before sleeping, idle_arm asks the DPU to ring
 * over the control session, whose fd the core nests in every EQ. Custody ACKs
 * (the DPU's consumer_head) have neither, so stripe_arm keeps the consumer
 * polling while any is outstanding.
 *
 * A FIN or reset closes the flow without waiting for the DPU: the slot sends
 * CLOSE and stays CLOSING, its ring still exported, until a later poll sees
 * CLOSED and releases it. A backend flow the DPU requests opens the same way:
 * OPENING until a poll sees READY. Neither slot belongs to a QP, so the
 * channel's poller completes both. The DPU's teardown (DMA stop, DPA thread
 * release) and setup take tens of milliseconds, and a caller blocked on them
 * would stall every other stream its thread polls. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "native_transport.h"
#include "service_resolve.h"
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
#include <sys/socket.h>
#include <unistd.h>

#define SLOTS 32                       /* DPUMesh connections per worker */
#define TICKETS CHANNEL_RING_SIZE      /* one per forward ring cell */
#define UPORT_BASE 32768u
#define BACKEND_MAX_DEFAULT SLOTS      /* demand-created flows, shared per DPU worker */
static int trace_enabled = -1;
#define TRACE(...) do { if (trace_enabled < 0) trace_enabled = getenv("DPUMESH_CARRIER_TRACE") != NULL; \
    if (trace_enabled) { fprintf(stderr, "carrier: " __VA_ARGS__); fputc('\n', stderr); } } while (0)

enum { SLOT_FREE = 0, SLOT_OPENING, SLOT_OPEN, SLOT_CLOSING, SLOT_CLOSED };
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
    int close_failed;                  /* CLOSING without CLOSED: quarantined, ring kept */
    uint32_t open_worker, open_token;  /* OPENING backend: the DPU request it answers */
    uint32_t dst_ip;                   /* the flow's destination, network order */
    uint16_t dst_port;
    uint16_t fin_seq, rx_seq;
    struct carrier_rx_window window;
};
struct dmesh_native_transport {
    struct channel_dev *dev;
    struct channel_mem *tx, *rx;
    char server[64], workload[64];
    uint32_t pod_ip;
    int pod_id, service_id, backend_max;
    uint16_t next_uport;
    pthread_mutex_t lock;
    int lost;                          /* sticky errno of a failed session; it never recovers */
    int flow_wake;                     /* channel_dev_flow_wake, fixed once the session is open */
    uint32_t active_stripes;
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
/* The Pod's IPv4 address, network order: $DPUMESH_POD_IP, or else the address
 * this network namespace sends from toward the Kubernetes API Service
 * ($KUBERNETES_SERVICE_HOST, which kubelet sets in every container), which is
 * the Pod IP. Connecting a UDP socket only picks the route; nothing is sent. */
static int pod_ip_lookup(uint32_t *out)
{
    const char *env = getenv("DPUMESH_POD_IP"), *api = getenv("KUBERNETES_SERVICE_HOST");
    if (env && *env) {
        if (inet_pton(AF_INET, env, out) == 1) return 0;
        fprintf(stderr, "dpumesh: DPUMESH_POD_IP '%s' is not an IPv4 address\n", env);
        errno = EINVAL;
        return -1;
    }
    struct sockaddr_in dst = { .sin_family = AF_INET, .sin_port = htons(443) }, src;
    socklen_t len = sizeof(src);
    if (api && inet_pton(AF_INET, api, &dst.sin_addr) == 1) {
        int fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        int found = fd >= 0 && connect(fd, (struct sockaddr *)&dst, sizeof(dst)) == 0 &&
                    getsockname(fd, (struct sockaddr *)&src, &len) == 0;
        if (fd >= 0) close(fd);
        if (found) { *out = src.sin_addr.s_addr; return 0; }
    }
    fprintf(stderr, "dpumesh: set DPUMESH_POD_IP; outside a Pod there is no route to find it by\n");
    errno = EINVAL;
    return -1;
}
/* The target a channel serves: $DPUMESH_SERVICE, or with only $DPUMESH_PORT
 * set, "<pod ip>:<port>", the endpoint Kubernetes lists for a Service whose
 * targetPort it is. NULL for a client-only channel. */
static const char *served_target(char *buf, size_t len, const char *service, uint32_t pod_ip, int port)
{
    if (service && *service) return service;
    if (port < 0) return NULL;
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &pod_ip, ip, sizeof(ip));
    snprintf(buf, len, "%s:%u", ip, (unsigned)(uint16_t)port);
    return buf;
}
static int slot_index(const struct dmesh_native_transport *t, const struct slot *s) { return (int)(s - t->slots); }
static void note_lost(struct dmesh_native_transport *t, int error)
{
    int none = 0;
    (void)__atomic_compare_exchange_n(&t->lost, &none, error ? error : EIO, 0,
                                      __ATOMIC_RELEASE, __ATOMIC_RELAXED);
}

static void slot_watch(struct dmesh_native_transport *t, struct slot *s, struct channel_conn *c);
/* Opens one flow on a free slot; with wait 0 it only sends OPEN and leaves
 * the slot OPENING for slot_opened(). Caller holds t->lock. */
static int slot_open_mode(struct dmesh_native_transport *t, struct slot *s, uint32_t mode,
                          uint16_t port, int service_id, uint32_t worker, uint32_t token, int wait)
{
    uint32_t dst_ip; uint16_t dst_port;
    if (dmesh_target_addr(service_id, &dst_ip, &dst_port) != 0) return -1;
    /* host-dpa reverse path: the same roles on the export flow modes */
    if (channel_dev_host_dpa(t->dev))
        mode = mode == CHANNEL_MODE_BACKEND_DPU_DMA ? CHANNEL_MODE_BACKEND_HOST_DPA : CHANNEL_MODE_CLIENT_HOST_DPA;
    struct channel_conn_config cfg = {
        .flow_id = (uint32_t)slot_index(t, s) + 1, .workload = t->workload,
        .src_ip = t->pod_ip, .dst_ip = dst_ip, .src_port = port, .dst_port = dst_port,
        .mode = mode, .backend_worker = worker, .backend_token = token,
        .tx = t->tx, .rx = t->rx,
        .rx_offset = (size_t)slot_index(t, s) * CHANNEL_WINDOW,
    };
    struct channel_conn *c = NULL;
    if ((wait ? channel_conn_open(t->dev, &cfg, &c) : channel_conn_open_start(t->dev, &cfg, &c)) != 0)
        return -1;
    if (wait) slot_watch(t, s, c);
    s->armed = 0;
    s->conn = c; s->port = port; s->service_id = service_id;
    s->backend = mode == CHANNEL_MODE_BACKEND_DPU_DMA || mode == CHANNEL_MODE_BACKEND_HOST_DPA;
    s->peer = s->backend ? (uint16_t)(1 + slot_index(t, s)) : (uint16_t)(UPORT_BASE + slot_index(t, s));
    s->t_head = s->t_tail = 0; s->fin_pending = 0; s->peer_gone_reported = 0; s->claimed = 0;
    s->fin_seq = 0; s->rx_seq = 0; s->close_failed = 0;
    s->open_worker = worker; s->open_token = token;
    s->dst_ip = dst_ip; s->dst_port = dst_port;
    carrier_window_init(&s->window);
    /* Published last: a drainer that reads OPEN without the slot lock sees
     * the connection it describes. Dials can run beside another EQ's drain. */
    __atomic_store_n(&s->state, wait ? SLOT_OPEN : SLOT_OPENING, __ATOMIC_RELEASE);
    t->port_slot[port] = (uint16_t)(slot_index(t, s) + 1);
    __atomic_fetch_or(&t->active_stripes, UINT32_C(1) << slot_index(t, s), __ATOMIC_RELEASE);
    TRACE("slot %d open%s mode %u port %u peer %u service %d", slot_index(t, s), wait ? "" : "ing",
          mode, port, s->peer, service_id);
    return 0;
}
static int slot_open(struct dmesh_native_transport *t, struct slot *s, uint32_t mode,
                     uint16_t port, int service_id, uint32_t worker, uint32_t token)
{
    return slot_open_mode(t, s, mode, port, service_id, worker, token, 1);
}
static void slot_watch(struct dmesh_native_transport *t, struct slot *s, struct channel_conn *c)
{
    int fds[CHANNEL_CONN_FDS], nfd = channel_conn_fds(c, fds, CHANNEL_CONN_FDS);
    for (int i = 0; i < nfd; ++i) {
        struct epoll_event ev = { .events = EPOLLIN, .data.fd = fds[i] };
        if (epoll_ctl(s->epfd, EPOLL_CTL_ADD, fds[i], &ev) != 0) TRACE("slot %d doorbell %d not registered (%s)", slot_index(t, s), fds[i], strerror(errno));
    }
}
static void slot_unwatch(struct slot *s, const int *fds, int nfd)
{
    for (int i = 0; i < nfd; ++i) (void)epoll_ctl(s->epfd, EPOLL_CTL_DEL, fds[i], NULL);
    s->armed = 0;
}
/* Waits for CLOSED. Under the slot lock. */
static int slot_close(struct dmesh_native_transport *t, struct slot *s)
{
    (void)t;
    if (s->conn) {
        int fds[CHANNEL_CONN_FDS], nfd = channel_conn_fds(s->conn, fds, CHANNEL_CONN_FDS);
        if (channel_conn_close(s->conn) != 0) return -1;
        s->conn = NULL;
        slot_unwatch(s, fds, nfd);
    }
    s->armed = 0;
    s->state = SLOT_CLOSED;
    return 0;
}
/* Sends CLOSE and leaves the slot CLOSING; slot_closed completes it. Under the slot lock. */
static int slot_close_start(struct dmesh_native_transport *t, struct slot *s)
{
    if (!s->conn) return slot_close(t, s);
    int fds[CHANNEL_CONN_FDS], nfd = channel_conn_fds(s->conn, fds, CHANNEL_CONN_FDS);
    if (channel_conn_close_start(s->conn) != 0) return -1;
    /* The doorbells serve a reader that is gone; CLOSED wakes the EQ through
     * the control session. */
    slot_unwatch(s, fds, nfd);
    s->state = SLOT_CLOSING;
    TRACE("slot %d closing port %u", slot_index(t, s), s->port);
    return 0;
}
/* OPENING -> OPEN once READY arrived; a refused or timed-out open closes
 * like an open flow and answers the DPU's request either way. Under the slot
 * lock. */
static void slot_opened(struct dmesh_native_transport *t, struct slot *s)
{
    int rc = channel_conn_open_finish(s->conn);
    if (rc > 0) return;
    int error = rc == 0 ? 0 : (errno ? errno : EIO);
    if (rc == 0) {
        slot_watch(t, s, s->conn);
        __atomic_store_n(&s->state, SLOT_OPEN, __ATOMIC_RELEASE);
        TRACE("slot %d opened port %u", slot_index(t, s), s->port);
    } else {
        fprintf(stderr, "dpumesh: backend flow %d port %u did not open (%s)\n",
                slot_index(t, s) + 1, s->port, strerror(error));
        (void)channel_conn_close_start(s->conn);   /* a refused CLOSE is asked again */
        s->state = SLOT_CLOSING;
    }
    if (s->backend) channel_backend_finish(t->dev, s->open_worker, s->open_token, error);
}
/* CLOSING -> CLOSED once the DPU confirmed. A CLOSE the session refused is
 * sent again, and one unconfirmed in time keeps the slot until CLOSED comes:
 * the DPU may still finish an OPEN it sat on. Under the slot lock. */
static void slot_closed(struct dmesh_native_transport *t, struct slot *s)
{
    if (s->close_failed || channel_conn_close_start(s->conn) != 0) return;
    int rc = channel_conn_close_finish(s->conn);
    if (rc == 0) {
        s->conn = NULL;
        s->state = SLOT_CLOSED;
        TRACE("slot %d closed port %u", slot_index(t, s), s->port);
    } else if (rc < 0 && errno != ETIMEDOUT) {
        /* CLOSED with an error, or a lost session: the DPU may still read
         * the ring; keep it, and the slot, until dmesh_native_close retries. */
        s->close_failed = 1;
        fprintf(stderr, "dpumesh: flow %d port %u close failed (%s); slot quarantined\n",
                slot_index(t, s) + 1, s->port, strerror(errno));
    }
}
/* CLOSED stops the DPU's writes into the window, not the application's reads
 * of batches it holds: the slot is free only once they are released too.
 * Caller holds t->lock and the slot lock. */
static void slot_reap_locked(struct dmesh_native_transport *t, struct slot *s)
{
    if (s->state != SLOT_CLOSED || s->t_head != s->t_tail || s->fin_pending) return;
    if (t->port_slot[s->port] == slot_index(t, s) + 1) t->port_slot[s->port] = 0;
    s->backend = 0;
    __atomic_fetch_and(&t->active_stripes, ~(UINT32_C(1) << slot_index(t, s)), __ATOMIC_RELEASE);
    if (carrier_window_empty(&s->window)) __atomic_store_n(&s->state, SLOT_FREE, __ATOMIC_RELEASE);
}
static void slot_reap(struct dmesh_native_transport *t, struct slot *s)
{
    pthread_mutex_lock(&t->lock); pthread_mutex_lock(&s->lock);
    slot_reap_locked(t, s);
    pthread_mutex_unlock(&s->lock); pthread_mutex_unlock(&t->lock);
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
/* The first free slot whose flow id the channel holds no more: a timed-out
 * open keeps its id until the DPU confirms the close. Caller holds t->lock. */
static struct slot *free_slot(struct dmesh_native_transport *t)
{
    for (int i = 0; i < SLOTS; ++i)
        if (t->slots[i].state == SLOT_FREE && channel_flow_free(t->dev, (uint32_t)i + 1)) return &t->slots[i];
    return NULL;
}
/* Listener-only startup; service one DPU request per progress pass. The
 * session callback queues work and never recursively opens a flow. */
static void backend_progress(struct dmesh_native_transport *t)
{
    uint32_t worker, token;
    if (!channel_backend_next(t->dev, &worker, &token)) return;
    pthread_mutex_lock(&t->lock);
    int total = 0, error = 0;
    struct slot *s = NULL;
    for (int i = 0; i < SLOTS; ++i)
        if (t->slots[i].state != SLOT_FREE && t->slots[i].backend) ++total;
    if (t->service_id == DMESH_SVC_NONE) error = ECONNREFUSED;
    else if (total >= t->backend_max || !(s = free_slot(t))) error = ENOSPC;
    else {
        uint16_t up = next_uport(t);
        if (!up) error = ENOSPC;
        else {
            /* The request is answered once the flow is OPEN (slot_opened), so
             * the polling thread never waits for the DPU's flow setup. */
            int wait = channel_dev_host_dpa(t->dev);
            if (slot_open_mode(t, s, CHANNEL_MODE_BACKEND_DPU_DMA,
                               up, t->service_id, worker, token, wait) != 0) error = errno;
            else if (!wait) { pthread_mutex_unlock(&t->lock); return; }
        }
    }
    pthread_mutex_unlock(&t->lock);
    channel_backend_finish(t->dev, worker, token, error);
}
static struct slot *slot_of_port(struct dmesh_native_transport *t, uint16_t port)
{
    uint16_t i = t->port_slot[port];
    return i ? &t->slots[i - 1] : NULL;
}
/* The open client slot of port, locked, or NULL. */
static struct slot *client_slot(struct dmesh_native_transport *t, uint16_t port)
{
    pthread_mutex_lock(&t->lock);
    struct slot *s = slot_of_port(t, port);
    if (s) pthread_mutex_lock(&s->lock);
    pthread_mutex_unlock(&t->lock);
    if (s && (s->state != SLOT_OPEN || s->backend)) { pthread_mutex_unlock(&s->lock); s = NULL; }
    return s;
}
int dmesh_native_take_request(struct dmesh_native_transport *t, uint16_t port, int *kind,
                              uint32_t *ip, uint16_t *dst_port, uint32_t *count, uint32_t *token)
{
    struct slot *s = client_slot(t, port);
    if (!s) return 0;
    int k, found = channel_conn_take_request(s->conn, &k, count, token);
    *ip = s->dst_ip; *dst_port = s->dst_port;
    pthread_mutex_unlock(&s->lock);
    if (found) *kind = k == CHANNEL_REQ_PREPARE ? DMESH_REQ_PREPARE : DMESH_REQ_SCALE;
    return found;
}
int dmesh_native_spare_ready(struct dmesh_native_transport *t, uint16_t port)
{
    struct slot *s = client_slot(t, port);
    if (!s) { errno = ENOTCONN; return -1; }
    int rc = channel_conn_spare_ready(s->conn);
    pthread_mutex_unlock(&s->lock);
    return rc;
}
int dmesh_native_scale_ack(struct dmesh_native_transport *t, uint16_t port, uint32_t token, uint32_t applied)
{
    struct slot *s = client_slot(t, port);
    if (!s) { errno = ENOTCONN; return -1; }
    int rc = channel_conn_scale_ack(s->conn, token, applied);
    pthread_mutex_unlock(&s->lock);
    return rc;
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
    const char *pci = getenv("DPUMESH_PCI_ADDR"), *server = getenv("DPUMESH_SERVER");
    const char *workload = getenv("DPUMESH_WORKLOAD");
    if (!pci || !*pci) { errno = EINVAL; goto fail; }
    if (pod_ip_lookup(&t->pod_ip) != 0) goto fail;
    snprintf(t->server, sizeof(t->server), "%s", server && *server ? server : "DPUMesh0");
    snprintf(t->workload, sizeof(t->workload), "%s", workload ? workload : "");
    t->pod_id = env_int("DPUMESH_POD_ID", 0, 0, 126);
    t->backend_max = env_int("DPUMESH_BACKEND_MAX", BACKEND_MAX_DEFAULT, 1, SLOTS);
    t->next_uport = UPORT_BASE;
    t->service_id = DMESH_SVC_NONE;
    /* The served address is resolved once: every BACKEND flow carries it. */
    char served_buf[INET_ADDRSTRLEN + 6];
    const char *served = served_target(served_buf, sizeof(served_buf), cfg->service_name, t->pod_ip,
                                       dmesh_config_listen_port());
    if (served) {
        t->service_id = dmesh_target_resolve(served);
        if (t->service_id < 0) {
            int saved = errno;
            fprintf(stderr, "dpumesh: served target '%s' did not resolve (%s)\n", served, strerror(saved));
            errno = saved;
            goto fail;
        }
    }
    if (channel_dev_open(pci, &t->dev) != 0) goto fail;
    if (channel_session_open(t->dev, t->server) != 0) goto fail;
    t->flow_wake = channel_dev_flow_wake(t->dev);
    if (channel_mem_alloc(t->dev, cfg->bytes, &t->tx) != 0) goto fail;
    if (channel_mem_alloc(t->dev, (size_t)SLOTS * CHANNEL_WINDOW, &t->rx) != 0) goto fail;
    if (t->service_id != DMESH_SVC_NONE) {
        uint32_t ip; uint16_t port;
        if (dmesh_target_addr(t->service_id, &ip, &port) != 0 ||
            channel_session_listen(t->dev, ip, port) != 0) goto fail;
    }
    cfg->tx = channel_mem_base(t->tx); cfg->rx = channel_mem_base(t->rx);
    cfg->rx_bytes = (size_t)SLOTS * CHANNEL_WINDOW;
    cfg->pod_id = t->pod_id; cfg->service_id = t->service_id; cfg->stripes = SLOTS;
    cfg->flow_depth = TICKETS / 2;     /* a descriptor takes at most two cells (carrier_chunks) */
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
    /* A receive buffer the application still holds lives in a window. */
    for (int i = 0; i < SLOTS; ++i)
        if (!carrier_window_empty(&t->slots[i].window)) { errno = EBUSY; return -1; }
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
    struct slot *s = free_slot(t);
    if (!s) { pthread_mutex_unlock(&t->lock); errno = ENOSPC; return -1; }
    int rc = slot_open(t, s, CHANNEL_MODE_CLIENT_DPU_DMA, port, service_id, 0, 0);
    pthread_mutex_unlock(&t->lock);
    if (rc != 0) {
        /* A dial is often the first to meet a dead session: an idle client
         * progresses nothing in between. */
        int saved = errno;
        if (channel_dev_progress(t->dev) != 0) note_lost(t, errno);
        errno = saved;
    }
    return rc;
}
int dmesh_native_disconnect(struct dmesh_native_transport *t, uint16_t port)
{
    pthread_mutex_lock(&t->lock);
    struct slot *s = slot_of_port(t, port);
    int rc = 0;
    if (s && !s->backend) {
        pthread_mutex_lock(&s->lock);
        if (s->state == SLOT_OPEN && s->t_head == s->t_tail) { rc = slot_close(t, s); if (rc == 0) slot_reap_locked(t, s); }
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
    if (d->body_len == 0 && (d->flags & DMESH_DESC_HALF_CLOSE) && s->state == SLOT_OPEN) {
        /* The DPU's reader ends after every descriptor posted so far; the flow
         * keeps receiving. The FIN takes no ring cell: its ticket is consumed
         * and its ACK follows the custody ahead of it. */
        rc = channel_conn_shutdown(s->conn);
        if (rc == 0) { s->tickets[s->t_tail % TICKETS] = (struct ticket){d->seq, 0}; s->t_tail++; }
    } else if (d->body_len == 0) {
        /* The QP's FIN or reset: the stream ends with the connection. */
        if (s->state == SLOT_OPEN) rc = slot_close_start(t, s);
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
    /* End of stream holds no batch: its token, the window base, is below the
     * data ring, so its release is ignored. */
    e->desc.body_buf_slot = (int32_t)((size_t)slot_index(t, s) * CHANNEL_WINDOW + (len ? CHANNEL_DATA_OFF + pos : 0));
    e->desc.body_len = len;
    e->desc.src_port = s->peer; e->desc.dst_port = s->port;
    e->desc.src_pod = 0; e->desc.dst_pod = t->pod_id;
    e->desc.src_service = s->service_id; e->desc.dst_service = t->service_id;
    e->desc.seq = ++s->rx_seq; e->desc.valid = 1;
    TRACE("rx slot %d port %u seq %u pos %u len %u", slot_index(t, s), s->port, e->desc.seq, pos, len);
}
/* Retires every consumed ticket with consecutive sequences as one ACK. */
static void fill_acks(struct slot *s, struct dmesh_native_event *e, uint64_t consumed)
{
    uint16_t seq = s->tickets[s->t_head % TICKETS].seq;
    uint32_t count = 0;
    do { s->t_head++; count++; }
    while (s->t_head != s->t_tail && s->tickets[s->t_head % TICKETS].ticket <= consumed &&
           s->tickets[s->t_head % TICKETS].seq == (uint16_t)(seq + count));
    memset(e, 0, sizeof(*e));
    e->kind = DMESH_NATIVE_ACK; e->port = s->port; e->seq = seq; e->seq_count = count;
    TRACE("ack port %u seq %u count %u", s->port, seq, count);
}
uint32_t dmesh_native_progress(struct dmesh_native_transport *t)
{
    if (channel_dev_progress(t->dev) != 0) note_lost(t, errno);
    else if (t->service_id != DMESH_SVC_NONE) backend_progress(t);
    return __atomic_load_n(&t->active_stripes, __ATOMIC_ACQUIRE);
}
int dmesh_native_lost(struct dmesh_native_transport *t)
{
    return __atomic_load_n(&t->lost, __ATOMIC_ACQUIRE);
}
int dmesh_native_poll(struct dmesh_native_transport *t, int stripe, struct dmesh_native_event *e, int max)
{
    if (stripe < 0 || stripe >= SLOTS) return 0;
    struct slot *s = &t->slots[stripe];
    if (__atomic_load_n(&s->state, __ATOMIC_ACQUIRE) == SLOT_FREE) return 0;
    pthread_mutex_lock(&s->lock);
    int n = 0;
    /* A flow whose READY landed is read in the same pass: its first batches
     * may already be published, with no doorbell left to ring. */
    if (s->state == SLOT_OPENING) slot_opened(t, s);
    if (s->state == SLOT_OPENING) {
        pthread_mutex_unlock(&s->lock);
        return 0;
    }
    if (s->state == SLOT_OPEN) {
        if (t->flow_wake) channel_flow_progress(t->dev, (uint32_t)stripe + 1);
        /* No custody to retire and nothing received, closed or failed: done
         * without the session's state. Most polls look like this. */
        if (s->t_head == s->t_tail && channel_conn_idle(s->conn)) {
            pthread_mutex_unlock(&s->lock);
            return 0;
        }
        /* A DPU request on the flow is announced once; the QP's poll takes it. */
        if (channel_conn_announced(s->conn)) {
            memset(&e[n], 0, sizeof(e[n]));
            e[n].kind = DMESH_NATIVE_REQUEST; e[n].port = s->port;
            n++;
        }
        int gone = channel_conn_poll(s->conn), error = gone < 0 ? (errno ? errno : EIO) : 0;
        uint64_t consumed = channel_conn_consumed(s->conn);
        while (n < max) {
            if (gone >= 0 && s->t_head != s->t_tail && s->tickets[s->t_head % TICKETS].ticket <= consumed) {
                fill_acks(s, &e[n++], consumed);
            } else if (gone && !s->peer_gone_reported) {
                s->peer_gone_reported = 1;
                if (gone < 0) {
                    memset(&e[n], 0, sizeof(e[n]));
                    e[n].kind = DMESH_NATIVE_ERROR; e[n].port = s->port; e[n].error = error;
                    n++;
                } else fill_rx(t, s, &e[n++], 0, 0);
            } else {
                uint64_t seq; uint32_t pos, len;
                int r = channel_conn_rx_next(s->conn, &seq, &pos, &len);
                if (r > 0 && carrier_window_add(&s->window, seq, pos, len) == 0) {
                    fill_rx(t, s, &e[n++], pos, len);
                    if (s->backend && !s->claimed) { s->claimed = 1; break; }
                } else if (r < 0 && !s->peer_gone_reported) {
                    s->peer_gone_reported = 1; fill_rx(t, s, &e[n++], 0, 0);   /* end of stream or malformed batch */
                } else break;
            }
        }
    } else {
        /* Closed: retire outstanding custody, then the close marker, then reap. */
        if (s->state == SLOT_CLOSING) slot_closed(t, s);
        if (s->state != SLOT_CLOSED) {
            pthread_mutex_unlock(&s->lock);
            return 0;
        }
        while (n < max && s->t_head != s->t_tail) fill_acks(s, &e[n++], UINT64_MAX);
        if (n < max && s->fin_pending) {
            s->fin_pending = 0;
            memset(&e[n], 0, sizeof(e[n]));
            e[n].kind = DMESH_NATIVE_ACK; e[n].port = s->port; e[n].seq = s->fin_seq; e[n].seq_count = 1;
            n++;
        }
        if (s->t_head == s->t_tail && !s->fin_pending) {
            pthread_mutex_unlock(&s->lock);
            slot_reap(t, s);
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
    int last = 0;
    if (carrier_window_release(&s->window, (uint32_t)(local - CHANNEL_DATA_OFF)) == 0) {
        uint64_t seq, bytes;
        if (carrier_window_advance(&s->window, &seq, &bytes) && s->state == SLOT_OPEN) { channel_conn_rx_consumed(s->conn, seq, bytes); TRACE("release slot %zu cursor seq %lu bytes %lu", idx, (unsigned long)seq, (unsigned long)bytes); }
        last = s->state == SLOT_CLOSED && carrier_window_empty(&s->window);
    }
    pthread_mutex_unlock(&s->lock);
    if (last) slot_reap(t, s);         /* a closed flow's last held batch frees its slot */
}
/* With flow wake a stripe's doorbell is its slot's own consumer fd; otherwise
 * the slot's epoll of its connection's private engines (host-dpa). */
int dmesh_native_stripe_fd(struct dmesh_native_transport *t, int stripe)
{
    if (stripe < 0 || stripe >= SLOTS) return -1;
    return t->flow_wake ? channel_flow_fd(t->dev, (uint32_t)stripe + 1) : t->slots[stripe].epfd;
}
int dmesh_native_stripe_of(struct dmesh_native_transport *t, uint16_t port)
{
    uint16_t i = t->port_slot[port];
    return i ? (int)i - 1 : -1;
}
/* An armed doorbell is one-shot: it fires on the next completion of any of the
 * connection's engines and stays readable until cleared. `rang` acknowledges
 * one that fired; it is rearmed only after that, so a sleeping consumer costs
 * one request per wake. A flow wake's consumer outlives its flows, and only
 * its request clears what raised it as a flow closed, so it is rearmed
 * whatever the slot's state; a closed or free slot's consumer may still hold
 * descriptors of its last flow, which no poll publishes, and unpolled they
 * would fire its next request at once, so the clear drops them. The ticket
 * read under the slot lock pairs with the sender's check of the EQ's sleep
 * flag: either this sees the new custody or the sender wakes the EQ. */
int dmesh_native_stripe_arm(struct dmesh_native_transport *t, int stripe, int rang)
{
    if (stripe < 0 || stripe >= SLOTS) return 0;
    struct slot *s = &t->slots[stripe];
    if (!t->flow_wake && __atomic_load_n(&s->state, __ATOMIC_ACQUIRE) == SLOT_FREE) return 0;
    pthread_mutex_lock(&s->lock);
    if (rang && t->flow_wake) {
        if (s->state != SLOT_OPEN) channel_flow_progress(t->dev, (uint32_t)stripe + 1);
        s->armed = 0;      /* the request below clears the raised fd */
    } else if (rang && s->state == SLOT_OPEN) {
        struct epoll_event evs[CHANNEL_CONN_FDS];
        int n = epoll_wait(s->epfd, evs, CHANNEL_CONN_FDS, 0);
        for (int i = 0; i < n; ++i) channel_conn_clear(s->conn, evs[i].data.fd);
        if (n > 0) s->armed = 0;
    }
    int busy = 0;
    if (!s->armed && (t->flow_wake ? channel_flow_arm(t->dev, (uint32_t)stripe + 1) == 0
                                   : s->state == SLOT_OPEN && channel_conn_arm(s->conn) == 0))
        s->armed = 1;
    if (s->state == SLOT_OPEN) {
        if (s->t_head != s->t_tail) busy = DMESH_STRIPE_CUSTODY;
    } else if (s->state == SLOT_CLOSED && (s->t_head != s->t_tail || s->fin_pending)) {
        busy = DMESH_STRIPE_RETIRING;   /* retired by the next poll */
    }
    pthread_mutex_unlock(&s->lock);
    return busy;
}
int dmesh_native_flow_wake(struct dmesh_native_transport *t)
{
    return t->flow_wake;
}
uint32_t dmesh_native_take_news(struct dmesh_native_transport *t)
{
    return channel_dev_take_news(t->dev);   /* bit flow_id - 1 = slot index = stripe */
}
int dmesh_native_rx_signaled(struct dmesh_native_transport *t)
{
    return channel_dev_rx_signaled(t->dev);
}
int dmesh_native_wake_fd(struct dmesh_native_transport *t)
{
    return channel_dev_fd(t->dev);
}
int dmesh_native_idle_arm(struct dmesh_native_transport *t)
{
    return channel_dev_arm(t->dev);
}
void dmesh_native_wake_counters(struct dmesh_native_transport *t, uint64_t *arms, uint64_t *doorbells)
{
    channel_dev_wake_counters(t->dev, arms, doorbells);
}
int dmesh_native_resolve(struct dmesh_native_transport *t, const char *name, uint32_t addr, uint16_t port)
{
    (void)t;
    return name ? dmesh_target_resolve(name) : dmesh_target_member(addr, port);
}
