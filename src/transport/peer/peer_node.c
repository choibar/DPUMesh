#include "peer_node.h"
#include "peer_crypto_ipc.h"
#include "peer_dpa_owner.h"
#include "peer_manager.h"
#include "peer_wire_write_imm.h"
#include "peer_wire_write_sync.h"
#include "peer_write_sync_dpa.h"
#include "../common/dpa_runtime.h"

#include <arpa/inet.h>
#include <doca_dev.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <openssl/crypto.h>
#include <openssl/rand.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

extern struct doca_dpa_app *DPU_mesh_dpa_app;
#define MAX_WORKERS PEER_RDMA_MAX_WORKERS

struct peer_node_worker {
    struct doca_dev *dev;
    struct peer_pair_transport *transport;
    struct peer_dpa_owner *dpa;
    struct peer_stream_worker *streams;
    _Atomic int stopped;
};
struct peer_node {
    struct peer_session_control *control;
    struct peer_authority *authority;
    struct peer_manager *manager;
    struct peer_crypto_ipc *crypto;
    struct ibv_context *verbs;
    struct dmesh_dpa_runtime *runtime;
    struct peer_node_worker workers[MAX_WORKERS];
    unsigned count;
    uint64_t next_control, lease_sequence;
    struct { uint64_t id, epoch; } notified[8192];
    unsigned notified_count;
    char name[DMESH_K8S_NAME_MAX];
};
static pthread_mutex_t route_lock = PTHREAD_MUTEX_INITIALIZER;
static struct peer_node *route_node;

int peer_node_remote(uint32_t ip, uint16_t port)
{
    pthread_mutex_lock(&route_lock);
    struct peer_authority_view *v = route_node ? peer_authority_acquire(route_node->authority) : NULL;
    const struct dmesh_topology_tables *t = peer_authority_tables(v);
    int result = 0;
    for (size_t i = 0; t && i < t->pod_count; ++i) {
        if (htonl(t->pods[i].ip_be) != ip || !strcmp(t->pods[i].node_name, route_node->name)) continue;
        result = -1;
        for (size_t e = 0; e < t->endpoint_count; ++e)
            if (t->endpoints[e].pod == i && t->services[t->endpoints[e].service].port == port) {
                result = 1;
                break;
            }
        break;
    }
    peer_authority_release(v);
    pthread_mutex_unlock(&route_lock);
    return result;
}

static uint64_t now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + t.tv_nsec;
}
static int number(const char *name, unsigned fallback, unsigned min, unsigned max, unsigned *out)
{
    const char *value = getenv(name);
    if (!value || !*value) { *out = fallback; return 0; }
    char *end;
    errno = 0;
    unsigned long n = strtoul(value, &end, 10);
    if (errno || *end || n < min || n > max) return EINVAL;
    *out = (unsigned)n;
    return 0;
}
static int binding(void *ctx, const char *name, const uint8_t **key, uint32_t *ip, uint16_t *port)
{
    struct peer_node *n = ctx;
    /* The manager consumes the key during this call's progress turn. Copying
     * into its thread-local storage avoids a dangling topology pointer. */
    static _Thread_local uint8_t copied[32];
    struct peer_authority_view *view = peer_authority_acquire(n->authority);
    const uint8_t *found = NULL;
    int rc = peer_authority_node(view, name, &found, ip, port);
    if (rc == 1) { memcpy(copied, found, sizeof(copied)); *key = copied; }
    peer_authority_release(view);
    return rc;
}
static int by_key(void *ctx, const uint8_t key[32], char name[DMESH_K8S_NAME_MAX])
{
    struct peer_node *n = ctx;
    struct peer_authority_view *view = peer_authority_acquire(n->authority);
    int rc = peer_authority_node_by_key(view, key, name);
    peer_authority_release(view);
    return rc;
}
static const struct peer_manager_ops MANAGER_OPS = {.node_binding = binding, .node_by_key = by_key};

int peer_node_new(struct peer_session_control *control, struct doca_dev **devices, unsigned count,
                   int (*listener)(uint32_t, uint16_t), struct peer_node **out)
{
    if (!out) return EINVAL;
    *out = NULL;
    const char *backend = getenv("DPUMESH_PEER_TRANSPORT");
    if (!backend || !*backend) return 0;
    int send = !strcmp(backend, "rdma-send"), imm = !strcmp(backend, "rdma-write-imm");
    int sync = !strcmp(backend, "rdma-write-sync");
    const char *node = getenv("DPUMESH_NODE_NAME"), *cluster = getenv("DPUMESH_CLUSTER_ID");
    const char *key_path = getenv("DPUMESH_NODE_KEY"), *device = getenv("DPUMESH_PEER_RDMA_DEVICE");
    const char *crypto = getenv("DPUMESH_PEER_CRYPTO");
    unsigned offload, gid, mtu, first_eu;
    if ((!send && !imm && !sync) || !control || !count || count > MAX_WORKERS || !devices || !listener ||
        !node || !cluster || !key_path || !device || !crypto || strncmp(crypto, "unix:", 5) || !crypto[5] ||
        number("DMESH_PEER_DPA", 1, 0, 1, &offload) ||
        number("DPUMESH_PEER_GID_INDEX", 0, 0, 255, &gid) ||
        number("DPUMESH_PEER_MTU", 1024, 256, 4096, &mtu) ||
        number("DMESH_PEER_FIRST_EU", 32, 0, 63, &first_eu) || first_eu + count > 64 ||
        (mtu != 256 && mtu != 512 && mtu != 1024 && mtu != 2048 && mtu != 4096)) {
        fprintf(stderr, "[peer node] invalid transport/authentication/device configuration\n");
        return EINVAL;
    }
    struct peer_node *n = calloc(1, sizeof(*n));
    if (!n) return ENOMEM;
    *out = n; /* Failed SDK cleanup must remain reachable by the caller. */
    n->control = control;
    n->authority = peer_session_control_authority(control);
    n->count = count;
    snprintf(n->name, sizeof(n->name), "%s", node);
    for (unsigned i = 0; i < count; ++i) atomic_init(&n->workers[i].stopped, 0);
    uint8_t key[32], seed[32] = {0};
    char error[256] = {0};
    int rc = EIO;
    if (dmesh_peer_node_key_load(key_path, key, seed, error, sizeof(error))) goto done;
    struct peer_authority_view *v = peer_authority_acquire(n->authority);
    const uint8_t *expected;
    uint32_t ip;
    uint16_t port;
    int bound = peer_authority_node(v, node, &expected, &ip, &port);
    int match = bound == 1 && !CRYPTO_memcmp(key, expected, 32);
    peer_authority_release(v);
    if (!match) { snprintf(error, sizeof(error), "node key does not match signed topology"); goto done; }
    int total = 0;
    struct ibv_device **list = ibv_get_device_list(&total);
    for (int i = 0; list && i < total && !n->verbs; ++i)
        if (!strcmp(ibv_get_device_name(list[i]), device)) n->verbs = ibv_open_device(list[i]);
    if (list) ibv_free_device_list(list);
    if (!n->verbs) { snprintf(error, sizeof(error), "cannot open RDMA device %s", device); goto done; }
    if (peer_crypto_ipc_new(crypto + 5, &n->crypto, error, sizeof(error))) goto done;
    if (offload && dmesh_dpa_runtime_acquire(devices[0], DPU_mesh_dpa_app, &n->runtime) != DOCA_SUCCESS)
        goto done;

    for (unsigned i = 0; i < count; ++i) {
        struct peer_node_worker *w = &n->workers[i];
        if (doca_dev_open(doca_dev_as_devinfo(devices[i]), &w->dev) != DOCA_SUCCESS) goto done;
        struct peer_pair_transport_config cfg = {.worker = i, .connections = PEER_RDMA_LANES,
            .qps_per_peer_node = PEER_RDMA_QPS_PER_PEER,
            .setup_timeout_ns = 30ull * 1000000000ull, .dma_fenced = dmesh_peer_channel_dma_fenced};
        const struct peer_dpa_runtime *dpa = NULL;
        if (offload) {
            if (peer_dpa_owner_new(w->dev, dmesh_dpa_runtime_context(n->runtime), i, count,
                                   first_eu + i, &w->dpa)) goto done;
            dpa = peer_dpa_owner_runtime(w->dpa);
        }
        int failed;
        if (offload) {
            failed = imm ? peer_pair_write_imm_dpa_new(&cfg, n->verbs, 1, gid, mtu,
                PEER_WRITE_IMM_RING_DEFAULT, dpa, &w->transport, error, sizeof(error)) :
                sync ? peer_pair_write_sync_dpa_new(&cfg, n->verbs, 1, gid, mtu,
                PEER_WRITE_SYNC_RING_DEFAULT, dpa, &w->transport, error, sizeof(error)) :
                peer_pair_dpa_new(&cfg, n->verbs, 1, gid, mtu, dpa, &w->transport, error, sizeof(error));
        } else {
            failed = imm ? peer_pair_write_imm_new(&cfg, n->verbs, 1, gid, mtu,
                PEER_WRITE_IMM_RING_DEFAULT, &w->transport, error, sizeof(error)) :
                sync ? peer_pair_write_sync_new(&cfg, n->verbs, 1, gid, mtu,
                PEER_WRITE_SYNC_RING_DEFAULT, &w->transport, error, sizeof(error)) :
                peer_pair_transport_new(&cfg, n->verbs, 1, gid, mtu, &w->transport, error, sizeof(error));
        }
        if (failed || peer_stream_worker_new(n->authority, node, key, w->transport, listener, &w->streams))
            goto done;
    }
    struct peer_manager_config cfg = {.seed = seed, .workers = count, .mtu = mtu,
        .crypto = peer_crypto_ipc_adapter(n->crypto), .lease_ns = 30000000000ull,
        .ops = &MANAGER_OPS, .ops_ctx = n};
    snprintf(cfg.node, sizeof(cfg.node), "%s", node);
    snprintf(cfg.cluster, sizeof(cfg.cluster), "%s", cluster);
    if (RAND_bytes(cfg.boot, sizeof(cfg.boot)) != 1 ||
        peer_wire_tcp_new(ip, port, &cfg.wire, &cfg.wire_ctx, error, sizeof(error))) goto done;
    if (peer_manager_new(&cfg, &n->manager, error, sizeof(error))) {
        cfg.wire->ctx_free(cfg.wire_ctx);
        goto done;
    }
    for (unsigned i = 0; i < count; ++i)
        if (peer_manager_attach_worker(n->manager, i, n->workers[i].transport)) goto done;
    if (peer_manager_start(n->manager)) goto done;
    pthread_mutex_lock(&route_lock);
    if (route_node) {
        pthread_mutex_unlock(&route_lock);
        goto done;
    }
    route_node = n;
    pthread_mutex_unlock(&route_lock);
    fprintf(stderr, "[peer node] backend=%s offload=%u workers=%u rdma_engines=%u "
            "peer_budget=%u qps_per_peer=%u qps_per_worker=%u qp_budget=%u "
            "admission=pod-pair codec=dmesh4/1 header_bytes=4 "
            "reuse=bilateral-drain/1 topology=authenticated\n", backend, offload, count,
            offload ? count : 0, PEER_RDMA_PEERS, PEER_RDMA_QPS_PER_PEER,
            PEER_RDMA_LANES, count * PEER_RDMA_LANES);
    rc = 0;
done:
    OPENSSL_cleanse(seed, sizeof(seed));
    if (rc) {
        fprintf(stderr, "[peer node] startup failed: %s\n", error);
        for (unsigned i = 0; i < count; ++i) (void)peer_node_worker_stop(&n->workers[i]);
        (void)peer_node_free(out);
    }
    return rc;
}
struct peer_node_worker *peer_node_worker(struct peer_node *n, unsigned i)
{ return n && i < n->count ? &n->workers[i] : NULL; }
struct peer_stream_worker *peer_node_streams(struct peer_node_worker *w)
{ return w ? w->streams : NULL; }
int peer_node_worker_progress(struct peer_node_worker *w)
{
    if (!w || atomic_load(&w->stopped)) return 0;
    int msgq = w->dpa ? peer_dpa_owner_progress(w->dpa) : 0;
    if (msgq < 0) peer_stream_worker_stop(w->streams);
    return peer_stream_worker_progress(w->streams) + (msgq > 0);
}
int peer_node_worker_fds(struct peer_node_worker *w, int *carrier, int *msgq)
{
    if (!w || !carrier || !msgq) return -EINVAL;
    *carrier = peer_pair_transport_epfd(w->transport);
    *msgq = w->dpa ? peer_dpa_owner_fd(w->dpa) : -1;
    return 0;
}
int peer_node_worker_arm(struct peer_node_worker *w)
{ return w && w->dpa ? peer_dpa_owner_arm(w->dpa) : 0; }
int peer_node_worker_disarm(struct peer_node_worker *w)
{ return w && w->dpa ? peer_dpa_owner_disarm(w->dpa) : 0; }
int peer_node_worker_stop(struct peer_node_worker *w)
{
    if (!w || atomic_load(&w->stopped)) return 0;
    peer_stream_worker_stop(w->streams);
    /* The Rust owner has to detach all IO first. Failed DMA retirement retains
     * the worker; never free a live device mapping to force shutdown. */
    if (w->streams) {
        peer_node_worker_progress(w);
        if (peer_stream_worker_quiesce(w->streams)) return EBUSY;
    }
    /* The manager retains its mailbox pointers. The stream worker also keeps
     * its registered arena backing. Free transport/SDK objects first in
     * peer_node_free, after every worker and the manager have stopped. */
    atomic_store(&w->stopped, 1);
    return 0;
}
void peer_node_control_progress(struct peer_node *n)
{
    if (!n || !n->manager) return;
    uint64_t now = now_ns();
    if (now < n->next_control) return;
    n->next_control = now + 100000000ull;
    if (peer_session_control_healthy(n->control)) {
        /* Internal lease sequence, not the signed document's version. The
         * latter may stay unchanged while the authenticated watch is healthy. */
        if (n->lease_sequence != UINT64_MAX &&
            peer_manager_notify_authority(n->manager, n->lease_sequence + 1,
                                           now + 3000000000ull) == 1)
            ++n->lease_sequence;
    } else {
        peer_authority_retire_all(n->authority);
    }
    unsigned cursor = 0;
    struct peer_local_registration r;
    while (peer_authority_retiring(n->authority, &cursor, &r)) {
        unsigned i = 0;
        for (; i < n->notified_count; ++i)
            if (n->notified[i].id == r.session_id && n->notified[i].epoch == r.session_epoch) break;
        if (i < n->notified_count) continue;
        if (peer_manager_notify_unregister(n->manager, r.identity.pod_uid) != 1) break;
        if (n->notified_count < 8192) {
            n->notified[n->notified_count].id = r.session_id;
            n->notified[n->notified_count++].epoch = r.session_epoch;
        }
    }
}
int peer_node_free(struct peer_node **ptr)
{
    if (!ptr || !*ptr) return 0;
    struct peer_node *n = *ptr;
    for (unsigned i = 0; i < n->count; ++i)
        if (!atomic_load(&n->workers[i].stopped)) return EBUSY;
    if (n->manager) {
        peer_manager_stop(n->manager);
        if (peer_manager_free(n->manager)) return EBUSY;
        n->manager = NULL;
    }
    for (unsigned i = 0; i < n->count; ++i) {
        struct peer_node_worker *w = &n->workers[i];
        if (w->transport) {
            if (w->dpa) peer_dpa_owner_progress(w->dpa);
            if (peer_pair_transport_free(w->transport)) return EBUSY;
            w->transport = NULL;
        }
        if (peer_stream_worker_free(&w->streams)) return EBUSY;
        if (peer_dpa_owner_free(&w->dpa)) return EBUSY;
        if (w->dev) {
            if (doca_dev_close(w->dev) != DOCA_SUCCESS) return EBUSY;
            w->dev = NULL;
        }
    }
    if (n->runtime && dmesh_dpa_runtime_release(&n->runtime) != DOCA_SUCCESS) return EBUSY;
    pthread_mutex_lock(&route_lock);
    if (route_node == n) route_node = NULL;
    pthread_mutex_unlock(&route_lock);
    peer_crypto_ipc_free(n->crypto);
    if (n->verbs) ibv_close_device(n->verbs);
    free(n);
    *ptr = NULL;
    return 0;
}
