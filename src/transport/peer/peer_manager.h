#ifndef DMESH_PEER_MANAGER_H
#define DMESH_PEER_MANAGER_H
#include "peer_config.h"
#include "peer_control_session.h"
#include "peer_crypto.h"
#include "peer_pair_transport.h"

/* The node crypto manager: the one owner that turns a worker's Pod-pair OPEN
 * into a negotiated association and back into worker grants.
 *
 * It holds the node control sessions (TLS 1.3 over the control carrier), the
 * association registry, and one mailbox pair per data worker. It never touches
 * a worker's channel, QP or table and never performs hardware work: workers act
 * on copied commands, the crypto adapter acts on copied requests, and both
 * report completions the manager feeds back into the registry.
 *
 * Sessions are per node and direction. A proposal travels only on the session
 * this node initiated, so two nodes proposing to each other hold two sessions
 * and never race to converge on one. Every function here runs on the manager
 * owner (its own thread in the runtime, the caller in tests) except the two
 * notify calls, which any thread may use. */

struct peer_manager_ops {
    /* Both read the held authority snapshot; neither consults the peer. */
    int (*node_binding)(
        void *ctx, const char *node, const uint8_t **key, uint32_t *ip_be, uint16_t *port);
    int (*node_by_key)(void *ctx, const uint8_t key[32], char node[DMESH_K8S_NAME_MAX]);
};

struct peer_manager_config {
    char node[DMESH_K8S_NAME_MAX];
    char cluster[PEER_SEC_CLUSTER_MAX];
    const uint8_t *seed;              /* node private half; read once by the constructor */
    uint8_t boot[16];                 /* this process incarnation, fresh per start */
    unsigned workers;                 /* data workers on this node, 1..16 */
    uint16_t mtu;                     /* RoCE path MTU every lane uses */
    const struct peer_carrier_ops *carrier; /* control carrier; owned on success */
    void *carrier_ctx;
    struct peer_crypto_adapter crypto;
    struct peer_assoc_limits limits;
    /* Authority and policy leases granted to a pair until the signed security
     * feed and policy-watch freshness replace them. */
    uint64_t lease_ns;
    uint64_t control_lease_ns, setup_timeout_ns;
    /* The proposer rekeys an association this long after it (re)activates.
     * Zero disables it. A packet-count hard stop belongs to the adapter. */
    uint64_t rekey_ns;
    uint64_t (*now_ns)(void *);
    void *now_ctx;
    const struct peer_manager_ops *ops;
    void *ops_ctx;
};

struct peer_manager_stats {
    unsigned sessions, pairs, active, quarantined;
    uint64_t refused, faults;
};
struct peer_manager;
int peer_manager_new(const struct peer_manager_config *,
                     struct peer_manager **,
                     char *error,
                     size_t error_len);
/* -1 while any pair still holds worker lanes, hardware state or quarantine. */
int peer_manager_free(struct peer_manager *);
/* Workers are attached before either owner runs; the transport stays owned by
 * the runtime. */
int peer_manager_attach_worker(struct peer_manager *,
                               unsigned worker,
                               struct peer_pair_transport *);
/* One turn of the owner: sessions, worker events, hardware completions, the
 * registry and every pair. Returns 1 when anything moved. Time is monotonic and
 * never runs backwards. */
int peer_manager_progress(struct peer_manager *, uint64_t now_ns);
/* From the control thread. Bounded, copied, never blocking. */
int peer_manager_notify_unregister(struct peer_manager *, const char *pod_uid);
int peer_manager_notify_authority(struct peer_manager *, uint64_t generation, uint64_t deadline_ns);
void peer_manager_stats(struct peer_manager *, struct peer_manager_stats *);
/* The runtime's owner thread: progress on readiness, a 100 ms backstop tick. */
int peer_manager_start(struct peer_manager *);

/* Read-only owner-thread telemetry, safe while the manager thread runs.
 * cpu is the CPU observed at thread entry.
 * pairs=UINT32_MAX until its first completed turn. Sample CPU before stop. */
struct peer_manager_thread_snapshot {
    uint64_t cpu_ns;
    unsigned tid, cpu, pairs;
};

int peer_manager_thread_snapshot(struct peer_manager *, struct peer_manager_thread_snapshot *);
void peer_manager_stop(struct peer_manager *);
#endif
