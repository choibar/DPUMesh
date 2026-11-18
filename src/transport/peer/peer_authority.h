#ifndef DMESH_PEER_AUTHORITY_H
#define DMESH_PEER_AUTHORITY_H

#include "peer_identity.h"
#include "peer_topology.h"
#include <stdatomic.h>

/* The dispatcher publishes authority. Worker and manager readers retain a
 * snapshot for one progress turn, including any pointers returned by lookup.
 * No wall-clock grace period is used to free a displaced snapshot. */
struct peer_authority;
struct peer_authority_view;

enum peer_registration_state {
    PEER_REGISTRATION_EMPTY,
    PEER_REGISTRATION_ACTIVE,
    PEER_REGISTRATION_RETIRING,
};

struct peer_local_registration {
    uint64_t session_id, session_epoch;
    struct dmesh_identity_claims identity;
    struct dmesh_peer_registration peer;
    uint32_t pod_ip;
    unsigned peer_references;
    enum peer_registration_state state;
};

int peer_authority_new(const char *cluster, const char *node, unsigned capacity,
                       struct peer_authority **out);
/* Refuses while a registration still owns a session/flow/QP. */
int peer_authority_free(struct peer_authority *);

enum dmesh_topology_result peer_authority_adopt(
    struct peer_authority *, const char *document, size_t length, const char *key_dir);
struct peer_authority_view *peer_authority_acquire(struct peer_authority *);
uint64_t peer_authority_generation(struct peer_authority *);
void peer_authority_release(struct peer_authority_view *);
const struct dmesh_topology_tables *peer_authority_tables(const struct peer_authority_view *);
const struct dmesh_gen_pod *peer_authority_pod(const struct peer_authority_view *, const char *uid);
const struct dmesh_gen_service *peer_authority_service(const struct peer_authority_view *, const char *key);
int peer_authority_pod_in_service(const struct peer_authority_view *, const char *uid, const char *key);
int peer_authority_node(const struct peer_authority_view *, const char *node,
                        const uint8_t **key, uint32_t *ip_be, uint16_t *port);
int peer_authority_node_by_key(const struct peer_authority_view *, const uint8_t key[32],
                               char node[DMESH_K8S_NAME_MAX]);

/* The authenticated paired-host TLS session supplies seal_key; the Comch
 * session supplies a fresh nonce. Neither value comes from the assertion.
 * Returns an errno. A replay is accepted only for that exact live session. */
int peer_authority_register(struct peer_authority *, uint64_t session_id,
                            uint64_t session_epoch, const uint8_t seal_key[16],
                            const uint8_t nonce[32], uint64_t now_sec,
                            const struct dmesh_registration_assertion_msg *);
int peer_authority_session(struct peer_authority *, uint64_t session_id,
                           uint64_t session_epoch, struct peer_local_registration *out);
int peer_authority_local(struct peer_authority *, const char *uid,
                         struct peer_local_registration *out);
/* A peer connection/QP holds registration ownership until its DMA fence. The
 * dispatcher cannot recycle an otherwise closed Host session while held. */
int peer_authority_hold(struct peer_authority *, uint64_t id, uint64_t epoch);
/* Pins the registration and exposes only its revocation flag. Immutable
 * identity stays in the caller's copy; unhold ends pointer validity. */
int peer_authority_watch(struct peer_authority *, uint64_t id, uint64_t epoch,
                          const _Atomic unsigned **active);
int peer_authority_unhold(struct peer_authority *, uint64_t id, uint64_t epoch);

/* Revocation is a latch: losing authority cannot be undone by a later update.
 * Session/QP teardown must finish before retired_done allows reuse. */
void peer_authority_retire_session(struct peer_authority *, uint64_t id, uint64_t epoch);
void peer_authority_retire_nonce(struct peer_authority *, const uint8_t nonce[32]);
int peer_authority_nonce_present(struct peer_authority *, const uint8_t nonce[32]);
void peer_authority_retire_all(struct peer_authority *);
int peer_authority_retiring(struct peer_authority *, unsigned *cursor,
                            struct peer_local_registration *out);
int peer_authority_retired_done(struct peer_authority *, uint64_t id, uint64_t epoch);

#endif
