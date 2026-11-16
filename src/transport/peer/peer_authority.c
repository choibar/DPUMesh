#include "peer_authority.h"

#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <openssl/crypto.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct peer_authority_view {
    _Atomic unsigned references;
    struct dmesh_topology_tables *tables;
    uint8_t digest[SHA256_DIGEST_LENGTH];
};

struct peer_authority {
    pthread_mutex_t lock;
    char cluster[DMESH_CLUSTER_ID_MAX], node[DMESH_K8S_NAME_MAX];
    struct peer_authority_view *current;
    struct peer_local_registration *registrations;
    unsigned capacity;
    _Atomic unsigned *active;
    _Atomic uint64_t generation;
};

int peer_authority_new(const char *cluster, const char *node, unsigned capacity,
                       struct peer_authority **out)
{
    if (!out || !cluster || !*cluster || !node || !*node || !capacity || capacity > 8192 ||
        strlen(cluster) >= DMESH_CLUSTER_ID_MAX || strlen(node) >= DMESH_K8S_NAME_MAX)
        return EINVAL;

    struct peer_authority *a = calloc(1, sizeof(*a));
    if (!a)
        return ENOMEM;

    a->registrations = calloc(capacity, sizeof(*a->registrations));
    a->active = calloc(capacity, sizeof(*a->active));
    if (!a->registrations || !a->active) {
        free(a->registrations);
        free(a->active);
        free(a);
        return ENOMEM;
    }

    atomic_init(&a->generation, 0);
    for (unsigned i = 0; i < capacity; ++i) atomic_init(&a->active[i], 0);

    int error = pthread_mutex_init(&a->lock, NULL);
    if (error) {
        free(a->registrations);
        free(a->active);
        free(a);
        return error;
    }

    strcpy(a->cluster, cluster);
    strcpy(a->node, node);
    a->capacity = capacity;
    *out = a;
    return 0;
}

struct peer_authority_view *peer_authority_acquire(struct peer_authority *a)
{
    if (!a)
        return NULL;

    pthread_mutex_lock(&a->lock);
    struct peer_authority_view *view = a->current;
    if (view)
        atomic_fetch_add_explicit(&view->references, 1, memory_order_relaxed);
    pthread_mutex_unlock(&a->lock);
    return view;
}

void peer_authority_release(struct peer_authority_view *view)
{
    if (view && atomic_fetch_sub_explicit(&view->references, 1, memory_order_acq_rel) == 1) {
        dmesh_topology_tables_free(view->tables);
        free(view);
    }
}

const struct dmesh_topology_tables *peer_authority_tables(const struct peer_authority_view *view)
{
    return view ? view->tables : NULL;
}

const struct dmesh_gen_pod *peer_authority_pod(const struct peer_authority_view *v, const char *uid)
{
    if (!v || !uid)
        return NULL;

    size_t first = 0, last = v->tables->pod_count;
    while (first < last) {
        size_t mid = first + (last - first) / 2;
        int order = strcmp(uid, v->tables->pods[mid].uid);
        if (!order)
            return &v->tables->pods[mid];
        if (order < 0)
            last = mid;
        else
            first = mid + 1;
    }
    return NULL;
}

const struct dmesh_gen_service *peer_authority_service(const struct peer_authority_view *v,
                                                      const char *key)
{
    if (!v || !key)
        return NULL;

    size_t first = 0, last = v->tables->service_count;
    while (first < last) {
        size_t mid = first + (last - first) / 2;
        int order = strcmp(key, v->tables->services[mid].key);
        if (!order)
            return &v->tables->services[mid];
        if (order < 0)
            last = mid;
        else
            first = mid + 1;
    }
    return NULL;
}

int peer_authority_pod_in_service(const struct peer_authority_view *v, const char *uid,
                                  const char *key)
{
    const struct dmesh_gen_service *service = peer_authority_service(v, key);
    if (!service || !uid)
        return 0;

    for (uint32_t i = 0; i < service->endpoint_count; ++i) {
        unsigned pod = v->tables->endpoints[service->endpoint_first + i].pod;
        if (!strcmp(v->tables->pods[pod].uid, uid))
            return 1;
    }
    return 0;
}

int peer_authority_node(const struct peer_authority_view *v, const char *node,
                        const uint8_t **key, uint32_t *ip_be, uint16_t *port)
{
    if (!v || !node || !key || !ip_be || !port)
        return 0;

    for (size_t i = 0; i < v->tables->node_count; ++i) {
        const struct dmesh_gen_node *n = &v->tables->nodes[i];
        if (strcmp(n->name, node))
            continue;

        static const uint8_t zero[32];
        if (!memcmp(n->dpu_static_public_key, zero, sizeof(zero)))
            return 0;
        *key = n->dpu_static_public_key;
        *ip_be = htonl(n->rdma_ip_be);
        *port = n->rdma_port;
        return 1;
    }
    return 0;
}

int peer_authority_node_by_key(const struct peer_authority_view *v, const uint8_t key[32],
                               char node[DMESH_K8S_NAME_MAX])
{
    static const uint8_t zero[32];
    if (!v || !key || !node || !memcmp(key, zero, sizeof(zero)))
        return 0;

    const struct dmesh_gen_node *found = NULL;
    for (size_t i = 0; i < v->tables->node_count; ++i) {
        const struct dmesh_gen_node *n = &v->tables->nodes[i];
        if (!memcmp(n->dpu_static_public_key, key, 32)) {
            if (found)
                return 0;
            found = n;
        }
    }
    if (!found)
        return 0;

    memcpy(node, found->name, DMESH_K8S_NAME_MAX);
    return 1;
}

static int registration_authorized(const struct peer_authority *a,
                                   const struct peer_authority_view *view,
                                   const struct peer_local_registration *r)
{
    const struct dmesh_gen_pod *pod = peer_authority_pod(view, r->identity.pod_uid);
    if (!pod || strcmp(pod->node_name, a->node) ||
        strcmp(pod->namespace_name, r->identity.namespace_name) ||
        strcmp(pod->service_account, r->identity.service_account) || pod->ip_be != r->pod_ip)
        return 0;

    /* Readiness removes a Service endpoint; it does not invalidate the Pod's
     * identity. Service membership is checked separately when opening a pair. */
    return 1;
}

enum dmesh_topology_result peer_authority_adopt(struct peer_authority *a,
    const char *document, size_t length, const char *key_dir)
{
    if (!a || !document || !length)
        return DMESH_TOPOLOGY_MALFORMED;
    if (length > DMESH_TOPOLOGY_MAX_BYTES)
        return DMESH_TOPOLOGY_OVERFLOW;

    size_t signed_length = 0;
    enum dmesh_feed_result verified = dmesh_gen_verify(document, length, key_dir, &signed_length);
    if (verified != DMESH_FEED_OK)
        return verified == DMESH_FEED_UNSIGNED ? DMESH_TOPOLOGY_UNSIGNED :
               verified == DMESH_FEED_BAD_KEY_ID ? DMESH_TOPOLOGY_BAD_KEY_ID : DMESH_TOPOLOGY_BAD_SIG;

    struct peer_authority_view *next = calloc(1, sizeof(*next));
    if (!next)
        return DMESH_TOPOLOGY_OVERFLOW;

    atomic_init(&next->references, 1);
    enum dmesh_topology_result result = dmesh_topology_parse(document, signed_length, NULL, &next->tables);
    if (result != DMESH_TOPOLOGY_ADOPTED) {
        free(next);
        return result;
    }
    SHA256((const unsigned char *)document, signed_length, next->digest);

    pthread_mutex_lock(&a->lock);
    struct peer_authority_view *old = a->current;
    if (old && next->tables->version <= old->tables->version) {
        result = next->tables->version < old->tables->version ? DMESH_TOPOLOGY_ROLLBACK :
            CRYPTO_memcmp(next->digest, old->digest, sizeof(next->digest)) ?
                DMESH_TOPOLOGY_MALFORMED : DMESH_TOPOLOGY_SAME_VERSION;
        pthread_mutex_unlock(&a->lock);
        peer_authority_release(next);
        return result;
    }

    a->current = next;
    for (unsigned i = 0; i < a->capacity; ++i) {
        struct peer_local_registration *r = &a->registrations[i];
        if (r->state == PEER_REGISTRATION_ACTIVE && !registration_authorized(a, next, r))
        {
            r->state = PEER_REGISTRATION_RETIRING;
            atomic_store_explicit(&a->active[i], 0, memory_order_release);
        }
    }
    atomic_store_explicit(&a->generation, next->tables->version, memory_order_release);
    pthread_mutex_unlock(&a->lock);
    peer_authority_release(old);
    return DMESH_TOPOLOGY_ADOPTED;
}

int peer_authority_register(struct peer_authority *a, uint64_t id, uint64_t epoch,
                            const uint8_t seal_key[16], const uint8_t nonce[32], uint64_t now_sec,
                            const struct dmesh_registration_assertion_msg *assertion)
{
    static const uint8_t zero[32];
    if (!a || !id || !epoch || !seal_key || !nonce || !assertion ||
        !memcmp(seal_key, zero, 16) || !memcmp(nonce, zero, 32))
        return EINVAL;
    if (assertion->type != DMESH_MSG_REG_ASSERTION || assertion->version != DMESH_LOCAL_VERSION ||
        assertion->reserved[0] || assertion->reserved[1] || !dmesh_registration_verify(seal_key, assertion))
        return EACCES;

    struct peer_local_registration candidate = {.session_id = id, .session_epoch = epoch,
                                                .state = PEER_REGISTRATION_ACTIVE};
    if (dmesh_identity_decode(&assertion->identity, a->cluster, a->node, nonce, now_sec,
                              &candidate.identity) != DMESH_IDENTITY_OK)
        return EACCES;

    struct in_addr ip;
    if (inet_pton(AF_INET, candidate.identity.pod_ip, &ip) != 1)
        return EACCES;
    candidate.pod_ip = ntohl(ip.s_addr);
    memcpy(candidate.peer.daemon, candidate.identity.daemon_incarnation, 16);
    memcpy(candidate.peer.nonce, nonce, 32);
    candidate.peer.slot = candidate.identity.channel_slot;
    candidate.peer.generation = candidate.identity.channel_generation;
    if (!dmesh_peer_registration_valid(&candidate.peer))
        return EACCES;

    pthread_mutex_lock(&a->lock);
    int result = ENOSPC;
    struct peer_local_registration *empty = NULL;
    if (!peer_authority_pod(a->current, candidate.identity.pod_uid)) {
        /* The seal/identity has already been checked. A newer signed feed may
         * still be in flight after kubelet reports the container as running.
         * EAGAIN grants no registration and creates no live DMA/QP state. */
        result = EAGAIN;
        goto done;
    }
    if (!registration_authorized(a, a->current, &candidate)) {
        result = EACCES;
        goto done;
    }
    for (unsigned i = 0; i < a->capacity; ++i) {
        struct peer_local_registration *r = &a->registrations[i];
        if (r->state == PEER_REGISTRATION_EMPTY) {
            if (!empty)
                empty = r;
            continue;
        }
        if (r->session_id == id && r->session_epoch == epoch) {
            result = r->state == PEER_REGISTRATION_ACTIVE &&
                dmesh_peer_registration_equal(&r->peer, &candidate.peer) &&
                !memcmp(&r->identity, &candidate.identity, sizeof(r->identity)) ? 0 : ESTALE;
            goto done;
        }
        if (!strcmp(r->identity.pod_uid, candidate.identity.pod_uid) ||
            (r->peer.slot == candidate.peer.slot && !memcmp(r->peer.daemon, candidate.peer.daemon, 16)) ||
            !memcmp(r->peer.nonce, nonce, 32)) {
            result = EBUSY;
            goto done;
        }
    }
    if (empty) {
        *empty = candidate;
        atomic_store_explicit(&a->active[empty - a->registrations], 1, memory_order_release);
        result = 0;
    }
done:
    pthread_mutex_unlock(&a->lock);
    return result;
}

int peer_authority_session(struct peer_authority *a, uint64_t id, uint64_t epoch,
                           struct peer_local_registration *out)
{
    if (!a || !out)
        return 0;

    int found = 0;
    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        const struct peer_local_registration *r = &a->registrations[i];
        if (r->state == PEER_REGISTRATION_ACTIVE && r->session_id == id && r->session_epoch == epoch) {
            *out = *r;
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return found;
}

int peer_authority_local(struct peer_authority *a, const char *uid, struct peer_local_registration *out)
{
    if (!a || !uid || !out)
        return 0;

    int found = 0;
    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        const struct peer_local_registration *r = &a->registrations[i];
        if (r->state == PEER_REGISTRATION_ACTIVE && !strcmp(r->identity.pod_uid, uid)) {
            *out = *r;
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return found;
}

void peer_authority_retire_session(struct peer_authority *a, uint64_t id, uint64_t epoch)
{
    if (!a)
        return;

    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        struct peer_local_registration *r = &a->registrations[i];
        if (r->state == PEER_REGISTRATION_ACTIVE && r->session_id == id && r->session_epoch == epoch)
        {
            r->state = PEER_REGISTRATION_RETIRING;
            atomic_store_explicit(&a->active[i], 0, memory_order_release);
        }
    }
    pthread_mutex_unlock(&a->lock);
}

static int reference(struct peer_authority *a, uint64_t id, uint64_t epoch, int retain)
{
    if (!a)
        return EINVAL;

    int error = ENOENT;
    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        struct peer_local_registration *r = &a->registrations[i];
        if (r->state == PEER_REGISTRATION_EMPTY || r->session_id != id || r->session_epoch != epoch)
            continue;

        if (retain) {
            error = r->state != PEER_REGISTRATION_ACTIVE ? ESTALE :
                r->peer_references == UINT_MAX ? EOVERFLOW : 0;
            if (!error)
                r->peer_references++;
        } else {
            error = r->peer_references ? 0 : EINVAL;
            if (!error)
                r->peer_references--;
        }
        break;
    }
    pthread_mutex_unlock(&a->lock);
    return error;
}

int peer_authority_hold(struct peer_authority *a, uint64_t id, uint64_t epoch)
{
    return reference(a, id, epoch, 1);
}

int peer_authority_unhold(struct peer_authority *a, uint64_t id, uint64_t epoch)
{
    return reference(a, id, epoch, 0);
}

void peer_authority_retire_nonce(struct peer_authority *a, const uint8_t nonce[32])
{
    if (!a || !nonce)
        return;

    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        struct peer_local_registration *r = &a->registrations[i];
        if (r->state == PEER_REGISTRATION_ACTIVE && !memcmp(r->peer.nonce, nonce, 32))
        {
            r->state = PEER_REGISTRATION_RETIRING;
            atomic_store_explicit(&a->active[i], 0, memory_order_release);
        }
    }
    pthread_mutex_unlock(&a->lock);
}

void peer_authority_retire_all(struct peer_authority *a)
{
    if (!a)
        return;

    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i)
        if (a->registrations[i].state == PEER_REGISTRATION_ACTIVE)
        {
            a->registrations[i].state = PEER_REGISTRATION_RETIRING;
            atomic_store_explicit(&a->active[i], 0, memory_order_release);
        }
    pthread_mutex_unlock(&a->lock);
}

int peer_authority_nonce_present(struct peer_authority *a, const uint8_t nonce[32])
{
    if (!a || !nonce)
        return 0;

    int present = 0;
    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        const struct peer_local_registration *r = &a->registrations[i];
        if (r->state != PEER_REGISTRATION_EMPTY && !memcmp(r->peer.nonce, nonce, 32)) {
            present = 1;
            break;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return present;
}

int peer_authority_retiring(struct peer_authority *a, unsigned *cursor, struct peer_local_registration *out)
{
    if (!a || !cursor || !out)
        return 0;

    int found = 0;
    pthread_mutex_lock(&a->lock);
    while (*cursor < a->capacity) {
        const struct peer_local_registration *r = &a->registrations[(*cursor)++];
        if (r->state == PEER_REGISTRATION_RETIRING) {
            *out = *r;
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return found;
}

int peer_authority_retired_done(struct peer_authority *a, uint64_t id, uint64_t epoch)
{
    if (!a)
        return EINVAL;

    int result = ENOENT;
    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        struct peer_local_registration *r = &a->registrations[i];
        if (r->state != PEER_REGISTRATION_EMPTY && r->session_id == id && r->session_epoch == epoch) {
            result = EBUSY;
            if (r->state == PEER_REGISTRATION_RETIRING && !r->peer_references) {
                memset(r, 0, sizeof(*r));
                result = 0;
            }
            break;
        }
    }
    pthread_mutex_unlock(&a->lock);
    return result;
}

int peer_authority_free(struct peer_authority *a)
{
    if (!a)
        return 0;

    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        if (a->registrations[i].state != PEER_REGISTRATION_EMPTY) {
            pthread_mutex_unlock(&a->lock);
            return EBUSY;
        }
    }
    struct peer_authority_view *view = a->current;
    a->current = NULL;
    pthread_mutex_unlock(&a->lock);

    peer_authority_release(view);
    pthread_mutex_destroy(&a->lock);
    free(a->registrations);
    free(a->active);
    free(a);
    return 0;
}


uint64_t peer_authority_generation(struct peer_authority *a)
{
    return a ? atomic_load_explicit(&a->generation, memory_order_acquire) : 0;
}

int peer_authority_watch(struct peer_authority *a, uint64_t id, uint64_t epoch,
                          const _Atomic unsigned **active)
{
    if (!a || !active) return EINVAL;
    int error = ESTALE;
    pthread_mutex_lock(&a->lock);
    for (unsigned i = 0; i < a->capacity; ++i) {
        struct peer_local_registration *r = &a->registrations[i];
        if (r->state != PEER_REGISTRATION_ACTIVE || r->session_id != id || r->session_epoch != epoch)
            continue;
        if (r->peer_references == UINT_MAX) { error = EOVERFLOW; break; }
        ++r->peer_references;
        *active = &a->active[i];
        error = 0;
        break;
    }
    pthread_mutex_unlock(&a->lock);
    return error;
}
