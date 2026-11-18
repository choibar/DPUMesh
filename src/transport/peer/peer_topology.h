#ifndef DMESH_TOPOLOGY_GEN_H
#define DMESH_TOPOLOGY_GEN_H

#include <stddef.h>
#include <stdint.h>

#include "peer_config.h"

struct dmesh_gen_node {
    char name[DMESH_K8S_NAME_MAX];
    uint32_t rdma_ip_be;
    uint16_t rdma_port;


    uint8_t dpu_static_public_key[32];
};

struct dmesh_gen_service {
    char key[DMESH_K8S_NAMESPACE_MAX + DMESH_SVC_NAME_MAX]; /* "namespace/name" */
    uint32_t cluster_ip_be;
    uint16_t port;
    uint8_t is_protected;
    /* Node-local interned id, stable across adoptions while the Service
     * persists; -1 once the 128-id space is exhausted. */
    int16_t interned;
    uint32_t endpoint_first;      /* into tables->endpoints, grouped per service */
    uint32_t endpoint_count;
};

struct dmesh_gen_pod {
    char uid[DMESH_POD_UID_MAX];  /* RFC 4122 text, 36 used */
    char node_name[DMESH_K8S_NAME_MAX];
    char namespace_name[DMESH_K8S_NAMESPACE_MAX];
    char service_account[DMESH_K8S_NAME_MAX];
    uint32_t ip_be;
};

struct dmesh_gen_endpoint {
    uint32_t service;             /* index into tables->services */
    uint32_t pod;                 /* index into tables->pods */
};

/* One adopted generation, heap-allocated whole and swapped on success, exactly
 * during adoption. pods are sorted by uid and services by
 * key, so lookups are bsearch. */
struct dmesh_topology_tables {
    uint64_t version;
    struct dmesh_gen_node *nodes;
    size_t node_count;
    struct dmesh_gen_service *services;
    size_t service_count;
    struct dmesh_gen_pod *pods;
    size_t pod_count;
    struct dmesh_gen_endpoint *endpoints;
    size_t endpoint_count;
};

struct dmesh_topology_intern {
    char key[DMESH_K8S_NAMESPACE_MAX + DMESH_SVC_NAME_MAX];
    uint8_t in_use;
};

enum dmesh_topology_result {
    DMESH_TOPOLOGY_UNCHANGED = 0,
    /* Read, verified and parsed, and the version equals the held one — the
     * countable "unchanged" adoption outcome, distinct from a file the stamp
     * says was never touched. */
    DMESH_TOPOLOGY_SAME_VERSION,
    DMESH_TOPOLOGY_ADOPTED,
    DMESH_TOPOLOGY_UNREADABLE,
    DMESH_TOPOLOGY_MALFORMED,
    DMESH_TOPOLOGY_ROLLBACK,
    DMESH_TOPOLOGY_OVERFLOW,
    DMESH_TOPOLOGY_UNSIGNED,
    DMESH_TOPOLOGY_BAD_KEY_ID,
    DMESH_TOPOLOGY_BAD_SIG,
};

const char *dmesh_topology_result_name(enum dmesh_topology_result result);

/* Parse one verified document body into freshly allocated tables. Exposed for
 * unit tests; `interned` carries the stable-id state across adoptions. */
enum dmesh_topology_result
dmesh_topology_parse(const char *document, size_t length,
                     struct dmesh_topology_intern *interned,
                     struct dmesh_topology_tables **out);

void dmesh_topology_tables_free(struct dmesh_topology_tables *tables);

#endif /* DMESH_TOPOLOGY_GEN_H */
