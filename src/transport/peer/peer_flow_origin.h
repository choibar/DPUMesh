#ifndef DMESH_PEER_FLOW_ORIGIN_H
#define DMESH_PEER_FLOW_ORIGIN_H

#include <stdint.h>
#include "workload_limits.h"

/* Private Rust/C bridge, never Host-controlled wire metadata. */
struct peer_flow_origin {
    uint64_t session_id, session_epoch, generation;
    uint32_t channel_slot;
    char pod_uid[DMESH_POD_UID_MAX];
    char namespace_name[DMESH_K8S_NAMESPACE_MAX];
    char service_account[DMESH_K8S_NAME_MAX];
    char service_name[DMESH_SVC_NAME_MAX];
};
_Static_assert(sizeof(struct peer_flow_origin) == 480, "peer origin Rust/C ABI");

struct objects;
/* 1 verified/current; 0 legacy mode; negative errno when retired/invalid. */
int dmesh_dispatch_flow_origin(struct objects *, unsigned slot, struct peer_flow_origin *);

#endif
