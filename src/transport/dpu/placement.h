#ifndef DMESH_PLACEMENT_H
#define DMESH_PLACEMENT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Policy sees immutable snapshots, never DOCA resources. Admission and slot
 * reservation remain the dispatcher's responsibility. All workers in a
 * dispatcher belong to the same device domain. */
struct dmesh_flow_spec {
    uint64_t session_id, session_epoch;
    uint32_t flow_id, generation, mode, service_ip;
    uint16_t service_port;
};
struct dmesh_worker_load {
    bool eligible;
    unsigned capacity, assigned, opening;
};
struct dmesh_placement_policy {
    int (*select)(const struct dmesh_flow_spec *, const struct dmesh_worker_load *,
                  size_t count, size_t cursor, void *context);
    void *context;
};
extern const struct dmesh_placement_policy dmesh_placement_least_flows;
extern const struct dmesh_placement_policy dmesh_placement_round_robin;

#endif
