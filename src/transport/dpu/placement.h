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
    /* Client flows of the placed flow's svc-to-svc pair on this worker,
     * opening ones included. The dispatcher already left eligible only the
     * workers holding the fewest; a policy may still weigh it. */
    unsigned pair_flows;
    /* Over the last flow-statistics period (DMESH_FLOWSTAT_MS): the worker's
     * client requests per second, the CPU its client connections were
     * measured to use, and its thread's busy time (both per mille of a core;
     * busy minus flow CPU is work no flow is charged for, such as backend
     * connections and the DMA driver). */
    unsigned rps, flow_cpu_permille, busy_permille;
    /* Sum over the worker's client flows of rps x instructions per request:
     * the instructions per second its flows were measured to retire (0 when
     * the proxy does not count instructions). */
    uint64_t flow_instr_ps;
    /* The same assumed for its client flows not yet measured (each at the
     * mean measured flow), and what its thread retired in all
     * (DMESH_WORKER_INSTR=1, else 0). */
    uint64_t pending_instr_ps, worker_instr_ps;
};
struct dmesh_placement_policy {
    int (*select)(const struct dmesh_flow_spec *, const struct dmesh_worker_load *,
                  size_t count, size_t cursor, void *context);
    void *context;
};
extern const struct dmesh_placement_policy dmesh_placement_least_flows;
extern const struct dmesh_placement_policy dmesh_placement_round_robin;
extern const struct dmesh_placement_policy dmesh_placement_least_instructions;
extern const struct dmesh_placement_policy dmesh_placement_first_eligible; /* test only */

#endif
