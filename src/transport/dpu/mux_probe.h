#ifndef DMESH_MUX_PROBE_H
#define DMESH_MUX_PROBE_H

#include <stdint.h>
#include <stdio.h>

/*
 * Pre-implementation measurements for DPA flow multiplexing: one extra DPA
 * thread on the worker's shared DPA context, next to the per-flow threads.
 * State layout and modes: common/dpa_probe.h. Recipe and expected outcomes:
 * docs/2026-10-13_dpa-mux-probe.md.
 *
 * DPU only; elsewhere start returns NULL. A test tool: the probe's thread,
 * endpoint and scratch buffer stay allocated until the process exits.
 */

struct objects;
struct dmesh_mux_probe;

struct dmesh_mux_probe_config {
    uint32_t mode;          /* MUX_PROBE_POLL or MUX_PROBE_FAULT */
    const int *slots;       /* POLL: 1..16 rings to read; FAULT: exactly 2 sources */
    int nslots;
    uint32_t passes;        /* POLL: passes per (variant, R) */
    uint32_t copy_len;      /* FAULT: bytes per copy, from offset 0 of each sndbuf */
    uint32_t gap_us;        /* FAULT: idle time between copies */
    uint32_t timeout_us;    /* FAULT: a CQE later than this marks the producer stalled */
    uint32_t pin_eu;        /* exact EU + 1 for the probe thread; 0 = the scheduler places it */
};

struct dmesh_mux_probe *dmesh_mux_probe_start(struct objects *objs, const struct dmesh_mux_probe_config *cfg);
/* Reads the DPA state back: 1 once the probe finished, 0 while it runs,
 * -1 if the read failed or the DPA context reports a device error. */
int dmesh_mux_probe_read(struct dmesh_mux_probe *p);
/* Asks the kernel to finish after its current slice. */
void dmesh_mux_probe_stop(struct dmesh_mux_probe *p);
/* POLL: the cycle table. FAULT: one line of per-source counters. */
void dmesh_mux_probe_print(const struct dmesh_mux_probe *p, FILE *out);

#endif
