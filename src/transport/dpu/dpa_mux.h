#ifndef DMESH_DPA_MUX_H
#define DMESH_DPA_MUX_H

#include <stdint.h>
#include <doca_error.h>

/*
 * Flow-multiplexed forward DMA on the DPU. DMESH_DPA_MUX=n starts n DPA
 * threads per worker (run_dma_mux, layout in common/dpa_common.h); each takes
 * up to DMESH_MUX_RINGS of the worker's session flows. A flow's slot is driven
 * through the thread's local storage, as a per-flow thread is: the ARM fills
 * the slot and sets cmd = OPEN, and the flow runs at once; to leave, the ARM
 * sets cmd = CLOSE and waits until the slot is CLOSED and every copy's message
 * has arrived. Unset or 0 keeps one DPA thread per flow.
 *
 * Known limit (docs/2026-10-13_dpa-mux-probe.md): a copy from a host process
 * that died blocks its thread's producer for good, and with it every flow on
 * that thread. dmesh_dpa_mux_watch only reports it; the fix belongs with the
 * broker, which keeps a client's memory until the DPU has closed its flows.
 */

struct objects;
struct dmesh_conn;

/* DMESH_DPA_MUX, read once: threads per worker, 0 = off. */
int dmesh_dpa_mux_threads(void);
/* Starts the threads; call once the worker's DPA context and consumer PE exist. */
doca_error_t dmesh_dpa_mux_init(struct objects *objs);
/* Gives a flow whose buffers are mapped a ring slot and opens it. */
doca_error_t dmesh_dpa_mux_add(struct dmesh_conn *conn);
/* Asks for the close on the first call. DOCA_SUCCESS once the slot is CLOSED
 * and every copy has been accounted for, DOCA_ERROR_AGAIN while waiting,
 * DOCA_ERROR_TIME_OUT after DMESH_DPA_QUIESCE_TIMEOUT_MS. */
doca_error_t dmesh_dpa_mux_del_step(struct dmesh_conn *conn);
/* Returns the ring slot; only after dmesh_dpa_mux_del_step succeeded. */
void dmesh_dpa_mux_release(struct dmesh_conn *conn);
/* Publishes how far the reader released this flow's staging. */
doca_error_t dmesh_dpa_mux_rx_consumed(struct dmesh_conn *conn, uint32_t pos);
/* At most once a second: marks flows whose ring the DPA stopped (bad
 * descriptor, failed copy), and logs a thread whose oldest copy is older than
 * DMESH_DPA_MUX_STALL_MS (100) or that stopped running, and its recovery.
 * DMESH_DPA_MUX_STATS=1 adds one line per thread: passes, credit waits, FIFO. */
void dmesh_dpa_mux_watch(struct objects *objs);

#endif
