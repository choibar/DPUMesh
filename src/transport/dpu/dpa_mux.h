#ifndef DMESH_DPA_MUX_H
#define DMESH_DPA_MUX_H

#include <stdint.h>
#include <doca_error.h>

/*
 * Pod-multiplexed forward DMA on the DPU. With DMESH_DPA_MUX=n each host
 * session (the process of one pod) gets one DPA thread, run_dma_mux (layout
 * in common/dpa_common.h), for all its flows on every worker, and at most n
 * such threads run in the process. Ring r of a thread is the session's flow
 * r + 1. The thread polls the session's tail table (TAILS), one cache line
 * per sixteen flows, and visits only rings with new descriptors. Its copies'
 * messages go to a consumer of each worker that owns one of its rings.
 *
 * A flow's ring is driven through the thread's local storage: the ARM fills
 * the ring and sets cmd = OPEN, and the flow runs at once; to leave, the ARM
 * sets cmd = CLOSE and waits until the ring is CLOSED and every copy's
 * message has arrived. A worker's consumer leaves with the worker's last
 * ring of that session, and the thread with the session's last ring.
 * Unset or 0 keeps one DPA thread per flow.
 *
 * A copy from a host process that died blocks its thread's producer for good
 * (docs/2026-10-13_dpa-mux-probe.md); with one thread per session that stops
 * only the dead process's own flows. dmesh_dpa_mux_watch reports it.
 */

struct objects;
struct dmesh_conn;
struct dmesh_pod_thread;

/* DMESH_DPA_MUX, read once: the most session threads at a time, 0 = off. */
int dmesh_dpa_mux_threads(void);
/* Joins the worker; call once the worker's DPA context and consumer PE exist. */
doca_error_t dmesh_dpa_mux_init(struct objects *objs);
/* Gives a flow whose buffers are mapped its ring of its session's thread
 * (started with the session's first flow) and opens it. */
doca_error_t dmesh_dpa_mux_add(struct dmesh_conn *conn);
/* Asks for the close on the first call. DOCA_SUCCESS once the ring is CLOSED
 * and every copy has been accounted for, DOCA_ERROR_AGAIN while waiting,
 * DOCA_ERROR_TIME_OUT after DMESH_DPA_QUIESCE_TIMEOUT_MS. */
doca_error_t dmesh_dpa_mux_del_step(struct dmesh_conn *conn);
/* Returns the ring; only after dmesh_dpa_mux_del_step succeeded. The
 * session's last ring stops and frees its thread. */
void dmesh_dpa_mux_release(struct dmesh_conn *conn);
/* Publishes how far the reader released this flow's staging. */
doca_error_t dmesh_dpa_mux_rx_consumed(struct dmesh_conn *conn, uint32_t pos);
/* enum dmesh_mux_ring_error once the DPA stopped this flow's ring, else 0. */
uint32_t dmesh_dpa_mux_error(struct dmesh_conn *conn);
/* Checks one session thread per call, each about once a second, from any
 * worker: marks rings the DPA stopped (bad descriptor, failed copy), and logs
 * a thread whose oldest copy is older than DMESH_DPA_MUX_STALL_MS (100) or
 * that stopped running, and its recovery. DMESH_DPA_MUX_STATS=1 adds one line
 * per check: passes, tail lines read, rings visited, copies, credit waits. */
void dmesh_dpa_mux_watch(struct objects *objs);

#endif
