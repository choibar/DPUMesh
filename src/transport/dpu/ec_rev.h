#ifndef DMESH_EC_REV_H
#define DMESH_EC_REV_H

#include <stdint.h>
#include <doca_error.h>

/*
 * DPU -> host push through a DPU-owned DPA thread (DMESH_D2H_EC=1). Each
 * worker owns one reverse DPA thread (device: run_ec_rev; layout and protocol
 * in common/dpa_common.h). A push flow's batches go into the thread's ring
 * instead of the flow's doca_dma engine: the DPA copies them, publishes their
 * descriptors and wakes the host through the flow slot's sync event; the ARM
 * takes no completion and sends no message. Several batches of a flow may be
 * in flight; push_seq and pushed_bytes follow the thread's completions, read
 * whenever the worker asks (dmesh_ec_reap). The host must create the slot
 * sync events (DPUMESH_D2H_EC=1 in the host library).
 */

struct objects;
struct dmesh_conn;
struct dmesh_ec_ctl;

#ifdef DOCA_ARCH_DPU

/* DMESH_D2H_EC, read once. */
int dmesh_ec_enabled(void);
/* Starts the worker's reverse DPA thread once its DPA context exists. */
doca_error_t dmesh_ec_init(struct objects *objs);
/* A push flow's metadata arrived (its session's TAILS still pending): imports
 * the tail table page and marks the flow for the reverse thread. */
doca_error_t dmesh_ec_conn_setup(struct dmesh_conn *conn);
/* DPA handles of the flow's tx_staging and rcvbuf window, taken on its first batch. */
doca_error_t dmesh_ec_conn_handles(struct dmesh_conn *conn);
/* The worker's ring and completion lines; NULL when off. */
struct dmesh_ec_ctl *dmesh_ec_ctl_of(struct objects *objs);
/* Appends one entry; DOCA_ERROR_AGAIN when the ring is full. */
doca_error_t dmesh_ec_post(struct dmesh_conn *conn, uint64_t seq, uint32_t pos, uint32_t len, uint64_t src_addr,
                           uint64_t cum_bytes);
/* Takes the thread's completions of this flow into push_seq / pushed_bytes.
 * Returns 1 when push_seq advanced. */
int dmesh_ec_reap(struct dmesh_conn *conn);
/* Teardown: DOCA_SUCCESS once every batch of the flow completed (or after one
 * second, logged), DOCA_ERROR_AGAIN before. */
doca_error_t dmesh_ec_quiesce_step(struct dmesh_conn *conn);
/* Releases the flow's import and clears its E-c state. */
void dmesh_ec_conn_reset(struct dmesh_conn *conn);

#else /* !DOCA_ARCH_DPU: host builds and unit tests, never on */

static inline int dmesh_ec_enabled(void) { return 0; }
static inline doca_error_t dmesh_ec_init(struct objects *objs) { (void)objs; return DOCA_SUCCESS; }
static inline doca_error_t dmesh_ec_conn_setup(struct dmesh_conn *conn) { (void)conn; return DOCA_SUCCESS; }
static inline doca_error_t dmesh_ec_conn_handles(struct dmesh_conn *conn) { (void)conn; return DOCA_ERROR_NOT_SUPPORTED; }
static inline struct dmesh_ec_ctl *dmesh_ec_ctl_of(struct objects *objs) { (void)objs; return NULL; }
static inline doca_error_t dmesh_ec_post(struct dmesh_conn *conn, uint64_t seq, uint32_t pos, uint32_t len,
                                         uint64_t src_addr, uint64_t cum_bytes)
{
    (void)conn; (void)seq; (void)pos; (void)len; (void)src_addr; (void)cum_bytes;
    return DOCA_ERROR_NOT_SUPPORTED;
}
static inline int dmesh_ec_reap(struct dmesh_conn *conn) { (void)conn; return 0; }
static inline doca_error_t dmesh_ec_quiesce_step(struct dmesh_conn *conn) { (void)conn; return DOCA_SUCCESS; }
static inline void dmesh_ec_conn_reset(struct dmesh_conn *conn) { (void)conn; }

#endif

#endif
