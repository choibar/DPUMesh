#ifndef DMESH_EC_REV_H
#define DMESH_EC_REV_H

#include <stdint.h>
#include <doca_error.h>

/*
 * DPU -> host through a DPU-owned DPA thread. Each worker owns one reverse
 * DPA thread (device: run_ec_rev; layout and protocol in common/dpa_common.h)
 * and each of its connection slots one queue of it. A flow's batches go into
 * the thread's ring: the DPA copies them on the flow's queue, publishes their
 * descriptors and wakes the host through the flow slot's sync event; the ARM
 * takes no completion, sends no message and does not read the host: the
 * thread waits for the host's room itself. Several batches of a flow may be
 * in flight; push_seq and pushed_bytes follow the thread's completions, read
 * whenever the worker asks (dmesh_ec_reap). A flow that ends with batches the
 * thread still holds leaves its queue behind: the slot's next flow gets a new
 * one.
 */

struct objects;
struct dmesh_conn;
struct dmesh_ec_ctl;

/* DMESH_EC_EXTERN: a host unit test supplies these instead of the stubs. */
#if defined(DOCA_ARCH_DPU) || defined(DMESH_EC_EXTERN)

/* Starts the worker's reverse DPA thread once its DPA context exists. */
doca_error_t dmesh_ec_init(struct objects *objs);
/* A flow's metadata arrived (its session's TAILS still pending): imports the
 * tail table page, makes sure the slot has a queue and marks the flow for the
 * reverse thread. */
doca_error_t dmesh_ec_conn_setup(struct dmesh_conn *conn);
/* The flow's line for the thread (its queue, the DPA handles of its tx_staging
 * and rcvbuf window, the data ring bytes it places batches in and its largest
 * batch), written on its first batch. */
doca_error_t dmesh_ec_conn_handles(struct dmesh_conn *conn, uint32_t ring, uint32_t max_batch);
/* The worker's ring and completion lines; NULL when off. */
struct dmesh_ec_ctl *dmesh_ec_ctl_of(struct objects *objs);
/* Appends one entry; DOCA_ERROR_AGAIN when the ring is full. */
doca_error_t dmesh_ec_post(struct dmesh_conn *conn, uint64_t seq, uint32_t pos, uint32_t len, uint64_t src_addr,
                           uint64_t cum_bytes);
/* Takes the thread's completions of this flow into push_seq / pushed_bytes.
 * Returns 1 when push_seq advanced, -1 when a copy of the flow failed. */
int dmesh_ec_reap(struct dmesh_conn *conn);
/* Teardown: DOCA_SUCCESS once every batch of the flow completed (or after one
 * second, logged), DOCA_ERROR_AGAIN before. */
doca_error_t dmesh_ec_quiesce_step(struct dmesh_conn *conn);
/* Releases the flow's import and clears its reverse state; replaces the slot's
 * queue when the flow ended with batches the thread holds. Runs before the
 * flow's tx_staging and rcvbuf mmaps go. */
void dmesh_ec_conn_reset(struct dmesh_conn *conn);

#else /* !DOCA_ARCH_DPU: host builds and unit tests, no DPA */

static inline doca_error_t dmesh_ec_init(struct objects *objs) { (void)objs; return DOCA_SUCCESS; }
static inline doca_error_t dmesh_ec_conn_setup(struct dmesh_conn *conn) { (void)conn; return DOCA_SUCCESS; }
static inline doca_error_t dmesh_ec_conn_handles(struct dmesh_conn *conn, uint32_t ring, uint32_t max_batch)
{
    (void)conn; (void)ring; (void)max_batch;
    return DOCA_ERROR_NOT_SUPPORTED;
}
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
