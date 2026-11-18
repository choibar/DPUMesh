#ifndef DMA_H_
#define DMA_H_

#include <doca_error.h>
#include <doca_buf.h>
#include <doca_dev.h>
#include <doca_mmap.h>

#include <stdbool.h>

struct objects;
struct dmesh_conn;
struct doca_dma_task_memcpy;
struct dmesh_doca_dpa_thread;

/* Source bytes may be reused only up to this successful completion prefix. */
doca_error_t dmesh_dma_tx_completed(struct dmesh_conn *conn, uint64_t *bytes);

/* Size of the local DMA buffer allocated on the DPU for PCI export. */
#define BUFFER_SIZE (1024 * 1024)

/* DMA memcpy tasks allocated for each connection's private DMA engine */
#define DMA_TASKS_PER_CONN 2048

doca_error_t
alloc_buffer_and_set_mmap(struct doca_mmap **mmap, struct doca_dev *dev,
                        void **buffer, size_t buffer_size, uint32_t access_mask);

doca_error_t
init_dma_resources(struct objects *objs);

/* Create this connection's private DMA engine: doca_dma ctx (own QP, connected
 * to the shared consumer PE), buf inventory and task pool. */
doca_error_t
init_dma_tasks(struct dmesh_conn *conn, int num_tasks);

/* Permanently stop admission and drain this flow's CPU DMA tasks. Only
 * unsubmitted/completed tasks are freed; remaining resources survive an error
 * and the caller must retry before releasing any mapped backing memory.
 * Serialized with submit/progress by the owning worker; safe on NULL. */
doca_error_t
cleanup_dma_tasks(struct dmesh_conn *conn);

/* DMESH_DPA_REUSE=1: a closing flow leaves its idle DMA engine (ctx, task
 * pool, inventory) on its DPA pool slot instead of destroying it, and
 * init_dma_tasks hands it to the slot's next flow. Returns false (nothing
 * moved) when it must be cleaned up instead. */
bool dmesh_dma_engine_keep(struct dmesh_conn *conn);
/* Creates an idle DMA engine for a pool slot before any flow. */
doca_error_t dmesh_dma_engine_prewarm(struct objects *objs, struct dmesh_doca_dpa_thread *thread);

/* Successful submit/enqueue transfers the src/dst references to the task.
 * On failure both references remain with the caller. */
doca_error_t
submit_dma_task(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst);

/* Largest batch the reverse DPA thread copies at once. */
#define DMESH_PUSH_MAX_BATCH 65536u

/* DPU -> host: hand up to one batch already staged at tx_staging+src_pos to
 * the worker's reverse DPA thread, which copies it into the host's data ring
 * and publishes its descriptor (struct dmesh_push_desc). Several batches of
 * a flow may be in flight; the thread starts each once the host has room for
 * it. Returns bytes accepted (0 = the thread holds as many of the flow's
 * batches as it can, or its ring is full: retry) or a negative doca_error_t. */
int
dmesh_dma_push_staged(struct dmesh_conn *conn, uint32_t src_pos, uint32_t len);

/* End a push stream once push_fin_requested is set: a zero-length descriptor
 * after the last batch, which the host reads as end of stream. Returns 0
 * (posted, or not due yet: the flow's advance repeats the call) or a negative
 * doca_error_t when the flow no longer admits DMA. */
int
dmesh_dma_push_fin(struct dmesh_conn *conn);

doca_error_t
enqueue_dma_task(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst);

/* Submit up to max_tasks queued tasks. A max_tasks value of zero processes the entire queue. */
doca_error_t
progress_dma_submission_queue(struct dmesh_conn *conn, int max_tasks, int *num_submitted);

struct doca_dma_task_memcpy *
get_free_dma_task(struct dmesh_conn *conn);

doca_error_t
put_free_dma_task(struct dmesh_conn *conn, struct doca_dma_task_memcpy *dma_task);

struct doca_dma_task_memcpy *
get_submission_dma_task(struct dmesh_conn *conn);

doca_error_t
put_submission_dma_task(struct dmesh_conn *conn, struct doca_dma_task_memcpy *dma_task);

int
get_num_free_dma_tasks(const struct dmesh_conn *conn);

int
get_num_submission_dma_tasks(const struct dmesh_conn *conn);

doca_error_t
send_dma_request_to_dpa(struct dmesh_conn *conn);

/* Submit the second-stage copy (connection staging buffer -> rcvbuf) as a DMA
 * task. Returns DOCA_ERROR_AGAIN when all DMA tasks are in flight (nothing is
 * leaked); other errors are non-retryable. */
doca_error_t
dmesh_dma_copy_to_rcvbuf(struct dmesh_conn *conn, uint32_t pos, uint32_t length);

/* Queue a copy that found no free DMA task on this connection; retried as
 * tasks complete. Drops (counted) if the connection's pending ring is full. */
void
dmesh_dma_defer_copy(struct dmesh_conn *conn, uint32_t pos, uint32_t length);

/* Submit as many of this connection's deferred copies as task capacity allows */
void
dmesh_dma_pending_drain(struct dmesh_conn *conn);
#endif  /* DMA_H_ */
