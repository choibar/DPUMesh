#ifndef DMA_H_
#define DMA_H_

#include <doca_error.h>
#include <doca_buf.h>
#include <doca_dev.h>
#include <doca_mmap.h>

struct objects;
struct dmesh_conn;
struct doca_dma_task_memcpy;

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

/* Successful submit/enqueue transfers the src/dst references to the task.
 * On failure both references remain with the caller. */
doca_error_t
submit_dma_task(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst);

doca_error_t
submit_dma_task_kind(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst,
                     int kind);

/* Backend (안 2) push: DMA up to one <=8KB batch already staged at
 * tx_staging+src_pos, DPU -> host rcvbuf data ring, and publish it via a 16B
 * descriptor slot (see struct dmesh_push_desc). No host DPA, no memcpy.
 * Returns bytes accepted (0 = batch in flight, retry) or a negative
 * doca_error_t. */
int
dmesh_dma_push_staged(struct dmesh_conn *conn, uint32_t src_pos, uint32_t len);

/* End a push stream once push_fin_requested is set: after the last batch is
 * visible, publish one zero-length descriptor, which the host reads as end of
 * stream. Returns 0 (sent, or not due yet: the call is repeated from the
 * descriptor completion and the flow's advance) or a negative doca_error_t
 * when the flow no longer admits DMA. */
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
