#include "dma.h"

#include <stdlib.h>

#include <doca_log.h>
#include <doca_buf.h>
#include <doca_dma.h>
#include <doca_mmap.h>
#include <doca_error.h>
#include <errno.h>
#include <arpa/inet.h>

#include "dpa_common.h"
#include "object.h"
#include "common.h"
#include "dpa.h"
#include "buffer.h"
#include "ring.h"

DOCA_LOG_REGISTER(DMA);

/* backend push chaining stages (defined below, called from the completion cb) */
static void dmesh_dma_push_submit_desc(struct dmesh_conn *conn);
static void dmesh_dma_push_desc_done(struct dmesh_conn *conn);

#ifndef DMESH_DMA_STOP_MAX_POLLS
#define DMESH_DMA_STOP_MAX_POLLS 100000u
#endif

static bool dma_admission_closed(const struct dmesh_conn *conn)
{
    return conn == NULL || conn->dma_closing || conn->state == DMESH_CONN_CLOSING;
}

/* Tasks do not own an extra doca_buf reference. Clear each attachment before
 * returning its reference; restore a failed release so cleanup can retry. */
static doca_error_t dma_release_task_buffers(struct doca_dma_task_memcpy *task)
{
    const struct doca_buf *src = doca_dma_task_memcpy_get_src(task);
    struct doca_buf *dst = doca_dma_task_memcpy_get_dst(task);
    doca_error_t result;
    if (src != NULL) {
        doca_dma_task_memcpy_set_src(task, NULL);
        result = doca_buf_dec_refcount((struct doca_buf *)src, NULL);
        if (result != DOCA_SUCCESS) {
            doca_dma_task_memcpy_set_src(task, src);
            return result;
        }
    }
    if (dst != NULL) {
        doca_dma_task_memcpy_set_dst(task, NULL);
        result = doca_buf_dec_refcount(dst, NULL);
        if (result != DOCA_SUCCESS) {
            doca_dma_task_memcpy_set_dst(task, dst);
            return result;
        }
    }
    return DOCA_SUCCESS;
}

doca_error_t
init_dma_resources(struct objects *objs)
{
    doca_error_t result;

    result = alloc_buffer_and_set_mmap(&objs->local_mmap, objs->dev,
                                   &objs->dma_buffer, 1024 * 1024, 
                                   DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DMA mmap and buffer - %s",
                doca_error_get_name(result));
        return result;
    }

    /* wait for remote mmap info from the host */
    while (objs->remote_mmap == NULL) {
        doca_pe_progress(objs->pe);
    }

    DOCA_LOG_INFO("Remote mmap is ready for DMA operations");
    return DOCA_SUCCESS;    
}

doca_error_t
send_dma_request_to_dpa(struct dmesh_conn *conn)
{
#ifndef DOCA_ARCH_DPU
    (void)conn;
    DOCA_LOG_ERR("Sending a DMA request to DPA is only supported on DPU");
    return DOCA_ERROR_NOT_SUPPORTED;
#else
    struct objects *objs = conn->objs;
    doca_error_t result;
    doca_dpa_dev_mmap_t src_mmap, dst_mmap;
    struct comch_dma_req_msg dma_req_msg;

    if (conn->sndbuf.mmap == NULL || conn->rcvbuf.mmap == NULL) {
        DOCA_LOG_ERR("Remote buffers are not ready for this connection");
        return DOCA_ERROR_BAD_STATE;
    }

    result = doca_mmap_dev_get_dpa_handle(conn->sndbuf.mmap, objs->dev, &src_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get local mmap DPA handle: %s",
                     doca_error_get_descr(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(conn->local_mmap, objs->dev, &dst_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get remote mmap DPA handle: %s",
                     doca_error_get_descr(result));
        return result;
    }

    dma_req_msg.type = COMCH_MSG_TYPE_DMA_REQ;
    dma_req_msg.dpa_producer = objs->remote_dpa_producer;
    dma_req_msg.dpa_producer_comp = objs->remote_dpa_producer_comp;
    dma_req_msg.src_mmap = src_mmap;
    dma_req_msg.dst_mmap = dst_mmap;
    dma_req_msg.src_addr = (uint64_t)conn->sndbuf.buf;
    dma_req_msg.dst_addr = (uint64_t)conn->dma_buffer;
    dma_req_msg.length = 1024;
    DOCA_LOG_INFO("Sending DMA request to DPA: producer: 0x%lx, src_mmap=%u, dst_mmap=%u, src_addr=0x%lx, dst_addr=0x%lx, length=%u",
                    dma_req_msg.dpa_producer,          
                    dma_req_msg.src_mmap,
                  dma_req_msg.dst_mmap,
                  dma_req_msg.src_addr,
                  dma_req_msg.dst_addr,
                  dma_req_msg.length);

    result = dmesh_doca_dpa_msgq_send(&conn->dpa_comch->send,
                              &dma_req_msg,
                              sizeof(dma_req_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send DMA request to DPA: %s",
                     doca_error_get_descr(result));
        return result;
    }

    DOCA_LOG_INFO("DMA request sent to DPA successfully");
    return DOCA_SUCCESS;
#endif
}

doca_error_t
dmesh_dma_copy_to_rcvbuf(struct dmesh_conn *conn, uint32_t pos, uint32_t length)
{
    struct doca_buf *sbuf = NULL, *dbuf = NULL;
    doca_error_t result;

    if (dma_admission_closed(conn))
        return DOCA_ERROR_BAD_STATE;
    if (get_num_free_dma_tasks(conn) == 0)
        return DOCA_ERROR_AGAIN;

    /* For src doca buffer, data len must be specified */
    result = doca_buf_inventory_buf_get_by_data(conn->buf_inv,
                                                conn->local_mmap,
                                                conn->dma_buffer + pos,
                                                length,
                                                &sbuf);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get src buffer from inventory: %s", doca_error_get_descr(result));
        return result;
    }

    result = doca_buf_inventory_buf_get_by_addr(conn->buf_inv,
                                                conn->rcvbuf.mmap,
                                                conn->rcvbuf.buf,
                                                length,
                                                &dbuf);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get dst buffer from inventory: %s", doca_error_get_descr(result));
        (void)doca_buf_dec_refcount(sbuf, NULL);
        return result;
    }

    result = submit_dma_task(conn, sbuf, dbuf);
    if (result != DOCA_SUCCESS) {
        /* A failed submission leaves both references with this caller. */
        (void)doca_buf_dec_refcount(dbuf, NULL);
        (void)doca_buf_dec_refcount(sbuf, NULL);
    }
    return result;
}

void
dmesh_dma_defer_copy(struct dmesh_conn *conn, uint32_t pos, uint32_t length)
{
    int tail;

    if (dma_admission_closed(conn))
        return;
    if (conn->dma_pending == NULL || conn->dma_pending_cnt >= DMA_PENDING_MAX) {
        /* Sustained overload: inflow exceeds this connection's DMA throughput.
         * Counted silently; the throughput report surfaces pending/dropped. */
        conn->dma_dropped_copies++;
        return;
    }

    tail = (conn->dma_pending_head + conn->dma_pending_cnt) % DMA_PENDING_MAX;
    conn->dma_pending[tail].pos = pos;
    conn->dma_pending[tail].length = length;
    conn->dma_pending_cnt++;
}

void
dmesh_dma_pending_drain(struct dmesh_conn *conn)
{
    struct dma_pending_copy *p;
    doca_error_t result;

    if (conn == NULL)
        return;
    if (dma_admission_closed(conn) || conn->state != DMESH_CONN_RUNNING) {
        /* connection went away; discard its deferred copies */
        conn->dma_pending_cnt = 0;
        return;
    }

    while (conn->dma_pending_cnt > 0) {
        p = &conn->dma_pending[conn->dma_pending_head];

        result = dmesh_dma_copy_to_rcvbuf(conn, p->pos, p->length);
        if (result == DOCA_ERROR_AGAIN)
            break; /* still no capacity; retry on the next completion */

        /* submitted (or non-retryable failure: drop) */
        conn->dma_pending_head = (conn->dma_pending_head + 1) % DMA_PENDING_MAX;
        conn->dma_pending_cnt--;
    }
}

/*
 * DMA Memcpy task completed callback
 *
 * @dma_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dmesh_doca_dpa_dma_task_completed_cb(struct doca_dma_task_memcpy *dma_task,
					  union doca_data task_user_data,
					  union doca_data ctx_user_data)
{
    struct dmesh_conn *conn = (struct dmesh_conn *)ctx_user_data.ptr;
    struct dma_task_entry *entry = (struct dma_task_entry *)task_user_data.ptr;
    int kind = entry->kind;
    doca_error_t result;

    entry->in_flight = false;
    entry->result = DOCA_SUCCESS;
    result = put_free_dma_task(conn, dma_task);
    if (result != DOCA_SUCCESS)
        DOCA_LOG_ERR("Failed to return completed DMA task to queue: %s",
                     doca_error_get_descr(result));

    if (result != DOCA_SUCCESS) {
        conn->dma_closing = true;
        if (conn->state != DMESH_CONN_CLOSING) {
            conn->state = DMESH_CONN_ERROR;
            conn->error_status = EIO;
        }
    }
    if (dma_admission_closed(conn)) {
        conn->push_state = 0;
        conn->cursor_state = 0;
        conn->dma_pending_cnt = 0;
        return;
    }

    /* Backend push chaining (task submission is allowed inside a completion
     * callback): data landed -> publish its descriptor; descriptor landed ->
     * the batch is visible, accept the next one. */
    if (kind == DMESH_TASK_PUSH_DATA) {
        dmesh_dma_push_submit_desc(conn);
        return;
    }
    if (kind == DMESH_TASK_PUSH_DESC) {
        dmesh_dma_push_desc_done(conn);
        return;
    }
    if (kind == DMESH_TASK_PULL_CURSOR) {
        static int probed;

        conn->cursor_state = 0;
        if (probed++ < 3)
            DOCA_LOG_INFO("cursor probe: magic=%lx seq=%lu bytes=%lu",
                          conn->cursor_shadow != NULL ? conn->cursor_shadow->magic : 0,
                          conn->cursor_shadow != NULL ? conn->cursor_shadow->consumed_seq : 0,
                          conn->cursor_shadow != NULL ? conn->cursor_shadow->consumed_bytes : 0);
        if (conn->cursor_shadow != NULL &&
            conn->cursor_shadow->magic == DMESH_PUSH_FC_MAGIC) {
            if (!conn->host_fc) {
                conn->host_fc = 1;
                DOCA_LOG_INFO("push flow control enabled (host cursor live)");
            }
            conn->host_consumed_seq = conn->cursor_shadow->consumed_seq;
            conn->host_consumed_bytes = conn->cursor_shadow->consumed_bytes;
        }
        return;
    }

    /* a task (and its bufs) just freed up: run this connection's deferred
     * copies. Task submission is allowed inside a completion callback. */
    dmesh_dma_pending_drain(conn);
}

/*
 * Memcpy task error callback
 *
 * @dma_task [in]: failed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dmesh_doca_dpa_dma_task_error_cb(struct doca_dma_task_memcpy *dma_task,
				      union doca_data task_user_data,
				      union doca_data ctx_user_data)
{
    struct dmesh_conn *conn = (struct dmesh_conn *)ctx_user_data.ptr;
	struct doca_task *task = doca_dma_task_memcpy_as_task(dma_task);
    struct dma_task_entry *entry = (struct dma_task_entry *)task_user_data.ptr;
    int kind = entry->kind;
    doca_error_t result;

    entry->in_flight = false;
    entry->result = doca_task_get_status(task);
    result = put_free_dma_task(conn, dma_task);
    if (result != DOCA_SUCCESS)
        DOCA_LOG_ERR("Failed to return errored DMA task to queue: %s",
                     doca_error_get_descr(result));

    /* A flushed task has returned from the device, but other tasks may still
     * be submitted. Only the checked cleanup loop may free the whole pool. */
    if (!dma_admission_closed(conn)) {
        DOCA_LOG_ERR("DMA task failed: %s", doca_error_get_descr(entry->result));
        conn->state = DMESH_CONN_ERROR;
        conn->error_status = EIO;
    }
    conn->dma_closing = true;
    conn->dma_pending_cnt = 0;
    if (kind == DMESH_TASK_PUSH_DATA || kind == DMESH_TASK_PUSH_DESC)
        conn->push_state = 0;
    if (kind == DMESH_TASK_PULL_CURSOR)
        conn->cursor_state = 0;

}

/**
 * Callback triggered whenever DMA context state changes
 *
 * @user_data [in]: User data associated with the DMA context. Will hold struct dma_resources *
 * @ctx [in]: The DMA context that had a state change
 * @prev_state [in]: Previous context state
 * @next_state [in]: Next context state (context is already in this state when the callback is called)
 */
static void dma_state_changed_cb(const union doca_data user_data,
				       struct doca_ctx *ctx,
				       enum doca_ctx_states prev_state,
				       enum doca_ctx_states next_state)
{
	(void)user_data;
	(void)ctx;
	(void)prev_state;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		DOCA_LOG_INFO("DMA context has been stopped");
		/* We can stop progressing the PE */
		break;
	case DOCA_CTX_STATE_STARTING:
		/**
		 * The context is in starting state, this is unexpected for DMA.
		 */
		DOCA_LOG_ERR("DMA context entered into starting state. Unexpected transition");
		break;
	case DOCA_CTX_STATE_RUNNING:
		DOCA_LOG_INFO("DMA context is running");
		break;
	case DOCA_CTX_STATE_STOPPING:
		/**
		 * doca_ctx_stop() has been called.
		 * In this sample, this happens either due to a failure encountered, in which case doca_pe_progress()
		 * will cause any inflight task to be flushed, or due to the successful compilation of the sample flow.
		 * In both cases, in this sample, doca_pe_progress() will eventually transition the context to idle
		 * state.
		 */
		DOCA_LOG_INFO("DMA context entered into stopping state. Any inflight tasks will be flushed");
		break;
	default:
		break;
	}
}

doca_error_t
init_dma_tasks(struct dmesh_conn *conn, int num_tasks)
{
    struct objects *objs = conn->objs;
    uint32_t max_buf_list_len, max_tasks;
    union doca_data ctx_user_data = {0};
    struct doca_ctx *dma_ctx;
    doca_error_t result;
    int i = 0;
    uint64_t max_buf_size;

    if (num_tasks <= 0) {
        DOCA_LOG_ERR("Requested number of DMA tasks must be greater than zero");
        return DOCA_ERROR_INVALID_VALUE;
    }

    /* Each in-flight DMA task holds two bufs (src + dst); size the inventory so
     * the free-task queue, not the inventory, is what limits in-flight work. */
    result = doca_buf_inventory_create(2 * (size_t)num_tasks + 16, &conn->buf_inv);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create buffer inventory: %s", doca_error_get_descr(result));
        return result;
    }

    /* Start the buffer inventory */
    result = doca_buf_inventory_start(conn->buf_inv);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start buffer inventory: %s", doca_error_get_descr(result));
        return result;
    }

    /* Get the maximum buffer list length for DMA memcpy tasks */
    result = doca_dma_cap_task_memcpy_get_max_buf_list_len(doca_dev_as_devinfo(objs->dev), &max_buf_list_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max buffer list length for DMA memcpy task: %s",
                     doca_error_get_descr(result));
        return result;
    }

    result = doca_dma_cap_task_memcpy_get_max_buf_size(doca_dev_as_devinfo(objs->dev), &max_buf_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max buffer size for DMA memcpy task: %s",
                     doca_error_get_descr(result));
        return result;
    }
    DOCA_LOG_INFO("DOCA DMA memcpy task max buffer list length: %u, max buffer size: %lu", max_buf_list_len, (unsigned long)max_buf_size);

    /* Create the DOCA DMA context */
    result = doca_dma_create(objs->dev, &conn->dma_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DOCA DMA context: %s",
                     doca_error_get_descr(result));
        return result;
    }

    /* Get the maximum number of DMA tasks */
    result = doca_dma_cap_get_max_num_tasks(conn->dma_ctx, &max_tasks);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max number of DMA tasks: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }
    if (num_tasks > (int)max_tasks) {
        DOCA_LOG_ERR("Requested number of DMA tasks [%d] exceed the limitation [%u]",
                     num_tasks, max_tasks);
        result = DOCA_ERROR_INVALID_VALUE;
        goto destroy_dma;
    }
    if (num_tasks <= 0) {
        DOCA_LOG_ERR("Requested number of DMA tasks must be greater than zero");
        result = DOCA_ERROR_INVALID_VALUE;
        goto destroy_dma;
    }
    conn->num_dma_tasks = num_tasks;
    DOCA_LOG_INFO("DOCA DMA max number of tasks: %u", max_tasks);

    dma_ctx = doca_dma_as_ctx(conn->dma_ctx);

    /* Set the state changed callback for the DMA context */
    result = doca_ctx_set_state_changed_cb(dma_ctx, dma_state_changed_cb);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DMA context state changed callback: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Set callback functions for DMA completion/error */
    result = doca_dma_task_memcpy_set_conf(conn->dma_ctx,
                                           dmesh_doca_dpa_dma_task_completed_cb,
                                           dmesh_doca_dpa_dma_task_error_cb,
                                           num_tasks);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DMA memcpy task configuration: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Set the user data for the DMA context */
    ctx_user_data.ptr = conn;
    doca_ctx_set_user_data(dma_ctx, ctx_user_data);

    /* DMA completions are data-path work: connect to the consumer PE, which the
     * steady-state datapath loop progresses. The control PE is not progressed
     * after init in the event-driven worker, so completions (and the buf
     * inventory returns they trigger) would never run there. */
    result = doca_pe_connect_ctx(objs->consumer_pe, dma_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to connect PE to DMA context: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Start the DMA context */
    result = doca_ctx_start(dma_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DMA context: %s",
                     doca_error_get_descr(result));
        goto destroy_dma;
    }

    /* Deferred-copy ring for this connection */
    conn->dma_pending = calloc(DMA_PENDING_MAX, sizeof(*conn->dma_pending));
    if (conn->dma_pending == NULL) {
        result = DOCA_ERROR_NO_MEMORY;
        goto destroy_dma;
    }
    conn->dma_pending_head = 0;
    conn->dma_pending_cnt = 0;

    /* Completed-recv segment ring for zero-copy delivery to the Rust side */
    conn->recv_segs = calloc(DMESH_RECV_SEG_MAX, sizeof(*conn->recv_segs));
    if (conn->recv_segs == NULL) {
        result = DOCA_ERROR_NO_MEMORY;
        goto destroy_dma;
    }
    conn->recv_seg_head = 0;
    conn->recv_seg_cnt = 0;

    /* Allocate DMA tasks and put all of them in the free-task queue. */
    conn->dma_task_entries = calloc(num_tasks, sizeof(*conn->dma_task_entries));
    if (conn->dma_task_entries == NULL) {
        result = DOCA_ERROR_NO_MEMORY;
        goto destroy_dma;
    }

    TAILQ_INIT(&conn->free_dma_tasks);
    TAILQ_INIT(&conn->submission_dma_tasks);
    conn->num_free_dma_tasks = 0;
    conn->num_submission_dma_tasks = 0;
    for (i = 0; i < num_tasks; ++i) {
        struct dma_task_entry *entry = &conn->dma_task_entries[i];

        result = doca_dma_task_memcpy_alloc_init(conn->dma_ctx,
                                     NULL, /* src buf will be set when submitting the task */
                                     NULL, /* dst buf will be set when submitting the task */
                                     (union doca_data){.ptr = entry},
                                     &entry->task);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to allocate and initialize DMA memcpy task: %s",
                         doca_error_get_descr(result));
            goto free_tasks;
        }

        entry->owner = conn;
        entry->result = DOCA_SUCCESS;
        result = put_free_dma_task(conn, entry->task);
        if (result != DOCA_SUCCESS) {
            doca_task_free(doca_dma_task_memcpy_as_task(entry->task));
            entry->task = NULL;
            goto free_tasks;
        }
    }

    return DOCA_SUCCESS;

free_tasks:
    while (i-- > 0)
        doca_task_free(doca_dma_task_memcpy_as_task(conn->dma_task_entries[i].task));
    free(conn->dma_task_entries);
    conn->dma_task_entries = NULL;
    conn->num_dma_tasks = 0;
    conn->num_free_dma_tasks = 0;
    conn->num_submission_dma_tasks = 0;
destroy_dma:
    cleanup_dma_tasks(conn);
    return result;
}

doca_error_t
submit_dma_task_kind(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst,
                     int kind)
{
    struct doca_dma_task_memcpy *dma_task;
    struct dma_task_entry *entry;
    doca_error_t result;

    if (conn == NULL || src == NULL || dst == NULL)
        return DOCA_ERROR_INVALID_VALUE;
    if (dma_admission_closed(conn))
        return DOCA_ERROR_BAD_STATE;

    dma_task = get_free_dma_task(conn);
    if (dma_task == NULL)
        return DOCA_ERROR_AGAIN;

    entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;

    doca_dma_task_memcpy_set_src(dma_task, src);
    doca_dma_task_memcpy_set_dst(dma_task, dst);
    entry->result = DOCA_ERROR_IN_PROGRESS;
    entry->kind = kind;

    result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_task));
    if (result == DOCA_SUCCESS) {
        entry->in_flight = true;
    } else {
        entry->result = result;
        /* Submission did not transfer ownership: let the caller release or
         * retry its buffers, including SDK EAGAIN (not just an empty pool). */
        doca_dma_task_memcpy_set_src(dma_task, NULL);
        doca_dma_task_memcpy_set_dst(dma_task, NULL);
        if (put_free_dma_task(conn, dma_task) != DOCA_SUCCESS)
            DOCA_LOG_ERR("Failed to return unsubmitted DMA task to queue");
    }

    return result;
}

doca_error_t
submit_dma_task(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst)
{
    return submit_dma_task_kind(conn, src, dst, DMESH_TASK_NORMAL);
}

/* Backend (안 2) push: DPU -> host with this connection's doca_dma engine, no
 * host DPA. The batch is ALREADY staged at tx_staging+src_pos (the Rust writer
 * put it there); this DMAs up to <=8KB of it into the host's data ring
 * (rcvbuf + DMESH_PUSH_DATA_OFF) and - from that copy's completion callback -
 * DMAs a 16B dmesh_push_desc into slot seq % N. The host busy-polls the slots
 * in order. One batch outstanding at a time. Returns bytes accepted (0 while a
 * batch is in flight - caller retries), or a negative doca_error_t. */
/* Kick a read-DMA pulling the host's consumption cursor into the DPU-local
 * shadow (tx_staging reserved tail). Fire-and-forget; the completion callback
 * copies it into conn->host_consumed_*. Safe to call any time - no-ops while
 * a pull is already in flight or before the reverse path is exported. */
static void
dmesh_dma_pull_cursor(struct dmesh_conn *conn)
{
    struct doca_buf *sbuf = NULL, *dbuf = NULL;
    doca_error_t result;

    if (dma_admission_closed(conn) || conn->cursor_state != 0 || conn->tx_staging == NULL ||
        conn->rcvbuf.mmap == NULL || conn->rcvbuf.buf == NULL)
        return;
    conn->cursor_shadow = (struct dmesh_push_cursor *)((uint8_t *)conn->tx_staging +
                                                       conn->tx_staging_len - 48);
    result = doca_buf_inventory_buf_get_by_data(conn->buf_inv, conn->rcvbuf.mmap,
                                                (uint8_t *)conn->rcvbuf.buf +
                                                    DMESH_PUSH_CURSOR_OFF,
                                                sizeof(struct dmesh_push_cursor), &sbuf);
    if (result != DOCA_SUCCESS)
        return;
    result = doca_buf_inventory_buf_get_by_addr(conn->buf_inv, conn->tx_staging_mmap,
                                                conn->cursor_shadow,
                                                sizeof(struct dmesh_push_cursor), &dbuf);
    if (result != DOCA_SUCCESS) {
        (void)doca_buf_dec_refcount(sbuf, NULL);
        return;
    }
    result = submit_dma_task_kind(conn, sbuf, dbuf, DMESH_TASK_PULL_CURSOR);
    if (result == DOCA_SUCCESS)
        conn->cursor_state = 1;
    else {
        static int logged;

        if (logged++ < 3)
            DOCA_LOG_WARN("cursor pull submit failed: %s", doca_error_get_name(result));
        (void)doca_buf_dec_refcount(dbuf, NULL);
        (void)doca_buf_dec_refcount(sbuf, NULL);
    }
}

doca_error_t dmesh_dma_tx_completed(struct dmesh_conn *conn, uint64_t *bytes)
{
    if (!conn || !bytes) return DOCA_ERROR_INVALID_VALUE;
    if (conn->state != DMESH_CONN_RUNNING || dma_admission_closed(conn))
        return DOCA_ERROR_BAD_STATE;
    if (DMESH_FLOW_USES_PUSH(conn->flow.mode)) {
        /* This prefix advances only after both data and descriptor DMA. */
        *bytes = conn->pushed_bytes;
        return DOCA_SUCCESS;
    }
    if (!conn->rcv_ring) return DOCA_ERROR_BAD_STATE;
    if (__atomic_load_n(&conn->rcv_ring->ctrl->error, __ATOMIC_ACQUIRE)) {
        conn->state = DMESH_CONN_ERROR;
        conn->error_status = EIO;
        return DOCA_ERROR_IO_FAILED;
    }
    *bytes = __atomic_load_n(&conn->rcv_ring->ctrl->completed_bytes, __ATOMIC_ACQUIRE);
    return DOCA_SUCCESS;
}

int
dmesh_dma_push_staged(struct dmesh_conn *conn, uint32_t src_pos, uint32_t len)
{
    struct doca_buf *sbuf = NULL, *dbuf = NULL;
    size_t data_size, chunk;
    doca_error_t result;

    if (conn == NULL)
        return -(int)DOCA_ERROR_INVALID_VALUE;
    if (dma_admission_closed(conn))
        return -(int)DOCA_ERROR_BAD_STATE;
    if (conn->tx_staging == NULL || conn->rcvbuf.mmap == NULL || conn->rcvbuf.buf == NULL)
        return -(int)DOCA_ERROR_BAD_STATE;
    if (conn->push_fin_requested)
        return -(int)DOCA_ERROR_BAD_STATE; /* no bytes after end of stream */
    if (conn->push_state != 0)
        return 0;                       /* previous batch still in flight */
    if (len == 0)
        return 0;

    /* Flow control: once the host has published its cursor (magic seen),
     * refuse to publish past unconsumed slots or unread data-ring bytes -
     * the caller retries, so tx_staging soaks the backlog (upstream
     * backpressure). While gated, keep the cursor fresh. */
    if (conn->host_fc) {
        size_t ring = conn->rcvbuf.size - DMESH_PUSH_DATA_OFF;

        if (conn->push_seq - conn->host_consumed_seq >= DMESH_PUSH_DESC_N - 2 ||
            conn->pushed_bytes - conn->host_consumed_bytes + 2 * DMESH_PUSH_MAX_BATCH > ring) {
            /* Stale-pull recovery: a pull whose completion never arrived
             * would pin cursor_state=1 and freeze the cursor forever. After
             * enough consecutive gated calls (driver ticks ~1ms), assume it
             * was lost and allow a fresh pull. */
            if (++conn->gated_calls % 4096 == 0) {
                if (conn->cursor_state != 0) {
                    DOCA_LOG_WARN("push gate: pull stuck, resetting cursor_state");
                    conn->cursor_state = 0;
                }
                DOCA_LOG_WARN("push gate held %lu calls: push_seq=%lu consumed_seq=%lu "
                              "pushed_bytes=%lu consumed_bytes=%lu ring=%zu cursor_state=%d",
                              conn->gated_calls, conn->push_seq, conn->host_consumed_seq,
                              conn->pushed_bytes, conn->host_consumed_bytes, ring,
                              conn->cursor_state);
            }
            dmesh_dma_pull_cursor(conn);
            return 0;
        }
        conn->gated_calls = 0;
    }

    /* The bytes are already staged at tx_staging+src_pos (written once by the
     * Rust side). We only build the DMA and publish a descriptor - no memcpy.
     * push_pos is our own cursor into the HOST data ring (destination), tracked
     * independently of the source and advanced in dmesh_dma_push_desc_done. */
    data_size = conn->rcvbuf.size - DMESH_PUSH_DATA_OFF;
    if (data_size > conn->tx_staging_len - 64)
        data_size = conn->tx_staging_len - 64;

    chunk = len > DMESH_PUSH_MAX_BATCH ? DMESH_PUSH_MAX_BATCH : len;
    if ((size_t)conn->push_pos + chunk > data_size)
        conn->push_pos = 0;             /* wrap the destination before the end */

    result = doca_buf_inventory_buf_get_by_data(conn->buf_inv, conn->tx_staging_mmap,
                                                (uint8_t *)conn->tx_staging + src_pos,
                                                chunk, &sbuf);
    if (result != DOCA_SUCCESS)
        return -(int)result;
    result = doca_buf_inventory_buf_get_by_addr(conn->buf_inv, conn->rcvbuf.mmap,
                                                (uint8_t *)conn->rcvbuf.buf + DMESH_PUSH_DATA_OFF +
                                                    conn->push_pos,
                                                chunk, &dbuf);
    if (result != DOCA_SUCCESS) {
        (void)doca_buf_dec_refcount(sbuf, NULL);
        return -(int)result;
    }

    result = submit_dma_task_kind(conn, sbuf, dbuf, DMESH_TASK_PUSH_DATA);
    if (result != DOCA_SUCCESS) {
        (void)doca_buf_dec_refcount(dbuf, NULL);
        (void)doca_buf_dec_refcount(sbuf, NULL);
        return result == DOCA_ERROR_AGAIN ? 0 : -(int)result;
    }

    conn->push_len = (uint32_t)chunk;
    conn->push_state = 1;
    return (int)chunk;
}

/* Second stage of a backend push, run from the data copy's completion
 * callback: publish the batch by DMAing its 16B descriptor into the host's
 * slot ring. The shadow (descriptor source) lives in tx_staging's reserved
 * tail so it is mmap'd for DMA. */
static void
dmesh_dma_push_submit_desc(struct dmesh_conn *conn)
{
    struct doca_buf *sbuf = NULL, *dbuf = NULL;
    uint64_t next_seq = conn->push_seq + 1;
    doca_error_t result;

    if (dma_admission_closed(conn))
        return;
    conn->push_shadow = (struct dmesh_push_desc *)((uint8_t *)conn->tx_staging +
                                                   conn->tx_staging_len - 64);
    conn->push_shadow->seq = next_seq;
    conn->push_shadow->pos = conn->push_pos;
    conn->push_shadow->len = conn->push_len;

    result = doca_buf_inventory_buf_get_by_data(conn->buf_inv, conn->tx_staging_mmap,
                                                conn->push_shadow,
                                                sizeof(struct dmesh_push_desc), &sbuf);
    if (result == DOCA_SUCCESS)
        result = doca_buf_inventory_buf_get_by_addr(conn->buf_inv, conn->rcvbuf.mmap,
                                                    (uint8_t *)conn->rcvbuf.buf +
                                                        (next_seq % DMESH_PUSH_DESC_N) *
                                                            sizeof(struct dmesh_push_desc),
                                                    sizeof(struct dmesh_push_desc), &dbuf);
    if (result == DOCA_SUCCESS)
        result = submit_dma_task_kind(conn, sbuf, dbuf, DMESH_TASK_PUSH_DESC);

    if (result == DOCA_SUCCESS) {
        conn->push_state = 2;
        return;
    }

    /* Batch is lost to the stream if we cannot publish it - log loudly. */
    DOCA_LOG_ERR("backend push: failed to submit desc DMA (seq=%lu): %s",
                 next_seq, doca_error_get_descr(result));
    if (dbuf != NULL)
        (void)doca_buf_dec_refcount(dbuf, NULL);
    if (sbuf != NULL)
        (void)doca_buf_dec_refcount(sbuf, NULL);
    conn->push_state = 0;
    conn->dma_closing = true;
    conn->state = DMESH_CONN_ERROR;
    conn->error_status = EIO;
}

/* Final stage: descriptor landed; the batch is visible to the host. */
static void
dmesh_dma_push_desc_done(struct dmesh_conn *conn)
{
    conn->push_seq++;
    conn->push_pos += conn->push_len;
    conn->pushed_bytes += conn->push_len;
    conn->push_len = 0;
    conn->push_state = 0;
    /* Keep the host cursor reasonably fresh (also probes legacy hosts). */
    if ((conn->push_seq & 0xf) == 0 || conn->push_seq < 4)
        dmesh_dma_pull_cursor(conn);
    (void)dmesh_dma_push_fin(conn);
}

int
dmesh_dma_push_fin(struct dmesh_conn *conn)
{
    if (conn == NULL)
        return -(int)DOCA_ERROR_INVALID_VALUE;
    if (!conn->push_fin_requested || conn->push_fin_sent)
        return 0;
    if (dma_admission_closed(conn))
        return -(int)DOCA_ERROR_BAD_STATE;
    /* Due once the push channel exists and no batch is in flight. The data
     * gate leaves two descriptor slots free, so the FIN never overwrites a
     * descriptor the host has not read. */
    if (conn->tx_staging == NULL || conn->rcvbuf.mmap == NULL || conn->rcvbuf.buf == NULL ||
        conn->push_state != 0)
        return 0;
    conn->push_len = 0;
    dmesh_dma_push_submit_desc(conn);
    if (conn->push_state == 2)
        conn->push_fin_sent = true;
    return 0;
}

doca_error_t
enqueue_dma_task(struct dmesh_conn *conn, const struct doca_buf *src, struct doca_buf *dst)
{
    struct doca_dma_task_memcpy *dma_task;
    doca_error_t result;

    if (conn == NULL || src == NULL || dst == NULL)
        return DOCA_ERROR_INVALID_VALUE;
    if (dma_admission_closed(conn))
        return DOCA_ERROR_BAD_STATE;

    dma_task = get_free_dma_task(conn);
    if (dma_task == NULL)
        return DOCA_ERROR_AGAIN;

    doca_dma_task_memcpy_set_src(dma_task, src);
    doca_dma_task_memcpy_set_dst(dma_task, dst);

    result = put_submission_dma_task(conn, dma_task);
    if (result != DOCA_SUCCESS) {
        doca_dma_task_memcpy_set_src(dma_task, NULL);
        doca_dma_task_memcpy_set_dst(dma_task, NULL);
        if (put_free_dma_task(conn, dma_task) != DOCA_SUCCESS)
            DOCA_LOG_ERR("Failed to return DMA task after enqueue failure");
    }

    return result;
}

doca_error_t
progress_dma_submission_queue(struct dmesh_conn *conn, int max_tasks, int *num_submitted)
{
    struct doca_dma_task_memcpy *dma_task;
    struct dma_task_entry *entry;
    doca_error_t result;
    int submitted = 0;

    if (num_submitted != NULL)
        *num_submitted = 0;

    if (conn == NULL || max_tasks < 0)
        return DOCA_ERROR_INVALID_VALUE;
    if (dma_admission_closed(conn))
        return DOCA_ERROR_BAD_STATE;

    while (max_tasks == 0 || submitted < max_tasks) {
        dma_task = get_submission_dma_task(conn);
        if (dma_task == NULL)
            break;

        entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;
        entry->result = DOCA_ERROR_IN_PROGRESS;
        result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_task));
        if (result == DOCA_SUCCESS) {
            entry->in_flight = true;
            submitted++;
            continue;
        }

        entry->result = result;
        if (result == DOCA_ERROR_AGAIN) {
            TAILQ_INSERT_HEAD(&conn->submission_dma_tasks, entry, entries);
            entry->in_submission_queue = true;
            conn->num_submission_dma_tasks++;
        } else if (put_free_dma_task(conn, dma_task) != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to return rejected DMA task to free queue");
        }

        if (num_submitted != NULL)
            *num_submitted = submitted;
        return result;
    }

    if (num_submitted != NULL)
        *num_submitted = submitted;
    return DOCA_SUCCESS;
}

struct doca_dma_task_memcpy *
get_free_dma_task(struct dmesh_conn *conn)
{
    struct dma_task_entry *entry;

    if (dma_admission_closed(conn))
        return NULL;

    entry = TAILQ_FIRST(&conn->free_dma_tasks);
    if (entry == NULL)
        return NULL;

    TAILQ_REMOVE(&conn->free_dma_tasks, entry, entries);
    entry->in_free_queue = false;
    conn->num_free_dma_tasks--;

    return entry->task;
}

doca_error_t
put_free_dma_task(struct dmesh_conn *conn, struct doca_dma_task_memcpy *dma_task)
{
    struct dma_task_entry *entry;
    doca_error_t result;

    if (conn == NULL || dma_task == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;
    if (entry == NULL || entry->owner != conn || entry->task != dma_task ||
        entry->in_free_queue || entry->in_submission_queue || entry->in_flight)
        return DOCA_ERROR_INVALID_VALUE;

    result = dma_release_task_buffers(dma_task);
    if (result != DOCA_SUCCESS)
        return result;

    TAILQ_INSERT_TAIL(&conn->free_dma_tasks, entry, entries);
    entry->in_free_queue = true;
    conn->num_free_dma_tasks++;

    return DOCA_SUCCESS;
}

struct doca_dma_task_memcpy *
get_submission_dma_task(struct dmesh_conn *conn)
{
    struct dma_task_entry *entry;

    if (dma_admission_closed(conn))
        return NULL;

    entry = TAILQ_FIRST(&conn->submission_dma_tasks);
    if (entry == NULL)
        return NULL;

    TAILQ_REMOVE(&conn->submission_dma_tasks, entry, entries);
    entry->in_submission_queue = false;
    conn->num_submission_dma_tasks--;

    return entry->task;
}

doca_error_t
put_submission_dma_task(struct dmesh_conn *conn, struct doca_dma_task_memcpy *dma_task)
{
    struct dma_task_entry *entry;

    if (conn == NULL || dma_task == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    if (dma_admission_closed(conn))
        return DOCA_ERROR_BAD_STATE;

    entry = doca_task_get_user_data(doca_dma_task_memcpy_as_task(dma_task)).ptr;
    if (entry == NULL || entry->owner != conn || entry->task != dma_task ||
        entry->in_free_queue || entry->in_submission_queue || entry->in_flight ||
        doca_dma_task_memcpy_get_src(dma_task) == NULL ||
        doca_dma_task_memcpy_get_dst(dma_task) == NULL)
        return DOCA_ERROR_INVALID_VALUE;

    TAILQ_INSERT_TAIL(&conn->submission_dma_tasks, entry, entries);
    entry->in_submission_queue = true;
    conn->num_submission_dma_tasks++;

    return DOCA_SUCCESS;
}

int
get_num_free_dma_tasks(const struct dmesh_conn *conn)
{
    return conn == NULL ? 0 : conn->num_free_dma_tasks;
}

int
get_num_submission_dma_tasks(const struct dmesh_conn *conn)
{
    return conn == NULL ? 0 : conn->num_submission_dma_tasks;
}

/* Reap only tasks owned by the application. DOCA requires all allocated
 * tasks freed before STOPPING can become IDLE, so this runs between progress
 * calls instead of waiting for IDLE with the task pool still allocated. */
static doca_error_t dma_reap_idle_tasks(struct dmesh_conn *conn, bool *in_flight)
{
    *in_flight = false;
    if (conn->dma_task_entries == NULL)
        return DOCA_SUCCESS;
    for (int i = 0; i < conn->num_dma_tasks; ++i) {
        struct dma_task_entry *entry = &conn->dma_task_entries[i];
        if (entry->task == NULL)
            continue;
        if (entry->in_flight) {
            *in_flight = true;
            continue;
        }
        doca_error_t result = dma_release_task_buffers(entry->task);
        if (result != DOCA_SUCCESS)
            return result;
        if (entry->in_free_queue) {
            TAILQ_REMOVE(&conn->free_dma_tasks, entry, entries);
            entry->in_free_queue = false;
            --conn->num_free_dma_tasks;
        }
        if (entry->in_submission_queue) {
            TAILQ_REMOVE(&conn->submission_dma_tasks, entry, entries);
            entry->in_submission_queue = false;
            --conn->num_submission_dma_tasks;
        }
        doca_task_free(doca_dma_task_memcpy_as_task(entry->task));
        entry->task = NULL;
    }
    return DOCA_SUCCESS;
}

doca_error_t
cleanup_dma_tasks(struct dmesh_conn *conn)
{
    doca_error_t result;
    bool in_flight;
    if (conn == NULL)
        return DOCA_SUCCESS;
    conn->dma_closing = true;
    conn->dma_pending_cnt = 0;

    if (conn->dma_ctx != NULL) {
        struct doca_ctx *ctx = doca_dma_as_ctx(conn->dma_ctx);
        enum doca_ctx_states state;
        result = doca_ctx_get_state(ctx, &state);
        if (result != DOCA_SUCCESS)
            return result;
        if (state != DOCA_CTX_STATE_IDLE && state != DOCA_CTX_STATE_STOPPING) {
            result = doca_ctx_stop(ctx);
            if (result != DOCA_SUCCESS && result != DOCA_ERROR_IN_PROGRESS)
                return result;
        }
        for (unsigned poll = 0;; ++poll) {
            result = dma_reap_idle_tasks(conn, &in_flight);
            if (result != DOCA_SUCCESS)
                return result;
            result = doca_ctx_get_state(ctx, &state);
            if (result != DOCA_SUCCESS)
                return result;
            if (state == DOCA_CTX_STATE_IDLE) {
                if (in_flight)
                    return DOCA_ERROR_BAD_STATE;
                break;
            }
            if (poll == DMESH_DMA_STOP_MAX_POLLS)
                return DOCA_ERROR_TIME_OUT;
            if (conn->objs == NULL || conn->objs->consumer_pe == NULL)
                return DOCA_ERROR_BAD_STATE;
            (void)doca_pe_progress(conn->objs->consumer_pe);
        }
        result = doca_dma_destroy(conn->dma_ctx);
        if (result != DOCA_SUCCESS)
            return result;
        conn->dma_ctx = NULL;
    } else {
        result = dma_reap_idle_tasks(conn, &in_flight);
        if (result != DOCA_SUCCESS)
            return result;
        if (in_flight)
            return DOCA_ERROR_BAD_STATE;
    }

    /* Inventory destruction implicitly stops it and fails with IN_USE while
     * any reference remains. Preserve the handle on failure for a later retry. */
    if (conn->buf_inv != NULL) {
        result = doca_buf_inventory_destroy(conn->buf_inv);
        if (result != DOCA_SUCCESS)
            return result;
        conn->buf_inv = NULL;
    }
    free(conn->dma_task_entries);
    conn->dma_task_entries = NULL;
    conn->num_dma_tasks = 0;
    conn->num_free_dma_tasks = 0;
    conn->num_submission_dma_tasks = 0;
    TAILQ_INIT(&conn->free_dma_tasks);
    TAILQ_INIT(&conn->submission_dma_tasks);
    free(conn->dma_pending);
    conn->dma_pending = NULL;
    conn->dma_pending_head = 0;
    free(conn->recv_segs);
    conn->recv_segs = NULL;
    conn->recv_seg_head = 0;
    conn->recv_seg_cnt = 0;
    conn->push_state = 0;
    conn->cursor_state = 0;
    return DOCA_SUCCESS;
}
