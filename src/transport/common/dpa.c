#include "dpa.h"

#include <stddef.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_comch_msgq.h>
#include <doca_buf.h>
#include <doca_buf_array.h>
#include <doca_mmap.h>

#include "object.h"
#include "dpa_common.h"
#include "ring.h"
#include "dma.h"
#include <arpa/inet.h>
#include <time.h>
#include <stdlib.h>

DOCA_LOG_REGISTER(DPA);

/* DPU-side staging flow control opt-in (set by apps that publish rd_pos). */
int dmesh_staging_fc = 0;


/* Kernel function declaration */
extern doca_dpa_func_t run_dma_manager;
extern doca_dpa_func_t thread_init_rpc;

extern struct doca_dpa_app *DPU_mesh_dpa_app;

#define TEST_DPA_MEMORY

/*
 * Callback invoked once a message is received from DPA successfully
 *
 * @recv_task [in]: The receive task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the consumer context
 */
static void dmesh_doca_dpa_msgq_recv_cb(struct doca_comch_consumer_task_post_recv *recv_task,
				       union doca_data task_user_data,
				       union doca_data ctx_user_data)
{
	(void)task_user_data;

    struct dmesh_doca_dpa_comch *comch = ctx_user_data.ptr;
    struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);
    const struct comch_dma_comp_msg *msg = (const void *)doca_comch_consumer_task_post_recv_get_imm_data(recv_task);
    uint32_t len = doca_comch_consumer_task_post_recv_get_imm_data_len(recv_task);
    if (msg == NULL || len != sizeof(*msg) || msg->type != COMCH_MSG_TYPE_DMA_COMPLETED ||
        comch->dma_completed == UINT64_MAX) {
        comch->completion_error = true;
    } else {
        ++comch->dma_completed;
        if (comch->received) comch->received(comch->owner, msg->pos, msg->length, msg->count);
    }
    if (comch->stopping) {
        doca_task_free(task);
        return;
    }
    doca_error_t result = doca_task_submit(task);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA MsgQ receive callback failed: Failed to resubmit receive task - %s",
                     doca_error_get_name(result));
        doca_task_free(task);
    }
}

/*
 * Callback invoked once consumer encounters a receive error
 *
 * @recv_task [in]: The receive task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the consumer context
 */
static void dmesh_doca_dpa_msgq_recv_error_cb(struct doca_comch_consumer_task_post_recv *recv_task,
					     union doca_data task_user_data,
					     union doca_data ctx_user_data)
{
	(void)task_user_data;
    struct dmesh_doca_dpa_comch *comch = ctx_user_data.ptr;
    if (comch != NULL && !comch->stopping) comch->completion_error = true;

	struct doca_task *task = doca_comch_consumer_task_post_recv_as_task(recv_task);

	doca_task_free(task);
}
/*
 * Callback invoked once a message is sent to DPA successfully
 *
 * @send_task [in]: The send task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the producer context
 */
static void dmesh_doca_dpa_msgq_send_cb(struct doca_comch_producer_task_send *send_task,
				       union doca_data task_user_data,
				       union doca_data ctx_user_data)
{
    struct comch_msg msg;
	(void)task_user_data;
	
    
    struct dmesh_doca_dpa_comch *comch = ctx_user_data.ptr;
    if (comch->sent) comch->sent(comch->owner);
    
    // DOCA_LOG_INFO("Sent msg to DPA successfully, cnt: %d", objs->sent_msg_cnt);
    doca_comch_producer_task_send_set_imm_data(send_task, (uint8_t *)&msg, sizeof(struct comch_msg));
    
	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    doca_task_free(task);

    /* Below code is used to resubmit the send task if needed, 
     * but this leads to deadlock in our current design
    do {
        result = doca_task_submit(task);
    } while (result == DOCA_ERROR_AGAIN);
    
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to resubmit msgQ send task - %s",
            doca_error_get_name(result));
            doca_task_free(task);
    }
    */
}

/*
 * Callback invoked once producer encounters a send error
 *
 * @send_task [in]: The send task
 * @task_user_data [in]: User data that was previously provided with the task
 * @ctx_user_data [in]: User data that was previously set for the producer context
 */
static void dmesh_doca_dpa_msgq_send_error_cb(struct doca_comch_producer_task_send *send_task,
					     union doca_data task_user_data,
					     union doca_data ctx_user_data)
{
	(void)task_user_data;
	(void)ctx_user_data;

	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);
    DOCA_LOG_ERR("Failed to send msg");
	doca_task_free(task);
}

/*
 * Callback invoked once consumer/producer state changes
 *
 * @user_data [in]: The user data associated with the context
 * @ctx [in]: The consumer/producer context
 * @prev_state [in]: The previous state
 * @next_state [in]: The new state
 */
void dmesh_doca_dpa_comch_msgq_ctx_state_changed_cb(const union doca_data user_data,
							  struct doca_ctx *ctx,
							  enum doca_ctx_states prev_state,
							  enum doca_ctx_states next_state)
{
	(void)user_data;
	(void)ctx;
	(void)prev_state;

	// struct nvmf_doca_io *io = user_data.ptr;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
        DOCA_LOG_DBG("DPA comch msgQ state is idle.");
		// nvmf_doca_dpa_comch_stop_continue(&io->comch);
		break;
    case DOCA_CTX_STATE_STARTING:
    case DOCA_CTX_STATE_RUNNING:
        DOCA_LOG_DBG("DPA comch msgQ state is running.");
	case DOCA_CTX_STATE_STOPPING:
	default:
		break;
	}
}

doca_error_t
init_dpa_objects(struct objects *objs)
{
    doca_error_t result;

    if (!objs->dpa_pool) {
		objs->dpa_pool = calloc(1, sizeof(struct dmesh_dpa_thread_pool));
		if (!objs->dpa_pool) {
			DOCA_LOG_ERR("Failed to allocate memory for DPA thread pool");
			return DOCA_ERROR_NO_MEMORY;
		}
	}

	if (!objs->dpa_comch) {
		objs->dpa_comch = malloc(sizeof(struct dmesh_doca_dpa_comch));
		if (!objs->dpa_comch) {
			DOCA_LOG_ERR("Failed to allocate memory for dpa_comch");
			return DOCA_ERROR_NO_MEMORY;
		}
	}

    result = doca_dpa_create(objs->dev, &objs->dpa_pool->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create DOCA DPA with error = %s", doca_error_get_name(result));
        return result;
    }

    result = doca_dpa_set_app(objs->dpa_pool->dpa, DPU_mesh_dpa_app);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DPA application with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    result = doca_dpa_start(objs->dpa_pool->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DOCA DPA with error = %s", doca_error_get_name(result));
        goto destroy_dpa;
    }

    DOCA_LOG_INFO("Init DOCA DPA done.");
    return DOCA_SUCCESS;

destroy_dpa:
    doca_dpa_destroy(objs->dpa_pool->dpa);
    objs->dpa_pool->dpa = NULL;
    return result;
}

doca_error_t
dmesh_dpa_thread_pool_init(struct objects *objs)
{
    struct dmesh_dpa_thread_pool *pool = objs->dpa_pool;
    doca_error_t result;
    int i;

    if (pool == NULL || pool->dpa == NULL) {
        DOCA_LOG_ERR("DPA thread pool: init_dpa_objects must run first");
        return DOCA_ERROR_BAD_STATE;
    }

    for (i = 0; i < DPA_THREAD_POOL_SIZE; i++) {
        pool->threads[i].dpa = pool->dpa;
        result = dmesh_doca_dpa_thread_create(&pool->threads[i]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to create DPA pool thread %d: %s", i, doca_error_get_name(result));
            return result;
        }
        pool->owner[i] = NULL;
    }
    pool->size = DPA_THREAD_POOL_SIZE;

    DOCA_LOG_INFO("Created DPA thread pool with %d threads", pool->size);
    return DOCA_SUCCESS;
}

struct dmesh_doca_dpa_thread *
dmesh_dpa_thread_pool_alloc(struct objects *objs, struct dmesh_conn *conn)
{
    struct dmesh_dpa_thread_pool *pool = objs->dpa_pool;
    int i;

    if (pool == NULL || pool->size == 0 || conn == NULL)
        return NULL;

    /* already assigned to this connection? return the same thread */
    for (i = 0; i < pool->size; i++) {
        if (pool->owner[i] == conn)
            return &pool->threads[i];
    }

    for (i = 0; i < pool->size; i++) {
        if (pool->owner[i] == NULL) {
            /* A recycled slot has its thread destroyed by teardown; recreate a
             * fresh one before handing it out. */
            if (pool->threads[i].thread == NULL) {
                pool->threads[i].dpa = pool->dpa;
                if (dmesh_doca_dpa_thread_create(&pool->threads[i]) != DOCA_SUCCESS) {
                    DOCA_LOG_ERR("Failed to recreate DPA pool thread %d", i);
                    return NULL;
                }
            }
            pool->owner[i] = conn;
            DOCA_LOG_INFO("Assigned DPA pool thread %d to connection %p", i, (void *)conn);
            return &pool->threads[i];
        }
    }

    DOCA_LOG_WARN("DPA thread pool exhausted (%d threads in use)", pool->size);
    return NULL;
}

void
dmesh_dpa_thread_pool_release(struct objects *objs, struct dmesh_conn *conn)
{
    struct dmesh_dpa_thread_pool *pool = objs->dpa_pool;
    int i;

    if (pool == NULL || conn == NULL)
        return;

    for (i = 0; i < pool->size; i++) {
        if (pool->owner[i] == conn) {
            pool->owner[i] = NULL;
            if (objs->dpa_thread == &pool->threads[i])
                objs->dpa_thread = NULL;
            DOCA_LOG_INFO("Released DPA pool thread %d from connection %p", i, (void *)conn);
            return;
        }
    }
}

doca_error_t
dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *dpa_thread)
{
    doca_error_t result;

    result = doca_dpa_mem_alloc(dpa_thread->dpa, sizeof(struct dpa_thread_arg), &dpa_thread->arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to alloc dpa mem: %s",
            doca_error_get_descr(result));
        return result;
    }

// #ifdef TEST_DPA_MEMORY
//     result = doca_dpa_mem_alloc(dpa_thread->dpa, 1024, &dpa_thread->buf);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to alloc dpa mem for buffer: %s",
//             doca_error_get_descr(result));
//         return result;
//     }

//     char *temp = "Hello from Host to DPA via DPA memory!";
//     result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->buf,
//                                 temp, strlen(temp) + 1);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to copy data from host to DPA memory: %s",
//             doca_error_get_descr(result));
//         return result;
//     }

//     DOCA_LOG_INFO("Copied data to DPA memory at device pointer: 0x%lx", dpa_thread->buf);
// #endif

    result = doca_dpa_thread_create(dpa_thread->dpa, &dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create dpa thread: %s",
            doca_error_get_descr(result));
        return result;
    }
    
    result = doca_dpa_thread_set_func_arg(dpa_thread->thread, run_dma_manager, dpa_thread->arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set DPA thread func: %s",
            doca_error_get_descr(result));
        return result;
    }
    
    result = doca_dpa_thread_start(dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start DPA thread: %s",
            doca_error_get_descr(result));
        return result;
    }

    return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_msgq_create(const struct dmesh_doca_dpa_msgq_create_attr *attr,
                            struct dmesh_doca_dpa_msgq *msgq)
{
    doca_error_t result;
    struct doca_ctx *consumer_ctx;
    struct doca_ctx *producer_ctx;

    memset(msgq, 0, sizeof(*msgq));

    msgq->is_send = attr->is_send;

    /* No private PE here. The MsgQ's producer/consumer ctxs are driven by the
     * worker's shared consumer PE (passed to every progress/stop call), and
     * nothing ever progressed a per-MsgQ PE. Creating one anyway cost an
     * epoll + eventfd pair per MsgQ (two per connection) that teardown never
     * released, so every reconnect leaked 4 fds until the proxy hit
     * RLIMIT_NOFILE and new channels died mid-setup at
     * "Failed to create epoll file descriptor (errno 24)". */

    result = doca_comch_msgq_create(attr->dev, &msgq->msgq);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create comch msgq - %s",
                doca_error_get_name(result));
        return result;
    }
    
    result = doca_comch_msgq_set_max_num_consumers(msgq->msgq, 1);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num consumers - %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_comch_msgq_set_max_num_producers(msgq->msgq, 1);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num producers - %s",
                doca_error_get_name(result));
        return result;
    }
    
    /* if true, DPA is consumer */
    if (attr->is_send) {
        result = doca_comch_msgq_set_dpa_consumer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set dpa consumer - %s",
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* else, DPA is producer */
        result = doca_comch_msgq_set_dpa_producer(msgq->msgq, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set dpa producer - %s",
                    doca_error_get_name(result));
            return result;
        }
    }
    
    result = doca_comch_msgq_start(msgq->msgq);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start msgq - %s",
                doca_error_get_name(result));
        return result;
    }
    
    msgq->started = true;
    result = doca_comch_msgq_consumer_create(msgq->msgq, &msgq->consumer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create msgq consumer - %s",
                doca_error_get_name(result));
        return result;
    }
    
    consumer_ctx = doca_comch_consumer_as_ctx(msgq->consumer);
    result = doca_comch_consumer_set_imm_data_len(msgq->consumer, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set imm data len to 64 - %s",
                doca_error_get_name(result));
        return result;
    }
    
    if (attr->is_send) {
        /* consumer on DPA */
        result = doca_ctx_set_datapath_on_dpa(consumer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer datapath on dpa - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_set_completion(msgq->consumer, attr->consumer_comp, 0);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer completion - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_set_dev_max_num_recv(msgq->consumer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer max # of recv messages - %s",
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* consumer on DPU */
        union doca_data ctx_user_data;
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(consumer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer ctx user data - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_ctx_set_state_changed_cb(consumer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set state changed cb - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, consumer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to connect consumer to pe - %s",
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_consumer_task_post_recv_set_conf(msgq->consumer,
                                        dmesh_doca_dpa_msgq_recv_cb,
                                        dmesh_doca_dpa_msgq_recv_error_cb,
                                        attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set consumer task config - %s",
                    doca_error_get_name(result));
            return result;
        }
    }

    result = doca_ctx_start(consumer_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start consumer ctx - %s", 
                doca_error_get_name(result));
        return result;
    }

    result = doca_comch_msgq_producer_create(msgq->msgq, &msgq->producer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create msgq producer - %s", 
                doca_error_get_name(result));
        return result;
    }
    producer_ctx = doca_comch_producer_as_ctx(msgq->producer);
    if (attr->is_send) {
        /* producer on DPU */
        union doca_data ctx_user_data;
        ctx_user_data.ptr = attr->ctx_user_data;
        result = doca_ctx_set_user_data(producer_ctx, ctx_user_data);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer ctx user data - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_ctx_set_state_changed_cb(producer_ctx, attr->ctx_state_changed_cb);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set state changed cb - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_pe_connect_ctx(attr->pe, producer_ctx);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to connect producer to pe - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_task_send_set_conf(msgq->producer,
                                dmesh_doca_dpa_msgq_send_cb,
                                dmesh_doca_dpa_msgq_send_error_cb,
                                attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer task config - %s", 
                    doca_error_get_name(result));
            return result;
        }
    } else {
        /* producer on DPA */
        result = doca_ctx_set_datapath_on_dpa(producer_ctx, attr->dpa);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer datapath on dpa - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_set_dev_max_num_send(msgq->producer, attr->max_num_msg);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set producer max # of send messages - %s", 
                    doca_error_get_name(result));
            return result;
        }
        result = doca_comch_producer_dpa_completion_attach(msgq->producer, attr->producer_comp);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to attach producer dpa completion - %s", 
                    doca_error_get_name(result));
            return result;
        }
    }
    result = doca_ctx_start(producer_ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start producer ctx - %s",
                doca_error_get_name(result));
        return result;
    }

    /* Pre-post recv tasks if MsgQ is used for receiving from DPA */
    if (attr->is_send == false) {
        for (uint32_t idx = 0; idx < attr->max_num_msg; idx++) {
            struct doca_comch_consumer_task_post_recv *recv_task;
            result = doca_comch_consumer_task_post_recv_alloc_init(msgq->consumer, NULL, &recv_task);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to allocate consumer post recv task - %s",
                        doca_error_get_name(result));
                return result;
            }
            result = doca_task_submit(doca_comch_consumer_task_post_recv_as_task(recv_task));
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to submit consumer post recv task - %s",
                        doca_error_get_name(result));
                return result;
            }
        }
    }

    return DOCA_SUCCESS;
}

doca_error_t
dmesh_dpa_comch_create(struct dmesh_doca_dpa_thread *dpa_thread,
                       struct dmesh_doca_dpa_comch **out)
{
    if (*out != NULL) return DOCA_ERROR_BAD_STATE;
    struct dmesh_doca_dpa_comch *comch = calloc(1, sizeof(*comch));
    if (comch == NULL) return DOCA_ERROR_NO_MEMORY;
    *out = comch; /* Retain partial setup for checked cleanup. */
    doca_error_t result;

    /* The DPA completion goes first: on an extended DPA context (a host PF
     * process extended to an SF) a thread whose first attached completion is
     * the comch consumer completion fails to start it (DOCA_ERROR_DRIVER,
     * reproduced 2026-09-24); with a doca_dpa_completion attached first both
     * start. Order is irrelevant on a base context. */
    result = doca_dpa_completion_create(dpa_thread->dpa, CC_DPA_MAX_MSG_NUM, &comch->producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create producer completion - %s",
                doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_set_thread(comch->producer_comp, dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set dpa thread to producer completion - %s",
                doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_start(comch->producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start producer completion - %s",
                doca_error_get_name(result));
        return result;
    }

    comch->producer_comp_started = true;
    result = doca_comch_consumer_completion_create(&(comch->consumer_comp));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create consumer completion - %s",
                doca_error_get_name(result));
        assert(0);
        return result;
    }
    
    result = doca_comch_consumer_completion_set_max_num_recv(comch->consumer_comp,
            CC_DPA_MAX_MSG_NUM);    
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max num recv - %s",
            doca_error_get_name(result));
        return result;
    }

    result = doca_comch_consumer_completion_set_imm_data_len(comch->consumer_comp, sizeof(struct comch_msg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set imm data len - %s",
            doca_error_get_name(result));
        return result;
        }
        
    result = doca_comch_consumer_completion_set_dpa_thread(comch->consumer_comp, dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set dpa thread - %s",
            doca_error_get_name(result));
        return result;
    }

    result = doca_comch_consumer_completion_start(comch->consumer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start consumer completion - %s",
			     doca_error_get_name(result));
		return result;
	}

    comch->consumer_comp_started = true;
    return DOCA_SUCCESS;
}

static uint64_t
monotonic_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t
quiesce_deadline_ns(void)
{
    return monotonic_ns() + (uint64_t)DMESH_DPA_QUIESCE_TIMEOUT_MS * 1000000ull;
}

/* A new run starts a new stop handshake. */
static void
dpa_thread_run_reset(struct dmesh_doca_dpa_thread *thread)
{
    thread->quiesced = false;
    thread->stop_sent = false;
    thread->submitted_known = false;
    thread->submitted = 0;
    thread->quiesce_deadline_ns = 0;
}

bool
dmesh_doca_dpa_failed(struct doca_dpa *dpa)
{
    static bool reported;

    if (dpa == NULL || doca_dpa_peek_at_last_error(dpa) != DOCA_ERROR_BAD_STATE)
        return false;
    if (!__atomic_exchange_n(&reported, true, __ATOMIC_RELAXED))
        DOCA_LOG_ERR("DPA context reported a device-side error; its threads no longer run and new flows fail until the process restarts");
    return true;
}

/* One step of the checked native-flow close. The kernel publishes its copy
 * count before the stopped flag. DMA-copy immediate messages arrive only after
 * the copy (DOCA producer_dma_copy contract), so every submitted copy must be
 * observed here before any local or exported mapping can be released. Keep
 * receive tasks posted until that fence; otherwise a credit-starved kernel
 * cannot stop. */
doca_error_t
dmesh_dpa_quiesce_step(struct dmesh_doca_dpa_thread *thread,
                       struct dmesh_doca_dpa_comch *comch, struct doca_pe *pe)
{
    doca_error_t result;
    uint32_t one = 1, stopped = 0;

    if (thread == NULL || !thread->running || thread->quiesced)
        return DOCA_SUCCESS;
    if (thread->arg == 0 || thread->thread == NULL || comch == NULL)
        return DOCA_ERROR_BAD_STATE;
    if (dmesh_doca_dpa_failed(thread->dpa))
        return DOCA_ERROR_BAD_STATE;
    if (!thread->stop_sent) {
        result = doca_dpa_h2d_memcpy(thread->dpa,
            thread->arg + offsetof(struct dpa_thread_arg, stop), &one, sizeof(one));
        if (result != DOCA_SUCCESS)
            return result;
        thread->stop_sent = true;
        thread->quiesce_deadline_ns = quiesce_deadline_ns();
    }
    if (pe != NULL)
        (void)doca_pe_progress(pe);
    if (!thread->submitted_known) {
        result = doca_dpa_d2h_memcpy(thread->dpa, &stopped,
            thread->arg + offsetof(struct dpa_thread_arg, stopped), sizeof(stopped));
        if (result != DOCA_SUCCESS)
            return result;
        if (stopped == 0)
            return monotonic_ns() >= thread->quiesce_deadline_ns ? DOCA_ERROR_TIME_OUT
                                                                 : DOCA_ERROR_AGAIN;
        result = doca_dpa_d2h_memcpy(thread->dpa, &thread->submitted,
            thread->arg + offsetof(struct dpa_thread_arg, dma_submitted), sizeof(thread->submitted));
        if (result != DOCA_SUCCESS)
            return result;
        thread->submitted_known = true;
    }
    if (comch->completion_error || comch->dma_completed > thread->submitted)
        return DOCA_ERROR_IO_FAILED;
    if (comch->dma_completed == thread->submitted) {
        thread->quiesced = true;
        return DOCA_SUCCESS;
    }
    return monotonic_ns() >= thread->quiesce_deadline_ns ? DOCA_ERROR_TIME_OUT : DOCA_ERROR_AGAIN;
}

doca_error_t
dmesh_dpa_quiesce_checked(struct dmesh_doca_dpa_thread *thread,
                          struct dmesh_doca_dpa_comch *comch, struct doca_pe *pe)
{
    doca_error_t result;

    /* Each blocking call waits a full window, including a retried close. */
    if (thread != NULL && thread->stop_sent)
        thread->quiesce_deadline_ns = quiesce_deadline_ns();
    while ((result = dmesh_dpa_quiesce_step(thread, comch, pe)) == DOCA_ERROR_AGAIN)
        ;
    return result;
}

static doca_error_t
stop_ctx_checked(struct doca_ctx *ctx, struct doca_pe *pe)
{
    enum doca_ctx_states state;
    doca_error_t result;
    if (ctx == NULL)
        return DOCA_SUCCESS;
    result = doca_ctx_get_state(ctx, &state);
    if (result != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
        return result;
    if (state != DOCA_CTX_STATE_STOPPING) {
        result = doca_ctx_stop(ctx);
        if (result != DOCA_SUCCESS && result != DOCA_ERROR_IN_PROGRESS)
            return result;
    }
    for (unsigned spins = 0; spins < 100000; ++spins) {
        result = doca_ctx_get_state(ctx, &state);
        if (result != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
            return result;
        if (pe != NULL)
            (void)doca_pe_progress(pe);
    }
    return DOCA_ERROR_TIME_OUT;
}

static doca_error_t
msgq_destroy_checked(struct dmesh_doca_dpa_msgq *msgq, struct doca_pe *pe)
{
    doca_error_t result;
    if (msgq->producer != NULL) {
        result = stop_ctx_checked(doca_comch_producer_as_ctx(msgq->producer), pe);
        if (result != DOCA_SUCCESS)
            return result;
        result = doca_comch_producer_destroy(msgq->producer);
        if (result != DOCA_SUCCESS)
            return result;
        msgq->producer = NULL;
    }
    if (msgq->consumer != NULL) {
        result = stop_ctx_checked(doca_comch_consumer_as_ctx(msgq->consumer), pe);
        if (result != DOCA_SUCCESS)
            return result;
        result = doca_comch_consumer_destroy(msgq->consumer);
        if (result != DOCA_SUCCESS)
            return result;
        msgq->consumer = NULL;
    }
    if (msgq->msgq != NULL) {
        if (msgq->started) {
            result = doca_comch_msgq_stop(msgq->msgq);
            if (result != DOCA_SUCCESS)
                return result;
            msgq->started = false;
        }
        result = doca_comch_msgq_destroy(msgq->msgq);
        if (result != DOCA_SUCCESS)
            return result;
        msgq->msgq = NULL;
    }
    return DOCA_SUCCESS;
}

doca_error_t
dmesh_dpa_comch_destroy_checked(struct dmesh_doca_dpa_thread *thread,
                                struct dmesh_doca_dpa_comch **handle, struct doca_pe *pe)
{
    struct dmesh_doca_dpa_comch *comch;
    doca_error_t result;
    if (handle == NULL || (comch = *handle) == NULL)
        return DOCA_SUCCESS;
    if (thread != NULL && thread->running && !thread->quiesced)
        return DOCA_ERROR_BAD_STATE;
    comch->stopping = true;
    result = msgq_destroy_checked(&comch->send, pe);
    if (result != DOCA_SUCCESS)
        return result;
    result = msgq_destroy_checked(&comch->recv, pe);
    if (result != DOCA_SUCCESS)
        return result;
    if (comch->consumer_comp != NULL) {
        if (comch->consumer_comp_started) {
            result = doca_comch_consumer_completion_stop(comch->consumer_comp);
            if (result != DOCA_SUCCESS)
                return result;
            comch->consumer_comp_started = false;
        }
        result = doca_comch_consumer_completion_destroy(comch->consumer_comp);
        if (result != DOCA_SUCCESS)
            return result;
        comch->consumer_comp = NULL;
    }
    if (comch->producer_comp != NULL) {
        if (comch->producer_comp_started) {
            result = doca_dpa_completion_stop(comch->producer_comp);
            if (result != DOCA_SUCCESS)
                return result;
            comch->producer_comp_started = false;
        }
        result = doca_dpa_completion_destroy(comch->producer_comp);
        if (result != DOCA_SUCCESS)
            return result;
        comch->producer_comp = NULL;
    }
    free(comch);
    *handle = NULL;
    return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_dpa_thread_destroy_checked(struct dmesh_doca_dpa_thread *thread)
{
    doca_error_t result;
    if (thread == NULL)
        return DOCA_SUCCESS;
    if (thread->running && !thread->quiesced)
        return DOCA_ERROR_BAD_STATE;
    if (thread->thread != NULL) {
        result = doca_dpa_thread_destroy(thread->thread);
        if (result != DOCA_SUCCESS)
            return result;
        thread->thread = NULL;
        thread->running = false;
    }
    if (thread->arg != 0) {
        result = doca_dpa_mem_free(thread->dpa, thread->arg);
        if (result != DOCA_SUCCESS)
            return result;
        thread->arg = 0;
    }
    dpa_thread_run_reset(thread);
    return DOCA_SUCCESS;
}

/* Stop a ctx and progress its PE until idle (bounded, to never hang). DPA-side
 * ctxs are progressed on `pe` too; they idle once the DPA thread is stopped. */
static void
stop_ctx_bounded(struct doca_ctx *ctx, struct doca_pe *pe)
{
    enum doca_ctx_states state;
    int spins = 0;

    if (ctx == NULL)
        return;
    if (doca_ctx_get_state(ctx, &state) != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
        return;

    (void)doca_ctx_stop(ctx);
    while (spins++ < 100000) {
        if (doca_ctx_get_state(ctx, &state) != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
            break;
        if (pe != NULL)
            doca_pe_progress(pe);
    }
}

/* Tear down one DPA MsgQ: stop both endpoint ctxs, then destroy them and the
 * MsgQ. The DPA thread must already be stopped so the DPA-side ctx can idle. */
static void
dmesh_doca_dpa_msgq_destroy(struct dmesh_doca_dpa_msgq *msgq, struct doca_pe *pe)
{
    if (msgq == NULL)
        return;

    if (msgq->producer != NULL)
        stop_ctx_bounded(doca_comch_producer_as_ctx(msgq->producer), pe);
    if (msgq->consumer != NULL)
        stop_ctx_bounded(doca_comch_consumer_as_ctx(msgq->consumer), pe);

    if (msgq->producer != NULL) {
        (void)doca_comch_producer_destroy(msgq->producer);
        msgq->producer = NULL;
    }
    if (msgq->consumer != NULL) {
        (void)doca_comch_consumer_destroy(msgq->consumer);
        msgq->consumer = NULL;
    }
    if (msgq->msgq != NULL) {
        (void)doca_comch_msgq_stop(msgq->msgq);
        (void)doca_comch_msgq_destroy(msgq->msgq);
        msgq->msgq = NULL;
    }
    /* The MsgQ owns no PE (see dmesh_doca_dpa_msgq_create). */
}

/* Stop (do not destroy) a connection's DPA comch contexts: both MsgQ
 * producer/consumer ctxs and both completion objects, progressed to IDLE on
 * the shared consumer PE. Used by the park path so the comch connection can
 * be disconnected: doca_comch_server_disconnect() refuses while the
 * connection "still has active consumers or producers", and a refused
 * disconnect leaves the dead connection live with 512 posted recvs failing
 * IO_FAILED forever. Destroy stays out of this path (flexio thread-destroy
 * hazard); teardown does the full sequence. */
void
dmesh_doca_dpa_comch_stop(struct dmesh_conn *conn)
{
    struct dmesh_doca_dpa_comch *comch = conn->dpa_comch;
    struct doca_pe *pe = conn->objs->consumer_pe;

    if (comch == NULL)
        return;
    if (comch->send.producer != NULL)
        stop_ctx_bounded(doca_comch_producer_as_ctx(comch->send.producer), pe);
    if (comch->send.consumer != NULL)
        stop_ctx_bounded(doca_comch_consumer_as_ctx(comch->send.consumer), pe);
    if (comch->recv.producer != NULL)
        stop_ctx_bounded(doca_comch_producer_as_ctx(comch->recv.producer), pe);
    if (comch->recv.consumer != NULL)
        stop_ctx_bounded(doca_comch_consumer_as_ctx(comch->recv.consumer), pe);
    if (comch->consumer_comp != NULL)
        (void)doca_comch_consumer_completion_stop(comch->consumer_comp);
    if (comch->producer_comp != NULL)
        (void)doca_dpa_completion_stop(comch->producer_comp);
}

/* Destroy a connection's DPA comch: both MsgQs and both completion objects.
 * Requires conn->dpa_thread to be stopped first. */
void
dmesh_doca_dpa_comch_destroy(struct dmesh_conn *conn)
{
    struct dmesh_doca_dpa_comch *comch = conn->dpa_comch;
    struct doca_pe *pe = conn->objs->consumer_pe;

    if (comch == NULL)
        return;

    dmesh_doca_dpa_msgq_destroy(&comch->send, pe);
    dmesh_doca_dpa_msgq_destroy(&comch->recv, pe);

    if (comch->consumer_comp != NULL) {
        (void)doca_comch_consumer_completion_stop(comch->consumer_comp);
        (void)doca_comch_consumer_completion_destroy(comch->consumer_comp);
        comch->consumer_comp = NULL;
    }
    if (comch->producer_comp != NULL) {
        (void)doca_dpa_completion_stop(comch->producer_comp);
        (void)doca_dpa_completion_destroy(comch->producer_comp);
        comch->producer_comp = NULL;
    }

    free(comch);
    conn->dpa_comch = NULL;
}

/* Bring a running DPA thread to a quiescent (finished) state so its completion
 * contexts and the thread itself can be destroyed. A thread executing the
 * descriptor-ring poll loop never yields, so we signal the kernel (stop=1) and
 * wait for it to acknowledge (stopped=1) - the kernel then calls
 * doca_dpa_dev_thread_finish() and returns, releasing the EU.
 *
 * Note: we deliberately do NOT call doca_dpa_thread_stop() here. Per the DOCA
 * DPA samples the teardown path is destroy-only (completions first, then the
 * thread); calling thread_stop on this comch-attached thread returns
 * DOCA_ERROR_DRIVER and corrupts the flexio thread/event-handler state, which
 * later crashes the process. Bounded throughout so teardown can never hang. */
void
dmesh_doca_dpa_thread_quiesce(struct dmesh_doca_dpa_thread *dpa_thread)
{
    doca_error_t result;
    uint32_t one = 1;
    uint32_t stopped = 0;
    int spins;

    if (dpa_thread == NULL || dpa_thread->thread == NULL)
        return;

    /* A thread that was created but never run (arg == 0) has nothing polling. */
    if (dpa_thread->arg == 0)
        return;

    result = doca_dpa_h2d_memcpy(dpa_thread->dpa,
                                 dpa_thread->arg + offsetof(struct dpa_thread_arg, stop),
                                 &one, sizeof(one));
    if (result != DOCA_SUCCESS)
        DOCA_LOG_ERR("Failed to signal DPA thread stop: %s", doca_error_get_name(result));

    for (spins = 0; spins < 100000; spins++) {
        result = doca_dpa_d2h_memcpy(dpa_thread->dpa, &stopped,
                                     dpa_thread->arg + offsetof(struct dpa_thread_arg, stopped),
                                     sizeof(stopped));
        if (result == DOCA_SUCCESS && stopped != 0)
            break;
    }
    if (stopped == 0)
        DOCA_LOG_WARN("DPA thread did not acknowledge stop within bound; destroying anyway");
}

/* Destroy a pool DPA thread and free its arg memory, so the slot can be
 * recreated fresh for the next connection (dmesh_dpa_thread_pool_alloc).
 * The thread must already be quiesced (dmesh_doca_dpa_thread_quiesce) and its
 * completion contexts destroyed (dmesh_doca_dpa_comch_destroy) first. */
/* Stop a runnable DPA thread (undo doca_dpa_thread_run) BEFORE its completion
 * contexts are destroyed and BEFORE doca_dpa_thread_destroy. The thread must
 * already be quiesced (kernel called doca_dpa_dev_thread_finish); stopping it
 * then transitions the flexio event handler out of the runnable state so the
 * later destroy does not fail ("Failed to destroy dpa thread event handler").
 * Gated by DMESH_THREAD_STOP for A/B comparison. */
void
dmesh_doca_dpa_thread_stop_only(struct dmesh_doca_dpa_thread *dpa_thread)
{
    doca_error_t result;

    if (dpa_thread == NULL || dpa_thread->thread == NULL)
        return;
    /* Opt-in (DMESH_THREAD_STOP): fixes the flexio thread-destroy wedge in the
     * standalone C-worker sibling-teardown reproducer (errors 2->0), but
     * REGRESSES the proxy INGRESS_PUSH path (N=1 DSB 7.6k->17 req/s, flexio
     * still fires) - the proxy client-channel teardown differs. Default off
     * until that path is understood. */
    if (getenv("DMESH_THREAD_STOP") == NULL)
        return;
    result = doca_dpa_thread_stop(dpa_thread->thread);
    if (result != DOCA_SUCCESS)
        DOCA_LOG_WARN("doca_dpa_thread_stop: %s", doca_error_get_name(result));
}
void
dmesh_doca_dpa_thread_destroy(struct dmesh_doca_dpa_thread *dpa_thread)
{
    if (dpa_thread == NULL || dpa_thread->thread == NULL)
        return;

    (void)doca_dpa_thread_destroy(dpa_thread->thread);
    dpa_thread->thread = NULL;

    if (dpa_thread->arg != 0) {
        (void)doca_dpa_mem_free(dpa_thread->dpa, dpa_thread->arg);
        dpa_thread->arg = 0;
    }
}

/*
 * Fills the DPA thread argument with the relevant DPA handles to be later copied to the DPA thread
 *
 * @arg [out]: The returned thread argument that was filled
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t 
dmesh_fill_dpa_thread_arg(struct dmesh_conn *conn, struct dpa_thread_arg *arg)
{
    struct objects *objs = conn->objs;
    (void)objs;
    doca_error_t result;
    struct dmesh_doca_dpa_comch *comch;
    doca_dpa_dev_comch_consumer_completion_t dpa_consumer_comp;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_comch_consumer_t dpa_consumer;
#ifdef DOCA_ARCH_DPU
    doca_dpa_dev_buf_arr_t dpa_buf_arr;
    doca_dpa_dev_mmap_t dpu_mmap, host_mmap;
#endif

    comch = conn->dpa_comch;

    result = doca_comch_consumer_completion_get_dpa_handle(comch->consumer_comp, &dpa_consumer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer completion DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }
    result = doca_dpa_completion_get_dpa_handle(comch->producer_comp, &dpa_producer_comp);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get producer completion DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }
    
    result = doca_comch_consumer_get_dpa_handle(comch->send.consumer, &dpa_consumer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get consumer DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_comch_producer_get_dpa_handle(comch->recv.producer, &dpa_producer);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get producer DPA handle: %s",   
                doca_error_get_name(result));
        return result;
    }
    
#ifdef DOCA_ARCH_DPU
    result = doca_buf_arr_get_dpa_handle(conn->buf_arr, &dpa_buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get buf array DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(conn->local_mmap, objs->dev, &dpu_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get mmap DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }

    result = doca_mmap_dev_get_dpa_handle(conn->sndbuf.mmap, objs->dev, &host_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get mmap DPA handle: %s",
                doca_error_get_name(result));
        return result;
    }
#endif

    *arg = (struct dpa_thread_arg) {
        .dpa_consumer_comp = dpa_consumer_comp,
        .dpa_producer_comp = dpa_producer_comp,
        .dpa_consumer = dpa_consumer,
        .dpa_producer = dpa_producer,
#ifdef DOCA_ARCH_DPU
        .dpa_buf_arr = dpa_buf_arr,
        .buf_arr_size = DMA_RING_SIZE,
        .host_mmap = host_mmap,
        .dpu_mmap = dpu_mmap,
        .src_addr = conn->dma_buffer,
        .buf_size = 1024 * 1024,
        .pos = 0,
        .rd_pos = 0,
        .rd_fc = (uint32_t)dmesh_staging_fc,
#endif
    };
#ifdef DOCA_ARCH_DPU
    conn->rx_wm_published = 0;
#endif

#ifdef DOCA_ARCH_DPU
    /* producer_dma_copy microbenchmark: DMESH_DPA_BENCH_MODE=1 (throughput) or
     * 2 (latency) makes the DPA thread run the bench loop before the normal
     * ring-polling datapath. Source is this connection's host sndbuf. */
    {
        const char *env = getenv("DMESH_DPA_BENCH_MODE");

        if (env != NULL && atoi(env) > 0) {
            arg->bench_mode = (uint32_t)atoi(env);
            arg->bench_msg_size = 4096;
            arg->bench_num_ops = 100000;
            if ((env = getenv("DMESH_DPA_BENCH_SIZE")) != NULL && atoi(env) > 0)
                arg->bench_msg_size = (uint32_t)atoi(env);
            if ((env = getenv("DMESH_DPA_BENCH_OPS")) != NULL && atoi(env) > 0)
                arg->bench_num_ops = (uint32_t)atoi(env);
            arg->bench_host_addr = (uint64_t)conn->sndbuf.buf;
            arg->bench_host_size = (uint32_t)conn->sndbuf.size;
            DOCA_LOG_INFO("DPA bench enabled: mode=%u size=%u ops=%u",
                          arg->bench_mode, arg->bench_msg_size, arg->bench_num_ops);
        }
    }
#endif

    DOCA_LOG_INFO("dpa_consumer_comp: 0x%lx, dpa_producer_comp: 0x%lx, dpa_consumer: 0x%lx, dpa_producer: 0x%lx",
        arg->dpa_consumer_comp,
        arg->dpa_producer_comp,
        arg->dpa_consumer,
        arg->dpa_producer);

    return DOCA_SUCCESS;
}

/*  
 *  Initialize and run the DOCA DPA thread
 *
 */
doca_error_t
dmesh_doca_run_dpa_thread(struct dmesh_conn *conn)
{
    struct dmesh_doca_dpa_thread *dpa_thread = conn->dpa_thread;
    doca_error_t result;
    struct dpa_thread_arg arg;

    result = dmesh_fill_dpa_thread_arg(conn, &arg);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to fill dpa thread argument - %s",
            doca_error_get_name(result));
        return result;
    }

    uint64_t rpc_ret;
    uint32_t num_msg = CC_DPA_MAX_MSG_NUM;
    result = doca_dpa_rpc(dpa_thread->dpa, 
                        thread_init_rpc,
                        &rpc_ret,
                        arg.dpa_consumer,
                        num_msg,
                        (uint64_t)0);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to issue init thread RPC - %s",
            doca_error_get_name(result));
        return result;
    }

    if (rpc_ret != 0) {
        DOCA_LOG_ERR("Failed to init thread RPC");
        return DOCA_ERROR_IO_FAILED;
    }

    result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg, 
                                &arg, sizeof(struct dpa_thread_arg));
    if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to update DPA thread argument - %s",
			     doca_error_get_name(result));
		return result;
	}

    dpa_thread->running = true;
    dpa_thread_run_reset(dpa_thread);
    result = doca_dpa_thread_run(dpa_thread->thread);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to run DPA thread - %s",
			     doca_error_get_name(result));
		return result;
	}             

    return DOCA_SUCCESS;
}

/*
 * Send message to DPA using NVMf DOCA DPA MsgQ
 *
 * @msgq [in]: The MsgQ to be used for the send operation
 * @msg [in]: The message to send
 * @msg_size [in]: The message size
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t 
dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *msgq, void *msg, uint32_t msg_size)
{
	doca_error_t result;
    union doca_data user_data;

	struct doca_comch_producer_task_send *send_task;
	result = doca_comch_producer_task_send_alloc_init(msgq->producer,
							  NULL,
							  msg,
							  msg_size,
							  /*consumer_id=*/1,
							  &send_task);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
			     doca_error_get_name(result));
		return result;
	}

	struct doca_task *task = doca_comch_producer_task_send_as_task(send_task);

    user_data.ptr = msgq;
    doca_task_set_user_data(task, user_data);

    do {
        result = doca_task_submit(task);
    } while (result == DOCA_ERROR_AGAIN);
    
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to submit send task - %s",
			     doca_error_get_name(result));
		doca_task_free(task);
		return result;
	}

	return DOCA_SUCCESS;
}

doca_error_t 
dmesh_doca_dpa_msgq_send_bulk(struct dmesh_doca_dpa_msgq *msgq, uint32_t num_msg,
                                void *msg, uint32_t msg_size)
{
	struct doca_comch_producer_task_send *send_task;
    struct doca_task *task;
	doca_error_t result;
    uint32_t i;
    for (i = 0; i < num_msg; i++) {
        result = doca_comch_producer_task_send_alloc_init(msgq->producer,
                                  NULL,
                                  msg,
                                  msg_size,
                                  /*consumer_id=*/1,
                                  &send_task);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to allocate send task - %s",
                     doca_error_get_name(result));
            return result;
        }
        task = doca_comch_producer_task_send_as_task(send_task);
        result = doca_task_submit(task);
        if (result != DOCA_SUCCESS) {
            if (result != DOCA_ERROR_AGAIN)
                DOCA_LOG_ERR("Failed to send msg using NVMf DOCA DPA MsgQ: Failed to submit send task - %s",
                     doca_error_get_name(result));
            doca_task_free(task);
            return result;
        }
    }
    // DOCA_LOG_INFO("Sent mesg done.");
	return DOCA_SUCCESS;
}

doca_error_t
setup_dpa_buf_array(struct dmesh_conn *conn, size_t num_elem, struct doca_mmap *mmap)
{
    doca_error_t result;

    result = doca_buf_arr_create(num_elem + 1, &conn->buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create buffer array: %s", doca_error_get_descr(result));
        return result;
    }

    result = doca_buf_arr_set_target_dpa(conn->buf_arr, conn->dpa_thread->dpa);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set buffer array target DPA: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_set_params(conn->buf_arr, mmap, sizeof(struct dma_desc), 0);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set buffer array params: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    result = doca_buf_arr_start(conn->buf_arr);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start buffer array: %s", doca_error_get_descr(result));
        goto destroy_buf_arr;
    }

    return DOCA_SUCCESS;

destroy_buf_arr:
    doca_buf_arr_destroy(conn->buf_arr);
    conn->buf_arr = NULL;
    return result;
}

/* DPU/legacy adapters: the SDK callbacks themselves only see a Comch endpoint. */
static void conn_received(void *owner, uint32_t pos, uint32_t len, uint32_t count)
{
    struct dmesh_conn *conn = owner;
    conn->objs->recv_bytes += len;
    conn->objs->recv_msg_cnt += count > 1 ? count : 1;
    if (!conn->recv_segs) return;
    if (conn->recv_seg_cnt == DMESH_RECV_SEG_MAX) { ++conn->recv_seg_dropped; return; }
    int tail = (conn->recv_seg_head + conn->recv_seg_cnt) % DMESH_RECV_SEG_MAX;
    conn->recv_segs[tail] = (struct dmesh_recv_seg){pos, len};
    ++conn->recv_seg_cnt;
}
static void conn_sent(void *owner) { ++((struct dmesh_conn *)owner)->objs->sent_msg_cnt; }

doca_error_t dmesh_doca_dpa_comch_create(struct dmesh_conn *conn)
{
    doca_error_t result = dmesh_dpa_comch_create(conn->dpa_thread, &conn->dpa_comch);
    if (conn->dpa_comch) {
        conn->dpa_comch->owner = conn;
        conn->dpa_comch->received = conn_received;
        conn->dpa_comch->sent = conn_sent;
    }
    return result;
}
doca_error_t dmesh_doca_dpa_quiesce_checked(struct dmesh_conn *conn)
{
    return conn ? dmesh_dpa_quiesce_checked(conn->dpa_thread, conn->dpa_comch,
                                          conn->objs ? conn->objs->consumer_pe : NULL) : DOCA_SUCCESS;
}
doca_error_t dmesh_doca_dpa_comch_destroy_checked(struct dmesh_conn *conn)
{
    return conn ? dmesh_dpa_comch_destroy_checked(conn->dpa_thread, &conn->dpa_comch,
                                               conn->objs ? conn->objs->consumer_pe : NULL) : DOCA_SUCCESS;
}

doca_error_t dmesh_doca_dpa_quiesce_step(struct dmesh_conn *conn)
{
    return conn ? dmesh_dpa_quiesce_step(conn->dpa_thread, conn->dpa_comch,
                                       conn->objs ? conn->objs->consumer_pe : NULL) : DOCA_SUCCESS;
}
