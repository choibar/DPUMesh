#include "comch_client.h"

#include <time.h>
#include <stdlib.h>
#include <string.h>

#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_pe.h>
#include <doca_error.h>
#include <doca_log.h>


DOCA_LOG_REGISTER(COMCH_CLIENT);

#ifndef SLEEP_IN_NANOS
#define SLEEP_IN_NANOS (10 * 1000)	       /* Sample tasks every 10 microseconds */
#endif

/**
 * Callback for client send task successful completion
 *
 * @task [in]: Send task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void client_send_task_completion_callback(struct doca_comch_task_send *task,
						 union doca_data task_user_data,
						 union doca_data ctx_user_data)
{
	(void)ctx_user_data;

	DOCA_LOG_DBG("Client task sent successfully");
	doca_task_free(doca_comch_task_send_as_task(task));
	free(task_user_data.ptr);
}

/**
 * Callback for client send task completion with error
 *
 * @task [in]: Send task object
 * @task_user_data [in]: User data for task
 * @ctx_user_data [in]: User data for context
 */
static void client_send_task_completion_err_callback(struct doca_comch_task_send *task,
						     union doca_data task_user_data,
						     union doca_data ctx_user_data)
{
	struct dmesh_comch_client *control;

	control = (struct dmesh_comch_client *)(ctx_user_data.ptr);
	control->peer_gone = 1;
    if (control->disconnected) control->disconnected(control->owner);
	{
		doca_error_t st = doca_task_get_status(doca_comch_task_send_as_task(task));

		/* Instrumented: this callback used to stop the whole client ctx
		 * silently on any failed send. Log what failed so a stopped channel
		 * can be attributed to a control-path send error vs a peer drop. */
		DOCA_LOG_WARN("comch client: send task failed (%s) - stopping client ctx",
			      doca_error_get_name(st));
	}
	doca_task_free(doca_comch_task_send_as_task(task));
	free(task_user_data.ptr);
	(void)doca_ctx_stop(doca_comch_client_as_ctx(control->cc_client));
}

/*
 * Client ctx state changes. The one that matters is the peer dropping us: the
 * DPU calls doca_comch_server_disconnect() when it tears a slot down, which
 * moves this client ctx RUNNING -> STOPPING -> IDLE. Without observing that,
 * the host channel never reports EOF, so the reader above it (dmeshgo's
 * net.Conn, then gRPC) hangs on a dead-but-READY connection. Runs inside
 * doca_pe_progress() on the caller's thread.
 */
static void client_state_changed_callback(const union doca_data user_data,
                                          struct doca_ctx *ctx,
                                          enum doca_ctx_states prev_state,
                                          enum doca_ctx_states next_state)
{
    struct dmesh_comch_client *control = (struct dmesh_comch_client *)user_data.ptr;

    (void)ctx;
    if (control == NULL)
        return;
    if (prev_state == DOCA_CTX_STATE_RUNNING &&
        (next_state == DOCA_CTX_STATE_STOPPING || next_state == DOCA_CTX_STATE_IDLE)) {
        if (!control->peer_gone)
            DOCA_LOG_INFO("comch client: peer disconnected (ctx %s)",
                          next_state == DOCA_CTX_STATE_IDLE ? "idle" : "stopping");
        control->peer_gone = 1;
        if (control->disconnected) control->disconnected(control->owner);
    }
}

/**
 * Callback for client message recv event
 *
 * @event [in]: Recv event object
 * @recv_buffer [in]: Message buffer
 * @msg_len [in]: Message len
 * @comch_connection [in]: Connection the message was received on
 */
static void client_message_recv_callback(struct doca_comch_event_msg_recv *event,
					 uint8_t *recv_buffer,
					 uint32_t msg_len,
					 struct doca_comch_connection *comch_connection)
{
	union doca_data user_data;
	struct doca_comch_client *comch_client;
	doca_error_t result;
	struct dmesh_comch_client *control;

	(void)event;

	comch_client = doca_comch_client_get_client_ctx(comch_connection);

	result = doca_ctx_get_user_data(doca_comch_client_as_ctx(comch_client), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

    control = user_data.ptr;
    if (control->message != NULL)
        control->message(control->owner, recv_buffer, msg_len);
}

/**
 * Client sends a message to server
 *
 * @control [in]: Channel control endpoint
 * @msg [in]: The msg to send
 * @len [in]: The msg length
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t dmesh_comch_client_send(struct dmesh_comch_client *control, const char *msg, size_t len)
{
	doca_error_t result;
	struct doca_comch_task_send *task;
	union doca_data data;
	if (msg == NULL || len == 0) return DOCA_ERROR_INVALID_VALUE;
	/* DOCA retains the send source until task completion. Callers may pass a
	 * stack frame or release an export message immediately after this call. */
	data.ptr = malloc(len);
	if (data.ptr == NULL) return DOCA_ERROR_NO_MEMORY;
	memcpy(data.ptr, msg, len);
	result = doca_comch_client_task_send_alloc_init(control->cc_client,
		control->connection, data.ptr, len, &task);
	if (result != DOCA_SUCCESS) { free(data.ptr); return result; }
	doca_task_set_user_data(doca_comch_task_send_as_task(task), data);
	result = doca_task_submit(doca_comch_task_send_as_task(task));
	if (result != DOCA_SUCCESS) {
		doca_task_free(doca_comch_task_send_as_task(task));
		free(data.ptr);
	}
	return result;
}

static void client_consumer_arrived(struct doca_comch_event_consumer *event,
                                    struct doca_comch_connection *connection, uint32_t id)
{
    (void)event;
    union doca_data data;
    struct doca_comch_client *client = doca_comch_client_get_client_ctx(connection);
    if (doca_ctx_get_user_data(doca_comch_client_as_ctx(client), &data) != DOCA_SUCCESS) return;
    struct dmesh_comch_client *control = data.ptr;
    if (control->consumer_arrived) control->consumer_arrived(control->owner, id);
}

static void client_consumer_expired(struct doca_comch_event_consumer *event,
                                    struct doca_comch_connection *connection, uint32_t id)
{
    (void)event; (void)connection; (void)id;
}

doca_error_t dmesh_comch_client_open(const char *server_name,
                    struct dmesh_comch_client *control)
{
    doca_error_t result;
	struct doca_ctx *ctx;
	union doca_data user_data;
	uint32_t max_msg_size, max_rq_size;
	enum doca_ctx_states state;
	struct timespec ts = {
		.tv_nsec = SLEEP_IN_NANOS,
	};

    result = doca_pe_create(&(control->pe));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
        return result;
    }
    /* The control PE holds one context. Progress it whole: in the default
     * selective mode an armed PE delivers no new event until its triggered
     * notification is cleared, which stalls a synchronous flow open behind
     * an idle wake. With PROGRESS_ALL a new request clears the old one. */
    result = doca_pe_set_event_mode(control->pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set Comch client PE event mode: %s", doca_error_get_descr(result));
        goto destroy_pe;
    }

    result = doca_comch_client_create(control->dev, server_name, &(control->cc_client));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create client with error = %s", doca_error_get_name(result));
        goto destroy_pe;
    }

    ctx = doca_comch_client_as_ctx(control->cc_client);

    result = doca_pe_connect_ctx(control->pe, ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding pe context to client with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

    result = doca_ctx_set_state_changed_cb(ctx, client_state_changed_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting state change callback with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

    result = doca_comch_client_task_send_set_conf(control->cc_client,
                                                  client_send_task_completion_callback,
                                                  client_send_task_completion_err_callback,
                                                  CC_SEND_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting send task cbs with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

    result = doca_comch_client_event_msg_recv_register(control->cc_client,
                                                    client_message_recv_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding message recv event cb with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

	/* register event callback for new comsumer and expired consumer */
	if (control->consumer_arrived != NULL) {
		result = doca_comch_client_event_consumer_register(control->cc_client,
									client_consumer_arrived, client_consumer_expired);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed adding consumer event cb with error = %s", doca_error_get_name(result));
			goto destroy_client;
		}
	}

    /* Set client properties */
	result = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(control->dev), &max_msg_size);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get max message size with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

     result = doca_comch_cap_get_max_recv_queue_size(doca_dev_as_devinfo(control->dev), &max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_client;
    }

    DOCA_LOG_INFO("CC client max msg size: %u B, max rq size: %u", max_msg_size, max_rq_size);

	result = doca_comch_client_set_max_msg_size(control->cc_client, max_msg_size);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set msg size property with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	result = doca_comch_client_set_recv_queue_size(control->cc_client, CC_RECV_QUEUE_SIZE);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set msg size property with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	user_data.ptr = (void *)control;
	result = doca_ctx_set_user_data(ctx, user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	/* Client is not started until connection is finished, so getting connection in progress */
	result = doca_ctx_start(ctx);
	if (result != DOCA_ERROR_IN_PROGRESS) {
		DOCA_LOG_ERR("Failed to start client context with error = %s", doca_error_get_name(result));
		goto destroy_client;
	}

	struct timespec start, now;
	clock_gettime(CLOCK_MONOTONIC, &start);
	(void)doca_ctx_get_state(ctx, &state);
	while (state != DOCA_CTX_STATE_RUNNING) {
		(void)doca_pe_progress(control->pe);
		nanosleep(&ts, &ts);
		(void)doca_ctx_get_state(ctx, &state);
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (control->peer_gone || now.tv_sec - start.tv_sec >= 5) {
			result = control->peer_gone ? DOCA_ERROR_CONNECTION_ABORTED : DOCA_ERROR_TIME_OUT;
			goto destroy_client;
		}
	}

	(void)doca_comch_client_get_connection(control->cc_client, &control->connection);
	doca_comch_connection_set_user_data(control->connection, user_data);
	DOCA_LOG_INFO("CC client connection established successfully");

    return DOCA_SUCCESS;

destroy_client:
    (void)doca_ctx_stop(doca_comch_client_as_ctx(control->cc_client));
    for (int i = 0; i < 100000; ++i) {
        if (doca_ctx_get_state(doca_comch_client_as_ctx(control->cc_client), &state) != DOCA_SUCCESS ||
            state == DOCA_CTX_STATE_IDLE) break;
        (void)doca_pe_progress(control->pe);
    }
    /* A still-running context must retain its callback objects and PE. */
    if (doca_ctx_get_state(doca_comch_client_as_ctx(control->cc_client), &state) != DOCA_SUCCESS ||
        state != DOCA_CTX_STATE_IDLE) return result;
    doca_comch_client_destroy(control->cc_client);
    control->cc_client = NULL;
destroy_pe:
    doca_pe_destroy(control->pe);
    control->pe = NULL;
    return result;
}
