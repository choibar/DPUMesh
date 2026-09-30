
#include "comch_server.h"

#include <time.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "session_protocol.h"

#include "common.h"
#include "object.h"
#include "dpa.h"
#include "dpa_common.h"
#include "comch_common.h"
#include "comch_consumer.h"
#include "comch_msgq.h"
#include "dma.h"
#include "ring.h"

#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_log.h>
#include <doca_comch_producer.h>


DOCA_LOG_REGISTER(COMCH_SERVER);

struct session_send {
    struct dmesh_session *session;
    uint8_t bytes[];
};

static uint64_t session_now_ms(void);

static struct dmesh_session *
session_get(struct objects *objs, struct doca_comch_connection *connection, bool create)
{
    struct dmesh_session *free_session = NULL;
    if (connection == NULL)
        return NULL;
    for (int i = 0; i < DMESH_MAX_SESSIONS; ++i) {
        struct dmesh_session *s = &objs->sessions[i];
        if (s->occupied && s->connection == connection)
            return s;
        if (!s->occupied && free_session == NULL)
            free_session = s;
    }
    if (create && free_session != NULL) {
        memset(free_session, 0, sizeof(*free_session));
        free_session->occupied = true;
        free_session->connection = connection;
        free_session->hello_deadline_ms = session_now_ms() + 5000u;
        return free_session;
    }
    return NULL;
}

static void session_fail(struct objects *objs, struct dmesh_session *s)
{
    if (s == NULL)
        return;
    s->closing = true;
    dmesh_flow_close_session(objs, s->connection);
}

/* A successful disconnect event already retired the SDK peer. Drop every
 * alias immediately: the SDK may reuse the address before deferred flow
 * teardown completes. Session ownership survives while sends/flows remain. */
static void session_peer_detached(struct objects *objs, struct dmesh_session *s)
{
    if (s == NULL)
        return;
    if (objs->connection == s->connection)
        objs->connection = NULL;
    for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
        if (objs->conns[i].session == s)
            objs->conns[i].connection = NULL;
    s->connection = NULL;
}

static void server_send_task_completion_callback(struct doca_comch_task_send *task,
                                                  union doca_data task_user_data,
                                                  union doca_data ctx_user_data)
{
    struct session_send *send = task_user_data.ptr;
    (void)ctx_user_data;
    if (send != NULL && send->session != NULL)
        --send->session->sends_pending;
    doca_task_free(doca_comch_task_send_as_task(task));
    free(send);
}

static void server_send_task_completion_err_callback(struct doca_comch_task_send *task,
                                                      union doca_data task_user_data,
                                                      union doca_data ctx_user_data)
{
    struct session_send *send = task_user_data.ptr;
    struct objects *objs = ctx_user_data.ptr;
    if (send != NULL) {
        session_fail(objs, send->session);
        if (send->session != NULL)
            --send->session->sends_pending;
    }
    doca_task_free(doca_comch_task_send_as_task(task));
    free(send);
}

doca_error_t
server_send_msg_conn(struct objects *objs, struct doca_comch_connection *connection,
                     const char *msg, size_t len)
{
    struct doca_comch_task_send *task;
    struct session_send *send;
    doca_error_t result;
    if (objs == NULL || connection == NULL || msg == NULL || len > UINT32_MAX)
        return DOCA_ERROR_INVALID_VALUE;
    send = malloc(sizeof(*send) + len);
    if (send == NULL)
        return DOCA_ERROR_NO_MEMORY;
    send->session = session_get(objs, connection, false);
    memcpy(send->bytes, msg, len);
    result = doca_comch_server_task_send_alloc_init(objs->cc_server, connection,
                                                   send->bytes, (uint32_t)len, &task);
    if (result != DOCA_SUCCESS) {
        free(send);
        return result;
    }
    doca_task_set_user_data(doca_comch_task_send_as_task(task), (union doca_data){.ptr = send});
    result = doca_task_submit(doca_comch_task_send_as_task(task));
    if (result != DOCA_SUCCESS) {
        doca_task_free(doca_comch_task_send_as_task(task));
        free(send);
        return result;
    }
    if (send->session != NULL)
        ++send->session->sends_pending;
    return DOCA_SUCCESS;
}

doca_error_t server_send_msg(struct objects *objs, const char *msg, size_t len)
{
    return server_send_msg_conn(objs, objs->connection, msg, len);
}

static doca_error_t
session_send_frame(struct objects *objs, struct dmesh_session *session, uint16_t type,
                   uint32_t flow_id, uint32_t generation, const void *payload,
                   size_t length, int32_t status)
{
    uint8_t frame[DMESH_SESSION_MAX_FRAME];
    size_t len = dmesh_session_encode(frame, sizeof(frame), type, flow_id, generation,
                                      status, payload, length);
    if (len == 0 || session == NULL || session->connection == NULL || session->closing)
        return DOCA_ERROR_INVALID_VALUE;
    return server_send_msg_conn(objs, session->connection, (const char *)frame, len);
}

doca_error_t server_send_flow_msg(struct dmesh_conn *conn, uint16_t type,
                                 const void *payload, size_t length, int32_t status)
{
    return session_send_frame(conn->objs, conn->session, type, conn->flow_id,
                              conn->generation, payload, length, status);
}

/**
 * Callback for server message recv event
 *
 * @event [in]: Recv event object
 * @recv_buffer [in]: Message buffer
 * @msg_len [in]: Message len
 * @comch_connection [in]: Connection the message was received on
 */
static void session_flow_error(struct objects *objs, struct dmesh_session *s,
                               const struct dmesh_session_header *h, int status)
{
    (void)session_send_frame(objs, s, DMESH_SESSION_ERROR, h->flow_id,
                             h->generation, NULL, 0, status);
}

static void server_message_recv_callback(struct doca_comch_event_msg_recv *event,
                                          uint8_t *buffer, uint32_t len,
                                          struct doca_comch_connection *connection)
{
    struct doca_comch_server *server = doca_comch_server_get_server_ctx(connection);
    union doca_data data;
    struct dmesh_session_header h;
    const uint8_t *payload;
    struct objects *objs;
    struct dmesh_session *s;
    struct dmesh_conn *conn;
    unsigned idx;
    (void)event;
    if (doca_ctx_get_user_data(doca_comch_server_as_ctx(server), &data) != DOCA_SUCCESS)
        return;
    objs = data.ptr;
    /* Only the connection callback admits a physical peer. In particular,
     * a late receive after disconnect must not recreate a retired session. */
    s = session_get(objs, connection, false);
    if (s == NULL)
        return;
    if (dmesh_session_decode(buffer, len, &h, &payload) != 0) {
        DOCA_LOG_WARN("Rejecting incompatible or malformed Comch session protocol");
        session_fail(objs, s);
        return;
    }
    if (s->closing)
        return;
    if (h.type == DMESH_SESSION_HELLO) {
        s->negotiated = true;
        s->hello_pending = true;
        return;
    }
    if (!s->negotiated || h.flow_id == 0 || h.flow_id > DMESH_MAX_CONNECTIONS) {
        session_fail(objs, s);
        return;
    }
    idx = h.flow_id - 1;
    conn = dmesh_flow_get(objs, connection, h.flow_id, h.generation);
    if (h.type == DMESH_SESSION_OPEN) {
        if (h.payload_len != sizeof(struct dmesh_export_metadata_msg)) {
            session_flow_error(objs, s, &h, EINVAL);
            return;
        }
        if (h.generation < s->generation[idx] ||
            (h.generation == s->generation[idx] && s->closed[idx])) {
            session_flow_error(objs, s, &h, ESTALE);
            return;
        }
        if (conn != NULL) {
            if (conn->ready_sent)
                (void)server_send_flow_msg(conn, DMESH_SESSION_READY, NULL, 0, 0);
            else if (conn->error_status)
                session_flow_error(objs, s, &h, conn->error_status);
            return;
        }
        for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
            if (objs->conns[i].state != DMESH_CONN_FREE &&
                objs->conns[i].session == s && objs->conns[i].flow_id == h.flow_id) {
                session_flow_error(objs, s, &h, EBUSY);
                return;
            }
        if (s->close_pending[idx]) {
            session_flow_error(objs, s, &h, EBUSY);
            return;
        }
        s->generation[idx] = h.generation;
        s->closed[idx] = false;
        s->close_status[idx] = 0;
        conn = dmesh_flow_open(objs, connection, h.flow_id, h.generation);
        if (conn == NULL) {
            s->closed[idx] = true;
            s->close_status[idx] = 0; /* no flow resources were acquired */
            session_flow_error(objs, s, &h, ENOSPC);
            return;
        }
        conn->session = s;
        conn->pending_metadata = malloc(sizeof(*conn->pending_metadata));
        if (conn->pending_metadata == NULL) {
            conn->error_status = ENOMEM;
            conn->state = DMESH_CONN_ERROR;
        } else {
            memcpy(conn->pending_metadata, payload, sizeof(*conn->pending_metadata));
        }
        return;
    }
    if (h.type == DMESH_SESSION_CLOSE) {
        if (conn != NULL) {
            conn->close_requested = true;
            s->close_pending[idx] = false;
            conn->state = DMESH_CONN_CLOSING;
        } else if (h.generation == s->generation[idx] && s->closed[idx]) {
            s->close_pending[idx] = true;
        } else {
            bool id_live = false;
            for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
                id_live |= objs->conns[i].state != DMESH_CONN_FREE &&
                           objs->conns[i].session == s && objs->conns[i].flow_id == h.flow_id;
            if (!id_live && h.generation >= s->generation[idx]) {
                s->generation[idx] = h.generation;
                s->closed[idx] = true;
                s->close_status[idx] = 0;
                s->close_pending[idx] = true;
            } else {
                /* A stale identity has no resources; never touch a newer flow. */
                (void)session_send_frame(objs, s, DMESH_SESSION_CLOSED, h.flow_id,
                                         h.generation, NULL, 0, 0);
            }
        }
        return;
    }
    session_flow_error(objs, s, &h, EPROTO);
}

static void dmesh_doca_comch_server_conn_ev_cb(struct doca_comch_event_connection_status_changed *event,
					       struct doca_comch_connection *comch_connection,
					       uint8_t change_success)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	if (change_success == 0) {
		DOCA_LOG_ERR("Failed connection received");
		return;
	}

	(void)event;
	comch_server = doca_comch_server_get_server_ctx(comch_connection);

	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;
	// objs->connection = comch_connection;

	DOCA_LOG_INFO("New connection established with client");
}

/**
 * Callback for disconnection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the disconnection was successful or not
 */
static void dmesh_doca_comch_server_disconn_ev_cb(struct doca_comch_event_connection_status_changed *event,
						struct doca_comch_connection *comch_connection,
						uint8_t change_success)
{
	(void)event;
	(void)comch_connection;

	if (change_success == 0)
		DOCA_LOG_ERR("Failed disconnection received");
}

/**
 * Callback for connection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the connection was successful or not
 */
static void server_connection_event_callback(struct doca_comch_event_connection_status_changed *event,
					     struct doca_comch_connection *comch_connection,
					     uint8_t change_success)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	if (change_success == 0) {
		DOCA_LOG_ERR("Failed connection received");
		return;
	}

	(void)event;

	comch_server = doca_comch_server_get_server_ctx(comch_connection);

	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get user data from ctx with error = %s", doca_error_get_name(result));
		return;
	}

	objs = (struct objects *)user_data.ptr;
	objs->connection = comch_connection;

	DOCA_LOG_INFO("New connection established with client");

	/* Physical Comch registration does not consume a data/DPA flow slot. */
    if (session_get(objs, comch_connection, true) == NULL)
        (void)doca_comch_server_disconnect(objs->cc_server, comch_connection);
}

/**
 * Callback for disconnection event
 *
 * @event [in]: Connection event object
 * @comch_connection [in]: Connection object
 * @change_success [in]: Whether the disconnection was successful or not
 */
static void server_disconnection_event_callback(struct doca_comch_event_connection_status_changed *event,
						struct doca_comch_connection *comch_connection,
						uint8_t change_success)
{
	union doca_data user_data;
	struct doca_comch_server *comch_server;
	struct objects *objs;
	doca_error_t result;

	(void)event;

	if (change_success == 0)
		DOCA_LOG_ERR("Failed disconnection received");

	/* Return the DPA thread owned by this connection to the pool */
	comch_server = doca_comch_server_get_server_ctx(comch_connection);
	if (comch_server == NULL)
		return;
	result = doca_ctx_get_user_data(doca_comch_server_as_ctx(comch_server), &user_data);
	if (result != DOCA_SUCCESS || user_data.ptr == NULL)
		return;

	objs = (struct objects *)user_data.ptr;

	/* Every logical flow on this session closes; other channels survive. */
    struct dmesh_session *session = session_get(objs, comch_connection, false);
    session_fail(objs, session);
    if (change_success != 0)
        session_peer_detached(objs, session);
}

doca_error_t
dmesh_doca_init_comch_server(struct dmesh_doca_objects *objs, const char *server_name, 
							 bool enable_fast_path)
{
	doca_error_t result;
	struct doca_ctx *ctx;
	union doca_data user_data;
	uint32_t max_msg_size, max_rq_size;

	/* create DOCA comch server */
	result = doca_comch_server_create(objs->dev, objs->rep_dev,
				server_name, &objs->cc_server);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create server with error = %s", doca_error_get_name(result));
		return result;
	}

	ctx = doca_comch_server_as_ctx(objs->cc_server);

	/* connect the ctx to the PE */
    result = doca_pe_connect_ctx(objs->pe, ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding pe context to server with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* configure send tasks completion Callbacks */
	result = doca_comch_server_task_send_set_conf(objs->cc_server,
			server_send_task_completion_callback,
			server_send_task_completion_err_callback,
			CC_SEND_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting send task cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* configure recv callback */
	result = doca_comch_server_event_msg_recv_register(objs->cc_server, server_message_recv_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding message recv event cb with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* configure connection event callback */
	result = doca_comch_server_event_connection_status_changed_register(objs->cc_server,
									dmesh_doca_comch_server_conn_ev_cb,
									dmesh_doca_comch_server_disconn_ev_cb);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding connection status changed event cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }      

	/* Config the data_path related events */
	if (enable_fast_path) {
		result = doca_comch_server_event_consumer_register(objs->cc_server,
									dmesh_doca_server_new_consumer_cb,
									expired_consumer_callback);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed adding consumer event cb with error = %s", doca_error_get_name(result));
			goto destroy_server;
		}
	}

	/* set max message and recv queue sizes as device-supported max size */
	result = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(objs->dev), &max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    } 

    result = doca_comch_cap_get_max_recv_queue_size(doca_dev_as_devinfo(objs->dev), &max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }
    
    result = doca_comch_server_set_max_msg_size(objs->cc_server, max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_set_recv_queue_size(objs->cc_server, max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* set user data for the ctx */
	user_data.ptr = (void *)objs;
	result = doca_ctx_set_user_data(ctx, user_data);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
		goto destroy_server;
	}

	/* start the comch server ctx */
	result = doca_ctx_start(ctx);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start server context with error = %s", doca_error_get_name(result));
		goto destroy_server;
	}

	return result;

destroy_server:
	doca_comch_server_destroy(objs->cc_server);
	objs->cc_server = NULL;
	return result;
}

doca_error_t
start_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path)
{
    doca_error_t result;
    struct doca_ctx *ctx;
    union doca_data udata;
    uint32_t max_msg_size, max_rq_size;

	/* create a progress engine */
	if (!objs->pe) {
		result = doca_pe_create(&(objs->pe));
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed creating pe with error = %s", doca_error_get_name(result));
			return result;
		}
	}

	/* The event-driven driver arms this PE (doca_pe_request_notification). In the
	 * default SELECTIVE event mode an armed PE only progresses contexts that
	 * received an event, which stalls outbound work such as the consumer
	 * registration handshake. PROGRESS_ALL keeps doca_pe_progress unconditional. */
	result = doca_pe_set_event_mode(objs->pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set PE event mode: %s", doca_error_get_name(result));
		goto destroy_pe;
	}
	
    result = doca_comch_server_create(objs->dev, objs->rep_dev,
                server_name, &objs->cc_server);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create server with error = %s", doca_error_get_name(result));
        goto destroy_pe;
    }
    objs->is_server = true;

    ctx = doca_comch_server_as_ctx(objs->cc_server);

    result = doca_pe_connect_ctx(objs->pe, ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding pe context to server with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_task_send_set_conf(objs->cc_server,
                server_send_task_completion_callback,
                server_send_task_completion_err_callback,
                CC_SEND_TASK_NUM);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed setting send task cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_event_msg_recv_register(objs->cc_server, server_message_recv_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding message recv event cb with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_event_connection_status_changed_register(objs->cc_server,
                                        server_connection_event_callback,
                                        server_disconnection_event_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed adding connection status changed event cbs with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }                                        

    /* Config the data_path related events */
	if (is_fast_path) {
		result = doca_comch_server_event_consumer_register(objs->cc_server,
									server_new_consumer_callback,
									expired_consumer_callback);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed adding consumer event cb with error = %s", doca_error_get_name(result));
			goto destroy_server;
		}
	}

    result = doca_comch_cap_get_max_msg_size(doca_dev_as_devinfo(objs->dev), &max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    } 

    result = doca_comch_cap_get_max_recv_queue_size(doca_dev_as_devinfo(objs->dev), &max_rq_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to get max recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }
    
    result = doca_comch_server_set_max_msg_size(objs->cc_server, max_msg_size);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set max message size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_comch_server_set_recv_queue_size(objs->cc_server, CC_RECV_QUEUE_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set recv queue size with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    udata.ptr = (void *)objs;
    result = doca_ctx_set_user_data(ctx, udata);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to set ctx user data with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

    result = doca_ctx_start(ctx);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start server context with error = %s", doca_error_get_name(result));
        goto destroy_server;
    }

	/* Server ctx is live but not yet connected. The connection is established
	 * later, event-driven, by progressing objs->pe (see dmesh_doca_ctrl_advance). */
	objs->phase = DMESH_DOCA_STATE_SERVER_STARTED;

    return DOCA_SUCCESS;

destroy_server:
    doca_comch_server_destroy(objs->cc_server);
    objs->cc_server = NULL;
destroy_pe:
    doca_pe_destroy(objs->pe);
    objs->pe = NULL;
    return result;
}

/*
 * Baseline (busy-poll) control-path server init: start the server, then spin on
 * the control PE until the host connects. Preserved for the original
 * run_dpu_worker() path; new event-driven code uses start_comch_ctrl_path_server
 * plus the dmesh_doca_ctrl_* helpers instead.
 */
doca_error_t
init_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path)
{
    doca_error_t result;
	struct timespec ts = {
		.tv_nsec = SLEEP_IN_NANOS,
	};

	result = start_comch_ctrl_path_server(server_name, objs, is_fast_path);
	if (result != DOCA_SUCCESS)
		return result;

	while (objs->connection == NULL) {
		if (doca_pe_progress(objs->pe) == 0)
			nanosleep(&ts, &ts);
	}

	DOCA_LOG_INFO("Server connection established");

    return DOCA_SUCCESS;
}

doca_error_t
export_dpa_comp_to_host(struct objects *objs)
{
	doca_error_t result;
	struct dmesh_dpa_comp_msg dpa_comp_msg;
	dpa_comp_msg.type = DMESH_MSG_EXPORT_DPA_COMP;

	result = doca_comch_consumer_completion_get_dpa_handle(objs->dpa_comch->consumer_comp,
									&dpa_comp_msg.dpa_consumer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA consumer completion handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_dpa_completion_get_dpa_handle(objs->dpa_comch->producer_comp,
									&dpa_comp_msg.dpa_producer_comp);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA producer completion handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_comch_producer_get_dpa_handle(objs->dpa_comch->recv.producer,
									&dpa_comp_msg.dpa_producer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA producer handle - %s",
				doca_error_get_name(result));
		return result;
	}
	result = doca_comch_consumer_get_dpa_handle(objs->dpa_comch->send.consumer,
									&dpa_comp_msg.dpa_consumer);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA consumer handle - %s",
				doca_error_get_name(result));
		return result;
	}

	DOCA_LOG_INFO("dpa_consumer_comp: 0x%lx, dpa_producer_comp: 0x%lx, dpa_producer: 0x%lx, dpa_consumer: 0x%lx",
			dpa_comp_msg.dpa_consumer_comp,
			dpa_comp_msg.dpa_producer_comp,
			dpa_comp_msg.dpa_producer,
			dpa_comp_msg.dpa_consumer);

	return server_send_msg(objs, (const char *)&dpa_comp_msg, sizeof(dpa_comp_msg));
}

/*
 * ---------------------------------------------------------------------------
 * Event-driven (on-demand) control-path helpers.
 *
 * These replace the busy-poll loops that waited on the control PE (objs->pe).
 * A caller (the C test driver in dpu_worker.c, or later the Rust AsyncFd loop)
 * uses them as: get_fd once, then repeatedly arm -> wait on fd -> clear_and_drain
 * -> advance, until the state machine reaches DMESH_DOCA_STATE_RUNNING.
 * ---------------------------------------------------------------------------
 */

doca_error_t
dmesh_doca_ctrl_get_fd(struct objects *objs, int *out_fd)
{
	doca_error_t result;
	doca_notification_handle_t handle;

	if (objs == NULL || objs->pe == NULL || out_fd == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	result = doca_pe_get_notification_handle(objs->pe, &handle);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get control PE notification handle: %s", doca_error_get_name(result));
		return result;
	}

	*out_fd = (int)handle;
	return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_ctrl_arm(struct objects *objs)
{
	if (objs == NULL || objs->pe == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	return doca_pe_request_notification(objs->pe);
}

doca_error_t
dmesh_doca_ctrl_drain(struct objects *objs)
{
	if (objs == NULL || objs->pe == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	while (doca_pe_progress(objs->pe) != 0)
		;

	return DOCA_SUCCESS;
}

doca_error_t
dmesh_doca_ctrl_clear_and_drain(struct objects *objs, int fd)
{
	doca_error_t result;

	if (objs == NULL || objs->pe == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	result = doca_pe_clear_notification(objs->pe, (doca_notification_handle_t)fd);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to clear control PE notification: %s", doca_error_get_name(result));
		return result;
	}

	/* Drain until no more progress: mandatory before re-arming, otherwise the
	 * fd may not signal again (no new edge) and the waiter would deadlock. */
	while (doca_pe_progress(objs->pe) != 0)
		;

	return DOCA_SUCCESS;
}

/* Tear down all DOCA resources a connection acquired, so its slot (and its DPA
 * pool thread) can be reused without restarting the proxy. Runs from advance()
 * - never inside the disconnect callback - so progressing PEs here is safe. */
static void
dmesh_conn_teardown(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;

    DOCA_LOG_INFO("Tearing down connection slot %ld", conn - objs->conns);

    /* Quiesce the DPA thread first: signal its poll loop to exit, wait for the
     * ack, then stop it. Its DPA-side msgq/completion ctxs can only idle (and
     * later be destroyed) once the thread is no longer hot-looping. */
    dmesh_doca_dpa_thread_quiesce(conn->dpa_thread);
    /* Stop the runnable thread while its completion contexts are still
     * attached (undo doca_dpa_thread_run); the later destroy then succeeds.
     * DMESH_THREAD_STOP-gated. */
    dmesh_doca_dpa_thread_stop_only(conn->dpa_thread);

    /* Datapath consumer (DPU side, on the shared consumer PE). */
    if (conn->consumer != NULL) {
        enum doca_ctx_states st;
        int spins = 0;
        struct doca_ctx *cctx = doca_comch_consumer_as_ctx(conn->consumer);

        if (doca_ctx_get_state(cctx, &st) == DOCA_SUCCESS && st != DOCA_CTX_STATE_IDLE) {
            (void)doca_ctx_stop(cctx);
            while (spins++ < 100000 &&
                   doca_ctx_get_state(cctx, &st) == DOCA_SUCCESS && st != DOCA_CTX_STATE_IDLE)
                doca_pe_progress(objs->consumer_pe);
        }
        (void)doca_comch_consumer_destroy(conn->consumer);
        conn->consumer = NULL;
    }
    if (conn->consumer_mem != NULL) {
        clean_local_mem_bufs(conn->consumer_mem);
        free(conn->consumer_mem);
        conn->consumer_mem = NULL;
    }

    /* DPA comch (both MsgQs + completions) and the DPA thread itself. */
    dmesh_doca_dpa_comch_destroy(conn);
    dmesh_doca_dpa_thread_destroy(conn->dpa_thread);

    /* DPA buffer array over the DMA ring. */
    if (conn->buf_arr != NULL) {
        (void)doca_buf_arr_stop(conn->buf_arr);
        (void)doca_buf_arr_destroy(conn->buf_arr);
        conn->buf_arr = NULL;
    }

    /* Remote mmaps imported from the host's metadata message. */
    if (conn->ring_mmap != NULL) {
        (void)doca_mmap_destroy(conn->ring_mmap);
        conn->ring_mmap = NULL;
    }
    if (conn->sndbuf.mmap != NULL) {
        (void)doca_mmap_destroy(conn->sndbuf.mmap);
        conn->sndbuf.mmap = NULL;
    }
    if (conn->rcvbuf.mmap != NULL) {
        (void)doca_mmap_destroy(conn->rcvbuf.mmap);
        conn->rcvbuf.mmap = NULL;
    }
    conn->sndbuf.buf = NULL;
    conn->rcvbuf.buf = NULL;

    /* Local staging buffer + its mmap. */
    if (conn->local_mmap != NULL) {
        destroy_mmap_and_free_buffer(conn->local_mmap, conn->dma_buffer);
        conn->local_mmap = NULL;
        conn->dma_buffer = NULL;
    }

    /* Reverse (response) path resources owned by the DPU. */
    if (conn->rcv_ring != NULL) {
        free_dma_ring(conn->rcv_ring);
        conn->rcv_ring = NULL;
    }
    if (conn->tx_staging_mmap != NULL) {
        destroy_mmap_and_free_buffer(conn->tx_staging_mmap, conn->tx_staging);
        conn->tx_staging_mmap = NULL;
        conn->tx_staging = NULL;
    }
    conn->tx_staging_len = 0;
    conn->tx_pos = 0;
    conn->reverse_exported = false;
    conn->push_seq = 0;
    conn->push_pos = 0;
    conn->push_len = 0;
    conn->push_state = 0;
    conn->push_unreported_pos = conn->push_unreported_len = 0;
    conn->push_unreported_seq = 0;
    conn->push_shadow = NULL;

    /* Per-connection DMA engine (ctx, inventory, task pool, recv/pending rings). */
    cleanup_dma_tasks(conn);

    /* Return the DPA pool thread and unbind the slot. */
    dmesh_dpa_thread_pool_release(objs, conn);

    /* Release the server-side comch connection object. Without this the
     * firmware channel resources of dead clients leak, and the NEXT client
     * registration fails at devx-object creation (syndrome 0xe5300 ->
     * DOCA_ERROR_CONNECTION_ABORTED at client ctx start) - the historical
     * "reconnect needs a proxy restart" limitation. DOCA_ERROR_AGAIN means
     * the ctrl send queue is full; progress it and retry (we run from
     * advance(), never inside a PE callback, so progressing here is safe). */
    if (conn->connection != NULL) {
        doca_error_t dres;
        int spins = 0;

        do {
            dres = doca_comch_server_disconnect(objs->cc_server, conn->connection);
            if (dres != DOCA_ERROR_AGAIN)
                break;
            (void)doca_pe_progress(objs->pe);
        } while (spins++ < 100000);
        if (dres != DOCA_SUCCESS)
            DOCA_LOG_WARN("Server-side disconnect of dead connection: %s",
                          doca_error_get_name(dres));
    }

    /* Reset the slot for reuse. */
    {
        struct dmesh_flow_id flow = conn->flow;
        (void)flow;
        conn->connection = NULL;
        conn->remote_consumer_id = 0;
        conn->dpa_thread = NULL;
        conn->state = DMESH_CONN_FREE;
        memset(&conn->flow, 0, sizeof(conn->flow));
    }
}

/* A successful close is a resource fence, not just a software state change. */
static doca_error_t flow_mmap_destroy(struct doca_mmap **mmap)
{
    if (*mmap == NULL)
        return DOCA_SUCCESS;
    doca_error_t result = doca_mmap_destroy(*mmap);
    if (result == DOCA_SUCCESS)
        *mmap = NULL;
    return result;
}

static doca_error_t flow_local_buffer_destroy(struct doca_mmap **mmap, void **buffer)
{
    if (*mmap == NULL)
        return DOCA_SUCCESS;
    doca_error_t result = flow_mmap_destroy(mmap);
    if (result == DOCA_SUCCESS) {
        free(*buffer);
        *buffer = NULL;
    }
    return result;
}

static bool flow_has_resources(const struct dmesh_conn *conn)
{
    return conn->dpa_thread || conn->dpa_comch || conn->dma_ctx || conn->buf_arr ||
           conn->ring_mmap || conn->sndbuf.mmap || conn->rcvbuf.mmap ||
           conn->local_mmap || conn->rcv_ring || conn->tx_staging_mmap;
}

static doca_error_t dmesh_flow_teardown_checked(struct dmesh_conn *conn)
{
    doca_error_t result;
    if (getenv("DMESH_NO_TEARDOWN") != NULL && flow_has_resources(conn))
        return DOCA_ERROR_NOT_SUPPORTED;
    conn->dma_closing = true;
    result = dmesh_doca_dpa_quiesce_step(conn);
    if (result != DOCA_SUCCESS)
        return result;
    result = cleanup_dma_tasks(conn);
    if (result != DOCA_SUCCESS)
        return result;
    result = dmesh_doca_dpa_comch_destroy_checked(conn);
    if (result != DOCA_SUCCESS)
        return result;
    result = dmesh_doca_dpa_thread_destroy_checked(conn->dpa_thread);
    if (result != DOCA_SUCCESS)
        return result;
    if (conn->buf_arr != NULL) {
        result = doca_buf_arr_destroy(conn->buf_arr); /* also stops the array */
        if (result != DOCA_SUCCESS)
            return result;
        conn->buf_arr = NULL;
    }
    if ((result = flow_mmap_destroy(&conn->ring_mmap)) != DOCA_SUCCESS ||
        (result = flow_mmap_destroy(&conn->sndbuf.mmap)) != DOCA_SUCCESS ||
        (result = flow_mmap_destroy(&conn->rcvbuf.mmap)) != DOCA_SUCCESS ||
        (result = flow_local_buffer_destroy(&conn->local_mmap, &conn->dma_buffer)) != DOCA_SUCCESS)
        return result;
    if (conn->rcv_ring != NULL) {
        result = flow_local_buffer_destroy(&conn->rcv_ring->mmap, &conn->rcv_ring->buffer);
        if (result != DOCA_SUCCESS)
            return result;
        free(conn->rcv_ring);
        conn->rcv_ring = NULL;
    }
    result = flow_local_buffer_destroy(&conn->tx_staging_mmap, &conn->tx_staging);
    if (result != DOCA_SUCCESS)
        return result;
    dmesh_dpa_thread_pool_release(conn->objs, conn);
    conn->dpa_thread = NULL;
    free(conn->pending_metadata);
    conn->pending_metadata = NULL;
    conn->sndbuf.buf = conn->rcvbuf.buf = NULL;
    return DOCA_SUCCESS;
}

static int flow_close_errno(doca_error_t result)
{
    if (result == DOCA_ERROR_NOT_SUPPORTED)
        return EOPNOTSUPP;
    if (result == DOCA_ERROR_TIME_OUT)
        return ETIMEDOUT;
    if (result == DOCA_ERROR_AGAIN || result == DOCA_ERROR_IN_PROGRESS || result == DOCA_ERROR_IN_USE)
        return EBUSY;
    return EIO;
}

static void dmesh_flow_close_advance(struct dmesh_conn *conn)
{
    struct dmesh_session *session = conn->session;
    unsigned idx = conn->flow_id - 1;
    /* SDK/DPA quiescence does not retire pointers held by proxy IO tasks.
     * The driver clears those pointers under its IO mutex before this ACK. */
    if (conn->objs->external_readers && !conn->readers_detached)
        return;
    if (!conn->close_requested && !session->closing) {
        /* A proxy-side EOF first asks the host to stop its reverse reader. */
        conn->error_status = ECONNRESET;
        conn->error_sent = false;
        conn->state = DMESH_CONN_ERROR;
        return;
    }
    if (!conn->close_requested && session->closing && conn->reverse_exported &&
        !DMESH_FLOW_USES_PUSH(conn->flow.mode)) {
        /* Lost control transport cannot prove that the host DPA stopped
         * reading exported DPU memory. Reserve this slot until recovery. */
        conn->quarantined = true;
        conn->dma_closing = true;
        /* Stop this DPU's reader as far as possible, but its completion is
         * not evidence that the remote host reader released our exports. */
        (void)dmesh_doca_dpa_quiesce_step(conn);
        conn->error_status = ECONNRESET;
        conn->error_sent = true;
        conn->state = DMESH_CONN_ERROR;
        return;
    }
    doca_error_t result = dmesh_flow_teardown_checked(conn);
    if (result == DOCA_ERROR_AGAIN)
        return; /* the DPA kernel has not stopped yet; retry on the next pass */
    if (result != DOCA_SUCCESS) {
        conn->error_status = flow_close_errno(result);
        conn->error_sent = true; /* report this attempt as CLOSED(error) */
        conn->state = DMESH_CONN_ERROR;
        if (!session->closing) {
            session->close_status[idx] = conn->error_status;
            session->close_pending[idx] = true;
        } else {
            conn->quarantined = true;
        }
        return;
    }
    session->closed[idx] = true;
    session->close_status[idx] = 0;
    session->close_pending[idx] = !session->closing && conn->close_requested;
    conn->connection = NULL;
    conn->state = DMESH_CONN_FREE;
}

static uint64_t session_now_ms(void)
{
    struct timespec now = {0};
    (void)clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u;
}

/* The physical connection belongs to the channel session. Flow cleanup must
 * never disconnect a sibling flow. Keep failed-session records occupied while
 * quarantined exported buffers still belong to them. */
static void sessions_advance(struct objects *objs)
{
    uint64_t now = session_now_ms();
    for (int n = 0; n < DMESH_MAX_SESSIONS; ++n) {
        struct dmesh_session *s = &objs->sessions[n];
        bool live = false, closing_flow = false;
        if (!s->occupied)
            continue;
        if (!s->negotiated && !s->closing && now >= s->hello_deadline_ms)
            session_fail(objs, s);
        if (!s->closing) {
            if (s->hello_pending &&
                session_send_frame(objs, s, DMESH_SESSION_HELLO_ACK, 0, 0, NULL, 0, 0) == DOCA_SUCCESS)
                s->hello_pending = false;
            for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
                if (s->close_pending[i] &&
                    session_send_frame(objs, s, DMESH_SESSION_CLOSED, i + 1, s->generation[i],
                                       NULL, 0, s->close_status[i]) == DOCA_SUCCESS)
                    s->close_pending[i] = false;
            continue;
        }
        for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
            struct dmesh_conn *conn = &objs->conns[i];
            if (conn->state != DMESH_CONN_FREE && conn->session == s) {
                live = true;
                closing_flow |= conn->state == DMESH_CONN_CLOSING;
            }
        }
        if (closing_flow || s->sends_pending != 0)
            continue;
        if (s->connection != NULL) {
            doca_error_t result = doca_comch_server_disconnect(objs->cc_server, s->connection);
            if (result != DOCA_SUCCESS && result != DOCA_ERROR_NOT_CONNECTED)
                continue; /* including AGAIN: retry after the shared PE progresses */
            /* A peer may have disconnected before its event was observed.
             * NOT_CONNECTED is terminal, never a send-queue retry condition. */
            session_peer_detached(objs, s);
        }
        if (!live)
            memset(s, 0, sizeof(*s));
    }
}

/* Advance one connection's setup state machine. A failure parks only this
 * connection (DMESH_CONN_ERROR); other connections keep running. */
static void
dmesh_doca_conn_advance(struct dmesh_conn *conn)
{
	struct objects *objs = conn->objs;
	doca_error_t result;

	switch (conn->state) {
	case DMESH_CONN_NEW:
        if (conn->multiplexed && conn->pending_metadata != NULL) {
            result = process_export_metadata_msg(conn, conn->pending_metadata);
            free(conn->pending_metadata);
            conn->pending_metadata = NULL;
            if (result != DOCA_SUCCESS) {
                conn->error_status = EINVAL;
                goto error;
            }
        }
		/* The connection callback normally assigns a pool thread; retry here
		 * in case the client connected before the pool existed. */
		if (conn->dpa_thread == NULL)
			conn->dpa_thread = dmesh_dpa_thread_pool_alloc(objs, conn);
		if (conn->dpa_thread == NULL) {
			DOCA_LOG_ERR("No DPA thread available for connection %p", (void *)conn->connection);
			goto error;
		}

		DOCA_LOG_INFO("Setting up DMA engine, datapath consumer and DPA msgq for connection %p",
			      (void *)conn->connection);

		/* private DMA engine (own QP + task pool) for this connection */
		result = init_dma_tasks(conn, DMA_TASKS_PER_CONN);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init DMA tasks: %s", doca_error_get_name(result));
			goto error;
		}

        if (!conn->multiplexed) {
		result = init_comch_datapath_consumer(conn);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init datapath consumer: %s", doca_error_get_name(result));
			goto error;
		}

        }
		result = init_comch_dpa_msgq(conn, objs->consumer_pe);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init comch DPA msgq: %s", doca_error_get_name(result));
			goto error;
		}

		conn->state = conn->multiplexed ? DMESH_CONN_AWAIT_METADATA : DMESH_CONN_CONSUMER_STARTING;
		break;

	case DMESH_CONN_CONSUMER_STARTING: {
		/* The consumer registration handshake with the peer is asynchronous;
		 * poll its ctx state (progressed by the driver's consumer-PE drain).
		 * If the peer died mid-handshake the ctx falls back to IDLE - park
		 * the slot instead of waiting forever. */
		enum doca_ctx_states cstate;

		if (conn->consumer == NULL ||
		    doca_ctx_get_state(doca_comch_consumer_as_ctx(conn->consumer), &cstate) != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Consumer vanished during startup for connection %p",
				     (void *)conn->connection);
			goto error;
		}

		if (cstate == DOCA_CTX_STATE_RUNNING) {
			DOCA_LOG_INFO("Consumer running for connection %p", (void *)conn->connection);
			conn->state = DMESH_CONN_AWAIT_METADATA;
		} else if (cstate == DOCA_CTX_STATE_IDLE) {
			DOCA_LOG_ERR("Consumer registration failed for connection %p (peer gone?)",
				     (void *)conn->connection);
			goto error;
		}
		/* STARTING/STOPPING: not ready yet; retry on the next advance */
		break;
	}

	case DMESH_CONN_AWAIT_METADATA:
		/* All three remote mmaps arrive in a single metadata message
		 * (process_export_metadata_msg), so they become ready together. */
		if (conn->ring_mmap == NULL || conn->sndbuf.mmap == NULL || conn->rcvbuf.mmap == NULL)
			break; /* not ready yet: awaiting this host's metadata export */

		DOCA_LOG_INFO("Received remote DMA metadata; completing DPA setup for connection %p",
			      (void *)conn->connection);

		result = setup_dpa_buf_array(conn, DMA_RING_SIZE, conn->ring_mmap);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to setup DPA buffer array: %s", doca_error_get_name(result));
			goto error;
		}

		result = alloc_buffer_and_set_mmap(&conn->local_mmap, objs->dev,
						   &conn->dma_buffer, BUFFER_SIZE,
						   DOCA_ACCESS_FLAG_PCI_READ_WRITE);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to allocate DMA buffer: %s", doca_error_get_name(result));
			goto error;
		}

		result = dmesh_doca_run_dpa_thread(conn);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to run DPA thread: %s", doca_error_get_name(result));
			goto error;
		}

		result = send_dma_request_to_dpa(conn);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to send DMA request to DPA: %s", doca_error_get_name(result));
			goto error;
		}

		conn->state = DMESH_CONN_RUNNING;
		DOCA_LOG_INFO("Connection %p is running", (void *)conn->connection);
		/* fall through to export the reverse path metadata this same tick */
		/* fallthrough */

	case DMESH_CONN_RUNNING:
		/* Reverse (DPU->host) path setup, once per connection.
		 *
		 * CLIENT flows (안 1): allocate rcv_ring + tx_staging and export them;
		 * the host's DPA thread pulls responses into the host rcvbuf.
		 *
		 * BACKEND flows (안 2): the host runs NO DPA - the DPU pushes with
		 * this connection's doca_dma engine straight into the host rcvbuf's
		 * push layout (dmesh_dma_push_backend). Only a local (non-exported)
		 * tx_staging is needed for staging + the descriptor shadow. */
		if (!conn->reverse_exported) {
			if (DMESH_FLOW_USES_PUSH(conn->flow.mode)) {
				if (conn->tx_staging == NULL) {
					result = alloc_buffer_and_set_mmap(&conn->tx_staging_mmap,
									   objs->dev, &conn->tx_staging,
									   BUFFER_SIZE,
									   DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
					if (result != DOCA_SUCCESS) {
						DOCA_LOG_ERR("Failed to alloc backend push staging (will retry): %s",
							     doca_error_get_name(result));
						break;
					}
					conn->tx_staging_len = BUFFER_SIZE;
					conn->push_seq = 0;
					conn->push_pos = 0;
					conn->push_state = 0;
					conn->push_unreported_pos = conn->push_unreported_len = 0;
					conn->push_unreported_seq = 0;
				}
				conn->reverse_exported = true;   /* nothing to export */
				DOCA_LOG_INFO("Push channel ready (mode %u) for %u.%u.%u.%u:%u",
					      conn->flow.mode,
					      conn->flow.dst_ip & 0xff, (conn->flow.dst_ip >> 8) & 0xff,
					      (conn->flow.dst_ip >> 16) & 0xff, (conn->flow.dst_ip >> 24) & 0xff,
					      conn->flow.dst_port);
			} else {
				result = export_rcv_ring_metadata(conn);
				if (result != DOCA_SUCCESS)
					DOCA_LOG_ERR("Failed to export reverse metadata (will retry): %s",
						     doca_error_get_name(result));
			}
		}
        if (conn->multiplexed && conn->reverse_exported && !conn->ready_sent &&
            server_send_flow_msg(conn, DMESH_SESSION_READY, NULL, 0, 0) == DOCA_SUCCESS)
            conn->ready_sent = true;
        if (conn->push_fin_requested && !conn->push_fin_sent)
            (void)dmesh_dma_push_fin(conn);
		break;

	case DMESH_CONN_CLOSING:
        if (conn->multiplexed) {
            dmesh_flow_close_advance(conn);
            break;
        }
		/* DMESH_NO_TEARDOWN=1: park the dead slot instead of tearing it
		 * down. Teardown's DPA-thread destroy fails inside flexio in the
		 * proxy process and wedges the whole comch function (every later
		 * client registration aborts at devx creation, syndrome 0xe5300).
		 * Parking leaks the slot + its DPA pool thread (8 per worker), but
		 * reconnects keep working - enough for benchmark/DSB sessions.
		 * Root cause of the flexio failure is still open. */
		if (getenv("DMESH_NO_TEARDOWN") != NULL) {
			dmesh_doca_dpa_thread_quiesce(conn->dpa_thread);
			/* Stop the data-path contexts first. With them still RUNNING,
			 * doca_comch_server_disconnect() fails ("connection still has
			 * active consumers or producers"), the dead connection stays
			 * live and its posted recvs fail IO_FAILED in a loop, and the
			 * refused disconnect was followed by healthy slots being
			 * disconnected too (the post-kill cascade). Stop, never destroy,
			 * here - see the flexio note above. */
			if (conn->consumer != NULL) {
				enum doca_ctx_states st;
				int spins = 0;
				struct doca_ctx *cctx = doca_comch_consumer_as_ctx(conn->consumer);

				if (doca_ctx_get_state(cctx, &st) == DOCA_SUCCESS && st != DOCA_CTX_STATE_IDLE) {
					(void)doca_ctx_stop(cctx);
					while (spins++ < 100000 &&
					       doca_ctx_get_state(cctx, &st) == DOCA_SUCCESS && st != DOCA_CTX_STATE_IDLE)
						doca_pe_progress(objs->consumer_pe);
				}
			}
			/* Stopping is not enough: doca_comch_server_disconnect() still
			 * reports "active consumers or producers" while the consumer and
			 * MsgQ objects merely exist on the connection (teardown succeeds
			 * because it destroys them first). Destroy them here too. Only
			 * the DPA thread itself is kept (that destroy is the flexio
			 * hazard) - the slot leaks its pool thread, as parking always did. */
			if (conn->consumer != NULL) {
				(void)doca_comch_consumer_destroy(conn->consumer);
				conn->consumer = NULL;
			}
			if (conn->consumer_mem != NULL) {
				clean_local_mem_bufs(conn->consumer_mem);
				free(conn->consumer_mem);
				conn->consumer_mem = NULL;
			}
			dmesh_doca_dpa_comch_destroy(conn);
			if (conn->connection != NULL) {
				doca_error_t dres = doca_comch_server_disconnect(objs->cc_server, conn->connection);

				if (dres != DOCA_SUCCESS)
					DOCA_LOG_WARN("park: server-side disconnect failed: %s", doca_error_get_name(dres));
			}
			/* Drop the pointer: the DOCA connection object is freed by the
			 * disconnect and its address gets reused by the next client.
			 * A parked slot that still held it would alias that new
			 * connection in dmesh_conn_get()/dmesh_conn_open(): the new
			 * client gets glued to this dead slot (never set up, silently
			 * hangs), its events land here, and its metadata is re-imported
			 * in a loop (the doca_mmap "isn't aligned" storm at churn). */
			conn->connection = NULL;
			conn->state = DMESH_CONN_ERROR;
			DOCA_LOG_INFO("Parked dead connection slot %ld (DMESH_NO_TEARDOWN)",
				      conn - objs->conns);
			break;
		}
		/* Host disconnected (or setup failed): release everything so the slot
		 * and its DPA pool thread can be reused. Sets state to FREE. */
		dmesh_conn_teardown(conn);
		break;

	case DMESH_CONN_ERROR:
        if (conn->multiplexed && !conn->error_sent) {
            if (conn->error_status == 0)
                conn->error_status = EIO;
            if (server_send_flow_msg(conn, DMESH_SESSION_ERROR, NULL, 0, conn->error_status) == DOCA_SUCCESS)
                conn->error_sent = true;
        }
        break;
	case DMESH_CONN_FREE:
	default:
		break;
	}
	return;

error:
	if (objs->dpa_pool != NULL)
		(void)dmesh_doca_dpa_failed(objs->dpa_pool->dpa);
	conn->state = DMESH_CONN_ERROR;
}

doca_error_t
dmesh_doca_ctrl_advance(struct objects *objs, enum dmesh_doca_init_state *out_state)
{
	doca_error_t result;
	int i;

	if (objs == NULL || out_state == NULL)
		return DOCA_ERROR_INVALID_VALUE;

	/* One-time shared infrastructure: DPA instance, thread pool, the shared
	 * consumer PE and the DMA engine. None of it depends on a connection. */
	if (objs->phase == DMESH_DOCA_STATE_SERVER_STARTED) {
		DOCA_LOG_INFO("Creating DPA objects, thread pool, consumer PE and DMA engine");

		result = init_dpa_objects(objs);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init DPA objects: %s", doca_error_get_name(result));
			goto error;
		}

		result = dmesh_dpa_thread_pool_init(objs);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to init DPA thread pool: %s", doca_error_get_name(result));
			goto error;
		}

		result = doca_pe_create(&objs->consumer_pe);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to create consumer PE: %s", doca_error_get_name(result));
			goto error;
		}
		result = doca_pe_set_event_mode(objs->consumer_pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to set consumer PE event mode: %s", doca_error_get_name(result));
			goto error;
		}

		/* DMA engines are per connection now (created in DMESH_CONN_NEW) */

		objs->phase = DMESH_DOCA_STATE_RUNNING;
	}

    sessions_advance(objs);

	/* Serve every bound connection; each has its own state machine. */
	for (i = 0; i < DMESH_MAX_CONNECTIONS; i++) {
		if (objs->conns[i].state != DMESH_CONN_FREE)
			dmesh_doca_conn_advance(&objs->conns[i]);
	}

    sessions_advance(objs);

	*out_state = objs->phase;
	return DOCA_SUCCESS;

error:
	objs->phase = DMESH_DOCA_STATE_ERROR;
	*out_state = DMESH_DOCA_STATE_ERROR;
	return result;
}
