#ifndef COMCH_SERVER_H
#define COMCH_SERVER_H

#include <stdbool.h>
#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_pe.h>

struct objects; /* Forward declaration */
struct dmesh_doca_objects; /* Forward declaration */

/* Global init state for the event-driven (on-demand) control path. Once the
 * shared infrastructure (DPA instance + thread pool, consumer PE, DMA engine)
 * is built the server is RUNNING and serves connections; each connection has
 * its own state machine (enum dmesh_conn_state, struct dmesh_conn). See
 * dmesh_doca_ctrl_advance(). */
enum dmesh_doca_init_state {
	DMESH_DOCA_STATE_ERROR = -1,		 /* shared infrastructure setup failed */
	DMESH_DOCA_STATE_SERVER_STARTED = 0,	 /* server ctx started; infra not yet created */
	DMESH_DOCA_STATE_RUNNING = 1,		 /* infra ready; serving connections */
};

#define CC_SEND_TASK_NUM 1024 /* Number of CC send tasks  */
#define CC_RECV_QUEUE_SIZE 1024 /* Size of CC receive queue */

#define STR_START_DATA_PATH_TEST "start_data_path_test" /* The negotiation message between client and server */
#define STR_STOP_DATA_PATH_TEST "stop_data_path_test"	/* The negotiation message between client and server */

#ifndef SLEEP_IN_NANOS
#define SLEEP_IN_NANOS (10 * 1000)	       /* Sample tasks every 10 microseconds */
#endif

struct comch_ctrl_path_server_cb_config {
	/* User specified callback when task completed successfully */
	doca_comch_task_send_completion_cb_t send_task_comp_cb;
	/* User specified callback when task completed with error */
	doca_comch_task_send_completion_cb_t send_task_comp_err_cb;
	/* User specified callback when a message is received */
	doca_comch_event_msg_recv_cb_t msg_recv_cb;
	/* User specified callback when server receives a new connection */
	doca_comch_event_connection_status_changed_cb_t server_connection_event_cb;
	/* User specified callback when server finds a disconnected connection */
	doca_comch_event_connection_status_changed_cb_t server_disconnection_event_cb;
	/* Whether need to configure data_path related event callback */
	bool data_path_mode;
	/* User specified callback when a new consumer registered */
	doca_comch_event_consumer_cb_t new_consumer_cb;
	/* User specified callback when a consumer expired event occurs */
	doca_comch_event_consumer_cb_t expired_consumer_cb;
	/* User specified context data */
	void *ctx_user_data;
	/* User specified PE context state changed event callback */
	doca_ctx_state_changed_callback_t ctx_state_changed_cb;
};

doca_error_t start_comch_data_path_server(const char *server_name,
							struct objects *objs);

doca_error_t 
init_comch_dpa_datapath(struct objects *objs);

doca_error_t
init_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path);

/* Non-blocking variant: create + configure + start the comch server context,
 * but DO NOT busy-wait for the host connection. The connection is established
 * later via the event-driven driver (see dmesh_doca_ctrl_advance). */
doca_error_t
start_comch_ctrl_path_server(const char *server_name, struct objects *objs, bool is_fast_path);

/* Event-driven control-path helpers (operate on the control PE objs->pe).
 * All return doca_error_t. See dmesh_doca_ctrl_advance for the state machine. */
doca_error_t
dmesh_doca_ctrl_get_fd(struct objects *objs, int *out_fd);

doca_error_t
dmesh_doca_ctrl_arm(struct objects *objs);

/* Progress the control PE until no more progress is made, WITHOUT clearing the
 * notification. Used to process events already pending (e.g. consumed by a
 * phase's internal progress) before blocking on the fd, closing the arm/ready
 * race. */
doca_error_t
dmesh_doca_ctrl_drain(struct objects *objs);

doca_error_t
dmesh_doca_ctrl_clear_and_drain(struct objects *objs, int fd);

doca_error_t
dmesh_doca_ctrl_advance(struct objects *objs, enum dmesh_doca_init_state *out_state);

doca_error_t
server_send_msg(struct objects *objs, const char *msg, size_t len);

struct doca_comch_connection;
doca_error_t
server_send_msg_conn(struct objects *objs, struct doca_comch_connection *connection,
		     const char *msg, size_t len);

struct dmesh_conn;
doca_error_t server_send_flow_msg(struct dmesh_conn *, uint16_t, const void *, size_t, int32_t);

doca_error_t
dmesh_doca_init_comch_server(struct dmesh_doca_objects *objs, const char *server_name, 
							 bool enable_fast_path);
#endif // COMCH_SERVER_H