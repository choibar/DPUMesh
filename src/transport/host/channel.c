#include "channel.h"

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <poll.h>

#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_mmap.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_pe.h>
#include <doca_sync_event.h>

#include "buffer.h"
#include "comch_client.h"
#include "comch_common.h"
#include "common.h"
#include "dma.h"
#include "dpa_common.h"
#include "ring.h"
#include "session_protocol.h"

/*
 * DPUMesh transport, host end. Each channel owns one serialized Comch control
 * session; logical flows retain private forward rings and reverse windows.
 *
 * Reverse path: the DPU's reverse DPA thread lands each batch in the window's
 * data ring and its descriptor in the slot ring, then stores a new value into
 * the flow slot's sync event word in the session's tail table page. The host
 * reads the slots in order, publishes a consumption cursor the DPA thread
 * reads, and sleeps on a wait task of the slot's sync event
 * (channel_flow_arm).
 */

DOCA_LOG_REGISTER(CHANNEL);

_Static_assert(CHANNEL_DESC_N == DMESH_PUSH_DESC_N, "push slot count");
_Static_assert(CHANNEL_DATA_OFF == DMESH_PUSH_DATA_OFF, "push data offset");
_Static_assert(CHANNEL_WINDOW == DMESH_PUSH_WINDOW, "push window");
_Static_assert(CHANNEL_RING_SIZE == DMA_RING_SIZE &&
               sizeof(struct dma_ring_ctrl) + CHANNEL_RING_SIZE * sizeof(struct dma_desc) <= CHANNEL_RING_STRIDE &&
               CHANNEL_RING_STRIDE % 4096 == 0, "a forward ring fits its stride");
_Static_assert(CHANNEL_MODE_CLIENT == DMESH_FLOW_MODE_CLIENT, "client mode");
_Static_assert(CHANNEL_MODE_BACKEND == DMESH_FLOW_MODE_BACKEND, "backend mode");

#ifndef CHANNEL_REPLY_MS
#define CHANNEL_REPLY_MS 5000              /* Wait for the DPU's READY or CLOSED */
#endif
#define CHANNEL_CTX_STOP_SPINS 100000      /* Bound on progressing a stopping ctx to IDLE */
#define CHANNEL_KEEPALIVE_MS 1000          /* PING a session that sent nothing for this long */

struct channel_dev {
	struct doca_dev *dev;               /* Comch / forward device */
	/* Lock covers every control PE call, send, callback and flow-map change.
	 * Callbacks only change flow control state: they never acquire slot locks.
	 * No shared control fd is registered in competing EQs; the existing carrier
	 * fallback tick drives control progress from whichever EQ is awake. */
	pthread_mutex_t session_lock;
	struct dmesh_comch_client *control;
	struct channel_conn *flows[33];
	uint32_t generations[33];
	int hello_ready, listen_ready, listen_error;
    struct { uint32_t token; int state, error; } backend[DMESH_SESSION_MAX_WORKERS];
    unsigned backend_pending, backend_rejections;
    /* state: 1=pending, 2=processing, 3=done, 4=reject pending */
	int session_error;
	uint64_t last_send_ms;              /* Last control message submitted (keepalive) */
	/* Flow wake: every flow slot's own PE and sync event, created with the
	 * session. news has bit (flow id - 1) set for a flow that got CLOSED,
	 * ERROR or a DPU request. */
	struct channel_flow_wake *flow_wake[33];
	uint32_t news;
	/* Tail table (TAILS): slot id - 1 holds the low 32 bits of flow id's
	 * forward producer_tail, so the DPA thread serving this session reads
	 * sixteen flows' tails in one cache line. The same page holds each flow
	 * slot's sync event word. Exported with every session. */
	struct doca_mmap *tails_mmap;
	volatile uint32_t *tails;
};

/* A flow slot's wake: a sync event over the slot's word of the tail table
 * page, which the DPU's reverse DPA thread changes with every batch it
 * publishes, on the slot's own PE (PROGRESS_ALL: a request clears the last
 * notification). A wait task on the word's current value raises the PE's fd
 * when the word changes. Progressed under the carrier's slot lock. */
struct channel_flow_wake {
	struct doca_pe *pe;
	uint32_t slot;                      /* flow slot id, 1..32 */
	struct doca_sync_event *se;
	volatile uint64_t *se_word;
	int se_pending;
	uint64_t se_waits, se_wakes;
};

struct channel_mem {
	struct doca_mmap *mmap;
	void *buf;
	size_t bytes;
	doca_dpa_dev_mmap_t dpa;            /* Handle on the forward device (the DPU DPA reads the TX pool) */
};

struct channel_conn {
    struct channel_dev *dev;
    struct dma_ring *forward_ring;
	doca_dpa_dev_mmap_t tx_dpa;
	volatile uint32_t *tail;            /* this flow's slot of dev->tails */
	volatile struct dmesh_push_desc *descs;
	volatile struct dmesh_push_cursor *cursor;
	size_t data_size;
	uint64_t expected;                  /* Push: next batch sequence */
	int rx_ended;                       /* Push: end of stream or a malformed batch was read */
	uint32_t flow_id, generation;
	int open_sent, ready, peer_closed, error, close_sent, close_error;
	int abandoned;                      /* a failed open whose close is unconfirmed: holds its id */
	uint32_t requests;                  /* CHANNEL_REQ_* not taken yet (session lock) */
	uint32_t scale_count, scale_token;  /* the SCALE waiting in requests */
	int announce;                       /* a request arrived since channel_conn_announced */
	uint64_t close_deadline_ms;         /* CLOSE sent: CLOSED expected by then */
	uint64_t open_deadline_ms;          /* OPEN sent: READY expected by then */
};

/**
 * Map a DOCA error to the errno the carrier reports
 *
 * @result [in]: DOCA error
 * @return: errno value
 */
static int error_number(doca_error_t result)
{
	switch (result) {
	case DOCA_ERROR_NO_MEMORY:
		return ENOMEM;
	case DOCA_ERROR_INVALID_VALUE:
		return EINVAL;
	case DOCA_ERROR_AGAIN:
		return EAGAIN;
	case DOCA_ERROR_TIME_OUT:
		return ETIMEDOUT;
	case DOCA_ERROR_CONNECTION_ABORTED:
		return ECONNRESET;
	case DOCA_ERROR_NOT_FOUND:
		return ENODEV;
	default:
		return EIO;
	}
}

/**
 * Register the DOCA log backends once per process
 *
 * DPUMESH_SDK_LOG=debug|info|warning surfaces the SDK's own reasons for a
 * failing DOCA call (silent otherwise).
 */
static void logging_once(void)
{
	static int logging;
	const char *level;
	struct doca_log_backend *sdk = NULL;

	if (logging)
		return;
	logging = 1;

	(void)doca_log_backend_create_standard();

	level = getenv("DPUMESH_SDK_LOG");
	if (level == NULL || *level == '\0')
		return;
	if (doca_log_backend_create_with_file_sdk(stderr, &sdk) != DOCA_SUCCESS)
		return;
	if (strcmp(level, "debug") == 0)
		(void)doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_DEBUG);
	else if (strcmp(level, "info") == 0)
		(void)doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_INFO);
	else
		(void)doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_WARNING);
}

/**
 * Progress a stopping ctx on its PE until it is IDLE (bounded)
 *
 * @ctx [in]: Context that was told to stop
 * @pe [in]: Progress engine the context is connected to
 */
static int wait_ctx_idle(struct doca_ctx *ctx, struct doca_pe *pe)
{
	enum doca_ctx_states state;
	int spins = 0;

	while (spins++ < CHANNEL_CTX_STOP_SPINS &&
	       doca_ctx_get_state(ctx, &state) == DOCA_SUCCESS &&
	       state != DOCA_CTX_STATE_IDLE)
		(void)doca_pe_progress(pe);
	return doca_ctx_get_state(ctx, &state) == DOCA_SUCCESS && state == DOCA_CTX_STATE_IDLE ? 0 : -1;
}

/* Called only from the control PE while session_lock is held. A flow id is
 * reusable only after CLOSED; generation checks discard delayed old replies. */
static void session_message(void *owner, const uint8_t *data, size_t len)
{
	struct channel_dev *dev = owner;
	struct dmesh_session_header h;
	const uint8_t *payload;
	if (dmesh_session_decode(data, len, &h, &payload) != 0) {
		dev->session_error = EPROTO;
		return;
	}
	if (h.type == DMESH_SESSION_HELLO_ACK && h.flow_id == 0 && h.generation == 0) {
		if (h.status) dev->session_error = h.status;
		else dev->hello_ready = 1;
		return;
	}
    if (h.type == DMESH_SESSION_LISTEN_ACK) {
        dev->listen_ready = 1; dev->listen_error = h.status;
        return;
    }
    if (h.type == DMESH_SESSION_BACKEND_REQUEST) {
        uint32_t w = dmesh_session_get_u32(payload);
        if (w >= DMESH_SESSION_MAX_WORKERS) { dev->session_error = EPROTO; return; }
        if (h.generation > dev->backend[w].token) {
            /* Dispatcher cannot replace a still-admitted request. */
            if (dev->backend[w].state == 1) --dev->backend_pending;
            if (dev->backend[w].state == 4) --dev->backend_rejections;
            ++dev->backend_pending;
            dev->backend[w].token = h.generation;
            dev->backend[w].state = 1; dev->backend[w].error = 0;
        }
        return;
    }
	if (h.flow_id == 0) {
		dev->session_error = h.status ? h.status : EPROTO;
		return;
	}
	if (h.flow_id > 32) { dev->session_error = EPROTO; return; }
	struct channel_conn *conn = dev->flows[h.flow_id];
	if (!conn || conn->generation != h.generation) return;
	switch (h.type) {
	case DMESH_SESSION_READY:
		if (h.status) conn->error = h.status;
		else conn->ready = 1;
		break;
	case DMESH_SESSION_CLOSED:
		if (h.status) conn->close_error = h.status;
		else conn->peer_closed = 1;
		break;
	case DMESH_SESSION_ERROR:
		conn->error = h.status ? h.status : EIO;
		if (conn->close_sent) conn->close_error = conn->error;
		break;
	case DMESH_SESSION_PREPARE:
		conn->requests |= CHANNEL_REQ_PREPARE;
		__atomic_store_n(&conn->announce, 1, __ATOMIC_RELEASE);
		break;
	case DMESH_SESSION_SCALE:
		if (dmesh_session_get_u32(payload) == 0) break;
		conn->scale_count = dmesh_session_get_u32(payload);
		conn->scale_token = dmesh_session_get_u32(payload + 4);
		conn->requests |= CHANNEL_REQ_SCALE;
		__atomic_store_n(&conn->announce, 1, __ATOMIC_RELEASE);
		break;
	default:
		conn->error = EPROTO;
		break;
	}
	/* A flow's own fd carries receives only: report the news. */
	if (h.type == DMESH_SESSION_CLOSED || h.type == DMESH_SESSION_ERROR ||
	    h.type == DMESH_SESSION_PREPARE || h.type == DMESH_SESSION_SCALE)
		__atomic_fetch_or(&dev->news, UINT32_C(1) << (h.flow_id - 1), __ATOMIC_RELEASE);
}

static uint64_t channel_now_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void backend_flush_rejections(struct channel_dev *dev)
{
    if (!dev->backend_rejections) return;
    for (unsigned w = 0; w < DMESH_SESSION_MAX_WORKERS; ++w) {
        if (dev->backend[w].state != 4) continue;
        uint8_t frame[DMESH_SESSION_HEADER_SIZE + 4], payload[4];
        dmesh_session_put_u32(payload, w);
        size_t n = dmesh_session_encode(frame, sizeof(frame), DMESH_SESSION_BACKEND_REJECT,
            0, dev->backend[w].token, dev->backend[w].error, payload, sizeof(payload));
        if (dmesh_comch_client_send(dev->control, (const char *)frame, n) == DOCA_SUCCESS) {
            dev->backend[w].state = 3; --dev->backend_rejections;
        }
    }
}

static uint64_t channel_now_ms(void);
static int session_send_locked(struct channel_dev *dev, uint16_t type,
	uint32_t id, uint32_t generation, const void *payload, size_t bytes);

static int session_progress_locked(struct channel_dev *dev)
{
	if (!dev->control) { errno = ENOTCONN; return -1; }
	(void)doca_pe_progress(dev->control->pe);
    backend_flush_rejections(dev);
	if (dev->control->peer_gone && !dev->session_error)
		dev->session_error = ECONNRESET;
	if (dev->session_error) { errno = dev->session_error; return -1; }
	/* The Comch client gets no event when the DPU proxy exits; a send to it
	 * fails (peer_gone). An idle session sends nothing else, so it PINGs. A
	 * PING that cannot be queued is retried on the next pass. */
	if (dev->hello_ready && channel_now_ms() - dev->last_send_ms >= CHANNEL_KEEPALIVE_MS)
		(void)session_send_locked(dev, DMESH_SESSION_PING, 0, 0, NULL, 0);
	return 0;
}

static int session_send_locked(struct channel_dev *dev, uint16_t type,
	uint32_t id, uint32_t generation, const void *payload, size_t bytes)
{
	uint8_t frame[DMESH_SESSION_MAX_FRAME];
	size_t len = dmesh_session_encode(frame, sizeof(frame), type, id, generation, 0, payload, bytes);
	if (!len) { errno = EINVAL; return -1; }
	doca_error_t result = dmesh_comch_client_send(dev->control, (const char *)frame, len);
	if (result != DOCA_SUCCESS) { errno = error_number(result); return -1; }
	dev->last_send_ms = channel_now_ms();
	return 0;
}

/* Synchronous setup/teardown is internal. Shared PE callbacks never enter a
 * sibling's data path, and no carrier slot lock is taken while progressing. */
static int session_wait_locked(struct channel_dev *dev, struct channel_conn *conn, int closing)
{
	uint64_t deadline = channel_now_ms() + CHANNEL_REPLY_MS;
	const struct timespec pause = { .tv_nsec = 10000 };
	for (;;) {
		if (session_progress_locked(dev) != 0) return -1;
		if (!conn) { if (dev->hello_ready) return 0; }
		else if (closing) {
			if (conn->peer_closed) return 0;
			if (conn->close_error) { errno = conn->close_error; return -1; }
		} else {
			if (conn->error || conn->peer_closed) { errno = conn->error ? conn->error : ECONNRESET; return -1; }
			if (conn->ready) return 0;
		}
		if (channel_now_ms() >= deadline) { errno = ETIMEDOUT; return -1; }
		/* The wait owns only this flow; another EQ may progress the shared
		 * control PE and its own flow while this caller sleeps. */
		pthread_mutex_unlock(&dev->session_lock);
		nanosleep(&pause, NULL);
		pthread_mutex_lock(&dev->session_lock);
	}
}

/*
 * ---------------------------------------------------------------------------
 * Flow wake
 * ---------------------------------------------------------------------------
 */

static void flow_se_done(struct doca_sync_event_task_wait_neq *task, union doca_data task_data,
			 union doca_data ctx_data)
{
	struct channel_flow_wake *fw = task_data.ptr;
	(void)ctx_data;
	fw->se_pending = 0;
	fw->se_wakes++;
	doca_task_free(doca_sync_event_task_wait_neq_as_doca_task(task));
}

/* Events a context queued on an armed PE are read before it is destroyed
 * (the PE event reads of common/object.h, which this layer does not include). */
static void flow_pe_take_events(struct doca_pe *pe)
{
	doca_notification_handle_t handle;
	if (doca_pe_get_notification_handle(pe, &handle) != DOCA_SUCCESS)
		return;
	for (int round = 0; round < 2; ++round) {
		struct pollfd fd = {.fd = (int)handle, .events = POLLIN};
		if (poll(&fd, 1, 0) <= 0)
			return;
		(void)doca_pe_clear_notification(pe, handle);
	}
}

/* Stop and release one slot's wake; a sync event that does not stop is kept
 * with its PE. */
static int flow_wake_free(struct channel_flow_wake *fw)
{
	if (fw->se != NULL) {
		struct doca_ctx *ctx = doca_sync_event_as_ctx(fw->se);
		enum doca_ctx_states state = DOCA_CTX_STATE_STOPPING;
		if (fw->se_waits)
			DOCA_LOG_INFO("flow wake: slot %u: %lu waits, %lu wakes", fw->slot,
				      (unsigned long)fw->se_waits, (unsigned long)fw->se_wakes);
		flow_pe_take_events(fw->pe);
		(void)doca_ctx_stop(ctx);
		flow_pe_take_events(fw->pe);
		for (int spins = 0; spins < CHANNEL_CTX_STOP_SPINS && state != DOCA_CTX_STATE_IDLE; ++spins) {
			(void)doca_pe_progress(fw->pe);
			if (doca_ctx_get_state(ctx, &state) != DOCA_SUCCESS)
				break;
		}
		if (state != DOCA_CTX_STATE_IDLE || doca_sync_event_destroy(fw->se) != DOCA_SUCCESS) {
			DOCA_LOG_ERR("flow wake: sync event of slot %u did not stop; retained", fw->slot);
			return -1;
		}
		fw->se = NULL;
	}
	if (fw->pe != NULL)
		(void)doca_pe_destroy(fw->pe);
	free(fw);
	return 0;
}

static void flow_wakes_close_locked(struct channel_dev *dev)
{
	for (uint32_t id = 1; id <= DMESH_SESSION_MAX_FLOWS; ++id)
		if (dev->flow_wake[id] != NULL && flow_wake_free(dev->flow_wake[id]) == 0)
			dev->flow_wake[id] = NULL;
}

/* The slot's PE and its sync event over the slot's word of the tail table
 * page (dev->tails, exported to the DPU with the session), running. */
static doca_error_t flow_wake_start(struct channel_dev *dev, struct channel_flow_wake *fw)
{
	struct doca_ctx *ctx;
	enum doca_ctx_states state = DOCA_CTX_STATE_IDLE;
	doca_error_t result;
	fw->se_word = (volatile uint64_t *)((uint8_t *)dev->tails + DMESH_EC_SE_OFF(fw->slot));
	*fw->se_word = 0;
	if ((result = doca_pe_create(&fw->pe)) != DOCA_SUCCESS ||
	    (result = doca_pe_set_event_mode(fw->pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL)) != DOCA_SUCCESS ||
	    (result = doca_sync_event_create(&fw->se)) != DOCA_SUCCESS ||
	    (result = doca_sync_event_add_publisher_location_remote_pci(fw->se)) != DOCA_SUCCESS ||
	    (result = doca_sync_event_add_subscriber_location_cpu(fw->se, dev->dev)) != DOCA_SUCCESS ||
	    (result = doca_sync_event_set_addr(fw->se, (uint64_t *)fw->se_word)) != DOCA_SUCCESS)
		return result;
	ctx = doca_sync_event_as_ctx(fw->se);
	if ((result = doca_pe_connect_ctx(fw->pe, ctx)) != DOCA_SUCCESS ||
	    (result = doca_sync_event_task_wait_neq_set_conf(fw->se, flow_se_done, flow_se_done, 2)) != DOCA_SUCCESS)
		return result;
	result = doca_ctx_start(ctx);
	if (result != DOCA_SUCCESS && result != DOCA_ERROR_IN_PROGRESS)
		return result;
	for (int spins = 0; spins < CHANNEL_CTX_STOP_SPINS && state != DOCA_CTX_STATE_RUNNING; ++spins) {
		if ((result = doca_ctx_get_state(ctx, &state)) != DOCA_SUCCESS)
			return result;
		if (state != DOCA_CTX_STATE_RUNNING)
			(void)doca_pe_progress(fw->pe);
	}
	return state == DOCA_CTX_STATE_RUNNING ? DOCA_SUCCESS : DOCA_ERROR_TIME_OUT;
}

/* Every slot's wake, before any flow opens. All or none: a failure fails
 * the session. */
static int flow_wakes_open_locked(struct channel_dev *dev)
{
	doca_error_t result = DOCA_SUCCESS;
	uint32_t id;
	for (id = 1; id <= DMESH_SESSION_MAX_FLOWS && result == DOCA_SUCCESS; ++id) {
		struct channel_flow_wake *fw = dev->flow_wake[id] = calloc(1, sizeof(*fw));
		if (fw == NULL) {
			result = DOCA_ERROR_NO_MEMORY;
			break;
		}
		fw->slot = id;
		result = flow_wake_start(dev, fw);
	}
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("flow wake failed (slot %u: %s)", id, doca_error_get_name(result));
		flow_wakes_close_locked(dev);
		errno = error_number(result);
		return -1;
	}
	return 0;
}

int channel_flow_fd(struct channel_dev *dev, uint32_t flow_id)
{
	doca_notification_handle_t handle;
	if (flow_id == 0 || flow_id > DMESH_SESSION_MAX_FLOWS || dev->flow_wake[flow_id] == NULL ||
	    doca_pe_get_notification_handle(dev->flow_wake[flow_id]->pe, &handle) != DOCA_SUCCESS)
		return -1;
	return (int)handle;
}

/* Completes the slot's wait task once its word changed. */
void channel_flow_progress(struct channel_dev *dev, uint32_t flow_id)
{
	struct doca_pe *pe = dev->flow_wake[flow_id]->pe;
	for (int n = 0; n < 4 && doca_pe_progress(pe); ++n) {}
}

/* A wait task on the word's current value, then the PE's request: the next
 * batch the DPU publishes raises the slot's fd. */
int channel_flow_arm(struct channel_dev *dev, uint32_t flow_id)
{
	struct channel_flow_wake *fw = dev->flow_wake[flow_id];
	if (!fw->se_pending) {
		struct doca_sync_event_task_wait_neq *task;
		uint64_t v = __atomic_load_n(fw->se_word, __ATOMIC_ACQUIRE);
		doca_error_t result = doca_sync_event_task_wait_neq_alloc_init(fw->se, v, ~0ull,
									     (union doca_data){.ptr = fw}, &task);
		if (result == DOCA_SUCCESS) {
			result = doca_task_submit(doca_sync_event_task_wait_neq_as_doca_task(task));
			if (result != DOCA_SUCCESS)
				doca_task_free(doca_sync_event_task_wait_neq_as_doca_task(task));
		}
		if (result != DOCA_SUCCESS) { errno = error_number(result); return -1; }
		fw->se_pending = 1;
		fw->se_waits++;
	}
	doca_error_t result = doca_pe_request_notification(fw->pe);
	if (result != DOCA_SUCCESS) { errno = error_number(result); return -1; }
	return 0;
}

uint32_t channel_dev_take_news(struct channel_dev *dev)
{
	return __atomic_exchange_n(&dev->news, 0, __ATOMIC_ACQUIRE);
}

/* The session's tail table: allocated with the first session, cleared and
 * exported with every one. Sent right after HELLO_ACK, so before any OPEN. */
static int tails_send_locked(struct channel_dev *dev)
{
	const void *desc;
	size_t len;
	doca_error_t result;
	if (!dev->tails) {
		void *buf = NULL;
		/* The whole page: the flow slots' sync event words follow the tails. */
		result = alloc_buffer_and_set_mmap(&dev->tails_mmap, dev->dev, &buf, DMESH_TAILS_ALLOC,
						   DOCA_ACCESS_FLAG_PCI_READ_WRITE);
		if (result != DOCA_SUCCESS) { errno = error_number(result); return -1; }
		dev->tails = buf;
	}
	memset((void *)dev->tails, 0, DMESH_TAILS_ALLOC);
	result = doca_mmap_export_pci(dev->tails_mmap, dev->dev, &desc, &len);
	if (result != DOCA_SUCCESS) { errno = error_number(result); return -1; }
	if (len == 0 || len > DMESH_SESSION_TAILS_MAX) { errno = EMSGSIZE; return -1; }
	return session_send_locked(dev, DMESH_SESSION_TAILS, 0, 0, desc, len);
}

int channel_session_open(struct channel_dev *dev, const char *server)
{
	if (!dev || !server || !*server) { errno = EINVAL; return -1; }
	pthread_mutex_lock(&dev->session_lock);
	if (dev->control) { pthread_mutex_unlock(&dev->session_lock); errno = EALREADY; return -1; }
	dev->control = calloc(1, sizeof(*dev->control));
	if (!dev->control) { pthread_mutex_unlock(&dev->session_lock); return -1; }
	dev->control->dev = dev->dev;
	dev->control->message = session_message;
	dev->control->owner = dev;
	doca_error_t result = dmesh_comch_client_open(server, dev->control);
	int rc = -1;
	if (result != DOCA_SUCCESS) errno = error_number(result);
	else if (session_send_locked(dev, DMESH_SESSION_HELLO, 0, 0, NULL, 0) == 0)
		rc = session_wait_locked(dev, NULL, 0);
	if (rc == 0)
		rc = tails_send_locked(dev);
	if (rc == 0)
		rc = flow_wakes_open_locked(dev);
	pthread_mutex_unlock(&dev->session_lock);
	return rc;
}

int channel_session_listen(struct channel_dev *dev, uint32_t ip, uint16_t port)
{
    uint8_t payload[8];
    dmesh_session_put_u32(payload, ip); dmesh_session_put_u32(payload + 4, port);
    pthread_mutex_lock(&dev->session_lock);
    dev->listen_ready = 0; dev->listen_error = 0;
    int rc = session_send_locked(dev, DMESH_SESSION_LISTEN, 0, 1, payload, sizeof(payload));
    uint64_t deadline = channel_now_ms() + CHANNEL_REPLY_MS;
    while (rc == 0 && !dev->listen_ready) {
        rc = session_progress_locked(dev);
        if (channel_now_ms() >= deadline) { errno = ETIMEDOUT; rc = -1; break; }
        if (rc == 0 && !dev->listen_ready) {
            pthread_mutex_unlock(&dev->session_lock);
            const struct timespec pause = {.tv_nsec = 10000}; nanosleep(&pause, NULL);
            pthread_mutex_lock(&dev->session_lock);
        }
    }
    if (rc == 0 && dev->listen_error) { errno = dev->listen_error; rc = -1; }
    pthread_mutex_unlock(&dev->session_lock);
    return rc;
}

int channel_backend_next(struct channel_dev *dev, uint32_t *worker, uint32_t *token)
{
    pthread_mutex_lock(&dev->session_lock);
    int found = 0;
    if (!dev->session_error && dev->backend_pending) for (unsigned w = 0; w < DMESH_SESSION_MAX_WORKERS; ++w) {
        if (dev->backend[w].state != 1) continue;
        *worker = w; *token = dev->backend[w].token;
        dev->backend[w].state = 2; --dev->backend_pending; found = 1; break;
    }
    pthread_mutex_unlock(&dev->session_lock);
    return found;
}
int channel_conn_announced(struct channel_conn *conn)
{
    return __atomic_load_n(&conn->announce, __ATOMIC_ACQUIRE) &&
           __atomic_exchange_n(&conn->announce, 0, __ATOMIC_ACQ_REL);
}
int channel_conn_take_request(struct channel_conn *conn, int *kind, uint32_t *count, uint32_t *token)
{
    struct channel_dev *dev = conn->dev;
    pthread_mutex_lock(&dev->session_lock);
    *kind = conn->requests & CHANNEL_REQ_PREPARE ? CHANNEL_REQ_PREPARE : conn->requests & CHANNEL_REQ_SCALE;
    conn->requests &= ~(uint32_t)*kind;
    *count = conn->scale_count; *token = conn->scale_token;
    pthread_mutex_unlock(&dev->session_lock);
    return *kind != 0;
}
int channel_conn_scale_ack(struct channel_conn *conn, uint32_t token, uint32_t applied)
{
    if (!token) { errno = EINVAL; return -1; }
    struct channel_dev *dev = conn->dev;
    uint8_t payload[8];
    dmesh_session_put_u32(payload, token);
    dmesh_session_put_u32(payload + 4, applied);
    pthread_mutex_lock(&dev->session_lock);
    int rc = session_send_locked(dev, DMESH_SESSION_SCALE_ACK, conn->flow_id, conn->generation,
                                 payload, sizeof(payload));
    pthread_mutex_unlock(&dev->session_lock);
    return rc;
}
int channel_conn_spare_ready(struct channel_conn *conn)
{
    struct channel_dev *dev = conn->dev;
    pthread_mutex_lock(&dev->session_lock);
    int rc = session_send_locked(dev, DMESH_SESSION_SPARE_READY, conn->flow_id,
                                 conn->generation, NULL, 0);
    pthread_mutex_unlock(&dev->session_lock);
    return rc;
}
void channel_backend_finish(struct channel_dev *dev, uint32_t worker, uint32_t token, int error)
{
    pthread_mutex_lock(&dev->session_lock);
    if (worker < DMESH_SESSION_MAX_WORKERS && dev->backend[worker].token == token) {
        if (dev->backend[worker].state == 4) --dev->backend_rejections;
        if (error) ++dev->backend_rejections;
        dev->backend[worker].error = error;
        dev->backend[worker].state = error ? 4 : 3;
    }
    pthread_mutex_unlock(&dev->session_lock);
}

int channel_session_close(struct channel_dev *dev)
{
	if (!dev) return 0;
	/* Channel destruction has exclusive ownership: no EQ or slot may still run. */
	for (int i = 1; i <= 32; ++i)
		if (dev->flows[i] && channel_conn_close(dev->flows[i]) != 0) return -1;
	pthread_mutex_lock(&dev->session_lock);
	struct dmesh_comch_client *control = dev->control;
	flow_wakes_close_locked(dev);
	if (control && control->cc_client) {
		struct doca_ctx *ctx = doca_comch_client_as_ctx(control->cc_client);
		(void)doca_ctx_stop(ctx);
		if (wait_ctx_idle(ctx, control->pe) != 0 || doca_comch_client_destroy(control->cc_client) != DOCA_SUCCESS) {
			pthread_mutex_unlock(&dev->session_lock); errno = EIO; return -1;
		}
		control->cc_client = NULL;
	}
	if (control && control->pe && doca_pe_destroy(control->pe) != DOCA_SUCCESS) {
		pthread_mutex_unlock(&dev->session_lock); errno = EIO; return -1;
	}
	free(control);
	dev->control = NULL;
	pthread_mutex_unlock(&dev->session_lock);
	return 0;
}

/*
 * ---------------------------------------------------------------------------
 * Device
 * ---------------------------------------------------------------------------
 */

int channel_dev_open(const char *pci, struct channel_dev **out)
{
	struct channel_dev *dev;
	doca_error_t result;
	int saved;

	logging_once();

	dev = calloc(1, sizeof(*dev));
	if (dev == NULL)
		return -1;

	pthread_mutex_init(&dev->session_lock, NULL);
	result = open_doca_device_with_pci(pci, NULL, &dev->dev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to open Comch device %s with error = %s", pci, doca_error_get_name(result));
		goto fail;
	}

	*out = dev;
	return 0;

fail:
	saved = error_number(result);
	channel_dev_close(dev);
	errno = saved;
	return -1;
}

void channel_dev_close(struct channel_dev *dev)
{
	if (dev == NULL)
		return;

	if (channel_session_close(dev) != 0) return;
	if (dev->tails_mmap != NULL)
		(void)destroy_mmap_and_free_buffer(dev->tails_mmap, (void *)dev->tails);
	if (dev->dev != NULL)
		(void)doca_dev_close(dev->dev);
	pthread_mutex_destroy(&dev->session_lock);
	free(dev);
}

/*
 * ---------------------------------------------------------------------------
 * Registered memory
 * ---------------------------------------------------------------------------
 */

/**
 * Allocate a buffer and register it with the device for PCI and local access
 *
 * @mmap [out]: Created mmap
 * @dev [in]: Comch / forward device
 * @buffer [out]: Allocated buffer
 * @bytes [in]: Buffer size
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t mem_register(struct doca_mmap **mmap, struct doca_dev *dev, void **buffer, size_t bytes)
{
	const char *step = "create";
	doca_error_t result;

	result = doca_mmap_create(mmap);
	if (result != DOCA_SUCCESS)
		goto out;
	step = "add_dev";
	result = doca_mmap_add_dev(*mmap, dev);
	if (result != DOCA_SUCCESS)
		goto destroy_mmap;
	step = "set_permissions";
	result = doca_mmap_set_permissions(*mmap, DOCA_ACCESS_FLAG_PCI_READ_WRITE | DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
	if (result != DOCA_SUCCESS)
		goto destroy_mmap;

	step = "posix_memalign";
	if (posix_memalign(buffer, 4096, bytes) != 0) {
		result = DOCA_ERROR_NO_MEMORY;
		goto destroy_mmap;
	}
	memset(*buffer, 0, bytes);

	step = "set_memrange";
	result = doca_mmap_set_memrange(*mmap, *buffer, bytes);
	if (result != DOCA_SUCCESS)
		goto free_buffer;
	step = "start";
	result = doca_mmap_start(*mmap);
	if (result != DOCA_SUCCESS)
		goto free_buffer;

	return DOCA_SUCCESS;

free_buffer:
	free(*buffer);
	*buffer = NULL;
destroy_mmap:
	(void)doca_mmap_destroy(*mmap);
	*mmap = NULL;
out:
	DOCA_LOG_ERR("Failed to register %zu bytes at %s with error = %s", bytes, step, doca_error_get_name(result));
	return result;
}

int channel_mem_alloc(struct channel_dev *dev, size_t bytes, struct channel_mem **out)
{
	struct channel_mem *mem;
	doca_error_t result;

	mem = calloc(1, sizeof(*mem));
	if (mem == NULL)
		return -1;

	result = mem_register(&mem->mmap, dev->dev, &mem->buf, bytes);
	if (result != DOCA_SUCCESS)
		goto free_mem;

	result = doca_mmap_dev_get_dpa_handle(mem->mmap, dev->dev, &mem->dpa);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA handle of the region with error = %s", doca_error_get_name(result));
		goto free_buffer;
	}

	mem->bytes = bytes;
	*out = mem;
	return 0;

free_buffer:
	(void)destroy_mmap_and_free_buffer(mem->mmap, mem->buf);
free_mem:
	free(mem);
	errno = error_number(result);
	return -1;
}

void *channel_mem_base(const struct channel_mem *mem)
{
	return mem != NULL ? mem->buf : NULL;
}

void channel_mem_free(struct channel_mem *mem)
{
	if (mem == NULL)
		return;
	if (mem->mmap != NULL)
		(void)destroy_mmap_and_free_buffer(mem->mmap, mem->buf);
	free(mem);
}

/*
 * ---------------------------------------------------------------------------
 * Connection
 * ---------------------------------------------------------------------------
 */

/**
 * Release a connection
 *
 * Release private host state after the tagged close fence. The shared control
 * session and registered TX/RX regions remain available to sibling flows.
 *
 * @conn [in]: Connection
 */
static int conn_teardown(struct channel_conn *conn)
{
	/* Only reached before OPEN or after a confirmed CLOSED. The ring stays in
	 * the channel's ring region; all memory and the control PE belong to the
	 * channel. */
	free(conn->forward_ring);
	free(conn);
	return 0;
}

/* Sends OPEN; with wait, also waits for READY. */
static int conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg,
                     struct channel_conn **out, int wait)
{
	struct channel_conn *conn;
	doca_error_t result;
	if (!dev || !cfg || !out || !cfg->flow_id || cfg->flow_id > 32 || !cfg->tx || !cfg->rx || !cfg->rings ||
	    cfg->rx_offset > cfg->rx->bytes || CHANNEL_WINDOW > cfg->rx->bytes - cfg->rx_offset ||
	    cfg->ring_offset % CHANNEL_RING_STRIDE != 0 || cfg->ring_offset > cfg->rings->bytes ||
	    CHANNEL_RING_STRIDE > cfg->rings->bytes - cfg->ring_offset) { errno = EINVAL; return -1; }
	*out = NULL;

	conn = calloc(1, sizeof(*conn));
    if (conn == NULL) {
		errno = ENOMEM;
		return -1;
	}
	conn->dev = dev;

	/* Install the flow before OPEN, so any completion is routed by id+generation. */
	pthread_mutex_lock(&dev->session_lock);
	if (!dev->control || !dev->hello_ready || dev->session_error || dev->flows[cfg->flow_id]) {
		int saved = dev->session_error ? dev->session_error : EBUSY;
		pthread_mutex_unlock(&dev->session_lock);
		conn_teardown(conn); errno = saved; return -1;
	}
	if (dev->generations[cfg->flow_id] == UINT32_MAX) {
		pthread_mutex_unlock(&dev->session_lock);
		conn_teardown(conn); errno = EOVERFLOW; return -1;
	}
	conn->flow_id = cfg->flow_id;
	conn->generation = ++dev->generations[conn->flow_id];
	dev->flows[conn->flow_id] = conn;
	pthread_mutex_unlock(&dev->session_lock);

	struct dmesh_flow_id flow = {0};
    struct dmesh_buffer sndbuf = {0}, rcvbuf = {0};
	/* flow identity */
	flow.src_ip = cfg->src_ip;
	flow.dst_ip = cfg->dst_ip;
	flow.src_port = cfg->src_port;
	flow.dst_port = cfg->dst_port;
	flow.mode = cfg->mode;
	snprintf(flow.src_workload, sizeof(flow.src_workload), "%s",
		 cfg->workload != NULL ? cfg->workload : "");

	/* The slot's forward ring in the channel's ring region, empty: the slot's
	 * last flow was closed (CLOSED), so no DPA reads it. The DPU imports the
	 * region and reads the ring at its offset. */
	conn->forward_ring = calloc(1, sizeof(*conn->forward_ring));
	if (conn->forward_ring == NULL) { result = DOCA_ERROR_NO_MEMORY; goto fail; }
	conn->forward_ring->mmap = cfg->rings->mmap;
	conn->forward_ring->size = CHANNEL_RING_SIZE;
	conn->forward_ring->buffer = (char *)cfg->rings->buf + cfg->ring_offset;
	memset(conn->forward_ring->buffer, 0, sizeof(struct dma_ring_ctrl) + CHANNEL_RING_SIZE * sizeof(struct dma_desc));
	conn->forward_ring->ctrl = (struct dma_ring_ctrl *)conn->forward_ring->buffer;
	conn->forward_ring->descs = (struct dma_desc *)((char *)conn->forward_ring->buffer + sizeof(struct dma_ring_ctrl));
	/* the shared regions as this connection's sndbuf/rcvbuf */
	sndbuf.mmap = cfg->tx->mmap;
	sndbuf.buf = cfg->tx->buf;
	sndbuf.size = cfg->tx->bytes;
	rcvbuf.mmap = cfg->rx->mmap;
	rcvbuf.buf = (char *)cfg->rx->buf + cfg->rx_offset;
	rcvbuf.size = CHANNEL_WINDOW;

	/* push layout of the window: slot ring, cursor, data ring */
	memset(rcvbuf.buf, 0, DMESH_PUSH_DATA_OFF);
	conn->descs = (volatile struct dmesh_push_desc *)rcvbuf.buf;
	conn->cursor = (volatile struct dmesh_push_cursor *)((char *)rcvbuf.buf + DMESH_PUSH_CURSOR_OFF);
	conn->cursor->consumed_seq = 0;
	conn->cursor->consumed_bytes = 0;
	conn->cursor->magic = DMESH_PUSH_FC_MAGIC;
	conn->data_size = CHANNEL_WINDOW - DMESH_PUSH_DATA_OFF;
	conn->expected = 1;
	conn->tx_dpa = cfg->tx->dpa;
	/* The ring starts at 0; the id's last flow is closed, so no DPA reads it. */
	conn->tail = &dev->tails[conn->flow_id - 1];
	*conn->tail = 0;

	pthread_mutex_lock(&dev->session_lock);
	struct dmesh_export_metadata_msg metadata;
	result = dmesh_build_dma_metadata(dev->dev, conn->forward_ring, &sndbuf, &rcvbuf, &flow, &metadata);
	int rc = -1;
	if (result != DOCA_SUCCESS) errno = error_number(result);
    else {
        uint8_t payload[8 + sizeof(metadata)];
        uint16_t type = cfg->backend_token ? DMESH_SESSION_BACKEND_OPEN : DMESH_SESSION_OPEN;
        size_t offset = cfg->backend_token ? 8 : 0, len = offset + sizeof(metadata);
        dmesh_session_put_u32(payload, cfg->backend_worker);
        dmesh_session_put_u32(payload + 4, cfg->backend_token);
        memcpy(payload + offset, &metadata, sizeof(metadata));
        if (session_send_locked(dev, type, conn->flow_id, conn->generation,
                               payload, len) == 0) {
            conn->open_sent = 1;
            conn->open_deadline_ms = channel_now_ms() + CHANNEL_REPLY_MS;
            rc = wait ? session_wait_locked(dev, conn, 0) : 0;
        }
    }
	int saved = errno;
	pthread_mutex_unlock(&dev->session_lock);
	if (rc != 0) {
		/* The DPU may still finish the OPEN after the wait gave up. A close it
		 * does not confirm keeps the map entry, its ring and the window under
		 * the id until CLOSED arrives; channel_flow_free releases them then. */
		if (channel_conn_close(conn) != 0) {
			pthread_mutex_lock(&dev->session_lock);
			conn->abandoned = 1;
			pthread_mutex_unlock(&dev->session_lock);
		}
		errno = saved;
		return -1;
	}

	*out = conn;
	return 0;

fail:
	(void)channel_conn_close(conn);
	errno = error_number(result);
	return -1;
}

int channel_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out)
{
	return conn_open(dev, cfg, out, 1);
}

int channel_conn_open_start(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out)
{
	return conn_open(dev, cfg, out, 0);
}

int channel_conn_open_finish(struct channel_conn *conn)
{
	struct channel_dev *dev = conn->dev;
	pthread_mutex_lock(&dev->session_lock);
	int rc = 1, saved = 0;
	if (dev->session_error) saved = dev->session_error;
	else if (conn->error || conn->peer_closed) saved = conn->error ? conn->error : ECONNRESET;
	else if (conn->ready) rc = 0;
	else if (channel_now_ms() >= conn->open_deadline_ms) saved = ETIMEDOUT;
	pthread_mutex_unlock(&dev->session_lock);
	if (saved) { errno = saved; return -1; }
	return rc;
}

/* Sends CLOSE once. Requires dev->session_lock. */
static int conn_close_send_locked(struct channel_dev *dev, struct channel_conn *conn)
{
	if (!conn->open_sent || conn->peer_closed || conn->close_sent) return 0;
	conn->close_error = 0;
	if (session_send_locked(dev, DMESH_SESSION_CLOSE, conn->flow_id, conn->generation, NULL, 0) != 0)
		return -1;
	conn->close_sent = 1;
	conn->close_deadline_ms = channel_now_ms() + CHANNEL_REPLY_MS;
	return 0;
}

/* Releases the ring once CLOSED proved the DPU stopped reading it. Requires
 * dev->session_lock; conn is freed on success. */
static int conn_close_finish_locked(struct channel_dev *dev, struct channel_conn *conn)
{
	uint32_t id = conn->flow_id;
	int rc = conn_teardown(conn);
	if (rc == 0)
		dev->flows[id] = NULL;
	return rc;
}

int channel_conn_close(struct channel_conn *conn)
{
	if (conn == NULL) return 0;
	struct channel_dev *dev = conn->dev;
	pthread_mutex_lock(&dev->session_lock);
	if (conn->close_error) conn->close_sent = 0;   /* a failed close is asked again */
	int rc = conn_close_send_locked(dev, conn);
	if (rc == 0 && conn->close_sent && !conn->peer_closed)
		rc = session_wait_locked(dev, conn, 1);
	if (rc == 0) rc = conn_close_finish_locked(dev, conn);
	pthread_mutex_unlock(&dev->session_lock);
	return rc;
}

int channel_conn_close_start(struct channel_conn *conn)
{
	if (conn == NULL) return 0;
	struct channel_dev *dev = conn->dev;
	pthread_mutex_lock(&dev->session_lock);
	int rc = conn_close_send_locked(dev, conn);
	pthread_mutex_unlock(&dev->session_lock);
	return rc;
}

int channel_conn_close_finish(struct channel_conn *conn)
{
	struct channel_dev *dev = conn->dev;
	pthread_mutex_lock(&dev->session_lock);
	int rc = 0;
	if (conn->close_sent && !conn->peer_closed) {
		int saved = conn->close_error ? conn->close_error : dev->session_error;
		if (!saved && channel_now_ms() >= conn->close_deadline_ms) saved = ETIMEDOUT;
		if (saved) { errno = saved; rc = -1; }
		else rc = 1;
	}
	if (rc == 0) rc = conn_close_finish_locked(dev, conn);
	pthread_mutex_unlock(&dev->session_lock);
	return rc;
}

int channel_flow_free(struct channel_dev *dev, uint32_t flow_id)
{
	pthread_mutex_lock(&dev->session_lock);
	struct channel_conn *conn = dev->flows[flow_id];
	if (conn != NULL && conn->abandoned) {
		/* A CLOSE the queue refused is sent now; the DPU confirms it after the OPEN. */
		(void)conn_close_send_locked(dev, conn);
		if (conn->peer_closed && conn_close_finish_locked(dev, conn) == 0) conn = NULL;
	}
	pthread_mutex_unlock(&dev->session_lock);
	return conn == NULL;
}

int channel_conn_shutdown(struct channel_conn *conn)
{
	struct channel_dev *dev = conn->dev;
	uint8_t posted[8];
	dmesh_session_put_u64(posted, conn->forward_ring->head);
	pthread_mutex_lock(&dev->session_lock);
	int rc = session_send_locked(dev, DMESH_SESSION_SHUTDOWN, conn->flow_id, conn->generation,
				     posted, sizeof(posted));
	pthread_mutex_unlock(&dev->session_lock);
	return rc;
}

int channel_dev_progress(struct channel_dev *dev)
{
	pthread_mutex_lock(&dev->session_lock);
	int rc = session_progress_locked(dev);
	pthread_mutex_unlock(&dev->session_lock);
	return rc;
}

int channel_conn_poll(struct channel_conn *conn)
{
	struct channel_dev *dev = conn->dev;
	/* Three ints the session callbacks write: read without the session lock,
	 * which a flow's poll never needs to wait for. */
	int saved = __atomic_load_n(&dev->control, __ATOMIC_ACQUIRE) == NULL ? ENOTCONN :
		__atomic_load_n(&dev->session_error, __ATOMIC_ACQUIRE);
	if (!saved) saved = __atomic_load_n(&conn->error, __ATOMIC_ACQUIRE);
	int gone = __atomic_load_n(&conn->peer_closed, __ATOMIC_ACQUIRE);
	if (conn->forward_ring && __atomic_load_n(&conn->forward_ring->ctrl->error, __ATOMIC_ACQUIRE))
		saved = EIO;
	if (saved) { errno = saved; return -1; }
	return gone;
}

int channel_conn_idle(const struct channel_conn *conn)
{
	const struct channel_dev *dev = conn->dev;
	if (conn->descs == NULL)
		return 0;
	if (__atomic_load_n(&dev->control, __ATOMIC_ACQUIRE) == NULL ||
	    __atomic_load_n(&dev->session_error, __ATOMIC_ACQUIRE) ||
	    __atomic_load_n(&conn->error, __ATOMIC_ACQUIRE) ||
	    __atomic_load_n(&conn->peer_closed, __ATOMIC_ACQUIRE))
		return 0;
	if ((conn->forward_ring && __atomic_load_n(&conn->forward_ring->ctrl->error, __ATOMIC_ACQUIRE)) ||
	    __atomic_load_n(&conn->announce, __ATOMIC_ACQUIRE))
		return 0;
	/* the slot channel_conn_rx_next would read next is not filled */
	return __atomic_load_n(&conn->descs[conn->expected % DMESH_PUSH_DESC_N].seq, __ATOMIC_ACQUIRE) !=
	       conn->expected;
}

int channel_dev_fd(struct channel_dev *dev)
{
	doca_notification_handle_t handle;
	if (dev->control == NULL ||
	    doca_pe_get_notification_handle(dev->control->pe, &handle) != DOCA_SUCCESS)
		return -1;
	return (int)handle;
}

int channel_dev_arm(struct channel_dev *dev)
{
	pthread_mutex_lock(&dev->session_lock);
	int rc = 0, saved = 0;
	if (dev->control == NULL || !dev->hello_ready || dev->session_error) {
		saved = dev->session_error ? dev->session_error : ENOTCONN;
		rc = -1;
		goto out;
	}
	/* Control messages (CLOSED, ERROR, DPU requests) now raise the fd. The
	 * DOCA flow progresses after arming, so an event that landed before the
	 * request is not slept through: the caller's next drain
	 * (channel_dev_progress) is that progress, not a second one here. The
	 * control PE runs in PROGRESS_ALL mode (comch_client.c): a new request
	 * clears the last notification, so the handle needs no clear. */
	doca_error_t result = doca_pe_request_notification(dev->control->pe);
	if (result != DOCA_SUCCESS) {
		saved = error_number(result);
		rc = -1;
		goto out;
	}
out:
	pthread_mutex_unlock(&dev->session_lock);
	if (rc != 0) errno = saved;
	return rc;
}

int channel_conn_progress(struct channel_conn *conn)
{
	(void)channel_dev_progress(conn->dev);
	return channel_conn_poll(conn);
}

/*
 * ---------------------------------------------------------------------------
 * Forward path
 * ---------------------------------------------------------------------------
 */

uint32_t channel_conn_ring_free(const struct channel_conn *conn)
{
	struct dma_ring *ring = conn->forward_ring;
	uint64_t used = ring->head - ring->ctrl->consumer_head;

	return used >= ring->size ? 0 : (uint32_t)(ring->size - used);
}

uint64_t channel_conn_post(struct channel_conn *conn, uint64_t addr, uint32_t bytes)
{
	struct dma_ring *ring = conn->forward_ring;
	struct dma_desc *desc = get_next_dma_desc(ring); /* the caller checked ring_free */

	desc->mmap = conn->tx_dpa;
	desc->addr = addr;
	desc->size = bytes;
	commit_dma_desc(ring);
	__atomic_store_n(conn->tail, (uint32_t)ring->head, __ATOMIC_RELEASE);
	return ring->head;
}

uint64_t channel_conn_consumed(const struct channel_conn *conn)
{
	return __atomic_load_n(&conn->forward_ring->ctrl->consumer_head, __ATOMIC_ACQUIRE);
}

/*
 * ---------------------------------------------------------------------------
 * Reverse path
 * ---------------------------------------------------------------------------
 */

int channel_conn_rx_next(struct channel_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len)
{
	volatile struct dmesh_push_desc *desc;
	uint32_t p, n;

	/* the next slot the DPU's reverse DPA thread filled */
	desc = &conn->descs[conn->expected % DMESH_PUSH_DESC_N];
	if (desc->seq != conn->expected)
		return 0;
	p = desc->pos;
	n = desc->len;
	if (n == 0 || (size_t)p + n > conn->data_size) {
		conn->rx_ended = 1;         /* nothing follows */
		return -1;
	}

	*seq = conn->expected;
	*pos = p;
	*len = n;
	conn->expected++;
	return 1;
}

void channel_conn_rx_consumed(struct channel_conn *conn, uint64_t seq, uint64_t bytes)
{
	/* the DPU's reverse DPA thread reads this cursor for its flow control */
	conn->cursor->consumed_seq = seq;
	conn->cursor->consumed_bytes = bytes;
}
