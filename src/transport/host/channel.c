#include "channel.h"

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <poll.h>

#include <doca_buf_array.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_pe.h>

#include "buffer.h"
#include "comch_client.h"
#include "comch_common.h"
#include "comch_msgq.h"
#include "common.h"
#include "dma.h"
#include "dpa.h"
#include "dpa_common.h"
#include "ring.h"
#include "session_protocol.h"

/*
 * DPUMesh transport, host end. Each channel owns one serialized Comch control
 * session; logical flows retain private forward rings and reverse DMA state.
 *
 * Dpu-dma reverse path: the DPU's DMA engine lands reverse batches in the window's slot
 * ring + data ring; the host polls the slots and publishes a consumption
 * cursor the DPU pulls.
 *
 * Host-dpa reverse path (DPUMESH_REVERSE=host-dpa): the reverse path mirrors the forward one. Per
 * connection the DPU exports its descriptor ring (rcv_ring) and tx_staging
 * (EXPORT_RCV_RING); the host imports both on the DPA device, runs one DPA
 * thread with the same poll_desc_ring kernel the DPU uses, and that thread
 * copies tx_staging into the window's data area, delivering one fused msgq
 * completion per descriptor (dpa.c's recv callback queues it in recv_segs).
 * The application's releases feed the kernel's staging gate (rd_pos) so it
 * never overwrites bytes the host still holds.
 */

DOCA_LOG_REGISTER(CHANNEL);

/* dpacc host stubs (dpa_kernel.a) */
extern doca_dpa_func_t thread_init_rpc;
extern struct doca_dpa_app *DPU_mesh_dpa_app;

_Static_assert(CHANNEL_DESC_N == DMESH_PUSH_DESC_N, "push slot count");
_Static_assert(CHANNEL_DATA_OFF == DMESH_PUSH_DATA_OFF, "push data offset");
_Static_assert(CHANNEL_WINDOW == BUFFER_SIZE, "push window");
_Static_assert(CHANNEL_MODE_CLIENT_HOST_DPA == DMESH_FLOW_MODE_CLIENT, "client host-dpa mode");
_Static_assert(CHANNEL_MODE_BACKEND_DPU_DMA == DMESH_FLOW_MODE_BACKEND, "backend mode");
_Static_assert(CHANNEL_MODE_CLIENT_DPU_DMA == DMESH_FLOW_MODE_INGRESS_PUSH, "ingress push mode");
_Static_assert(CHANNEL_MODE_BACKEND_HOST_DPA == DMESH_FLOW_MODE_BACKEND_PULL, "backend host-dpa mode");

#define CHANNEL_RING_SIZE 1024u            /* Forward ring depth of the host library */
#define CHANNEL_REV_READY_MS 5000          /* Wait for the DPU's EXPORT_RCV_RING */
#define CHANNEL_RD_POS_BATCH (64u * 1024u) /* Pull: bytes released between rd_pos publications */
#define CHANNEL_DEFAULT_REV_PCI "0b:00.0"  /* Pull: the host PF that runs the DPA process */
#define CHANNEL_CTX_STOP_SPINS 100000      /* Bound on progressing a stopping ctx to IDLE */

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
	int host_dpa;                       /* Nonzero: the host-dpa reverse path */
    struct doca_dev *reverse_dev;       /* base PF or optional SF */
    struct doca_dpa *reverse_dpa;       /* base context or SF extension */
    struct doca_dev *base_dev;
    struct doca_dpa *base_dpa;
};

struct channel_mem {
	struct doca_mmap *mmap;
	void *buf;
	size_t bytes;
	doca_dpa_dev_mmap_t dpa;            /* Handle on the forward device (the DPU DPA reads the TX pool) */
	doca_dpa_dev_mmap_t dpa_rev;        /* Pull: handle on the DPA device (the host DPA writes the RX region) */
};

struct channel_conn {
    struct channel_dev *dev;
    struct dma_ring *forward_ring;
    struct dmesh_export_rcv_ring_msg reverse_metadata;
    bool reverse_ready;
	doca_dpa_dev_mmap_t tx_dpa;
	volatile struct dmesh_push_desc *descs;
	volatile struct dmesh_push_cursor *cursor;
	size_t data_size;
	uint64_t expected;                  /* Push: next batch sequence */
	uint32_t flow_id, generation;
	int open_sent, ready, peer_closed, error, close_sent, close_error;
	/* host-dpa reverse path */
    struct dmesh_dpa_endpoint *reverse;
	struct doca_mmap *tx_mmap;          /* Imported DPU tx_staging */
	struct doca_mmap *ring_mmap;        /* Imported DPU rcv_ring */
	uint64_t rx_seq;                    /* Segments delivered to the carrier */
	uint64_t consumed_seq;              /* Segments the carrier released */
	uint32_t seg_end[CHANNEL_DESC_N]; /* End offset of delivered segment seq % N */
	uint32_t rd_pos;                    /* Kernel read watermark last published */
	uint64_t rd_published_bytes;
	uint64_t rd_published_seq;
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

int channel_dev_host_dpa(const struct channel_dev *dev)
{
	return dev != NULL && dev->host_dpa;
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
	case DMESH_SESSION_REVERSE_EXPORT:
		if (h.status || h.payload_len != sizeof(conn->reverse_metadata)) {
			conn->error = h.status ? h.status : EPROTO;
			break;
		}
		if (conn->reverse_ready) break;
		struct dmesh_export_rcv_ring_msg export;
		memcpy(&export, payload, sizeof(export));
		if (dmesh_validate_reverse_metadata(&export) != DOCA_SUCCESS)
            conn->error = EPROTO;
        else {
            conn->reverse_metadata = export;
            conn->reverse_ready = true;
        }
		break;
	case DMESH_SESSION_CLOSED:
		if (h.status) conn->close_error = h.status;
		else conn->peer_closed = 1;
		break;
	case DMESH_SESSION_ERROR:
		conn->error = h.status ? h.status : EIO;
		if (conn->close_sent) conn->close_error = conn->error;
		break;
	default:
		conn->error = EPROTO;
		break;
	}
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

static int session_progress_locked(struct channel_dev *dev)
{
	if (!dev->control) { errno = ENOTCONN; return -1; }
	(void)doca_pe_progress(dev->control->pe);
    backend_flush_rejections(dev);
	if (dev->control->peer_gone && !dev->session_error)
		dev->session_error = ECONNRESET;
	if (dev->session_error) { errno = dev->session_error; return -1; }
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
	return 0;
}

/* Synchronous setup/teardown is internal. Shared PE callbacks never enter a
 * sibling's data path, and no carrier slot lock is taken while progressing. */
static int session_wait_locked(struct channel_dev *dev, struct channel_conn *conn, int closing)
{
	uint64_t deadline = channel_now_ms() + CHANNEL_REV_READY_MS;
	const struct timespec pause = { .tv_nsec = 10000 };
	for (;;) {
		if (session_progress_locked(dev) != 0) return -1;
		if (!conn) { if (dev->hello_ready) return 0; }
		else if (closing) {
			if (conn->peer_closed) return 0;
			if (conn->close_error) { errno = conn->close_error; return -1; }
		} else {
			if (conn->error || conn->peer_closed) { errno = conn->error ? conn->error : ECONNRESET; return -1; }
			if (conn->ready && (!dev->host_dpa || conn->reverse_ready)) return 0;
		}
		if (channel_now_ms() >= deadline) { errno = ETIMEDOUT; return -1; }
		/* The wait owns only this flow; another EQ may progress the shared
		 * control PE and its own flow while this caller sleeps. */
		pthread_mutex_unlock(&dev->session_lock);
		nanosleep(&pause, NULL);
		pthread_mutex_lock(&dev->session_lock);
	}
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
    uint64_t deadline = channel_now_ms() + CHANNEL_REV_READY_MS;
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

/**
 * Bring up the DPA process for the host-dpa reverse path
 *
 * The DPA runs on a different host function than Comch (the DPU worker's DPA
 * counts against the Comch function); that PF's vhca must be in a DPA EU
 * partition. Several processes may each create their own DPA process on it.
 *
 * With DPUMESH_HOST_DPA_DEV the process is extended to an SF (doca_dpa_device_extend);
 * the SF then owns every DPA object and the kernel switches to it with
 * doca_dpa_dev_device_set (thread arg dpa_dev, also in the init RPC). On this
 * node's firmware processes may share one SF, but a second distinct SF extended
 * at the same time fails its consumer-completion CQ (devx syndrome 0x5ecb3).
 *
 * @dev [in]: Wire device being opened
 * @pci [in]: Comch PCI address (for the log line)
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t open_pull_dpa(struct channel_dev *dev, const char *pci)
{
	const char *rev_pci = getenv("DPUMESH_HOST_DPA_PCI");
	const char *rev_dev = getenv("DPUMESH_HOST_DPA_DEV");
	doca_error_t result;

	if (rev_pci == NULL || *rev_pci == '\0')
		rev_pci = CHANNEL_DEFAULT_REV_PCI;


	result = open_doca_device_with_pci(rev_pci, NULL, &dev->base_dev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to open DPA device %s with error = %s", rev_pci, doca_error_get_name(result));
		return result;
	}
	result = doca_dpa_create(dev->base_dev, &dev->base_dpa);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_set_app(dev->base_dpa, DPU_mesh_dpa_app);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_start(dev->base_dpa);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to start the DPA process on %s with error = %s (EU partition for its vhca? DPUMESH_HOST_DPA_PCI?)",
			     rev_pci, doca_error_get_name(result));
		return result;
	}

	if (rev_dev == NULL || *rev_dev == '\0') {
		dev->reverse_dev = dev->base_dev;
		dev->reverse_dpa = dev->base_dpa;
		DOCA_LOG_INFO("host-dpa reverse path: host DPA on %s, Comch on %s", rev_pci, pci);
		return DOCA_SUCCESS;
	}

	result = open_doca_device_with_ibdev_name((const uint8_t *)rev_dev, strlen(rev_dev), NULL, &dev->reverse_dev);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_device_extend(dev->base_dpa, dev->reverse_dev, &dev->reverse_dpa);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to extend the DPA process on %s to %s with error = %s", rev_pci, rev_dev,
			     doca_error_get_name(result));
		return result;
	}
	DOCA_LOG_INFO("host-dpa reverse path: DPA process on %s extended to %s, Comch on %s", rev_pci, rev_dev, pci);
	return DOCA_SUCCESS;
}

int channel_dev_open(const char *pci, struct channel_dev **out)
{
	struct channel_dev *dev;
	const char *reverse;
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

	reverse = getenv("DPUMESH_REVERSE");
	dev->host_dpa = reverse != NULL && strcmp(reverse, "host-dpa") == 0;
	if (dev->host_dpa) {
		result = open_pull_dpa(dev, pci);
		if (result != DOCA_SUCCESS)
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
    /* An extended context owns SF resources and retires before its base. */
    if (dev->reverse_dpa != NULL && dev->reverse_dpa != dev->base_dpa)
        (void)doca_dpa_destroy(dev->reverse_dpa);
    if (dev->base_dpa != NULL)
        (void)doca_dpa_destroy(dev->base_dpa);
    if (dev->reverse_dev != NULL && dev->reverse_dev != dev->base_dev)
        (void)doca_dev_close(dev->reverse_dev);
    if (dev->base_dev != NULL) {
        (void)doca_dev_close(dev->base_dev);
    }
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
 * Allocate a buffer and register it with one or two devices
 *
 * Like alloc_buffer_and_set_mmap, with the DPA device added as a second device
 * on the host-dpa reverse path (the host DPA writes the RX region; the TX pool stays on
 * the forward device only).
 *
 * @mmap [out]: Created mmap
 * @dev [in]: Forward (Comch) device
 * @dev2 [in]: DPA device, or NULL
 * @buffer [out]: Allocated buffer
 * @bytes [in]: Buffer size
 * @access [in]: mmap permissions
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t alloc_buffer_and_set_mmap2(struct doca_mmap **mmap, struct doca_dev *dev, struct doca_dev *dev2,
					       void **buffer, size_t bytes, uint32_t access)
{
	const char *step = "create";
	doca_error_t result;

	/* Opening the same function for Comch and DPA returns the same device. */
	if (dev2 == dev)
		dev2 = NULL;

	result = doca_mmap_create(mmap);
	if (result != DOCA_SUCCESS)
		goto out;

	if (dev2 != NULL) {
		step = "set_max_num_devices";
		result = doca_mmap_set_max_num_devices(*mmap, 2);
		if (result != DOCA_SUCCESS)
			goto destroy_mmap;
	}
	step = "add_dev";
	result = doca_mmap_add_dev(*mmap, dev);
	if (result != DOCA_SUCCESS)
		goto destroy_mmap;
	if (dev2 != NULL) {
		step = "add_dev(dpa)";
		result = doca_mmap_add_dev(*mmap, dev2);
		if (result != DOCA_SUCCESS)
			goto destroy_mmap;
	}
	step = "set_permissions";
	result = doca_mmap_set_permissions(*mmap, access);
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
	struct doca_dev *dev2 = dev->host_dpa ? dev->reverse_dev : NULL;
	doca_error_t result;

	mem = calloc(1, sizeof(*mem));
	if (mem == NULL)
		return -1;

	result = alloc_buffer_and_set_mmap2(&mem->mmap, dev->dev, dev2, &mem->buf, bytes,
					    DOCA_ACCESS_FLAG_PCI_READ_WRITE | DOCA_ACCESS_FLAG_LOCAL_READ_WRITE);
	if (result != DOCA_SUCCESS)
		goto free_mem;

	result = doca_mmap_dev_get_dpa_handle(mem->mmap, dev->dev, &mem->dpa);
	if (result == DOCA_SUCCESS && dev2 != NULL)
		result = doca_mmap_dev_get_dpa_handle(mem->mmap, dev2, &mem->dpa_rev);
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
 * Host-dpa reverse path: the reverse DPA thread of one connection
 * ---------------------------------------------------------------------------
 */

/**
 * Release the reverse DPA thread and everything bound to it
 *
 * Order: stop/stopped handshake (the poll loop calls thread_finish), then the
 * msgq/completion contexts, then the thread; never doca_dpa_thread_stop on a
 * comch-attached thread (see dpa.c). A failed cleanup preserves ownership and
 * prevents CLOSE, which is what permits the DPU to release its source memory.
 *
 * @conn [in]: Connection being torn down
 */
static int host_dpa_teardown(struct channel_conn *conn)
{
	struct dmesh_dpa_endpoint *rc = conn->reverse;
	doca_error_t result;

	if (rc != NULL) {
		result = dmesh_dpa_quiesce_checked(rc->dpa_thread, rc->dpa_comch, rc->pe);
		if (result != DOCA_SUCCESS) goto fail;
		result = dmesh_dpa_comch_destroy_checked(rc->dpa_thread, &rc->dpa_comch, rc->pe);
		if (result != DOCA_SUCCESS) goto fail;
		if (rc->buf_arr != NULL) {
			result = doca_buf_arr_destroy(rc->buf_arr);
			if (result != DOCA_SUCCESS) goto fail;
			rc->buf_arr = NULL;
		}
		if (rc->dpa_thread != NULL) {
			result = dmesh_doca_dpa_thread_destroy_checked(rc->dpa_thread);
			if (result != DOCA_SUCCESS) goto fail;
			free(rc->dpa_thread);
			rc->dpa_thread = NULL;
		}
	}
	if (conn->reverse != NULL && conn->reverse->pe != NULL) {
		result = doca_pe_destroy(conn->reverse->pe);
		if (result != DOCA_SUCCESS) goto fail;
		conn->reverse->pe = NULL;
	}
	/* Imported mmaps cannot be explicitly stopped; destroy implicitly stops.
	 * All their DMA users and buffer arrays have retired before this point. */
	if (conn->tx_mmap != NULL) {
		result = doca_mmap_destroy(conn->tx_mmap);
		if (result != DOCA_SUCCESS) goto fail;
		conn->tx_mmap = NULL;
	}
	if (conn->ring_mmap != NULL) {
		result = doca_mmap_destroy(conn->ring_mmap);
		if (result != DOCA_SUCCESS) goto fail;
		conn->ring_mmap = NULL;
	}
	if (rc != NULL) {
		free(rc->recv_segs);
		free(rc);
		conn->reverse = NULL;
	}
	return 0;
fail:
	errno = error_number(result);
	return -1;
}

/**
 * Import the DPU's exports for this connection
 *
 * The descriptor ring is polled through a DPA memory window
 * (doca_dpa_dev_buf_get_external_ptr), which the SDK supports only for a
 * buffer created on a PF device with the base (non-extended) DPA context; so
 * the ring import lives on the PF even when everything else is on the SF.
 *
 * @conn [in]: Connection
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t host_dpa_import_exports(struct channel_conn *conn)
{
	struct channel_dev *dev = conn->dev;
	doca_error_t result;

	result = doca_mmap_create_from_export(NULL, conn->reverse_metadata.tx_desc, conn->reverse_metadata.tx_desc_len,
					      dev->reverse_dev, &conn->tx_mmap);
	if (result == DOCA_SUCCESS)
		result = doca_mmap_create_from_export(NULL, conn->reverse_metadata.ring_desc, conn->reverse_metadata.ring_desc_len,
						      dev->base_dev, &conn->ring_mmap);
	if (result != DOCA_SUCCESS)
		DOCA_LOG_ERR("Failed to import the DPU exports with error = %s", doca_error_get_name(result));
	return result;
}

/**
 * Create the reverse connection: its PE, DPA thread, msgqs and ring buf_arr
 *
 * @conn [in]: Connection
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t host_dpa_create_thread(struct channel_conn *conn)
{
	struct channel_dev *dev = conn->dev;
	struct dmesh_dpa_endpoint *rc;
	doca_error_t result;

    rc = calloc(1, sizeof(*rc));
    if (rc == NULL) return DOCA_ERROR_NO_MEMORY;
    conn->reverse = rc;
    result = doca_pe_create(&rc->pe);
    if (result != DOCA_SUCCESS) return result;

	rc->dpa_thread = calloc(1, sizeof(*rc->dpa_thread));
	if (rc->dpa_thread == NULL)
		return DOCA_ERROR_NO_MEMORY;
	rc->dpa_thread->dpa = dev->reverse_dpa;

	result = dmesh_doca_dpa_thread_create(rc->dpa_thread);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to create DPA thread with error = %s", doca_error_get_name(result));
		return result;
	}
	result = dmesh_dpa_endpoint_init_comch(rc, dev->reverse_dev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to init DPA msgqs with error = %s", doca_error_get_name(result));
		return result;
	}

	/* descriptor ring: buf_array over the imported DPU rcv_ring, on the base context */
	result = doca_buf_arr_create(DMA_RING_SIZE + 1, &rc->buf_arr);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_set_target_dpa(rc->buf_arr, dev->base_dpa);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_set_params(rc->buf_arr, conn->ring_mmap, sizeof(struct dma_desc), 0);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_start(rc->buf_arr);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set up the descriptor ring buf array with error = %s", doca_error_get_name(result));
		return result;
	}

	/* completed-segment ring the recv callback fills; drained by channel_conn_rx_next */
	rc->recv_segs = calloc(DMESH_RECV_SEG_MAX, sizeof(struct dmesh_recv_seg));
	if (rc->recv_segs == NULL)
		return DOCA_ERROR_NO_MEMORY;

	return DOCA_SUCCESS;
}

/**
 * Point the kernel at this connection's window and start it
 *
 * @conn [in]: Connection
 * @cfg [in]: Connection configuration (RX window)
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t host_dpa_run_thread(struct channel_conn *conn, const struct channel_conn_config *cfg)
{
	struct channel_dev *dev = conn->dev;
	struct dmesh_dpa_endpoint *rc = conn->reverse;
	struct dpa_thread_arg arg;
	doca_dpa_dev_comch_consumer_completion_t consumer_comp;
	doca_dpa_dev_completion_t producer_comp;
	doca_dpa_dev_comch_producer_t producer;
	doca_dpa_dev_comch_consumer_t consumer;
	doca_dpa_dev_mmap_t src_mmap;
	doca_dpa_dev_buf_arr_t buf_arr;
	doca_dpa_dev_t dpa_dev = 0;
	struct comch_msg kick = {0};
	uint64_t rpc_ret;
	uint8_t *dst;
	doca_error_t result;

	/* DPA handles */
	result = doca_comch_consumer_completion_get_dpa_handle(rc->dpa_comch->consumer_comp, &consumer_comp);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_completion_get_dpa_handle(rc->dpa_comch->producer_comp, &producer_comp);
	if (result == DOCA_SUCCESS)
		result = doca_comch_consumer_get_dpa_handle(rc->dpa_comch->send.consumer, &consumer);
	if (result == DOCA_SUCCESS)
		result = doca_comch_producer_get_dpa_handle(rc->dpa_comch->recv.producer, &producer);
	if (result == DOCA_SUCCESS)
		result = doca_buf_arr_get_dpa_handle(rc->buf_arr, &buf_arr);
	if (result == DOCA_SUCCESS)
		result = doca_mmap_dev_get_dpa_handle(conn->tx_mmap, dev->reverse_dev, &src_mmap);
	if (result == DOCA_SUCCESS && dev->reverse_dpa != dev->base_dpa)
		result = doca_dpa_get_dpa_handle(dev->reverse_dpa, &dpa_dev); /* extended: the kernel switches device */
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to get DPA handles with error = %s", doca_error_get_name(result));
		return result;
	}

	/* destination: this connection's window data area, same layout as push */
	dst = (uint8_t *)cfg->rx->buf + cfg->rx_offset + CHANNEL_DATA_OFF;
	arg = (struct dpa_thread_arg) {
		.dpa_consumer_comp = consumer_comp,
		.dpa_producer_comp = producer_comp,
		.dpa_consumer = consumer,
		.dpa_producer = producer,
		.dpa_buf_arr = buf_arr,
		.buf_arr_size = DMA_RING_SIZE,
		.host_mmap = src_mmap,               /* DMA source: DPU tx_staging */
		.dpu_mmap = cfg->rx->dpa_rev,        /* DMA destination: host RX region */
		.src_addr = (uint64_t)(uintptr_t)dst, /* destination base */
		.buf_size = (uint32_t)conn->data_size,
		.rd_pos = 0,
		.rd_fc = 1,                          /* the application's releases gate reuse */
		.dpa_dev = (uint64_t)dpa_dev,
	};

	result = doca_dpa_rpc(rc->dpa_thread->dpa, thread_init_rpc, &rpc_ret, arg.dpa_consumer,
			      (uint32_t)CC_DPA_MAX_MSG_NUM, arg.dpa_dev);
	if (result == DOCA_SUCCESS)
		result = doca_dpa_h2d_memcpy(rc->dpa_thread->dpa, rc->dpa_thread->arg, &arg, sizeof(arg));
	if (result == DOCA_SUCCESS) {
		/* A failed run can be ambiguous: cleanup must establish quiescence. */
		rc->dpa_thread->running = true;
		result = doca_dpa_thread_run(rc->dpa_thread->thread);
	}
	/* kick the thread so it enters its poll loop (a thread only wakes on a completion) */
	if (result == DOCA_SUCCESS)
		result = dmesh_doca_dpa_msgq_send(&rc->dpa_comch->send, &kick, sizeof(kick));
	if (result != DOCA_SUCCESS)
		DOCA_LOG_ERR("Failed to start the DPA thread with error = %s", doca_error_get_name(result));
	return result;
}

/**
 * Set up the reverse path of one connection on the host-dpa reverse path
 *
 * Mirror of the DPU's per-connection setup (and of the legacy host worker's
 * setup_reverse_dpa): import the DPU's exports, bind a DPA thread + msgq to
 * them, point the kernel at this connection's window and kick it.
 *
 * @conn [in]: Connection
 * @cfg [in]: Connection configuration
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t host_dpa_setup(struct channel_conn *conn, const struct channel_conn_config *cfg)
{
	doca_error_t result;

	result = host_dpa_import_exports(conn);
	if (result != DOCA_SUCCESS)
		return result;
	result = host_dpa_create_thread(conn);
	if (result != DOCA_SUCCESS)
		return result;
	return host_dpa_run_thread(conn, cfg);
}

/*
 * ---------------------------------------------------------------------------
 * Connections
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
	/* Only reached before OPEN or after confirmed CLOSED and reverse quiescence. */
	if (conn->forward_ring != NULL) {
		if (conn->forward_ring->mmap != NULL) {
			doca_error_t result = destroy_mmap_and_free_buffer(conn->forward_ring->mmap, conn->forward_ring->buffer);
			if (result != DOCA_SUCCESS) { errno = error_number(result); return -1; }
		}
		free(conn->forward_ring);
	}
	/* All other memory and the control PE belong to the channel. */
	free(conn);
	return 0;
}

int channel_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out)
{
	struct channel_conn *conn;
	doca_error_t result;
	if (!dev || !cfg || !out || !cfg->flow_id || cfg->flow_id > 32 || !cfg->tx || !cfg->rx ||
	    cfg->rx_offset > cfg->rx->bytes || CHANNEL_WINDOW > cfg->rx->bytes - cfg->rx_offset) { errno = EINVAL; return -1; }
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

	/* forward ring and the shared regions as this connection's sndbuf/rcvbuf */
	result = alloc_dma_ring(&conn->forward_ring, dev->dev, CHANNEL_RING_SIZE);
	if (result != DOCA_SUCCESS) goto fail;
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

	pthread_mutex_lock(&dev->session_lock);
	struct dmesh_export_metadata_msg metadata;
	result = dmesh_build_dma_metadata(dev->dev, conn->forward_ring, &sndbuf, &rcvbuf, &flow, &metadata);
	int rc = -1;
	if (result != DOCA_SUCCESS) errno = error_number(result);
    else {
        uint8_t payload[8 + sizeof(metadata)];
        uint16_t type = cfg->backend_token ? DMESH_SESSION_BACKEND_OPEN : DMESH_SESSION_OPEN;
        size_t offset = cfg->backend_token ? 8 : 0;
        dmesh_session_put_u32(payload, cfg->backend_worker);
        dmesh_session_put_u32(payload + 4, cfg->backend_token);
        memcpy(payload + offset, &metadata, sizeof(metadata));
        if (session_send_locked(dev, type, conn->flow_id, conn->generation,
                               payload, offset + sizeof(metadata)) == 0) {
            conn->open_sent = 1;
            rc = session_wait_locked(dev, conn, 0);
        }
    }
	int saved = errno;
	pthread_mutex_unlock(&dev->session_lock);
	if (rc != 0) {
		/* On failed cleanup the session retains the map entry and all mmaps. */
		(void)channel_conn_close(conn);
		errno = saved;
		return -1;
	}

	if (dev->host_dpa) {
		result = host_dpa_setup(conn, cfg);
		if (result != DOCA_SUCCESS) {
			saved = error_number(result);
			(void)channel_conn_close(conn);
			errno = saved; return -1;
		}
	}

	*out = conn;
	return 0;

fail:
	(void)channel_conn_close(conn);
	errno = error_number(result);
	return -1;
}

int channel_conn_close(struct channel_conn *conn)
{
	if (conn == NULL) return 0;
	/* Stop the reverse reader BEFORE asking the DPU to release its source. */
	if (conn->dev->host_dpa && host_dpa_teardown(conn) != 0) return -1;
	struct channel_dev *dev = conn->dev;
	pthread_mutex_lock(&dev->session_lock);
	int rc = 0;
	if (conn->open_sent && !conn->peer_closed) {
		conn->close_error = 0;
		rc = session_send_locked(dev, DMESH_SESSION_CLOSE, conn->flow_id, conn->generation, NULL, 0);
		if (rc == 0) conn->close_sent = 1;
		if (rc == 0) rc = session_wait_locked(dev, conn, 1);
	}
	if (rc == 0) {
		uint32_t id = conn->flow_id;
		rc = conn_teardown(conn);
		if (rc == 0) dev->flows[id] = NULL;
	}
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
	pthread_mutex_lock(&dev->session_lock);
	int saved = !dev->control ? ENOTCONN :
		(dev->session_error ? dev->session_error : conn->error);
	int gone = conn->peer_closed;
	pthread_mutex_unlock(&dev->session_lock);
	if (conn->reverse != NULL && conn->reverse->pe != NULL)
		(void)doca_pe_progress(conn->reverse->pe);
	if (saved) { errno = saved; return -1; }
	return gone;
}

int channel_conn_progress(struct channel_conn *conn)
{
	(void)channel_dev_progress(conn->dev);
	return channel_conn_poll(conn);
}

/*
 * ---------------------------------------------------------------------------
 * Doorbells
 * ---------------------------------------------------------------------------
 */

/**
 * Collect the progress engines of a connection
 *
 * @conn [in]: Connection
 * @pes [out]: Engines (CHANNEL_CONN_FDS at most)
 * @return: Number of engines
 */
static int conn_engines(struct channel_conn *conn, struct doca_pe **pes)
{
	int n = 0;

	if (conn->reverse != NULL && conn->reverse->pe != NULL)
		pes[n++] = conn->reverse->pe;
	return n;
}

int channel_conn_fds(struct channel_conn *conn, int *fds, int max)
{
	struct doca_pe *pes[CHANNEL_CONN_FDS];
	doca_notification_handle_t handle;
	int n = conn_engines(conn, pes);
	int i, count = 0;

	for (i = 0; i < n && count < max; i++) {
		if (doca_pe_get_notification_handle(pes[i], &handle) != DOCA_SUCCESS)
			continue;
		fds[count++] = (int)handle;
	}
	return count;
}

int channel_conn_arm(struct channel_conn *conn)
{
	struct doca_pe *pes[CHANNEL_CONN_FDS];
	int n = conn_engines(conn, pes);
	int i;
	doca_error_t result;

	for (i = 0; i < n; i++) {
		result = doca_pe_request_notification(pes[i]);
		if (result != DOCA_SUCCESS) {
			DOCA_LOG_ERR("Failed to arm progress engine with error = %s", doca_error_get_name(result));
			errno = error_number(result);
			return -1;
		}
	}
	return 0;
}

void channel_conn_clear(struct channel_conn *conn, int fd)
{
	struct doca_pe *pes[CHANNEL_CONN_FDS];
	doca_notification_handle_t handle;
	int n = conn_engines(conn, pes);
	int i;

	for (i = 0; i < n; i++) {
		if (doca_pe_get_notification_handle(pes[i], &handle) != DOCA_SUCCESS || (int)handle != fd)
			continue;
		struct pollfd ready = { .fd = fd, .events = POLLIN };
		if (poll(&ready, 1, 0) > 0 && (ready.revents & POLLIN))
			(void)doca_pe_clear_notification(pes[i], handle);
		return;
	}
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
	return ring->head;
}

uint64_t channel_conn_consumed(const struct channel_conn *conn)
{
	return conn->forward_ring->ctrl->consumer_head;
}

/*
 * ---------------------------------------------------------------------------
 * Reverse path
 * ---------------------------------------------------------------------------
 */

/**
 * Host-dpa reverse path: next completed DPA copy, delivered in order
 *
 * The carrier tracks at most CHANNEL_DESC_N live batches, so a segment is
 * handed out only while that many are outstanding.
 *
 * @conn [in]: Connection
 * @seq [out]: Batch sequence
 * @pos [out]: Offset in the window's data ring
 * @len [out]: Length
 * @return: 1 with a batch, 0 when none, -1 on a malformed segment
 */
static int host_dpa_rx_next(struct channel_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len)
{
	struct dmesh_dpa_endpoint *rc = conn->reverse;
	struct dmesh_recv_seg *seg;
	uint32_t p, n;

	if (rc->recv_seg_cnt == 0 || conn->rx_seq - conn->consumed_seq >= CHANNEL_DESC_N)
		return 0;

	seg = &rc->recv_segs[rc->recv_seg_head];
	p = seg->pos;
	n = seg->len;
	rc->recv_seg_head = (rc->recv_seg_head + 1) % DMESH_RECV_SEG_MAX;
	rc->recv_seg_cnt--;
	if (n == 0 || (size_t)p + n > conn->data_size)
		return -1;

	conn->rx_seq++;
	conn->seg_end[conn->rx_seq % CHANNEL_DESC_N] = p + n;
	*seq = conn->rx_seq;
	*pos = p;
	*len = n;
	return 1;
}

int channel_conn_rx_next(struct channel_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len)
{
	volatile struct dmesh_push_desc *desc;
	uint32_t p, n;

	if (conn->dev->host_dpa)
		return conn->reverse != NULL ? host_dpa_rx_next(conn, seq, pos, len) : 0;

	/* push: the next slot the DPU's DMA engine filled */
	desc = &conn->descs[conn->expected % DMESH_PUSH_DESC_N];
	if (desc->seq != conn->expected)
		return 0;
	p = desc->pos;
	n = desc->len;
	if (n == 0 || (size_t)p + n > conn->data_size)
		return -1;

	*seq = conn->expected;
	*pos = p;
	*len = n;
	conn->expected++;
	return 1;
}

/**
 * Host-dpa reverse path: publish the kernel's read watermark
 *
 * The watermark is the end of the newest released segment (copies land in
 * order; the kernel wraps a copy that would cross the end, so bytes do not map
 * linearly to offsets). The device-side write is coalesced: the kernel gates
 * only when fewer than 3 x 8064 B of the 1 MiB ring look free, so publishing
 * every CHANNEL_RD_POS_BATCH bytes keeps it far from the gate.
 *
 * @conn [in]: Connection
 * @seq [in]: Newest released batch
 * @bytes [in]: Bytes released so far
 */
static void host_dpa_rx_consumed(struct channel_conn *conn, uint64_t seq, uint64_t bytes)
{
	if (seq <= conn->consumed_seq)
		return;
	conn->consumed_seq = seq;
	conn->rd_pos = conn->seg_end[seq % CHANNEL_DESC_N] % (uint32_t)conn->data_size;

	if (bytes - conn->rd_published_bytes < CHANNEL_RD_POS_BATCH &&
	    seq - conn->rd_published_seq < CHANNEL_DESC_N / 2)
		return;
	conn->rd_published_bytes = bytes;
	conn->rd_published_seq = seq;
	(void)doca_dpa_h2d_memcpy(conn->reverse->dpa_thread->dpa,
				  conn->reverse->dpa_thread->arg + offsetof(struct dpa_thread_arg, rd_pos),
				  &conn->rd_pos, sizeof(conn->rd_pos));
}

void channel_conn_rx_consumed(struct channel_conn *conn, uint64_t seq, uint64_t bytes)
{
	if (conn->dev->host_dpa) {
		/* RX credits may outlive a failed close. A completed DMA fence or a
		 * partially destroyed thread no longer needs a device watermark. */
		if (conn->reverse != NULL && conn->reverse->dpa_thread != NULL &&
		    conn->reverse->dpa_thread->running && !conn->reverse->dpa_thread->quiesced)
			host_dpa_rx_consumed(conn, seq, bytes);
		return;
	}
	/* push: the DPU pulls this cursor for its flow control */
	conn->cursor->consumed_seq = seq;
	conn->cursor->consumed_bytes = bytes;
}
