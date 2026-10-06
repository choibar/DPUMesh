#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* POLLRDHUP */
#endif
#include "channel_internal.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <doca_log.h>
#include <doca_pe.h>

#include "broker_ipc.h"
#include "comch_client.h"
#include "dpa_common.h"
#include "ring.h"
#include "session_protocol.h"

/*
 * The host broker owns the DOCA device of an application's channel (plan:
 * docs/2026-09-29_host-broker-plan.md, structure from ~/DPUmesh's per-Pod
 * broker). Both ends of broker_ipc.h live here:
 *
 *   client  the application's channel: every control operation is one request
 *           on the broker socket; the regions and forward rings arrive as
 *           memfds and the data path runs on their mappings unchanged.
 *   server  one broker child per client: runs channel.c as the device owner
 *           with shared memory, and publishes what the DPU reports about each
 *           flow (CLOSED, errors) in the status page the client polls.
 *
 * Idle wake: the client's channel_dev_arm lists its push flows in the arm page
 * and kicks the broker, which sends the DPU the ARM; the DOORBELL, and any
 * status the broker publishes, raise the wake eventfd the client sleeps on.
 *
 * Only dpu-dma flows are brokered; the host-dpa path, and any channel without
 * a broker configured, opens the device in the application (channel_dev_open).
 */

DOCA_LOG_REGISTER(CHANNEL_BROKER);

#define BROKER_MEMS 8                                   /* Regions per client (the carrier uses 2) */
#define BROKER_MAX_REGION (1ull << 32)                  /* Largest region a client may register */
#define BROKER_CHECK_NS (20ull * 1000 * 1000)           /* Client: broker liveness check interval */
#define BROKER_IDLE_MS 1000                             /* Server: wait bound with an armed PE */
#define BROKER_POLL_MS 10                               /* Server: wait bound without a PE doorbell */
#define BROKER_REPLY_TIMEOUT_S 30                       /* Client: longest request (open + failed-open close) */
#define BROKER_ARM_READ_TRIES 64                        /* Server: seqlock retries before the client is ignored */

_Static_assert(BROKER_FLOWS == sizeof(((struct channel_dev *)0)->flows) / sizeof(void *), "flow slots");
_Static_assert(BROKER_FLOWS - 1 == DMESH_SESSION_MAX_FLOWS, "one ARM lists every flow");

struct channel_broker {
	int sock;
	pthread_mutex_t lock;                           /* One request in flight on sock */
	const volatile struct broker_status *status;    /* Mapped read-only */
	struct broker_arm_page *arm;                    /* Written under the channel's session_lock */
	int wake_fd, kick_fd;                           /* Idle wake eventfds (-1 before HELLO) */
	uint64_t next_check_ns;
	int dead;                                       /* errno once the broker connection failed */
};

static int push_mode(uint32_t mode)
{
	return mode == CHANNEL_MODE_CLIENT_DPU_DMA || mode == CHANNEL_MODE_BACKEND_DPU_DMA;
}

static int name_ok(const char *name)
{
	return memchr(name, '\0', BROKER_NAME_LEN) != NULL;
}

/*
 * ---------------------------------------------------------------------------
 * Client
 * ---------------------------------------------------------------------------
 */

/**
 * Map a memfd the broker sent and close it
 *
 * @fd [in]: memfd (closed on return)
 * @bytes [in]: Size the broker reported
 * @prot [in]: PROT_READ, or PROT_READ | PROT_WRITE
 * @return: The mapping, or NULL with errno
 */
static void *map_memfd(int fd, size_t bytes, int prot)
{
	void *map = MAP_FAILED;

	if (broker_ipc_check_memfd(fd, bytes) == 0)
		map = mmap(NULL, bytes, prot, MAP_SHARED | MAP_POPULATE, fd, 0); /* no faults on the data path */
	int saved = errno;
	close(fd);
	errno = saved;
	return map == MAP_FAILED ? NULL : map;
}

static int broker_dead(struct channel_broker *b)
{
	return __atomic_load_n(&b->dead, __ATOMIC_ACQUIRE);
}

static void broker_fail(struct channel_broker *b, int error)
{
	int none = 0;
	if (__atomic_compare_exchange_n(&b->dead, &none, error ? error : EIO, 0, __ATOMIC_RELEASE, __ATOMIC_RELAXED))
		DOCA_LOG_ERR("Broker connection lost: %s", strerror(error ? error : EIO));
}

/**
 * One request/reply round trip
 *
 * A transport or framing failure ends the connection: the broker cleans up
 * after a lost client, so no retry can resume it.
 *
 * @b [in]: Broker connection
 * @req [in]: Request (magic and version are filled in)
 * @rep [out]: Reply
 * @fds [out]: The descriptors a successful reply carries
 * @nfds [in]: How many it must carry
 * @return: 0 on success, -1 with errno (the broker's status, or the failure)
 */
static int broker_call(struct channel_broker *b, struct broker_request *req, struct broker_reply *rep, int *fds,
		       int nfds)
{
	int got[BROKER_MAX_FDS], n = -1;

	memcpy(req->magic, BROKER_IPC_MAGIC, sizeof(req->magic));
	req->version = BROKER_IPC_VERSION;
	pthread_mutex_lock(&b->lock);
	if (broker_dead(b))
		errno = broker_dead(b);
	else if (broker_ipc_send(b->sock, req, sizeof(*req), NULL, 0) == 0)
		n = broker_ipc_recv(b->sock, rep, sizeof(*rep), got, BROKER_MAX_FDS);
	if (n < 0 && !broker_dead(b))
		broker_fail(b, errno == EAGAIN ? ETIMEDOUT : errno); /* a late reply would desync the socket */
	pthread_mutex_unlock(&b->lock);
	if (n < 0)
		return -1;

	if (memcmp(rep->magic, BROKER_IPC_MAGIC, sizeof(rep->magic)) != 0 || rep->type != BROKER_REPLY ||
	    rep->version != BROKER_IPC_VERSION || rep->fd_count != n || (rep->status == 0 && n != nfds)) {
		while (n > 0) close(got[--n]);
		broker_fail(b, EPROTO);
		errno = EPROTO;
		return -1;
	}
	if (rep->status != 0) {
		while (n > 0) close(got[--n]);
		errno = rep->status;
		return -1;
	}
	if (n > 0)
		memcpy(fds, got, sizeof(int) * (size_t)n);
	return 0;
}

int channel_broker_attach(const char *path, struct channel_dev **out)
{
	struct channel_dev *dev = calloc(1, sizeof(*dev));
	struct channel_broker *b = calloc(1, sizeof(*b));

	if (dev == NULL || b == NULL) {
		free(dev);
		free(b);
		errno = ENOMEM;
		return -1;
	}
	b->sock = broker_ipc_connect(path);
	if (b->sock < 0) {
		int saved = errno;
		DOCA_LOG_ERR("Failed to connect to the DPUmesh broker at %s: %s", path, strerror(saved));
		free(dev);
		free(b);
		errno = saved;
		return -1;
	}
	struct timeval timeout = { .tv_sec = BROKER_REPLY_TIMEOUT_S };
	(void)setsockopt(b->sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
	(void)setsockopt(b->sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
	pthread_mutex_init(&b->lock, NULL);
	pthread_mutex_init(&dev->session_lock, NULL);
	b->wake_fd = -1;
	b->kick_fd = -1;
	dev->broker = b;
	*out = dev;
	return 0;
}

void channel_broker_detach(struct channel_dev *dev)
{
	struct channel_broker *b = dev->broker;

	close(b->sock); /* the broker closes whatever the client left open */
	if (b->status != NULL)
		(void)munmap((void *)b->status, sizeof(struct broker_status));
	if (b->arm != NULL)
		(void)munmap(b->arm, sizeof(struct broker_arm_page));
	if (b->wake_fd >= 0)
		close(b->wake_fd);
	if (b->kick_fd >= 0)
		close(b->kick_fd);
	pthread_mutex_destroy(&b->lock);
	pthread_mutex_destroy(&dev->session_lock);
	free(b);
	free(dev);
}

int channel_broker_session_open(struct channel_dev *dev, const char *server)
{
	struct channel_broker *b = dev->broker;
	struct broker_request req = { .type = BROKER_HELLO };
	struct broker_reply rep;
	int fds[BROKER_HELLO_FDS];

	if (b->status != NULL) { errno = EALREADY; return -1; }
	if (strlen(server) >= sizeof(req.name)) { errno = EINVAL; return -1; }
	snprintf(req.name, sizeof(req.name), "%s", server);
	if (broker_call(b, &req, &rep, fds, BROKER_HELLO_FDS) != 0)
		return -1;
	if (rep.bytes == sizeof(struct broker_status) && broker_ipc_check_eventfd(fds[BROKER_FD_WAKE]) == 0 &&
	    broker_ipc_check_eventfd(fds[BROKER_FD_KICK]) == 0) {
		b->status = map_memfd(fds[BROKER_FD_STATUS], sizeof(struct broker_status), PROT_READ);
		b->arm = map_memfd(fds[BROKER_FD_ARM], sizeof(struct broker_arm_page), PROT_READ | PROT_WRITE);
		b->wake_fd = fds[BROKER_FD_WAKE];
		b->kick_fd = fds[BROKER_FD_KICK];
	} else {
		for (int i = 0; i < BROKER_HELLO_FDS; ++i) close(fds[i]);
	}
	if (b->status == NULL || b->arm == NULL) {
		broker_fail(b, EPROTO);
		errno = EPROTO;
		return -1;
	}
	dev->hello_ready = 1;
	return 0;
}

int channel_broker_session_close(struct channel_dev *dev)
{
	struct broker_request req = { .type = BROKER_SESSION_CLOSE };
	struct broker_reply rep;

	if (!dev->hello_ready)
		return 0;
	/* A lost broker took the session and its registrations with it. */
	if (broker_call(dev->broker, &req, &rep, NULL, 0) != 0 && !broker_dead(dev->broker))
		return -1;
	dev->hello_ready = 0;
	return 0;
}

int channel_broker_mem_alloc(struct channel_dev *dev, size_t bytes, struct channel_mem **out)
{
	struct broker_request req = { .type = BROKER_MEM_ALLOC, .bytes = bytes };
	struct broker_reply rep;
	struct channel_mem *mem;
	int fd;

	if (broker_call(dev->broker, &req, &rep, &fd, 1) != 0)
		return -1;
	mem = calloc(1, sizeof(*mem));
	if (mem == NULL) {
		close(fd);
		errno = ENOMEM;
		return -1;
	}
	mem->buf = rep.bytes == bytes ? map_memfd(fd, bytes, PROT_READ | PROT_WRITE) : NULL;
	if (mem->buf == NULL) {
		/* The broker keeps the region until the channel closes. */
		int saved = rep.bytes == bytes ? errno : EPROTO;
		if (rep.bytes != bytes) close(fd);
		free(mem);
		errno = saved;
		return -1;
	}
	mem->bytes = bytes;
	mem->dpa = rep.dpa;
	mem->shared = 1;
	mem->memfd = -1;
	mem->id = rep.id;
	mem->remote_base = rep.base;
	*out = mem;
	return 0;
}

int channel_broker_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg,
			     struct channel_conn **out)
{
	struct broker_request req = {
		.type = BROKER_CONN_OPEN,
		.flow_id = cfg->flow_id,
		.mode = cfg->mode,
		.src_ip = cfg->src_ip,
		.dst_ip = cfg->dst_ip,
		.src_port = cfg->src_port,
		.dst_port = cfg->dst_port,
		.tx_id = cfg->tx->id,
		.rx_id = cfg->rx->id,
		.rx_offset = cfg->rx_offset,
	};
	const size_t ring_bytes = sizeof(struct dma_ring_ctrl) + CHANNEL_RING_SIZE * sizeof(struct dma_desc);
	struct broker_reply rep;
	struct channel_conn *conn;
	struct dma_ring *ring;
	void *map;
	int fd;

	if (!push_mode(cfg->mode) || !cfg->tx->shared || !cfg->rx->shared) { errno = ENOTSUP; return -1; }
	if (dev->flows[cfg->flow_id] != NULL) { errno = EBUSY; return -1; }
	snprintf(req.name, sizeof(req.name), "%s", cfg->workload != NULL ? cfg->workload : "");
	if (broker_call(dev->broker, &req, &rep, &fd, 1) != 0)
		return -1;
	map = rep.bytes == ring_bytes ? map_memfd(fd, ring_bytes, PROT_READ | PROT_WRITE) : NULL;
	conn = calloc(1, sizeof(*conn));
	ring = calloc(1, sizeof(*ring));
	if (map == NULL || conn == NULL || ring == NULL) {
		/* The broker opened a flow this client cannot use: close just that one. */
		int saved = rep.bytes != ring_bytes ? EPROTO : map == NULL ? errno : ENOMEM;
		struct broker_request close_req = { .type = BROKER_CONN_CLOSE, .flow_id = cfg->flow_id };
		if (map != NULL) (void)munmap(map, ring_bytes);
		else if (rep.bytes != ring_bytes) close(fd);
		free(conn);
		free(ring);
		(void)broker_call(dev->broker, &close_req, &rep, NULL, 0);
		errno = saved;
		return -1;
	}

	/* The broker allocated and zeroed the ring and set up the push window. */
	ring->size = CHANNEL_RING_SIZE;
	ring->buffer = map;
	ring->ctrl = (struct dma_ring_ctrl *)map;
	ring->descs = (struct dma_desc *)((uint8_t *)map + sizeof(struct dma_ring_ctrl));
	char *window = (char *)cfg->rx->buf + cfg->rx_offset;
	conn->dev = dev;
	conn->forward_ring = ring;
	conn->ring_fd = -1;
	conn->ring_bytes = ring_bytes;
	conn->descs = (volatile struct dmesh_push_desc *)window;
	conn->cursor = (volatile struct dmesh_push_cursor *)(window + DMESH_PUSH_CURSOR_OFF);
	conn->data_size = CHANNEL_WINDOW - DMESH_PUSH_DATA_OFF;
	conn->expected = 1;
	conn->tx_dpa = cfg->tx->dpa;
	conn->tx_delta = cfg->tx->remote_base - (uint64_t)(uintptr_t)cfg->tx->buf;
	conn->flow_id = cfg->flow_id;
	conn->generation = rep.id;
	conn->open_sent = 1;
	conn->ready = 1;
	pthread_mutex_lock(&dev->session_lock);
	dev->flows[conn->flow_id] = conn;
	pthread_mutex_unlock(&dev->session_lock);
	*out = conn;
	return 0;
}

int channel_broker_conn_close(struct channel_conn *conn)
{
	struct channel_dev *dev = conn->dev;
	struct broker_request req = { .type = BROKER_CONN_CLOSE, .flow_id = conn->flow_id };
	struct broker_reply rep;

	/* A failed close retains the flow for a retry, unless the broker (and with
	 * it every DMA mapping of this memory) is gone. */
	if (broker_call(dev->broker, &req, &rep, NULL, 0) != 0 && !broker_dead(dev->broker))
		return -1;
	pthread_mutex_lock(&dev->session_lock);
	dev->flows[conn->flow_id] = NULL;
	pthread_mutex_unlock(&dev->session_lock);
	channel_shared_free(conn->forward_ring->buffer, conn->ring_bytes, -1);
	free(conn->forward_ring);
	free(conn);
	return 0;
}

int channel_broker_dev_progress(struct channel_dev *dev)
{
	struct channel_broker *b = dev->broker;
	struct timespec ts;
	uint64_t now;

	/* A dead broker leaves every flow silent, never closed: look for it now and then. */
	clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
	now = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
	if (now >= __atomic_load_n(&b->next_check_ns, __ATOMIC_RELAXED)) {
		struct pollfd p = { .fd = b->sock, .events = POLLRDHUP };
		__atomic_store_n(&b->next_check_ns, now + BROKER_CHECK_NS, __ATOMIC_RELAXED);
		if (poll(&p, 1, 0) > 0 && (p.revents & (POLLRDHUP | POLLHUP | POLLERR)))
			broker_fail(b, ECONNRESET);
	}
	if (broker_dead(b)) { errno = broker_dead(b); return -1; }
	return 0;
}

int channel_broker_conn_progress(struct channel_conn *conn)
{
	struct channel_broker *b = conn->dev->broker;

	if (channel_broker_dev_progress(conn->dev) != 0)
		return -1;

	const volatile struct broker_flow_status *f = &b->status->flow[conn->flow_id];
	if (__atomic_load_n(&f->generation, __ATOMIC_ACQUIRE) != conn->generation)
		return 0;
	int state = __atomic_load_n(&f->state, __ATOMIC_ACQUIRE);
	if (state < 0) {
		int error = __atomic_load_n(&f->error, __ATOMIC_RELAXED);
		errno = error ? error : EIO;
		return -1;
	}
	return state > 0;
}

int channel_broker_dev_fd(struct channel_dev *dev)
{
	return dev->broker->wake_fd;
}

/* The flow's incarnation is open in the broker's view (status page). */
static int flow_open(const struct channel_broker *b, const struct channel_conn *conn)
{
	const volatile struct broker_flow_status *f = &b->status->flow[conn->flow_id];
	return __atomic_load_n(&f->generation, __ATOMIC_ACQUIRE) == conn->generation &&
	       __atomic_load_n(&f->state, __ATOMIC_ACQUIRE) == 0;
}

int channel_broker_dev_arm(struct channel_dev *dev)
{
	struct channel_broker *b = dev->broker;
	const uint64_t one = 1;

	if (channel_broker_dev_progress(dev) != 0)
		return -1;
	if (b->arm == NULL || b->kick_fd < 0) { errno = ENOTCONN; return -1; }

	/* Same flows as the in-process ARM: each live push flow's next unread
	 * descriptor. The broker sends the ARM; this only publishes the list. */
	pthread_mutex_lock(&dev->session_lock);
	uint64_t seq = __atomic_load_n(&b->arm->seq, __ATOMIC_RELAXED);
	__atomic_store_n(&b->arm->seq, seq + 1, __ATOMIC_RELAXED);
	__atomic_thread_fence(__ATOMIC_RELEASE);
	for (uint32_t id = 1; id < BROKER_FLOWS; ++id) {
		struct channel_conn *conn = dev->flows[id];
		struct broker_arm_flow *f = &b->arm->flow[id];
		int armed = conn != NULL && conn->ready && conn->descs != NULL && !conn->rx_ended && flow_open(b, conn);
		__atomic_store_n(&f->generation, armed ? conn->generation : 0, __ATOMIC_RELAXED);
		__atomic_store_n(&f->expected, armed ? conn->expected : 0, __ATOMIC_RELAXED);
		__atomic_store_n(&f->armed, (uint32_t)armed, __ATOMIC_RELAXED);
	}
	__atomic_store_n(&b->arm->seq, seq + 2, __ATOMIC_RELEASE);
	pthread_mutex_unlock(&dev->session_lock);

	/* An eventfd write fails only at counter overflow, when a kick is pending anyway. */
	if (write(b->kick_fd, &one, sizeof(one)) < 0 && errno != EAGAIN)
		return -1;
	return 0;
}

void channel_broker_dev_clear(struct channel_dev *dev)
{
	uint64_t count;
	int fd = dev->broker->wake_fd;

	if (fd >= 0)
		(void)!read(fd, &count, sizeof(count)); /* nonblocking: a clear fd stays clear */
}

void channel_broker_wake_counters(struct channel_dev *dev, uint64_t *arms_sent, uint64_t *doorbells)
{
	const volatile struct broker_status *status = dev->broker->status;

	*arms_sent = status != NULL ? __atomic_load_n(&status->arms_sent, __ATOMIC_RELAXED) : 0;
	*doorbells = status != NULL ? __atomic_load_n(&status->doorbells, __ATOMIC_RELAXED) : 0;
}

/*
 * ---------------------------------------------------------------------------
 * Server
 * ---------------------------------------------------------------------------
 */

struct broker_server {
	const char *pci;
	struct channel_dev *dev;
	int pe_doorbell;                                /* The control PE may be armed while requests run */
	struct channel_mem *mems[BROKER_MEMS];
	struct broker_status *status;
	int status_fd;
	struct broker_arm_page *arm;                    /* Written by the client */
	int arm_fd;
	int wake_fd, kick_fd;                           /* Idle wake eventfds, shared with the client */
};

static volatile sig_atomic_t serve_stop;

static void serve_on_signal(int signo)
{
	(void)signo;
	serve_stop = 1;
}

/* Wakes the client, which then polls: a spurious wake costs one drain pass. */
static void raise_wake(struct broker_server *srv)
{
	const uint64_t one = 1;

	if (srv->wake_fd >= 0)
		(void)!write(srv->wake_fd, &one, sizeof(one));
}

/* Progresses the control session once and mirrors every open flow's status
 * into the status page; a changed status wakes the client to read it. */
static void publish(struct broker_server *srv)
{
	int changed = 0;

	if (srv->dev == NULL || srv->status == NULL)
		return;
	(void)channel_dev_progress(srv->dev); /* a session error reaches every flow's status */
	for (uint32_t id = 1; id < BROKER_FLOWS; ++id) {
		struct channel_conn *conn = srv->dev->flows[id];
		if (conn == NULL)
			continue;
		int state = channel_conn_status(conn);
		int error = state < 0 ? errno : 0;
		struct broker_flow_status *f = &srv->status->flow[id];
		changed |= f->state != state || f->error != error;
		__atomic_store_n(&f->error, error, __ATOMIC_RELAXED);
		__atomic_store_n(&f->state, state, __ATOMIC_RELEASE);
	}
	__atomic_store_n(&srv->status->arms_sent, srv->dev->arms_sent, __ATOMIC_RELAXED);
	__atomic_store_n(&srv->status->doorbells, srv->dev->doorbells, __ATOMIC_RELAXED);
	if (changed)
		raise_wake(srv);
}

int channel_broker_arm_collect(const struct broker_arm_page *page, const struct channel_dev *dev,
			       struct dmesh_session_arm_flow *flows)
{
	for (int tries = 0; tries < BROKER_ARM_READ_TRIES; ++tries) {
		uint64_t seq = __atomic_load_n(&page->seq, __ATOMIC_ACQUIRE);
		uint32_t count = 0;
		if (seq & 1)
			continue;
		for (uint32_t id = 1; id < BROKER_FLOWS; ++id) {
			const struct broker_arm_flow *f = &page->flow[id];
			uint32_t armed = __atomic_load_n(&f->armed, __ATOMIC_RELAXED);
			uint32_t generation = __atomic_load_n(&f->generation, __ATOMIC_RELAXED);
			uint64_t expected = __atomic_load_n(&f->expected, __ATOMIC_RELAXED);
			const struct channel_conn *conn = dev->flows[id];
			if (armed && conn != NULL && conn->generation == generation)
				flows[count++] = (struct dmesh_session_arm_flow){
					.flow_id = id, .generation = generation, .expected_seq = expected };
		}
		__atomic_thread_fence(__ATOMIC_ACQUIRE);
		if (__atomic_load_n(&page->seq, __ATOMIC_RELAXED) == seq)
			return (int)count;
	}
	return -1;
}

/**
 * ARM the flows the client listed in the arm page
 *
 * Whenever no ARM can stand for the client's sleep (a torn read, a failed
 * send) the client is woken to keep polling.
 *
 * @srv [in]: Server
 */
static void serve_arm(struct broker_server *srv)
{
	struct dmesh_session_arm_flow flows[DMESH_SESSION_MAX_FLOWS];
	uint64_t kicks;

	(void)!read(srv->kick_fd, &kicks, sizeof(kicks));
	if (srv->dev == NULL || srv->arm == NULL)
		return;
	int count = channel_broker_arm_collect(srv->arm, srv->dev, flows);
	if (count < 0 || channel_dev_send_arm(srv->dev, flows, (uint32_t)count) != 0)
		raise_wake(srv);
}

static int serve_hello(struct broker_server *srv, const struct broker_request *req, struct broker_reply *rep,
		       int *fds, int *nfds)
{
	if (srv->dev != NULL) return EALREADY;
	if (srv->status == NULL && (srv->status = channel_shared_alloc(sizeof(*srv->status), &srv->status_fd)) == NULL)
		return ENOMEM;
	if (srv->arm == NULL && (srv->arm = channel_shared_alloc(sizeof(*srv->arm), &srv->arm_fd)) == NULL)
		return ENOMEM;
	if (srv->wake_fd < 0 && (srv->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0)
		return errno;
	if (srv->kick_fd < 0 && (srv->kick_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0)
		return errno;
	if (channel_dev_open_owner(srv->pci, &srv->dev) != 0) return errno;
	srv->dev->wake_relay = 1;
	srv->dev->wake_relay_fd = srv->wake_fd;
	if (channel_session_open(srv->dev, req->name) != 0) {
		int saved = errno;
		channel_dev_close(srv->dev); /* HELLO may be retried */
		srv->dev = NULL;
		return saved;
	}
	/* A request can wake the loop while the control PE is armed. In the
	 * default mode an armed PE delivers nothing until the notification is
	 * cleared, so the request's own progress calls would never see READY;
	 * PROGRESS_ALL keeps them working (as in ~/DPUmesh's broker). Without it
	 * the loop polls on a short timer instead. */
	srv->pe_doorbell = doca_pe_set_event_mode(srv->dev->control->pe, DOCA_PE_EVENT_MODE_PROGRESS_ALL) ==
			   DOCA_SUCCESS;
	rep->bytes = sizeof(*srv->status);
	fds[BROKER_FD_STATUS] = srv->status_fd;
	fds[BROKER_FD_ARM] = srv->arm_fd;
	fds[BROKER_FD_WAKE] = srv->wake_fd;
	fds[BROKER_FD_KICK] = srv->kick_fd;
	*nfds = BROKER_HELLO_FDS;
	return 0;
}

static int serve_mem_alloc(struct broker_server *srv, const struct broker_request *req, struct broker_reply *rep,
			   int *fds, int *nfds)
{
	struct channel_mem *mem;
	uint32_t id = 0;

	while (id < BROKER_MEMS && srv->mems[id] != NULL) ++id;
	if (id == BROKER_MEMS) return ENOSPC;
	if (req->bytes == 0 || req->bytes > BROKER_MAX_REGION) return EINVAL;
	if (channel_mem_alloc(srv->dev, req->bytes, &mem) != 0) return errno;
	srv->mems[id] = mem;
	rep->id = id;
	rep->dpa = mem->dpa;
	rep->bytes = mem->bytes;
	rep->base = (uint64_t)(uintptr_t)mem->buf;
	fds[0] = mem->memfd;
	*nfds = 1;
	return 0;
}

static int serve_conn_open(struct broker_server *srv, const struct broker_request *req, struct broker_reply *rep,
			   int *fds, int *nfds)
{
	struct channel_conn *conn;

	if (req->flow_id == 0 || req->flow_id >= BROKER_FLOWS || req->tx_id >= BROKER_MEMS ||
	    req->rx_id >= BROKER_MEMS || srv->mems[req->tx_id] == NULL || srv->mems[req->rx_id] == NULL)
		return EINVAL;
	if (!push_mode(req->mode)) return ENOTSUP; /* the broker runs no host DPA yet */
	/* Identity fields are the client's claim until Pod authentication exists. */
	struct channel_conn_config cfg = {
		.flow_id = req->flow_id,
		.workload = req->name,
		.src_ip = req->src_ip,
		.dst_ip = req->dst_ip,
		.src_port = req->src_port,
		.dst_port = req->dst_port,
		.mode = req->mode,
		.tx = srv->mems[req->tx_id],
		.rx = srv->mems[req->rx_id],
		.rx_offset = (size_t)req->rx_offset,
	};
	if (channel_conn_open(srv->dev, &cfg, &conn) != 0) return errno;
	struct broker_flow_status *f = &srv->status->flow[conn->flow_id];
	__atomic_store_n(&f->state, 0, __ATOMIC_RELAXED);
	__atomic_store_n(&f->error, 0, __ATOMIC_RELAXED);
	__atomic_store_n(&f->generation, conn->generation, __ATOMIC_RELEASE);
	rep->id = conn->generation;
	rep->bytes = conn->ring_bytes;
	fds[0] = conn->ring_fd;
	*nfds = 1;
	return 0;
}

static int serve_conn_close(struct broker_server *srv, const struct broker_request *req)
{
	if (req->flow_id == 0 || req->flow_id >= BROKER_FLOWS) return EINVAL;
	if (srv->dev->flows[req->flow_id] == NULL) return ENOENT;
	return channel_conn_close(srv->dev->flows[req->flow_id]) == 0 ? 0 : errno;
}

/**
 * Handle one request and reply
 *
 * @return: 0 to keep serving, -1 when the connection is over
 */
static int serve_request(struct broker_server *srv, int sock)
{
	struct broker_request req;
	struct broker_reply rep = { .type = BROKER_REPLY, .version = BROKER_IPC_VERSION };
	int fds[BROKER_MAX_FDS], nfds = 0, status;

	if (broker_ipc_recv(sock, &req, sizeof(req), NULL, 0) < 0)
		return -1; /* EOF or a malformed request: the client is gone or broken */
	if (memcmp(req.magic, BROKER_IPC_MAGIC, sizeof(req.magic)) != 0 || req.version != BROKER_IPC_VERSION ||
	    !name_ok(req.name))
		status = EPROTO;
	else if (req.type != BROKER_HELLO && srv->dev == NULL)
		status = ENOTCONN;
	else {
		switch (req.type) {
		case BROKER_HELLO: status = serve_hello(srv, &req, &rep, fds, &nfds); break;
		case BROKER_MEM_ALLOC: status = serve_mem_alloc(srv, &req, &rep, fds, &nfds); break;
		case BROKER_CONN_OPEN: status = serve_conn_open(srv, &req, &rep, fds, &nfds); break;
		case BROKER_CONN_CLOSE: status = serve_conn_close(srv, &req); break;
		case BROKER_SESSION_CLOSE: status = channel_session_close(srv->dev) == 0 ? 0 : errno; break;
		default: status = EPROTO; break;
		}
	}
	memcpy(rep.magic, BROKER_IPC_MAGIC, sizeof(rep.magic));
	rep.status = status ? status : 0;
	rep.fd_count = status == 0 ? (uint16_t)nfds : 0;
	if (status != 0)
		DOCA_LOG_WARN("Broker request %u (flow %u) failed: %s", req.type, req.flow_id, strerror(status));
	return broker_ipc_send(sock, &rep, sizeof(rep), fds, rep.fd_count) == 0 ? 0 : -1;
}

int channel_broker_serve(int sock, const char *pci, const sigset_t *unblock)
{
	struct broker_server srv = { .pci = pci, .status_fd = -1, .arm_fd = -1, .wake_fd = -1, .kick_fd = -1 };
	struct sigaction action = { .sa_handler = serve_on_signal };
	int rc = 0;

	sigemptyset(&action.sa_mask);
	(void)sigaction(SIGTERM, &action, NULL);
	(void)sigaction(SIGINT, &action, NULL);
	if (unblock != NULL)
		(void)sigprocmask(SIG_UNBLOCK, unblock, NULL);

	while (!serve_stop) {
		struct pollfd fds[3] = { { .fd = sock, .events = POLLIN } };
		struct doca_pe *pe = srv.pe_doorbell && srv.dev->control != NULL ? srv.dev->control->pe : NULL;
		doca_notification_handle_t handle;
		int nfds = 1, timeout = BROKER_POLL_MS, pe_slot = -1, kick_slot = -1;

		publish(&srv);
		/* Sleep on the control PE's doorbell: arm, then progress once more
		 * before blocking, so an event that raced the arm is not missed. */
		if (pe != NULL && doca_pe_get_notification_handle(pe, &handle) == DOCA_SUCCESS) {
			pthread_mutex_lock(&srv.dev->session_lock);
			int armed = doca_pe_request_notification(pe) == DOCA_SUCCESS;
			uint8_t busy = doca_pe_progress(pe);
			pthread_mutex_unlock(&srv.dev->session_lock);
			if (busy)
				continue;
			if (armed) {
				pe_slot = nfds;
				fds[nfds++] = (struct pollfd){ .fd = (int)handle, .events = POLLIN };
				timeout = BROKER_IDLE_MS;
			}
		}
		if (srv.kick_fd >= 0) {
			kick_slot = nfds;
			fds[nfds++] = (struct pollfd){ .fd = srv.kick_fd, .events = POLLIN };
		}
		if (poll(fds, nfds, timeout) < 0) {
			if (errno == EINTR) continue;
			rc = -1;
			break;
		}
		if (pe_slot >= 0 && (fds[pe_slot].revents & POLLIN))
			(void)doca_pe_clear_notification(pe, handle);
		if (kick_slot >= 0 && (fds[kick_slot].revents & POLLIN))
			serve_arm(&srv);
		if (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) {
			if (serve_request(&srv, sock) != 0)
				break;
		}
	}

	/* The client is gone (or we are told to stop): close its flows with the
	 * DPU, every one even if one fails, then release the registrations. */
	if (srv.dev != NULL)
		for (uint32_t id = 1; id < BROKER_FLOWS; ++id)
			if (srv.dev->flows[id] != NULL && channel_conn_close(srv.dev->flows[id]) != 0)
				DOCA_LOG_ERR("Broker cleanup: flow %u close failed: %s", id, strerror(errno));
	if (srv.dev != NULL && channel_session_close(srv.dev) != 0) {
		DOCA_LOG_ERR("Broker cleanup: session close failed: %s", strerror(errno));
		rc = -1;
	} else {
		for (int i = 0; i < BROKER_MEMS; ++i)
			channel_mem_free(srv.mems[i]);
		channel_dev_close(srv.dev);
	}
	if (srv.status != NULL)
		channel_shared_free(srv.status, sizeof(*srv.status), srv.status_fd);
	if (srv.arm != NULL)
		channel_shared_free(srv.arm, sizeof(*srv.arm), srv.arm_fd);
	if (srv.wake_fd >= 0)
		close(srv.wake_fd);
	if (srv.kick_fd >= 0)
		close(srv.kick_fd);
	close(sock);
	return rc;
}
