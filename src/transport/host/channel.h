#ifndef CHANNEL_H
#define CHANNEL_H

/*
 * Host view of the DPUMesh transport. This is the only header that both the
 * carrier (src/core) and the DPUMesh transport sources are visible from; it
 * exposes no DOCA or DPUMesh types, so the carrier compiles against the public
 * core alone.
 *
 * Forward (host -> DPU): descriptors over the shared TX pool, copied by the
 * DPU's DPA. Reverse (DPU -> host): the DPU's reverse DPA thread copies each
 * batch into the connection's window (slot ring + data ring) and wakes the
 * flow's slot (channel_flow_fd); the host runs no DPA.
 */

#include <stddef.h>
#include <stdint.h>

struct channel_dev;  /* Forward declaration: one Comch device */
struct channel_mem;  /* Forward declaration: one registered, PCI-exported memory region */
struct channel_conn; /* Forward declaration: one logical flow with its own rings */

#define CHANNEL_DESC_N   128u             /* Reverse batches a window can hold */
#define CHANNEL_DATA_OFF 4096u            /* Data ring offset inside a window */
#define CHANNEL_WINDOW   (4u * 1024u * 1024u)  /* One connection's receive window (DMESH_PUSH_WINDOW) */
#define CHANNEL_DESC_MAX      8064u            /* Forward descriptor bytes */
#define CHANNEL_RING_SIZE     1024u            /* Forward ring depth of a flow */
#define CHANNEL_RING_STRIDE   (17u * 4096u)    /* A flow's forward ring in the ring region: control line + descriptors */
#define CHANNEL_DESC_ALIGN    128u             /* Forward descriptor alignment */

#define CHANNEL_MODE_BACKEND 1u    /* DPUMesh BACKEND: a server flow the proxy connects through */
#define CHANNEL_MODE_CLIENT  2u    /* DPUMesh CLIENT: a client flow the proxy serves */

struct channel_conn_config {
	uint32_t flow_id;       /* Channel-local slot, 1..32; generation is managed internally */
	const char *workload;   /* Workload label carried in the flow identity */
	uint32_t src_ip;        /* Network byte order */
	uint32_t dst_ip;        /* Network byte order */
	uint16_t src_port;      /* Host order, as the DPU expects */
	uint16_t dst_port;      /* Host order, as the DPU expects */
	uint32_t mode;          /* CHANNEL_MODE_* */
	uint32_t backend_worker, backend_token; /* DPU-requested backend, zero token for client */
	struct channel_mem *tx;    /* Shared registered TX pool */
	struct channel_mem *rx;    /* Registered RX region */
	size_t rx_offset;       /* This connection's window inside rx */
	struct channel_mem *rings; /* Registered forward ring region */
	size_t ring_offset;     /* This connection's forward ring inside rings */
};

/* Device: opens the Comch device */
int channel_dev_open(const char *pci, struct channel_dev **out);
void channel_dev_close(struct channel_dev *dev);
/* One serialized control connection per device/channel. */
int channel_session_open(struct channel_dev *dev, const char *server);
/* Close all retained flows before stopping Comch. Failure retains DMA resources. */
int channel_session_close(struct channel_dev *dev);
/* Register a service without allocating backend flows. Requests are consumed
 * outside control callbacks; completion retains errors until Comch accepts. */
int channel_session_listen(struct channel_dev *, uint32_t ip, uint16_t port);
int channel_backend_next(struct channel_dev *, uint32_t *worker, uint32_t *token);
/* The DPU's requests on a client flow, which concern that flow's
 * destination: PREPARE (open a spare stream to it) and SCALE (keep count
 * streams to it, answered with token). The session's progress records them;
 * channel_conn_announced reports once that one arrived since the last call,
 * and channel_conn_take_request returns the next (1) or none (0). */
enum { CHANNEL_REQ_PREPARE = 1, CHANNEL_REQ_SCALE = 2 };
int channel_conn_announced(struct channel_conn *);
int channel_conn_take_request(struct channel_conn *, int *kind, uint32_t *count, uint32_t *token);
/* Answers on the flow: a spare has its server's first bytes (SPARE_READY,
 * on the spare), and how many clients took a SCALE (SCALE_ACK). */
int channel_conn_spare_ready(struct channel_conn *);
int channel_conn_scale_ack(struct channel_conn *, uint32_t token, uint32_t applied);
void channel_backend_finish(struct channel_dev *, uint32_t worker, uint32_t token, int error);
/* Control wake. The fd is readable after channel_dev_arm when a control
 * message (CLOSED, ERROR, a DPU request) arrives; receives raise their flow's
 * fd instead (channel_flow_fd). It fails, leaving the caller to keep polling,
 * when the session is down. The control PE runs in PROGRESS_ALL, so the next
 * channel_dev_arm also clears a raised fd. */
int channel_dev_fd(struct channel_dev *dev);
int channel_dev_arm(struct channel_dev *dev);

/* Memory: allocates, registers and PCI-exports `bytes`; the buffer is owned by mem */
int channel_mem_alloc(struct channel_dev *dev, size_t bytes, struct channel_mem **out);
void *channel_mem_base(const struct channel_mem *mem);
void channel_mem_free(struct channel_mem *mem);

/* Connection: tagged OPEN/READY on the shared session, private DMA rings. */
int channel_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out);
/* The same open in two steps, for a caller that must not wait: start sends
 * OPEN and returns the handle; finish reports READY (0), still outstanding
 * (1), or -1 with errno (refused, lost session, or no READY within
 * CHANNEL_REPLY_MS), after which the handle is closed like an open one.
 * Neither progresses the session. */
int channel_conn_open_start(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out);
int channel_conn_open_finish(struct channel_conn *conn);
/* Send CLOSE, wait for CLOSED (the DPU's DMA on the flow ended), then release
 * the private ring. Failure leaves the handle and exported memory live for
 * safe retry. */
int channel_conn_close(struct channel_conn *conn);
/* The same close in two steps, for a caller that must not wait: start sends
 * CLOSE; finish releases the ring once CLOSED arrived (0), reports 1 while it
 * is outstanding, or -1 with errno (CLOSED with an error, a lost session, or
 * no CLOSED in time), leaving the handle live for channel_conn_close. Neither
 * progresses the session; CLOSED lands through channel_dev_progress. */
int channel_conn_close_start(struct channel_conn *conn);
int channel_conn_close_finish(struct channel_conn *conn);
/* Whether flow_id can open. An open that failed after OPEN was sent, and whose
 * close the DPU did not confirm, holds the id and its receive window; this
 * releases them once the DPU's CLOSED has arrived. */
int channel_flow_free(struct channel_dev *dev, uint32_t flow_id);
/* End the forward stream (SHUTDOWN): the DPU's reader sees end of stream
 * after every descriptor posted so far; the flow keeps receiving. */
int channel_conn_shutdown(struct channel_conn *conn);
/* Progresses the control path. Returns 1 for CLOSED, -1 with errno for
 * flow/session failure, otherwise 0. */
int channel_conn_progress(struct channel_conn *conn);
/* Batch pollers progress the shared session once, then inspect each flow.
 * Session errors remain visible to every flow even if another poller
 * performed the shared progress. */
int channel_dev_progress(struct channel_dev *dev);
int channel_conn_poll(struct channel_conn *conn);

/* Flow wake: every flow slot (1..32) has a sync event of its own on its own
 * PE, created with the session (a failure fails the session), over the
 * slot's word that the DPU's reverse DPA thread changes with every batch it
 * publishes, so a flow's receives raise its slot's fd alone. The slot calls
 * are serialized by the caller (the carrier's slot lock). The fd is stable
 * for the session; channel_flow_arm makes the next batch raise it and clears
 * the last raise; channel_flow_progress completes the raised wait. The fd
 * carries no control message: channel_dev_take_news returns, and clears, bit
 * (flow_id - 1) for each flow that got CLOSED, ERROR or a DPU request. */
int channel_flow_fd(struct channel_dev *dev, uint32_t flow_id);
void channel_flow_progress(struct channel_dev *dev, uint32_t flow_id);
int channel_flow_arm(struct channel_dev *dev, uint32_t flow_id);
uint32_t channel_dev_take_news(struct channel_dev *dev);
/* Nonzero when channel_conn_poll and channel_conn_rx_next have nothing to
 * report: no new batch, no peer close and no error. A hint read without the
 * session lock by the flow's single consumer (which owns the receive cursor);
 * a drain skips the flow on it. */
int channel_conn_idle(const struct channel_conn *conn);

/* Forward path: descriptors over the TX pool */
uint32_t channel_conn_ring_free(const struct channel_conn *conn);
/* Posts one forward descriptor; returns its ticket (the ring head after it) */
uint64_t channel_conn_post(struct channel_conn *conn, uint64_t addr, uint32_t bytes);
/* Tickets consumed so far by the DPU's DPA (custody ACK) */
uint64_t channel_conn_consumed(const struct channel_conn *conn);

/* Reverse path: next landed batch, pos/len relative to the window's data ring.
 * Returns 1 with a batch, 0 when none, -1 at end of stream (a zero-length
 * batch) or on a malformed batch */
int channel_conn_rx_next(struct channel_conn *conn, uint64_t *seq, uint32_t *pos, uint32_t *len);
/* Publishes the consumption cursor: every batch up to `seq` is released */
void channel_conn_rx_consumed(struct channel_conn *conn, uint64_t seq, uint64_t bytes);

#endif // CHANNEL_H
