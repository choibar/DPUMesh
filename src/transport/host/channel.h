#ifndef CHANNEL_H
#define CHANNEL_H

/*
 * Host view of the DPUMesh transport. This is the only header that both the
 * carrier (src/core) and the DPUMesh transport sources are visible from; it
 * exposes no DOCA or DPUMesh types, so the carrier compiles against the public
 * core alone.
 *
 * Two reverse paths share this interface, selected per device by DPUMESH_REVERSE:
 *   push (default)  DPU -> host bytes are pushed by the DPU's DMA engine into
 *                   the connection's window (slot ring + data ring); no host DPA
 *                   and no doorbell for the data, so the consumer polls the window
 *   pull            the mirror of the forward path: a host-owned DPA thread per
 *                   connection polls the DPU's descriptor ring and copies
 *                   tx_staging into the window's data area. DPUMESH_HOST_DPA_PCI names
 *                   the host PF that runs the DPA process (its vhca needs a DPA
 *                   EU partition); DPUMESH_HOST_DPA_DEV optionally extends that
 *                   process to an SF (ibdev name) that then owns the DPA objects
 */

#include <signal.h>
#include <stddef.h>
#include <stdint.h>

struct channel_dev;  /* Forward declaration: one Comch device (+ DPA device on the host-dpa reverse path) */
struct channel_mem;  /* Forward declaration: one registered, PCI-exported memory region */
struct channel_conn; /* Forward declaration: one logical flow with its own rings */

#define CHANNEL_DESC_N   128u             /* Reverse batches a window can hold */
#define CHANNEL_DATA_OFF 4096u            /* Data ring offset inside a window */
#define CHANNEL_WINDOW   (1024u * 1024u)  /* One connection's receive window */
#define CHANNEL_DESC_MAX      8064u            /* Forward descriptor bytes */
#define CHANNEL_DESC_ALIGN    128u             /* Forward descriptor alignment */

#define CHANNEL_MODE_CLIENT_HOST_DPA  0u /* DPUMesh CLIENT: client flow, reverse by the host DPA */
#define CHANNEL_MODE_BACKEND_DPU_DMA      1u /* DPUMesh BACKEND: server flow, reverse pushed by the DPU */
#define CHANNEL_MODE_CLIENT_DPU_DMA 2u /* DPUMesh INGRESS_PUSH: client flow, reverse pushed by the DPU */
#define CHANNEL_MODE_BACKEND_HOST_DPA 3u /* DPUMesh BACKEND_PULL: server flow, reverse by the host DPA */

struct channel_conn_config {
	uint32_t flow_id;       /* Channel-local slot, 1..32; generation is managed internally */
	const char *workload;   /* Workload label carried in the flow identity */
	uint32_t src_ip;        /* Network byte order */
	uint32_t dst_ip;        /* Network byte order */
	uint16_t src_port;      /* Host order, as the DPU expects */
	uint16_t dst_port;      /* Host order, as the DPU expects */
	uint32_t mode;          /* CHANNEL_MODE_* */
	struct channel_mem *tx;    /* Shared registered TX pool */
	struct channel_mem *rx;    /* Registered RX region */
	size_t rx_offset;       /* This connection's window inside rx */
};

/* Device. A dpu-dma channel is a broker client when DPUMESH_BROKER names the
 * broker's socket, or when it is unset and /run/dpumesh/broker.sock exists;
 * the broker owns the device and `pci` is unused. Otherwise (DPUMESH_BROKER=off,
 * no broker socket, or the host-dpa reverse path) this process opens the Comch
 * device, and on host-dpa the DPA process, at `pci`: the direct path. */
int channel_dev_open(const char *pci, struct channel_dev **out);
void channel_dev_close(struct channel_dev *dev);
/* One serialized control connection per device/channel. */
int channel_session_open(struct channel_dev *dev, const char *server);
/* Close all retained flows before stopping Comch. Failure retains DMA resources. */
int channel_session_close(struct channel_dev *dev);
/* Nonzero when the device runs the host-dpa reverse path (host DPA reverse path) */
int channel_dev_host_dpa(const struct channel_dev *dev);
/* Progress the channel's shared Comch control session once per drain pass. */
int channel_dev_progress(struct channel_dev *dev);
/* Idle wake. The fd is readable after channel_dev_arm when a control message
 * (DOORBELL, CLOSED, ERROR) arrives. channel_dev_arm first sends one ARM for
 * the push flows, so the DPU rings for descriptors this host has not read.
 * It fails, leaving the caller to keep polling, when the session is down or
 * the ARM cannot be queued. channel_dev_clear acknowledges a raised fd. */
int channel_dev_fd(struct channel_dev *dev);
int channel_dev_arm(struct channel_dev *dev);
void channel_dev_clear(struct channel_dev *dev);
void channel_dev_wake_counters(struct channel_dev *dev, uint64_t *arms_sent, uint64_t *doorbells);

/* Memory: allocates, registers and PCI-exports `bytes`; the buffer is owned by mem */
int channel_mem_alloc(struct channel_dev *dev, size_t bytes, struct channel_mem **out);
void *channel_mem_base(const struct channel_mem *mem);
void channel_mem_free(struct channel_mem *mem);

/* Connection: tagged OPEN/READY on the shared session, private DMA rings. */
int channel_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg, struct channel_conn **out);
/* Quiesce reverse DMA, wait for CLOSED, then release the private ring.
 * Failure leaves the handle and exported memory live for safe retry. */
int channel_conn_close(struct channel_conn *conn);
/* Progresses the control path (and the reverse completions on the host-dpa reverse path).
 * Returns 1 for CLOSED, -1 with errno for flow/session failure, otherwise 0. */
int channel_conn_progress(struct channel_conn *conn);
/* Read the flow status and progress its private reverse PE. Call after
 * channel_dev_progress; this never progresses the shared control PE again. */
int channel_conn_status(struct channel_conn *conn);

/* Doorbells: private reverse-completion engines only. The shared control PE
 * is serialized and polled through the carrier fallback tick. Returns the
 * count written to fds. The fds stay valid until channel_conn_close */
#define CHANNEL_CONN_FDS 3
int channel_conn_fds(struct channel_conn *conn, int *fds, int max);
/* Arms every engine so its fd signals the next completion (one-shot) */
int channel_conn_arm(struct channel_conn *conn);
/* Acknowledge a readable private engine; an already-cleared fd is a no-op. */
void channel_conn_clear(struct channel_conn *conn, int fd);

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

/* Broker: serves one client connection (dpumesh_broker's per-client child)
 * with the device at `pci`, until the client disconnects or SIGTERM/SIGINT;
 * `unblock` (may be NULL) is unblocked once those handlers are installed.
 * Closing the connection closes every flow the client left open. */
int channel_broker_serve(int sock, const char *pci, const sigset_t *unblock);

#endif // CHANNEL_H
