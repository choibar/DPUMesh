#ifndef CHANNEL_INTERNAL_H
#define CHANNEL_INTERNAL_H

/*
 * Private layout of the host channel objects, shared by channel.c (the process
 * that owns the DOCA device: the broker, or the application on the temporary
 * direct host-dpa path) and channel_broker.c (the broker's client and server).
 *
 * A zeroed object means "this process owns the device and its memory is
 * private": the broker fields are all opt-in.
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include <doca_mmap.h>

#include "channel.h"
#include "comch_common.h"
#include "session_protocol.h"

#define CHANNEL_RING_SIZE 1024u            /* Forward ring depth of the host library */

struct channel_broker;                      /* Client end of the broker connection */

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
	/* Idle wake (push reverse path): one ARM at a time, released by DOORBELL */
	int arm_outstanding;
	uint64_t arm_epoch;
	uint64_t arms_sent, doorbells;
	int host_dpa;                       /* Nonzero: the host-dpa reverse path */
    struct doca_dev *reverse_dev;       /* base PF or optional SF */
    struct doca_dpa *reverse_dpa;       /* base context or SF extension */
    struct doca_dev *base_dev;
    struct doca_dpa *base_dpa;
	int share_memory;                   /* Broker: registered memory is memfd-backed, for its client to map */
	int wake_relay;                     /* Broker: a DOORBELL raises wake_relay_fd for the client */
	int wake_relay_fd;
	struct channel_broker *broker;      /* Client: the broker owns the device; NULL when this process does */
};

struct channel_mem {
	struct doca_mmap *mmap;             /* NULL on a broker client: the broker registered the region */
	void *buf;
	size_t bytes;
	doca_dpa_dev_mmap_t dpa;            /* Handle on the forward device (the DPU DPA reads the TX pool) */
	doca_dpa_dev_mmap_t dpa_rev;        /* Pull: handle on the DPA device (the host DPA writes the RX region) */
	int shared;                         /* buf is a MAP_SHARED view of memfd */
	int memfd;
	uint32_t id;                        /* Client: the broker's id of the region */
	uint64_t remote_base;               /* Client: the broker's address of buf */
};

struct channel_conn {
    struct channel_dev *dev;
    struct dma_ring *forward_ring;
    struct dmesh_export_rcv_ring_msg reverse_metadata;
    bool reverse_ready;
	doca_dpa_dev_mmap_t tx_dpa;
	/* Added to a forward descriptor's address: the registering process's
	 * address of the TX pool minus this process's. 0 unless a broker registered it. */
	uint64_t tx_delta;
	volatile struct dmesh_push_desc *descs;
	volatile struct dmesh_push_cursor *cursor;
	size_t data_size;
	uint64_t expected;                  /* Push: next batch sequence */
	int rx_ended;                       /* Push: end of stream or a malformed batch was read */
	uint32_t flow_id, generation;
	int open_sent, ready, peer_closed, error, close_sent, close_error;
	int ring_fd;                        /* Shared forward ring memfd (valid when ring_bytes != 0) */
	size_t ring_bytes;
	/* host-dpa reverse path */
    struct dmesh_dpa_endpoint *reverse;
	struct doca_mmap *tx_mmap;          /* Imported DPU tx_staging */
	struct doca_mmap *ring_mmap;        /* Imported DPU rcv_ring */
	uint64_t rx_seq;                    /* Segments delivered to the carrier */
	uint64_t consumed_seq;              /* Segments the carrier released */
	uint32_t seg_end[CHANNEL_DESC_N]; /* End offset of delivered segment seq % N */
	uint32_t rx_consumed_pos;           /* End offset of the latest released prefix */
	uint64_t rd_published_bytes;
	uint64_t rd_published_seq;
};

/* Opens the device in this process with memfd-backed, shareable registered
 * memory and forward rings: the broker's side of a client channel. */
int channel_dev_open_owner(const char *pci, struct channel_dev **out);

/* The owner's ARM for flows a client listed (the broker): sends one ARM unless
 * one is outstanding or `count` is 0. Fails when the session is down or the
 * ARM cannot be queued. */
struct dmesh_session_arm_flow;
int channel_dev_send_arm(struct channel_dev *dev, const struct dmesh_session_arm_flow *flows, uint32_t count);

/* Sealed memfd-backed MAP_SHARED memory, zero-filled; free takes fd -1 when closed. */
void *channel_shared_alloc(size_t bytes, int *fd);
void channel_shared_free(void *buf, size_t bytes, int fd);

/* Broker client (channel_broker.c): the control operations of a channel whose
 * device a broker owns. The data path runs unchanged on the mapped memory. */
int channel_broker_attach(const char *path, struct channel_dev **out);
void channel_broker_detach(struct channel_dev *dev);
int channel_broker_session_open(struct channel_dev *dev, const char *server);
int channel_broker_session_close(struct channel_dev *dev);
int channel_broker_session_listen(struct channel_dev *dev, uint32_t ip, uint16_t port);
/* Backend requests the broker published in the status page, each taken once. */
int channel_broker_backend_next(struct channel_dev *dev, uint32_t *worker, uint32_t *token);
void channel_broker_backend_finish(struct channel_dev *dev, uint32_t worker, uint32_t token, int error);
int channel_broker_mem_alloc(struct channel_dev *dev, size_t bytes, struct channel_mem **out);
int channel_broker_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg,
			     struct channel_conn **out);
int channel_broker_conn_close(struct channel_conn *conn);
int channel_broker_conn_progress(struct channel_conn *conn);
/* Fails once the broker connection is gone. */
int channel_broker_dev_progress(struct channel_dev *dev);
/* Idle wake of a client channel (channel_dev_fd/arm/clear/wake_counters). */
int channel_broker_dev_fd(struct channel_dev *dev);
int channel_broker_dev_arm(struct channel_dev *dev);
void channel_broker_dev_clear(struct channel_dev *dev);
void channel_broker_wake_counters(struct channel_dev *dev, uint64_t *arms_sent, uint64_t *doorbells);
/* Broker: the flows a client listed in its arm page (a seqlock) that name an
 * incarnation `dev` still holds. Returns their count, or -1 when the client
 * kept the page torn for every retry. */
struct broker_arm_page;
struct broker_status;
/* Broker: takes the DPU's new backend requests from `dev` (channel_backend_next)
 * and publishes each in `status`. Returns how many. */
int channel_broker_backend_publish(struct channel_dev *dev, struct broker_status *status);
int channel_broker_arm_collect(const struct broker_arm_page *page, const struct channel_dev *dev,
			       struct dmesh_session_arm_flow *flows);

#endif /* CHANNEL_INTERNAL_H */
