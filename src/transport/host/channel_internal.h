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
	int hello_ready;
	int session_error;
	int host_dpa;                       /* Nonzero: the host-dpa reverse path */
    struct doca_dev *reverse_dev;       /* base PF or optional SF */
    struct doca_dpa *reverse_dpa;       /* base context or SF extension */
    struct doca_dev *base_dev;
    struct doca_dpa *base_dpa;
	int share_memory;                   /* Broker: registered memory is memfd-backed, for its client to map */
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
	uint32_t rd_pos;                    /* Kernel read watermark last published */
	uint64_t rd_published_bytes;
	uint64_t rd_published_seq;
};

/* Opens the device in this process with memfd-backed, shareable registered
 * memory and forward rings: the broker's side of a client channel. */
int channel_dev_open_owner(const char *pci, struct channel_dev **out);

/* Sealed memfd-backed MAP_SHARED memory, zero-filled; free takes fd -1 when closed. */
void *channel_shared_alloc(size_t bytes, int *fd);
void channel_shared_free(void *buf, size_t bytes, int fd);

/* Broker client (channel_broker.c): the control operations of a channel whose
 * device a broker owns. The data path runs unchanged on the mapped memory. */
int channel_broker_attach(struct channel_dev **out);
void channel_broker_detach(struct channel_dev *dev);
int channel_broker_session_open(struct channel_dev *dev, const char *server);
int channel_broker_session_close(struct channel_dev *dev);
int channel_broker_mem_alloc(struct channel_dev *dev, size_t bytes, struct channel_mem **out);
int channel_broker_conn_open(struct channel_dev *dev, const struct channel_conn_config *cfg,
			     struct channel_conn **out);
int channel_broker_conn_close(struct channel_conn *conn);
int channel_broker_conn_progress(struct channel_conn *conn);

#endif /* CHANNEL_INTERNAL_H */
