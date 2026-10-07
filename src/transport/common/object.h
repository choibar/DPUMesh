#ifndef OBJECT_H_
#define OBJECT_H_

#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_dma.h>

#include <sys/queue.h>
#include "buffer.h"
#include "comch_server.h"
#include "comch_common.h"
#include "comch_client.h"
#include "dpa.h"
#include "dispatcher.h"

struct dmesh_doca_dpa_thread;
struct dmesh_doca_dpa_comch;
struct dmesh_dpa_thread_pool;
struct dma_ring;
struct objects;
struct dmesh_conn;
/* What a DMA task carries; drives the completion callback's behavior. */
#define DMESH_TASK_NORMAL    0   /* legacy staging->rcvbuf copy */
#define DMESH_TASK_PUSH_DATA 1   /* backend push: batch data -> host data ring */
#define DMESH_TASK_PUSH_DESC 2   /* backend push: 16B descriptor -> host slot */
#define DMESH_TASK_PULL_CURSOR 3 /* flow control: host cursor -> DPU shadow */

struct dma_task_entry {
    struct dmesh_conn *owner;
    struct doca_dma_task_memcpy *task;
    doca_error_t result;
    int kind;                    /* DMESH_TASK_* */
    bool in_free_queue;
    bool in_submission_queue;
    bool in_flight;
    TAILQ_ENTRY(dma_task_entry) entries;
};
TAILQ_HEAD(dma_task_queue, dma_task_entry);

typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

#define MAX_CONSUMERS 16

/* Keep in sync with DPA_THREAD_POOL_SIZE (dpa.h): one DPA thread per connection.
 * This is the per-worker-thread limit; total = num_threads x this. */
#define DMESH_MAX_CONNECTIONS 32
#define DMESH_MAX_SESSIONS 64

/* A Comch peer is a channel, independently of its logical data flows. */
struct dmesh_session {
    uint64_t epoch;
    struct doca_comch_connection *connection;
    bool occupied, negotiated, hello_pending, closing;
    unsigned sends_pending;
    uint64_t hello_deadline_ms;
    uint32_t generation[DMESH_MAX_CONNECTIONS];
    int32_t close_status[DMESH_MAX_CONNECTIONS];
    bool closed[DMESH_MAX_CONNECTIONS], close_pending[DMESH_MAX_CONNECTIONS];
    /* Dispatcher-owned listener and one outstanding/active backend per worker.
     * A request remains reserved until its physical flow retires. */
    uint32_t listen_ip;
    uint16_t listen_port;
    bool listen_pending;
    int listen_status;
    struct {
        uint32_t token;
        bool sent;
    } backend[DMESH_SESSION_MAX_WORKERS];
};

/* Per-connection init state, advanced by dmesh_doca_ctrl_advance() */
enum dmesh_conn_state {
    DMESH_CONN_FREE = 0,           /* slot unused */
    DMESH_CONN_NEW,                /* connection bound, needs consumer + msgq setup */
    DMESH_CONN_AWAIT_METADATA,     /* consumer + msgq ready; awaiting host metadata msg */
    DMESH_CONN_RUNNING,            /* DPA thread running, DMA request sent */
    DMESH_CONN_ERROR,              /* a setup step failed; slot parked */
    /* appended to keep wire numbering stable */
    DMESH_CONN_CONSUMER_STARTING,  /* consumer ctx started; waiting for it to
                                      reach RUNNING (peer registration) */
    DMESH_CONN_CLOSING,            /* host disconnected; advance() tears the
                                      slot's DOCA resources down (never inside
                                      the disconnect callback - PE re-entrancy) */
};

/* A second-stage copy (staging buffer -> rcvbuf) deferred because all of this
 * connection's DMA tasks were in flight. Retried as tasks complete
 * (dmesh_dma_pending_drain). */
struct dma_pending_copy {
    uint32_t pos;
    uint32_t length;
};
#define DMA_PENDING_MAX 8192

/* All DPU-side state owned by one host connection. The shared infrastructure
 * (device, PEs, comch server, DPA instance/pool, DMA engine) stays in
 * struct objects; everything a second connection would clash on lives here. */
struct dmesh_conn {
    struct objects *objs;                     /* back pointer to shared state */
    struct dmesh_flow_key key;
    struct dmesh_flow_location location;
    struct doca_comch_connection *connection;
    enum dmesh_conn_state state;
    struct dmesh_session *session;
    uint32_t flow_id, generation;
    bool multiplexed, ready_sent, error_sent, close_requested, quarantined;
    bool readers_detached; /* proxy acknowledged local staging pointer retirement */
    int32_t error_status;
    struct dmesh_export_metadata_msg *pending_metadata;

    struct dmesh_doca_dpa_thread *dpa_thread; /* assigned from objs->dpa_pool */
    struct dmesh_doca_dpa_comch *dpa_comch;   /* msgqs bound to dpa_thread */

    struct local_mem_bufs *consumer_mem;
    struct doca_comch_consumer *consumer;
    uint32_t remote_consumer_id;

    /* flow identity received in the metadata message */
    struct dmesh_flow_id flow;

    /* remote metadata from this connection's host */
    struct doca_mmap *ring_mmap;
    struct dmesh_buffer sndbuf;
    struct dmesh_buffer rcvbuf;

    struct doca_buf_arr *buf_arr;
    struct doca_mmap *local_mmap;             /* local DMA staging buffer */
    void *dma_buffer;

    /* Per-connection DMA engine: own doca_dma ctx (own QP) + task pool +
     * buf inventory, so connections cannot starve each other's copies. */
    bool dma_closing;              /* terminal admission latch during checked cleanup */
    struct doca_dma *dma_ctx;
    struct doca_buf_inventory *buf_inv;
    struct dma_task_entry *dma_task_entries;
    int num_dma_tasks;
    int num_free_dma_tasks;
    int num_submission_dma_tasks;
    struct dma_task_queue free_dma_tasks;
    struct dma_task_queue submission_dma_tasks;

    /* Copies deferred while this connection's task pool was exhausted */
    struct dma_pending_copy *dma_pending;     /* ring of DMA_PENDING_MAX */
    int dma_pending_head;
    int dma_pending_cnt;
    long dma_dropped_copies;

    /* Completed recv segments, produced by the recv callback and popped by the
     * Rust driver (single worker thread owns both sides - no locking). The
     * Rust DmeshIo then reads the bytes directly from the staging buffer
     * (conn->dma_buffer + pos), so there is no extra copy on the DPU. */
    struct dmesh_recv_seg *recv_segs;         /* ring of DMESH_RECV_SEG_MAX */
    int recv_seg_head;                        /* consumer (Rust pop) */
    int recv_seg_cnt;
    long recv_seg_dropped;                    /* segments lost to a full ring */

    /* Reverse (response) path, DPU-owned. The DPU exports both to the host,
     * whose DPA thread polls rcv_ring for descriptors and DMAs the referenced
     * tx_staging bytes into the host's rcvbuf. The proxy writes response bytes
     * into tx_staging[tx_pos..] and appends a descriptor to rcv_ring. */
    struct dma_ring *rcv_ring;                /* descriptor ring (DPU memory) */
    struct doca_mmap *tx_staging_mmap;        /* mmap over tx_staging (PCI export) */
    void *tx_staging;                         /* DMA source for DPU->host copies */
    size_t tx_staging_len;
    uint32_t tx_pos;                          /* write cursor into tx_staging */
    bool reverse_exported;                    /* rcv_ring+tx_staging sent to host */

    /* Backend (안 2) push state: DPU -> host via this connection's doca_dma,
     * one batch outstanding at a time (data copy, then - from its completion
     * callback - the 16B descriptor copy; see struct dmesh_push_desc). The
     * batch staged in tx_staging stays untouched until the desc completes.
     * push_shadow is the DPU-local (mmap'd) source of the descriptor copy. */
    uint64_t push_seq;                        /* completed batches (next = +1) */
    uint32_t push_pos;                        /* data-ring write offset (host side) */
    uint32_t push_len;                        /* in-flight batch length */
    int push_state;                           /* 0 idle, 1 data in flight, 2 desc in flight */
    /* The proxy finished sending: a zero-length descriptor follows the last
     * batch and the host reads it as end of stream (dmesh_dma_push_fin). */
    bool push_fin_requested, push_fin_sent;
    struct dmesh_push_desc *push_shadow;      /* in tx_staging's reserved tail */

    /* Push flow control (see struct dmesh_push_cursor). The cursor shadow
     * lives in tx_staging's reserved tail right after push_shadow, so it is
     * mmap'd as a read-DMA destination. host_fc: 0 = probing (legacy host
     * until the magic shows up), 1 = enforced. pushed_bytes counts published
     * batch bytes for the data-ring occupancy check. */
    int cursor_state;                         /* 0 idle, 1 pull in flight */
    uint64_t gated_calls;                     /* consecutive gated push attempts */
    int host_fc;
    uint64_t host_consumed_seq;
    uint64_t host_consumed_bytes;
    uint64_t pushed_bytes;
    struct dmesh_push_cursor *cursor_shadow;  /* in tx_staging's reserved tail */
};

/* DOCA objects for DPUMesh thread */
struct dmesh_doca_objects {
    struct doca_dev *dev;
    struct doca_dev_rep *rep_dev;
    struct doca_pe *pe;
    struct doca_comch_server *cc_server;
    
};

struct objects {
    /* Exactly one is non-NULL: control endpoint or data worker. */
    struct dmesh_dispatcher *dispatcher;
    struct dmesh_worker_mailbox *mailbox;
    struct dmesh_comch_client legacy_client; /* compatibility adapter, not used by native channels */
    int worker_idx;                 /* index of the owning worker thread */
    struct doca_dev *dev;
    struct doca_dev_rep *rep_dev;
    struct doca_pe *pe;
    union {
        struct doca_comch_server *cc_server;
        struct doca_comch_client *cc_client;
    };
    /* Which arm of the union above is live. cleanup_objects() must stop and
     * destroy the comch endpoint as the right type; destroying a client as a
     * server leaves the channel service object alive on the device, and the
     * next process then fails to create one (devx syndrome 0x64b4). */
    bool is_server;
    /* Set by integrations that hand staging pointers to independent readers.
     * Such integrations must acknowledge detachment before flow teardown. */
    bool external_readers;
    /* Host side: set from the comch client ctx state-changed callback when the
     * DPU drops the connection (RUNNING -> STOPPING/IDLE). dmesh_chan_read /
     * write turn it into -1 (EOF) so the Go net.Conn - and gRPC's transport
     * reader on top of it - learns the peer is gone instead of spinning
     * forever on "no data yet" while its subconn stays READY. */
    int peer_gone;
    struct doca_comch_connection *connection;

    /* Host channel callback, serialized by the session owner. */
    void (*control_message_cb)(struct objects *, const uint8_t *, size_t);
    void *control_message_data;

    /* DMesh application recv/send buffers */
    struct dmesh_buffer sndbuf;
    struct dmesh_buffer rcvbuf;

    /* (host side) identity of the flow this worker carries; copied into the
     * metadata message by export_dma_metadata */
    struct dmesh_flow_id flow;

    struct doca_mmap *local_mmap;
    struct doca_mmap *remote_mmap;
    void *dma_buffer;
    void *remote_addr;
    size_t remote_buf_size;
    struct dma_ring *dma_ring;
    struct doca_mmap *ring_mmap;    /* used for DMA ring mmap in DPU */

    struct doca_buf_arr *buf_arr;

    doca_error_t (*release_dpa)(struct objects *); /* optional checked worker cleanup */
    struct dmesh_dpa_thread_pool *dpa_pool;   /* pre-created DPA threads */
    struct dmesh_doca_dpa_thread *dpa_thread; /* thread assigned to the active connection */
	struct dmesh_doca_dpa_comch *dpa_comch;
    doca_dpa_dev_comch_producer_t remote_dpa_producer;
    doca_dpa_dev_completion_t remote_dpa_producer_comp;

    /* comch control path related */
    bool server_finish;             /* Controls whether server progress loop should be run */
    enum dmesh_doca_init_state phase; /* event-driven init state machine progress */

    /* comch data path related */
    struct local_mem_bufs *consumer_mem;
    struct doca_comch_consumer *consumer;
    struct doca_pe *consumer_pe;
    
    struct local_mem_bufs *producer_mem;
    struct doca_comch_producer *producer;
    struct doca_pe *producer_pe;
    
    uint32_t remote_consumer_id;
    doca_error_t producer_result;		  /* Holds result will be updated in producer callbacks */
	bool producer_finish;			  /* Controls whether producer progress loop should be run */
	doca_error_t consumer_result;		  /* Holds result will be updated in consumer callbacks */
	bool consumer_finish;			  /* Controls whether consumer progress loop should be run */
    
    /* Monotonic message counters, incremented only by the owning worker
     * thread; the stats reporter thread reads them and computes deltas. */
    long recv_msg_cnt;
    long sent_msg_cnt;
    long recv_bytes;                /* payload bytes received via DMA */

    long unsigned int start_time_ns;
    long unsigned int end_time_ns;

    /* Reverse (response) path, host side. Handles imported from the DPU's
     * rcv_ring + tx_staging (DPU_TO_HOST metadata), and the host-launched DPA
     * thread that polls the ring and DMAs tx_staging -> local rcvbuf (mirror of
     * the DPU forward datapath). rev_conn carries the DPA thread + comch msgqs. */
    struct doca_mmap *rev_ring_mmap;   /* imported DPU rcv_ring */
    void *rev_ring_buf;
    size_t rev_ring_buf_len;
    struct doca_mmap *rev_tx_mmap;     /* imported DPU tx_staging */
    void *rev_tx_buf;
    size_t rev_tx_len;
    struct dmesh_conn *rev_conn;       /* host DPA thread + comch for reverse path */
    bool reverse_ready;

    /* The reverse DPA must run on a DIFFERENT host PCI function than the
     * forward path: a flexio process is one-per-function, and the DPU proxy
     * already holds this host's 94:00.1. rev_objs owns the reverse device
     * (94:00.0), its DPA instance, the local reverse rcvbuf and the imported
     * DPU handles. rev_msg stashes the export descriptors until rev_objs's
     * device exists to import them on. */
    struct dmesh_export_rcv_ring_msg rev_msg;  /* stashed DPU export descriptors */
    struct objects *rev_objs;                  /* reverse device (94:00.0) state */
    struct dmesh_buffer rev_rcvbuf;            /* reverse DMA destination (on rev dev) */

    /* DPU multi-connection slots (see struct dmesh_conn) */
    struct dmesh_conn conns[DMESH_MAX_CONNECTIONS];
    struct dmesh_session sessions[DMESH_MAX_SESSIONS];
};

doca_error_t
cleanup_objects(struct objects *objs);

/* Bind a new host connection to a free slot; returns NULL if all in use
 * (or the connection is already bound - then the existing slot is returned). */
struct dmesh_conn *
dmesh_conn_open(struct objects *objs, struct doca_comch_connection *connection);

/* Find the slot bound to a connection; NULL if none */
struct dmesh_conn *
dmesh_conn_get(struct objects *objs, struct doca_comch_connection *connection);

struct dmesh_conn *dmesh_flow_get(struct objects *, struct doca_comch_connection *, uint32_t, uint32_t);
struct dmesh_conn *dmesh_flow_open(struct objects *, struct doca_comch_connection *, uint32_t, uint32_t);

void dmesh_flow_close_session(struct objects *, struct doca_comch_connection *);

/* Call on the control owner thread after all external staging readers/writers
 * for a CLOSING slot have detached under their own synchronization. */
doca_error_t dmesh_flow_readers_detached(struct objects *, int slot);
bool dmesh_objects_have_live_flows(const struct objects *);

/* Unbind a connection's slot (DOCA resources are NOT torn down yet - TODO) */
void
dmesh_conn_close(struct objects *objs, struct doca_comch_connection *connection);

#endif // OBJECT_H_
