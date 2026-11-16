#ifndef OBJECT_H_
#define OBJECT_H_

#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_dma.h>

#include <poll.h>
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
struct dmesh_mux;
struct dmesh_ec_worker;
struct dma_ring;
struct objects;
struct dmesh_conn;
/* What a DMA task carries; drives the completion callback's behavior. */
#define DMESH_TASK_NORMAL    0   /* legacy staging->rcvbuf copy */
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
#define DMESH_MAX_CONNECTIONS 64
/* Per-flow statistics the proxy exports (struct dmesh_conn.flow_stats). */
#define DMESH_FLOW_STATS 5
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
    /* TAILS: the export descriptor of the host's tail table (0 = none yet).
     * Every flow of the session carries a copy to its worker, whose pod mux
     * thread maps the table once. */
    struct dmesh_tails_export tails;
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
    /* Dispatcher asked the proxy to drain this flow (GOAWAY) so the host
     * reconnects on another worker; taken once by the proxy driver. */
    bool migrate_requested;
    bool readers_detached; /* proxy acknowledged local staging pointer retirement */
    /* Flow statistics the proxy copies in (dmesh_dispatch_worker_flow_stats)
     * and the dispatcher samples: cumulative requests, CPU ns, residence ns,
     * backend ns and retired instructions (0 unless the proxy counts them) of
     * the client connection on this slot. Accessed with __atomic builtins;
     * OPEN clears them with the slot. */
    uint64_t flow_stats[DMESH_FLOW_STATS];
    int32_t error_status;
    struct dmesh_export_metadata_msg *pending_metadata;
    struct dmesh_tails_export *pending_tails; /* the session's TAILS, until its pod thread maps it */

    struct dmesh_doca_dpa_thread *dpa_thread; /* assigned from objs->dpa_pool */
    uint32_t rx_consumed_pos_published; /* last DPU RX consumed position sent to the DPA */
    struct dmesh_doca_dpa_comch *dpa_comch;   /* msgqs bound to dpa_thread */

    /* Pod-multiplexed forward DMA (dpu/dpa_mux.h) instead of dpa_thread/dpa_comch:
     * this flow's forward ring is ring mux_ring (flow id - 1) of its session's
     * thread mux_pod. */
    bool mux, mux_close_sent;
    struct dmesh_pod_thread *mux_pod;
    uint8_t mux_ring;
    uint32_t mux_gen;                 /* echoed by every message of this ring */
    uint32_t mux_error;               /* enum dmesh_mux_ring_error the DPA reported */
    uint64_t mux_received;            /* DMA_DONE messages taken for this flow */
    uint64_t mux_deadline_ns;         /* CLOCK_MONOTONIC bound for the close */

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
    /* The host closed its sending half after host_shut_descs forward
     * descriptors (SHUTDOWN): once all completed and were popped, the pop
     * reports end of stream. */
    bool host_shut;
    uint64_t host_shut_descs;

    /* Reverse (DPU -> host) path. The proxy writes a flow's bytes into
     * tx_staging; the worker's reverse DPA thread copies each batch into the
     * host's data ring and publishes its descriptor (struct dmesh_push_desc),
     * then wakes the host through the flow slot's sync event (dpu/ec_rev.h).
     * A batch stays untouched in tx_staging until its descriptor landed. */
    struct doca_mmap *tx_staging_mmap;        /* mmap over tx_staging */
    void *tx_staging;                         /* DMA source for DPU->host copies */
    size_t tx_staging_len;
    bool reverse_ready;                       /* tx_staging allocated: READY may go out */
    uint64_t push_seq;                        /* completed batches (next = +1) */
    uint32_t push_pos;                        /* data-ring write offset (host side) */
    /* The proxy finished sending: a zero-length descriptor follows the last
     * batch and the host reads it as end of stream (dmesh_dma_push_fin). */
    bool push_fin_requested, push_fin_sent;

    /* Push flow control (see struct dmesh_push_cursor). The cursor shadow
     * lives in tx_staging's reserved tail, so it is mmap'd as a read-DMA
     * destination. host_fc: the host's cursor was seen. pushed_bytes counts
     * the bytes of completed batches. */
    int cursor_state;                         /* 0 idle, 1 pull in flight */
    uint64_t cursor_pull_seq;                 /* push_seq at the last pull */
    uint64_t gated_calls;                     /* consecutive gated push attempts */
    int host_fc;
    uint64_t host_consumed_seq;
    uint64_t host_consumed_bytes;
    uint64_t pushed_bytes;
    struct dmesh_push_cursor *cursor_shadow;  /* in tx_staging's reserved tail */

    /* The flow's share of the reverse DPA thread: the session's tail table
     * page, the flow's line (written on its first batch) and the batches
     * handed to the thread so far. */
    bool ec;                                  /* the flow is known to the reverse DPA thread */
    bool ec_ready;                            /* the flow's line is written */
    uint32_t ec_gen;                          /* the flow, as its entries and completion line name it */
    uint32_t ec_se_h;                         /* tail table page, DPA handle */
    uint64_t ec_se_addr;                      /* host address of the flow slot's sync event word */
    struct doca_mmap *ec_se_mmap;             /* the session's tail table page, imported */
    uint64_t ec_issued_seq, ec_issued_bytes;  /* batches handed to the thread */
    uint64_t ec_quiesce_ns;                   /* teardown has waited for the thread since */
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
    int worker_idx;                 /* index of the owning worker thread */
    bool migrate_pending;           /* some conn has migrate_requested set */
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


    struct doca_mmap *local_mmap;
    struct doca_mmap *remote_mmap;
    void *dma_buffer;
    void *remote_addr;
    size_t remote_buf_size;
    struct doca_mmap *ring_mmap;    /* used for DMA ring mmap in DPU */

    struct doca_buf_arr *buf_arr;

    doca_error_t (*release_dpa)(struct objects *); /* optional checked worker cleanup */
    doca_error_t (*release_mux)(struct objects *); /* stops and frees the mux threads */
    struct dmesh_dpa_thread_pool *dpa_pool;   /* pre-created DPA threads */
    struct dmesh_mux *mux;                    /* DMESH_DPA_MUX threads, NULL when off */
    struct dmesh_ec_worker *ec;               /* the worker's reverse DPA thread */
    struct dmesh_doca_dpa_thread *dpa_thread; /* thread assigned to the active connection */
	struct dmesh_doca_dpa_comch *dpa_comch;

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

/* Reads the events waiting in a progress engine's notification channel.
 *
 * A completion on a context of an armed PE queues an event that refers to
 * that context's completion queue; the SDK reads the queue on the next
 * doca_pe_request_notification(). An event still waiting when its context is
 * destroyed then refers to freed memory, and reading it crashes inside
 * doca_pe_request_notification() (a host that dies with a DMA in flight
 * leaves exactly that: the failed task's completion). Call this for a
 * context that is idle, so it completes nothing more, right before
 * destroying it. */
static inline void dmesh_pe_read_events(struct doca_pe *pe)
{
    if (pe != NULL)
        (void)doca_pe_request_notification(pe);
}

/* Reads the events waiting in a progress engine's notification channel
 * without arming it again, after waiting up to wait_ms for one to arrive.
 *
 * For a context whose SDK object ends when it reaches idle, not at destroy (a
 * Comch consumer): stopping it flushes its posted receives through its
 * completion queue, and on an armed PE the flush raises an event. Read the
 * events before the stop (wait_ms 0: what earlier completions queued) and
 * again right after doca_ctx_stop(), before the PE is progressed: the context
 * is still stopping then, and its completion queue, having fired, raises
 * nothing more. Progressing first would end the context with its event still
 * queued. */
static inline void dmesh_pe_take_events(struct doca_pe *pe, int wait_ms)
{
    doca_notification_handle_t handle;

    if (pe == NULL || doca_pe_get_notification_handle(pe, &handle) != DOCA_SUCCESS)
        return;
    for (int round = 0; round < 2; ++round) {
        struct pollfd fd = {.fd = (int)handle, .events = POLLIN};
        if (poll(&fd, 1, wait_ms) <= 0)
            return;
        (void)doca_pe_clear_notification(pe, handle);
    }
}

#endif // OBJECT_H_
