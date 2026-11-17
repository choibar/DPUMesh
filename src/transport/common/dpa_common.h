#ifndef DPA_COMMON_H_
#define DPA_COMMON_H_

#include <stddef.h>
#include <stdint.h>
#include <doca_mmap.h>

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

/* Below both the producer SQ and completion CQ capacities (512). */
#define DMESH_DPA_MAX_INFLIGHT 128u

/* Native DMA thread-local state shared by CPU setup/teardown and the DPA.
 * Fixed-width fields are naturally aligned on both processors. */
struct dpa_thread_ctx {
    /* Comch handles and descriptor source. */
    uint64_t dpa_producer_comp;
    uint64_t dpa_producer;
    doca_dpa_dev_buf_arr_t dpa_buf_arr;

    uint64_t src_addr; /* destination staging base (legacy field name) */
    uint64_t dpa_dev;  /* extended context device; 0 selects the base context */

    /* Progress survives kernel retriggers. CPU cleanup additionally drains
     * dma_submitted matching receive messages before releasing mappings. */
    volatile uint64_t dma_submitted;
    uint64_t submit_head;

    uint32_t buf_arr_size;
    doca_dpa_dev_mmap_t host_mmap; /* DMA source mapping */
    doca_dpa_dev_mmap_t dpu_mmap;  /* DMA destination mapping */
    uint32_t buf_size;
    uint32_t pos; /* next destination staging offset */

    /* CPU -> DPA consumed position for host or DPU RX staging.
     * Every native DMA submission must respect this reuse boundary. */
    volatile uint32_t rx_consumed_pos;

    /* Stop admission, drain producer CQ, then publish stopped. A nonzero
     * dma_error prevents CPU cleanup from treating exit as a successful fence. */
    volatile uint32_t stop;
    volatile uint32_t stopped;
    volatile uint32_t dma_error;
    /* Pooled (DMESH_DPA_REUSE): a stopped kernel reschedules instead of
     * finishing, so the next flow's request on this thread activates it. */
    uint32_t pooled;

    /* CPU -> DPA msgq consumer and its completion (0: not drained). A pooled
     * thread is woken by the next flow's request, so every activation reads
     * the request completions, returns their receive credits and re-arms. */
    uint64_t dpa_consumer_comp;
    uint64_t dpa_consumer;
};

enum comch_msg_type {
	COMCH_MSG_TYPE_DMA_REQ = 1,
	COMCH_MSG_TYPE_DMA_COMPLETED = 2,
};

struct comch_dma_comp_msg {
	enum comch_msg_type type;
	uint32_t pos;       /* staging offset of the (batched) copy */
	uint32_t length;    /* total bytes covered by this message */
	uint32_t count;     /* number of descriptors coalesced into this copy */
};

typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;

struct comch_dma_req_msg {
	enum comch_msg_type type;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_mmap_t src_mmap;
	doca_dpa_dev_mmap_t dst_mmap;
	uint64_t src_addr;
	uint64_t dst_addr;
	uint32_t length;
} __attribute__((__packed__, aligned(8)));

struct comch_msg {
	enum comch_msg_type type;
	union
	{
		struct comch_dma_req_msg dma_req_msg;
		struct comch_dma_comp_msg dma_comp_msg;
	};
} __attribute__((__packed__, aligned(4)));

struct dma_ring_ctrl {
	volatile uint64_t producer_tail;
	volatile uint64_t consumer_head; /* contiguous DMA-completed descriptors */
	volatile uint64_t completed_bytes; /* source bytes safe to reuse */
	volatile uint32_t error; /* sticky device error; never acknowledge past it */
	uint8_t reserved[36];
} __attribute__((aligned(64)));

struct dma_desc {
	doca_dpa_dev_mmap_t mmap; 	// 4B
	uint64_t addr;			   // 8B
	size_t size;				   // 8B
	uint64_t idx;		   // 8B
	uint8_t reserved[35];	   // 35B
	volatile uint8_t valid;		   // 1B
} __attribute__((__packed__, aligned(8)));

/*
 * Pod-multiplexed forward DMA (DMESH_DPA_MUX, run_dma_mux). One DPA thread
 * serves the forward rings of one host session, the process of one pod, and
 * of no other; ring r is the session's flow r + 1, on whichever worker that
 * flow landed. A pod's rings therefore only ever wait behind the same pod's
 * copies, and a copy that never completes stops only that pod's flows.
 *
 * Tails: the host keeps one u32 per flow slot, the low 32 bits of the flow's
 * producer_tail, in a table of DMESH_TAILS_BYTES it exports once per session
 * (TAILS). Sixteen slots share a cache line: one read of a line tells the
 * thread which of sixteen rings have new descriptors, and only those rings
 * are visited. The ring's own ctrl still carries producer_tail (per-flow
 * threads read it there) and consumer_head, completed_bytes and error.
 *
 * Messages: the thread's one producer has a consumer on every worker that
 * owns one of its rings; a copy's DMA_DONE goes to its ring's worker
 * (cfg.consumer). Completions stay in submission order, one FIFO per thread.
 *
 * Slots: the ARM writes a ring's cfg while its cmd is NONE, then cmd = OPEN,
 * then bumps ctl_seq; the DPA reads the commands again only when ctl_seq
 * moved. CLOSE the same way; the DPA stops copying and sets state = CLOSED
 * once the ring's last copy has completed. Every line has one writer, the
 * ARM through h2d_memcpy or the DPA, and every field is 64-bit so both
 * toolchains agree on the layout. The ARM learns the DPA's side from the
 * thread's published status (struct dpa_mux_status), never from a
 * d2h_memcpy of local storage.
 *
 * Releases: a ring's reader returns staging with a plain store into ARM
 * memory (struct dpa_mux_release), not through local storage. The DPA reads
 * a ring's word only when its staging gate holds a copy back, and keeps the
 * last value it read (dpa_mux_ctx.released).
 */
#define DMESH_MUX_RINGS      32u  /* = DMESH_SESSION_MAX_FLOWS: ring r is flow r + 1 */
#define DMESH_TAILS_PER_LINE 16u
#define DMESH_TAILS_LINES    (DMESH_MUX_RINGS / DMESH_TAILS_PER_LINE)
#define DMESH_TAILS_BYTES    (DMESH_MUX_RINGS * 4u)
#define DMESH_MUX_FIFO       DMESH_DPA_MAX_INFLIGHT
#define DMESH_MUX_QUANTUM    32u  /* descriptors per ring per pass */

enum dmesh_mux_cmd {           /* ARM -> DPA */
	DMESH_MUX_CMD_NONE = 0,       /* the slot is the ARM's: the DPA leaves it alone */
	DMESH_MUX_CMD_OPEN = 1,
	DMESH_MUX_CMD_CLOSE = 2,
};

enum dmesh_mux_state {         /* DPA -> ARM */
	DMESH_MUX_IDLE = 0,
	DMESH_MUX_ACTIVE = 1,
	DMESH_MUX_ERROR = 2,          /* bad descriptor or failed copy: no new copies */
	DMESH_MUX_CLOSED = 3,         /* CLOSE seen and no copy left in flight */
};

enum dmesh_mux_ring_error {
	DMESH_MUX_ERR_TAIL = 1,   /* the published tail moved backwards or past the ring */
	DMESH_MUX_ERR_DESC = 2,   /* size or address outside the registered TX pool */
	DMESH_MUX_ERR_COPY = 3,   /* the copy completed with an error */
};

struct dpa_mux_ring_cfg {      /* ARM */
	/* Line 0, written while cmd is NONE */
	uint64_t buf_arr;         /* host ring: [0] = dma_ring_ctrl, [1..ring_size] = dma_desc */
	uint64_t host_mmap;       /* TX pool: copy source */
	uint64_t dpu_mmap;        /* this connection's staging: copy destination */
	uint64_t staging_addr;
	uint64_t buf_size;        /* staging bytes */
	uint64_t ring_size;       /* descriptors, a power of two */
	uint64_t host_base;       /* TX pool range every descriptor must fall in */
	uint64_t host_len;
	/* Line 1 */
	uint64_t slot;            /* connection slot on its worker, echoed in every message */
	uint64_t generation;      /* echoed in every message */
	uint64_t consumer;        /* consumer id of the ring's worker */
	volatile uint64_t cmd;    /* enum dmesh_mux_cmd */
	uint64_t reserved[4];
};

struct dpa_mux_ring_state {    /* DPA, one line */
	uint64_t pos;             /* next staging offset */
	uint64_t submit;          /* next ticket to copy */
	uint64_t complete;        /* contiguous completed tickets (custody ACK) */
	uint64_t completed_bytes;
	uint64_t inflight;        /* copies of this ring in the FIFO */
	uint64_t failed;          /* copies that completed with an error (no message) */
	volatile uint64_t state;  /* enum dmesh_mux_state */
	uint64_t error;           /* enum dmesh_mux_ring_error */
};

/* What the DPA writes. The thread works on its copy in local storage and
 * publishes it to ARM memory (dpa_mux_ctx.status): lines 0-1 at the end of
 * every activation and before it stops, a ring's line when the ring stops
 * (ERROR) or closes (CLOSED). The ARM reads only that memory. */
struct dpa_mux_status {
	/* Line 0: counters the watch reads (DMESH_DPA_MUX_STATS) */
	uint64_t heartbeat;                /* passes, for the ARM's liveness check */
	uint64_t tail_reads;               /* tail lines read from host memory */
	uint64_t visits;                   /* rings visited because their tail moved */
	uint64_t copies;
	uint64_t credit_waits;             /* visits cut short by the worker's empty consumer */
	uint64_t fifo_full;                /* visits cut short by a full FIFO */
	uint64_t err_cqes;                 /* failed completions */
	uint64_t bad_cqes;                 /* completions with an empty FIFO */
	/* Line 1 */
	uint64_t fifo_head, fifo_tail;     /* copies in submission order */
	uint64_t rr;                       /* first ring of the next pass */
	uint64_t open_mask;                /* rings ACTIVE */
	uint64_t closing_mask;             /* rings CLOSE seen, copies still in flight */
	uint64_t ctl_seen;                 /* ctl_seq the commands were last read at */
	uint64_t oldest_age_us;            /* age of the oldest copy in flight */
	uint64_t stopped;
	struct dpa_mux_ring_state st[DMESH_MUX_RINGS];
};

/* What the ARM writes for the DPA: the end of the staging each ring's reader
 * released. It follows the status in the same ARM memory, on lines of its own. */
struct dpa_mux_release {
	volatile uint64_t rx_consumed_pos[DMESH_MUX_RINGS];
};

/* A mux thread's ARM memory: the status the DPA writes, then the releases the
 * ARM writes. */
struct dpa_mux_arm {
	struct dpa_mux_status status;
	struct dpa_mux_release release;
};

struct dpa_mux_ctx {
	/* Line 0, ARM: set before run, then only ctl_seq and stop */
	uint64_t producer, producer_comp; /* DPA -> ARM msgq: copies and their messages */
	uint64_t dpa_dev;
	uint64_t tails;                   /* buf array over the host table, one line per buf */
	volatile uint64_t ctl_seq;        /* bumped after every cmd write */
	volatile uint64_t stop;
	uint64_t status, status_addr;     /* mmap handle and address of the ARM's dpa_mux_arm */
	struct dpa_mux_status s;          /* DPA */
	uint64_t fifo[DMESH_MUX_FIFO];     /* ring << 32 | length */
	uint64_t fifo_us[DMESH_MUX_FIFO];  /* __dpa_thread_time() at submission */
	struct dpa_mux_ring_cfg cfg[DMESH_MUX_RINGS];
	uint64_t released[DMESH_MUX_RINGS]; /* DPA: each ring's last release read */
};

_Static_assert(sizeof(struct dpa_mux_ring_cfg) == 128 && offsetof(struct dpa_mux_ring_cfg, slot) == 64,
	       "dpa_mux_ring_cfg: two ARM lines");
_Static_assert(sizeof(struct dpa_mux_ring_state) == 64, "dpa_mux_ring_state: one DPA line");
_Static_assert(sizeof(struct dpa_mux_status) % 64 == 0 && offsetof(struct dpa_mux_arm, release) % 64 == 0,
	       "dpa_mux_arm: the ARM's lines follow the DPA's");
_Static_assert(offsetof(struct dpa_mux_status, fifo_head) == 64 && offsetof(struct dpa_mux_status, st) == 128,
	       "dpa_mux_status: whole lines");
_Static_assert(offsetof(struct dpa_mux_ctx, s) == 64 && offsetof(struct dpa_mux_ctx, cfg) % 64 == 0,
	       "dpa_mux_ctx: one writer per cache line");
_Static_assert(DMESH_MUX_RINGS <= 64 && DMESH_MUX_RINGS % DMESH_TAILS_PER_LINE == 0,
	       "ring masks are 64-bit; tails fill whole lines");

/* DPA -> ARM: the immediate of every copy. */
#define DMESH_MUX_MSG_DMA_DONE 1u
struct dmesh_mux_msg {
	uint8_t type;
	uint8_t ring;
	uint16_t slot;
	uint32_t generation;
	uint32_t pos;
	uint32_t length;
};
_Static_assert(sizeof(struct dmesh_mux_msg) == 16, "dmesh_mux_msg must fit a copy's immediate");

/*
 * DPU -> host through a DPU-owned DPA thread (the reverse DPA thread).
 *
 * Each worker owns one reverse DPA thread, run_ec_rev, and each flow slot one
 * async ops queue of it. Before a flow's first batch the worker writes the
 * flow's line (its queue, DPA handles and host addresses); then it appends one
 * entry per batch to the thread's ring in ARM memory and advances the tail.
 * Once the host has room for the batch, the thread copies it from tx_staging
 * into the host's data ring on the flow's queue. Once the copy completed and
 * the flow's earlier batches were published, it stores the batch's descriptor
 * into the host's slot ring
 * (pos and len, then seq), a value no earlier store to it had
 * (wid << 48 | the thread's publish count) into the flow slot's sync event
 * word on the host, and the flow's completed sequence and bytes into the
 * worker's ARM memory. The host sleeps on a sync event wait task; neither
 * side's CPU takes part in the copy or the wake. A zero-length entry is the
 * end of stream: it publishes a descriptor of length 0 after every earlier
 * batch of the flow.
 *
 * Room is the host's consumption cursor (struct dmesh_push_cursor in the
 * window), which only the thread reads. A batch's copy starts when its
 * descriptor slot is free (seq - consumed_seq <= DMESH_EC_DESC_N - 2, one slot
 * more for the end of stream) and its bytes fit the data ring with one largest
 * batch to spare for the gap a wrap leaves
 * (cum_bytes - consumed_bytes + slack <= ring). The thread keeps the cursor it
 * read last and reads it again only when that leaves too little room; a flow
 * still waiting reads it at doubling intervals (EC_CURSOR_*_US in the kernel).
 * A cursor past what the thread published counts as what it published. The
 * worker does not look at the host: staging it cannot reuse yet holds its
 * flow back.
 *
 * Flows have queues of their own because a copy into the memory of a host
 * process that died never completes, and neither does any copy behind it on
 * its queue: one flow's dead host must not stop the others. The queues share
 * the thread's completion queue, and a completion names its queue by user
 * data. The thread publishes each flow in order and the flows independently.
 * The worker keeps a flow's batches in flight below DMESH_EC_FLOW_Q, so the
 * thread always takes the ring's next entry. A flow whose copy failed gets its
 * generation in `failed` and nothing more published. Completion lines carry
 * the flow's generation, so a slot's next flow never reads its last one's.
 * An entry with seq 0 retires the slot's queue: once the thread took it, no
 * completion of that queue matches the slot, and the worker may destroy it.
 *
 * The sync event words live in the session's tail table page, one line per
 * flow slot at DMESH_EC_SE_OFF(slot).
 */
#define DMESH_TAILS_ALLOC    4096u                     /* the tail table page the host exports */
#define DMESH_EC_SE_OFF(slot) (1024u + (uint32_t)(slot) * 64u)
#define DMESH_EC_RING        1024u                     /* entries per worker thread */
#define DMESH_EC_CONNS       64u                       /* = DMESH_MAX_CONNECTIONS */
#define DMESH_EC_DESC_N      128u                      /* = DMESH_PUSH_DESC_N */
#define DMESH_EC_FLOW_Q      DMESH_EC_DESC_N           /* a flow's batches in flight, and its queue's depth */
/* Every queue full, and the flush of queues replaced meanwhile. */
#define DMESH_EC_CQ          (2u * DMESH_EC_CONNS * DMESH_EC_FLOW_Q)
#define DMESH_EC_DESC_SZ     16u                       /* = sizeof(struct dmesh_push_desc): seq, pos, len */
#define DMESH_EC_DATA_OFF    4096u                     /* = DMESH_PUSH_DATA_OFF */
#define DMESH_EC_CURSOR_OFF  2048u                     /* = DMESH_PUSH_CURSOR_OFF */
#define DMESH_EC_FC_MAGIC    0xD3E5FC01D3E5FC01ull     /* = DMESH_PUSH_FC_MAGIC */
#define DMESH_EC_GEN_SHIFT   40                        /* completion line: generation << 40 | sequence */
#define DMESH_EC_GEN_MASK    0xffffffu

struct dmesh_ec_ent {               /* ARM -> DPA, one line per batch */
	uint32_t conn;                  /* worker's connection slot */
	uint32_t gen;                   /* the slot's flow (dmesh_ec_flow.gen) */
	uint64_t seq;                   /* batch sequence of the flow, 1-based; 0 retires the slot's queue */
	uint64_t pos_len;               /* data ring offset << 32 | length (0 = end of stream) */
	uint64_t src_addr;              /* tx_staging address of the batch */
	uint64_t cum_bytes;             /* the flow's bytes up to and including this batch */
	uint64_t l[3];
};

struct dmesh_ec_flow {              /* ARM -> DPA, one line per connection slot */
	uint64_t ops;                   /* the flow's queue, DPA handle */
	uint32_t gen;                   /* the flow, as its entries name it (masked by DMESH_EC_GEN_MASK) */
	uint32_t id;                    /* the queue's user data, which its completions carry */
	uint32_t src_mmap, win_mmap, se_mmap; /* DPA handles: tx_staging, host rcvbuf window, tail table page */
	uint32_t ring;                  /* data ring bytes the worker places batches in */
	uint64_t win_addr;              /* host address of the rcvbuf window */
	uint64_t se_addr;               /* host address of the flow slot's sync event word */
	uint32_t slack, pad;            /* the flow's largest batch: the most a wrap leaves unused */
	uint64_t l[1];
};

struct dmesh_ec_done {              /* DPA -> ARM, one line per connection slot */
	volatile uint64_t seq;          /* generation << DMESH_EC_GEN_SHIFT | last batch published */
	volatile uint64_t bytes;        /* the flow's bytes up to that batch */
	volatile uint64_t failed;       /* generation of a flow whose copy failed */
	volatile uint64_t l[5];
};

struct dmesh_ec_ctl {               /* ARM memory, one per worker */
	volatile uint64_t tail, l0[7];                  /* ARM -> DPA */
	volatile uint64_t head, copies, publishes, passes, activations, stale_cqes, err_cqes, cursor_reads; /* DPA -> ARM */
	struct dmesh_ec_done done[DMESH_EC_CONNS];
	struct dmesh_ec_flow flow[DMESH_EC_CONNS];
	struct dmesh_ec_ent ring[DMESH_EC_RING];
};

struct dmesh_ec_batch {             /* a batch the thread holds */
	uint64_t pos_len, cum_bytes, src_addr;
};

struct dmesh_ec_fq {                /* the thread's state of a slot's flow */
	uint64_t ops, win_addr, se_addr;
	uint32_t gen, id, src_mmap, win_mmap, se_mmap, failed, ring, slack;
	/* Batches published <= started <= taken; copies completed and not published. */
	uint64_t head, issued, tail, done;
	uint64_t pub_bytes;             /* the flow's bytes up to the last batch published */
	uint64_t cseq, cbytes;          /* the host's cursor as last read, at most what was published */
	uint64_t cur_us, cur_gap;       /* when it was last read; how long a waiting flow waits to read it again */
	struct dmesh_ec_batch b[DMESH_EC_FLOW_Q];
};

struct dmesh_ec_tls {               /* the thread's local storage */
	uint64_t acomp, ctl_mmap, ctl_addr;             /* set by the ARM */
	uint64_t head, copies, publishes, passes, activations, stale_cqes, err_cqes, cursor_reads;
	uint64_t wid;                                   /* worker index + 1: the high bits of every wake value */
	uint64_t waiting;                               /* slots with batches taken but not started */
	struct dmesh_ec_fq f[DMESH_EC_CONNS];
};

_Static_assert(sizeof(struct dmesh_ec_ent) == 64, "dmesh_ec_ent: one line");
_Static_assert(sizeof(struct dmesh_ec_flow) == 64, "dmesh_ec_flow: one line");
_Static_assert(sizeof(struct dmesh_ec_done) == 64, "dmesh_ec_done: one line");
_Static_assert(offsetof(struct dmesh_ec_ctl, done) == 128 && offsetof(struct dmesh_ec_ctl, ring) % 64 == 0,
	       "dmesh_ec_ctl: one writer per line");
_Static_assert(DMESH_EC_SE_OFF(33) + 64 <= DMESH_TAILS_ALLOC && DMESH_EC_SE_OFF(0) >= DMESH_TAILS_BYTES,
	       "sync event words fit the tail table page");

#endif
