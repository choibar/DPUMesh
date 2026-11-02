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
 * Flow-multiplexed forward DMA (DMESH_DPA_MUX, run_dma_mux). Each DPU worker
 * runs a few DPA threads; each drains up to DMESH_MUX_RINGS connections'
 * forward rings in turn through one producer.
 *
 * A ring slot is driven through thread-local storage, like a per-flow thread:
 * the ARM writes the slot (cmd NONE), then cmd = OPEN; the DPA starts it on its
 * next pass. To close, the ARM writes cmd = CLOSE; the DPA stops copying and
 * sets state = CLOSED once the ring's last copy has completed. Every field has
 * one writer, and the two writers' fields sit on different cache lines. Every
 * field is 64-bit so both toolchains agree on the layout.
 */
#define DMESH_MUX_RINGS   8u      /* about 0.87 us of polling per ring per pass */
#define DMESH_MUX_FIFO    DMESH_DPA_MAX_INFLIGHT
#define DMESH_MUX_QUANTUM 32u     /* descriptors per ring per pass */

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
	DMESH_MUX_ERR_TAIL = 1,   /* producer_tail moved backwards or past the ring */
	DMESH_MUX_ERR_DESC = 2,   /* size or address outside the registered TX pool */
	DMESH_MUX_ERR_COPY = 3,   /* the copy completed with an error */
};

struct dpa_mux_ring {
	/* Line 0, ARM, written while cmd is NONE */
	uint64_t buf_arr;         /* host ring: [0] = dma_ring_ctrl, [1..ring_size] = dma_desc */
	uint64_t host_mmap;       /* TX pool: copy source */
	uint64_t dpu_mmap;        /* this connection's staging: copy destination */
	uint64_t staging_addr;
	uint64_t buf_size;        /* staging bytes */
	uint64_t ring_size;       /* descriptors, a power of two */
	uint64_t host_base;       /* TX pool range every descriptor must fall in */
	uint64_t host_len;
	/* Line 1, ARM */
	uint64_t slot;            /* connection slot on the worker, echoed in every message */
	uint64_t generation;      /* echoed in every message */
	volatile uint64_t cmd;    /* enum dmesh_mux_cmd */
	volatile uint64_t rx_consumed_pos; /* staging the reader released */
	uint64_t arm_reserved[4];
	/* Line 2, DPA */
	uint64_t pos;             /* next staging offset */
	uint64_t submit;          /* next ticket to copy */
	uint64_t complete;        /* contiguous completed tickets (custody ACK) */
	uint64_t completed_bytes;
	uint64_t inflight;        /* copies of this ring in the FIFO */
	uint64_t failed;          /* copies that completed with an error (no message) */
	volatile uint64_t state;  /* enum dmesh_mux_state */
	uint64_t error;           /* enum dmesh_mux_ring_error */
};

struct dpa_mux_ctx {
	/* Line 0-1: written by the ARM before run, then by the DPA */
	uint64_t producer, producer_comp; /* DPA -> ARM msgq: copies and their messages */
	uint64_t dpa_dev;
	uint64_t rr;                       /* first ring of the next pass */
	uint64_t fifo_head, fifo_tail;     /* copies in submission order */
	uint64_t heartbeat;                /* passes, for the ARM's liveness check */
	uint64_t oldest_age_us;            /* age of the oldest copy in flight */
	uint64_t credit_waits;             /* passes cut short by an empty ARM consumer */
	uint64_t err_cqes;                 /* failed completions */
	uint64_t bad_cqes;                 /* completions with an empty FIFO */
	volatile uint64_t stop;            /* ARM */
	volatile uint64_t stopped;
	uint64_t reserved[3];
	uint64_t fifo[DMESH_MUX_FIFO];     /* ring << 32 | length */
	uint64_t fifo_us[DMESH_MUX_FIFO];  /* __dpa_thread_time() at submission */
	struct dpa_mux_ring ring[DMESH_MUX_RINGS];
};

_Static_assert(sizeof(struct dpa_mux_ring) == 192, "dpa_mux_ring: three cache lines");
_Static_assert(offsetof(struct dpa_mux_ring, slot) == 64 && offsetof(struct dpa_mux_ring, pos) == 128,
	       "dpa_mux_ring: one writer per cache line");
_Static_assert(offsetof(struct dpa_mux_ctx, fifo) == 128 && offsetof(struct dpa_mux_ctx, ring) % 64 == 0,
	       "dpa_mux_ctx: ring slots start on a cache line");

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

#endif
