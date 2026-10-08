#ifndef DPA_COMMON_H_
#define DPA_COMMON_H_

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

    /* CPU -> DPA consumed position; rd_fc enables staging flow control. */
    volatile uint32_t rx_consumed_pos;
    volatile uint32_t rd_fc;

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

#endif
