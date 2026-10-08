#ifndef DPA_COMMON_H_
#define DPA_COMMON_H_

#include <stdint.h>
#include <doca_mmap.h>

typedef uint64_t doca_dpa_dev_uintptr_t;
typedef uint64_t doca_dpa_dev_buf_arr_t;

/* Below both the producer SQ and completion CQ capacities (512). */
#define DMESH_DPA_MAX_INFLIGHT 128u

#define DMESH_DPA_BENCH_SCRATCH_SIZE (64u * 1024u)

struct dpa_thread_arg {
	uint64_t dpa_consumer_comp;
	uint64_t dpa_producer_comp;
	uint64_t dpa_producer;
	uint64_t dpa_consumer;
	doca_dpa_dev_buf_arr_t dpa_buf_arr;
	uint32_t buf_arr_size;

    doca_dpa_dev_mmap_t host_mmap;

	doca_dpa_dev_mmap_t dpu_mmap;
	uint64_t src_addr;
	uint32_t buf_size;
	uint32_t pos;

	/* producer_dma_copy microbenchmark (DMESH_DPA_BENCH_* env vars on the DPU
	 * app; bench_mode 0 = off -> normal ring-polling datapath) */
	uint64_t bench_host_addr;   /* host sndbuf base VA (DMA source) */
	uint32_t bench_host_size;   /* host sndbuf length */
	uint32_t bench_mode;        /* 0=off, 1=throughput, 2=latency */
	uint32_t bench_msg_size;    /* bytes per copy (max 8192 on this platform) */
	uint32_t bench_num_ops;     /* copies per run */

	/* Cooperative shutdown stops admission in the kernel and finishes the
	 * polling thread. CPU cleanup must also compare dma_submitted with the
	 * received DMA completion count before freeing any mapping. */
	volatile uint32_t stop;     /* host -> DPA: leave the poll loop */
	volatile uint32_t stopped;  /* DPA -> host: poll loop has exited */

	/* DPU-side staging flow control. rd_pos: offset up to which the DPU
	 * reader (proxy) has consumed the staging ring, published by the DPU app
	 * via h2d_memcpy on its tick. rd_fc: 1 = the DPU app publishes rd_pos,
	 * so the kernel must not copy past it (opt-in: apps that never publish
	 * keep the legacy free-running behavior). */
	volatile uint32_t rd_pos;
	volatile uint32_t rd_fc;

	/* Extended DPA context (host PF process extended to an SF): the handle
	 * from doca_dpa_get_dpa_handle(extended ctx); the kernel switches to it
	 * with doca_dpa_dev_device_set() before touching that device's objects
	 * (the official extended-context flow). 0 = base context, no switch. */
	uint64_t dpa_dev;

	/* Published before stopped: number of copies requiring CPU DMA-completed
	 * messages. Kernel exit alone does not retire producer DMA operations. */
	volatile uint64_t dma_submitted;
	uint64_t submit_head;
	volatile uint32_t dma_error;

    uint64_t bench_scratch; /* optional per-thread HPACK scratch in DPA heap */

} __attribute__((__packed__, aligned(8)));

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
