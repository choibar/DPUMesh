#ifndef COMCH_COMMON_H
#define COMCH_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <doca_error.h>
#include <doca_mmap.h>

struct objects;

enum msg_direction {
    HOST_TO_DPU = 0,
    DPU_TO_HOST = 1,
};

enum dmesh_msg_type {
    DMESH_MSG_EXPORT_METADATA = 1,
    DMESH_MSG_EXPORT_DPA_COMP = 2,
    DMESH_MSG_EXPORT_RCV_RING = 3,  /* DPU -> host: reverse (response) path metadata */
};

/* Identity of the flow carried by a dmesh connection. The DMA path has no
 * TCP/IP headers, so what the kernel used to provide on a TCP accept (peer
 * address) and what iptables interception used to provide (original
 * destination) must be conveyed explicitly by the host shim.
 * ips/ports are little-endian host order (both PCIe endpoints are LE). */

/* What role this dmesh connection plays. CLIENT: an intercepted outbound flow
 * (the proxy serves it through the outbound stack). BACKEND: the host process
 * is a backend provider - the proxy CONNECTS THROUGH this channel to reach the
 * service at flow.dst instead of dialing TCP. */
#define DMESH_FLOW_MODE_CLIENT  0u
#define DMESH_FLOW_MODE_BACKEND 1u
/* Client-semantics flow carried over the push transport (안 2): the DPU serves
 * it like CLIENT (h2 server), but the reverse path is the DPA-free push channel
 * so ingress host processes scale past the flexio one-per-function limit. */
#define DMESH_FLOW_MODE_INGRESS_PUSH 2u
/* BACKEND with the reverse path served like CLIENT: rcv_ring + tx_staging
 * exported, the host's DPA pulls (the host library's host-dpa reverse path). */
#define DMESH_FLOW_MODE_BACKEND_PULL 3u
/* True for modes whose reverse (DPU->host) path uses the push engine. */
#define DMESH_FLOW_USES_PUSH(m) ((m) == DMESH_FLOW_MODE_BACKEND || (m) == DMESH_FLOW_MODE_INGRESS_PUSH)

struct dmesh_flow_id {
    uint32_t src_ip;
    uint32_t dst_ip;            /* ORIGINAL destination (pre-rewrite); for a
                                   BACKEND flow: the service address provided */
    uint16_t src_port;
    uint16_t dst_port;
    uint32_t mode;              /* DMESH_FLOW_MODE_* */
    char src_workload[64];      /* source workload identity (pod/SA), NUL-terminated */
};

/* Backend (plan "안 2") push channel layout, in the HOST's rcvbuf of a BACKEND
 * connection. No host DPA is involved: the DPU pushes with its per-connection
 * doca_dma engine and the host busy-polls plain memory.
 *
 *   rcvbuf[0 .. 2048)                : desc slot ring, DMESH_PUSH_DESC_N x 16B
 *   rcvbuf[DMESH_PUSH_DATA_OFF .. )  : data ring
 *
 * Per batch (<= 8KB, the platform's single-DMA limit) the DPU DMAs the data
 * into the data ring, then - only after that copy completes - DMAs one 16B
 * descriptor into slot seq % N carrying {seq, pos, len}. The host consumes in
 * order: it waits for desc[expected % N].seq == expected, copies
 * data[pos, pos+len), and advances. Ordering is enforced by completion
 * chaining on the DPU (desc submitted from the data copy's completion
 * callback), and the slot ring tolerates the host lagging up to N batches
 * (N * 8KB = the data ring size, so data and descriptors overwrite together). */
struct dmesh_push_desc {
    volatile uint64_t seq;      /* 1-based batch sequence; 0 = empty slot */
    volatile uint32_t pos;      /* batch offset within the data ring */
    volatile uint32_t len;      /* batch length in bytes; 0 = end of stream */
};
#define DMESH_PUSH_DESC_N    128u
#define DMESH_PUSH_DATA_OFF  4096u

/* Push flow control: the host publishes its consumption cursor at a fixed
 * offset in its rcvbuf (the reserved gap between the desc slots and the data
 * ring). The DPU pulls it with a small read-DMA between batches and refuses
 * to publish a batch that would overrun unconsumed slots/bytes. MAGIC marks
 * a flow-control-aware host; without it the DPU keeps the legacy
 * no-backpressure behavior (old host + new DPU stays compatible). */
#define DMESH_PUSH_CURSOR_OFF   2048u
#define DMESH_PUSH_FC_MAGIC     0xD3E5FC01D3E5FC01ull
struct dmesh_push_cursor {
    uint64_t magic;
    uint64_t consumed_seq;      /* last fully consumed batch seq */
    uint64_t consumed_bytes;    /* total bytes drained from the data ring */
    uint64_t pad;
};
#define DMESH_PUSH_MAX_BATCH 8192u

/* Single control-path message carrying all host-side DMA metadata: the DMA
 * ring, the send buffer and the receive buffer (address, size and PCI export
 * descriptor for each), plus the flow identity. Sent once by the host after
 * the flow is opened and all buffers are allocated. */
struct dmesh_export_metadata_msg {
    enum dmesh_msg_type type;
    struct dmesh_flow_id flow;
    /* DMA ring */
    void *ring_buf;
    size_t ring_buf_len;
    size_t ring_desc_len;
    /* send/receive DMA buffers */
    void *sndbuf;
    void *rcvbuf;
    size_t sndbuf_len;
    size_t rcvbuf_len;
    size_t snd_desc_len;
    size_t rcv_desc_len;
    uint8_t ring_desc[512];
    uint8_t snd_desc[512];
    uint8_t rcv_desc[512];
};

typedef uint64_t doca_dpa_dev_comch_consumer_completion_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef uint64_t doca_dpa_dev_comch_producer_t;
typedef uint64_t doca_dpa_dev_comch_consumer_t;

struct dmesh_dpa_comp_msg {
    enum dmesh_msg_type type;
    doca_dpa_dev_comch_consumer_completion_t dpa_consumer_comp;
	doca_dpa_dev_completion_t dpa_producer_comp;
	doca_dpa_dev_comch_producer_t dpa_producer;
	doca_dpa_dev_comch_consumer_t dpa_consumer;
};

/* Reverse (response) path metadata, sent DPU -> host once the DPU has allocated
 * this connection's descriptor ring and staging buffer. It is the mirror of
 * dmesh_export_metadata_msg: the DPU exports the rcv_ring (host DPA polls it for
 * descriptors) and the tx_staging buffer (DMA source; host DPA copies it into
 * the host's rcvbuf). Both live in DPU memory and are PCI-exported to the host. */
struct dmesh_export_rcv_ring_msg {
    enum dmesh_msg_type type;
    /* descriptor ring (DPU memory) the host DPA thread polls */
    void *ring_buf;
    size_t ring_buf_len;
    size_t ring_desc_len;
    /* tx staging buffer (DPU memory) the host DPA thread DMAs into the rcvbuf */
    void *tx_staging;
    size_t tx_staging_len;
    size_t tx_desc_len;
    uint8_t ring_desc[512];
    uint8_t tx_desc[512];
};

struct dmesh_comch_msg {
    enum dmesh_msg_type type;
    union
    {
        struct dmesh_export_metadata_msg export_metadata_msg;
        struct dmesh_dpa_comp_msg dpa_comp_msg;
        struct dmesh_export_rcv_ring_msg export_rcv_ring_msg;
    };
};

struct dmesh_conn;

/* Build metadata without sending it; channel sessions add the flow envelope. */
doca_error_t
build_dma_metadata(struct objects *objs, struct dmesh_export_metadata_msg *msg);
doca_error_t
export_dma_metadata(struct objects *objs);
doca_error_t
process_export_metadata_msg(struct dmesh_conn *conn, struct dmesh_export_metadata_msg *metadata_msg);
doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg);

/* DPU side: allocate this connection's reverse rcv_ring + tx_staging (if not
 * already) and send their PCI export descriptors to the host. */
doca_error_t
export_rcv_ring_metadata(struct dmesh_conn *conn);
/* Host side: import the DPU's rcv_ring + tx_staging from the export message. */
doca_error_t
process_export_rcv_ring_msg(struct objects *objs, struct dmesh_export_rcv_ring_msg *msg);
#endif // COMCH_COMMON_H
