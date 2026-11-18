#ifndef COMCH_COMMON_H
#define COMCH_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <doca_error.h>
#include <doca_mmap.h>
#include "session_protocol.h"

struct objects;

enum msg_direction {
    HOST_TO_DPU = 0,
    DPU_TO_HOST = 1,
};

enum dmesh_msg_type {
    DMESH_MSG_EXPORT_METADATA = 1,
};

/* Identity of the flow carried by a dmesh connection. The DMA path has no
 * TCP/IP headers, so what the kernel used to provide on a TCP accept (peer
 * address) and what iptables interception used to provide (original
 * destination) must be conveyed explicitly by the host shim.
 * ips/ports are little-endian host order (both PCIe endpoints are LE). */

/* What role this dmesh connection plays. CLIENT: an intercepted outbound flow
 * (the proxy serves it through the outbound stack). BACKEND: the host process
 * is a backend provider - the proxy CONNECTS THROUGH this channel to reach the
 * service at flow.dst instead of dialing TCP. Both carry their DPU -> host
 * bytes through the worker's reverse DPA thread (dpu/ec_rev.h). */
#define DMESH_FLOW_MODE_BACKEND 1u
#define DMESH_FLOW_MODE_CLIENT  2u
#define DMESH_FLOW_MODE_VALID(m) ((m) == DMESH_FLOW_MODE_CLIENT || (m) == DMESH_FLOW_MODE_BACKEND)

struct dmesh_flow_id {
    uint32_t src_ip;
    uint32_t dst_ip;            /* ORIGINAL destination (pre-rewrite); for a
                                   BACKEND flow: the service address provided */
    uint16_t src_port;
    uint16_t dst_port;
    uint32_t mode;              /* DMESH_FLOW_MODE_* */
    char src_workload[64];      /* source workload identity (pod/SA), NUL-terminated */
};

/* DPU -> host layout, in the HOST's rcvbuf of every connection:
 *
 *   rcvbuf[0 .. 2048)                : desc slot ring, DMESH_PUSH_DESC_N x 16B
 *   rcvbuf[DMESH_PUSH_CURSOR_OFF ..) : the host's consumption cursor
 *   rcvbuf[DMESH_PUSH_DATA_OFF .. )  : data ring
 *
 * The worker's reverse DPA thread copies a batch into the data ring and, once
 * the copy completed, stores its 16B descriptor {seq, pos, len} into slot
 * seq % N, the sequence last. The host consumes in order: it waits for
 * desc[expected % N].seq == expected, reads data[pos, pos+len) and advances. */
struct dmesh_push_desc {
    volatile uint64_t seq;      /* 1-based batch sequence; 0 = empty slot */
    volatile uint32_t pos;      /* batch offset within the data ring */
    volatile uint32_t len;      /* batch length in bytes; 0 = end of stream */
};
#define DMESH_PUSH_DESC_N    128u
#define DMESH_PUSH_DATA_OFF  4096u
/* The rcvbuf (the host's receive window) and the DPU's tx_staging of every
 * connection: room for many batches in flight, so a full window, and the
 * reverse thread's wait for it, stays rare. */
#define DMESH_PUSH_WINDOW    (4u * 1024u * 1024u)

/* Push flow control: the host publishes its consumption cursor at a fixed
 * offset in its rcvbuf (the reserved gap between the desc slots and the data
 * ring). The worker's reverse DPA thread reads it and starts no copy that
 * would overrun unconsumed slots or bytes. MAGIC marks a published cursor;
 * until the thread sees it, it takes the host to have consumed nothing. */
#define DMESH_PUSH_CURSOR_OFF   2048u
#define DMESH_PUSH_FC_MAGIC     0xD3E5FC01D3E5FC01ull
struct dmesh_push_cursor {
    uint64_t magic;
    uint64_t consumed_seq;      /* last fully consumed batch seq */
    uint64_t consumed_bytes;    /* total bytes drained from the data ring */
    uint64_t pad;
};

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

/* A session's tail table as TAILS exported it (session_protocol.h). */
struct dmesh_tails_export {
    uint32_t desc_len;          /* 0: the session sent none */
    uint8_t desc[DMESH_SESSION_TAILS_MAX];
};

struct dmesh_comch_msg {
    enum dmesh_msg_type type;
    union
    {
        struct dmesh_export_metadata_msg export_metadata_msg;
    };
};

struct dmesh_conn;

/* Build metadata without sending it; channel sessions add the flow envelope. */
struct dma_ring;
struct dmesh_buffer;
doca_error_t dmesh_build_dma_metadata(struct doca_dev *dev, const struct dma_ring *ring,
    const struct dmesh_buffer *sndbuf, const struct dmesh_buffer *rcvbuf,
    const struct dmesh_flow_id *flow, struct dmesh_export_metadata_msg *msg);

doca_error_t
process_export_metadata_msg(struct dmesh_conn *conn, struct dmesh_export_metadata_msg *metadata_msg);
#endif // COMCH_COMMON_H
