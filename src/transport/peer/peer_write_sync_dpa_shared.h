#ifndef DMESH_WRITE_SYNC_DPA_SHARED_H
#define DMESH_WRITE_SYNC_DPA_SHARED_H
#include "peer_dpa_ring.h"
#include "peer_dpa_work.h"
#include "peer_write_fence_stats.h"
#include "peer_write_bootstrap.h"
/* DATA-ID completion ABI. Descriptor/SQ bounds are queue capacities, never
 * frame lifetime slots. A fence records one signalled batch, not each DATA.
 * Rebuild ARM and device together when changing this private ABI. */
#define WSD_ENGINE 0x5753594450410011ull
#define WSD_LANES PEER_RDMA_LANES
#define WSD_QPN_BUCKETS PEER_RDMA_QPN_BUCKETS
#define WSD_RING PEER_RDMA_DESC_RING_CAP
#define WSD_SQ 4096u
#define WSD_FENCES PEER_IMM_RECV_SLOTS
#define WSD_CREDIT 0u
#define WSD_BOOT 1u
#define WSD_RECV 2u
#define WSD_CONTROLS 3u
/* Preserve the previous per-QP CQ budget, rounded for the configured lanes. */
#define WSD_CQ PEER_RDMA_QUEUE_DEPTH(WSD_LANES * 256u)
#define WSD_NATIVE_HELLO PEER_WRITE_SYNC_HELLO
#define WSD_IMM_HELLO PEER_WRITE_IMM_HELLO
#define WSD_IMM_RQ PEER_IMM_RECV_SLOTS
#define WSD_IMM_CQ PEER_RDMA_QUEUE_DEPTH(WSD_LANES * 512u)
#define WSD_EVENT_CQ PEER_RDMA_QUEUE_DEPTH(WSD_LANES * 2u)
#define WSD_WAIT_TAG_SHIFT 8u
/* SDK 3.5 reserves user_data 0xffffff; leave the final serial unused. */
#define WSD_WAIT_SERIAL_MAX 0xfffeu

enum {
    WSD_EVENT_GET = 0,
    WSD_EVENT_ASYNC = 1
};

_Static_assert(WSD_SQ < 0x8000u, "raw16 CQ prefix must unwrap unambiguously");
_Static_assert(WSD_FENCES > 0, "batch completion queue must be bounded and nonempty");
_Static_assert(WSD_QPN_BUCKETS && !(WSD_QPN_BUCKETS & (WSD_QPN_BUCKETS - 1u)),
               "QPN bucket count must be a power of two");
_Static_assert(WSD_LANES <= UINT16_MAX, "QPN links store lane index plus one");
_Static_assert(WSD_LANES <= 128, "async wait tag reserves seven lane bits");

enum {
    WSD_SYNC = 1,
    WSD_IMMEDIATE = 2
};

static inline unsigned wsd_hello_size(unsigned mode)
{
    return peer_write_hello_size(mode == WSD_IMMEDIATE, 1);
}

enum {
    WSD_INSTALL = 1,
    WSD_REMOVE,
    WSD_RESUME,
    WSD_STOP,
    WSD_RECEIVE,
    WSD_SEND_BOOT,
    WSD_CONFIGURE,
    WSD_DATA,
    WSD_HEAD,
    WSD_FENCE_SNAPSHOT
};

/* One cache line per immutable submission: 32B header + 32B operation payload.
 * ticket publishes the context FIFO; sequence also identifies per-QP controls.
 * Remote metadata is installed once by CONFIGURE, in the same FIFO as DATA.
 * Private ARM->DPA ABI; never exported to a Host/Pod. */
struct wsd_request {
    uint64_t ticket, sequence;
    uint32_t qpn, generation;
    uint16_t op, lane;
    uint32_t bytes;

    union {
        struct {
            uint64_t src, delta; /* DATA: absolute byte end; HEAD: returned bytes. */
            uint32_t key, id; /* DATA-only logical WRITE ID, modulo 2^32. */
            uint32_t reserved[2];
        } data;

        struct {
            uint64_t base, tail_event, head_event;
            uint32_t key, event_key;
        } config;

        struct {
            uint64_t data_fence; /* REMOVE must first import this context ticket. */
            uint64_t reserved[3];
        } control;
    };
} __attribute__((aligned(64)));

static inline unsigned wsd_control_slot(unsigned op)
{
    switch (op) {
    case WSD_HEAD:
        return WSD_CREDIT;
    case WSD_SEND_BOOT:
        return WSD_BOOT;
    case WSD_RECEIVE:
        return WSD_RECV;
    default:
        return WSD_CONTROLS;
    }
}

/* DPA-private expanded WRITE range; never published through the shared FIFO. */
struct wsd_write {
    uint64_t src, remote;
    uint32_t key, rkey, bytes;
};

struct wsd_binding {
    uint64_t qp, device, local_tail, local_head;
    uint64_t wait_ops[2];
    uint32_t connection, native;
    uint32_t qpn, generation, capacity, mode;
    uint32_t wait_tag; /* Unique per INSTALL: serial16 | lane7 | event1. */
} __attribute__((aligned(64)));

struct wsd_done {
    uint64_t id;
    uint32_t bytes, reserved;
};

struct wsd_status {
    uint64_t consumed, tail, head, ack;
    uint32_t completed_id, reserved; /* Completed DATA prefix, not consumed descriptors. */
    uint32_t error, generation;
    uint64_t data_posts, tail_sets, head_sets, send_cqes;
    uint64_t recv_cqes, rq_posts;
    uint64_t event_reads, event_posts, event_completions;
    struct wsd_done done[WSD_CONTROLS];
} __attribute__((aligned(64)));

struct wsd_ring {
    struct wsd_request entry[WSD_RING];
};

/* Separate cache lines and single writers: ARM publishes a wait request,
 * DPA publishes a sequence only AFTER status writeback completes. */
struct wsd_wait {
    uint64_t epoch, seen, enabled;
} __attribute__((aligned(64)));

struct wsd_publication {
    uint64_t sequence;
} __attribute__((aligned(64)));

struct wsd_data_credit {
    uint64_t consumed;
} __attribute__((aligned(64)));

/* Only DPA writes this frozen snapshot. It writes back BEFORE the existing
 * control ACK; ARM reads it before issuing another snapshot request. */
struct wsd_fence_snapshot {
    uint32_t lane, generation, depth, reserved;
    struct peer_write_fence_stats stats;
} __attribute__((aligned(64)));

struct wsd_shared {
    uint64_t control_consumed, control_ack, stopped, notification;
    struct wsd_wait wait;
    struct wsd_publication publication;
    struct wsd_request control[WSD_RING];
    struct wsd_binding binding[WSD_LANES];
    /* One ARM producer / DPA consumer FIFO for every QP in this context. */
    struct wsd_data_credit data_credit;
    struct wsd_ring data;
    struct wsd_status status[WSD_LANES];
    struct wsd_fence_snapshot fence_snapshot;
} __attribute__((aligned(64)));

struct wsd_fence {
    uint32_t counter, data_id;
};

_Static_assert(sizeof(struct wsd_fence) == 8, "eight bytes per batch fence");

struct wsd_lane {
    struct wsd_binding binding;
    uint64_t consumed, tx_tail, tx_head, rx_tail, rx_head;
    uint64_t remote_base, tail_addr, head_addr;
    uint32_t remote_key, event_key, remote_capacity;
    uint64_t id[WSD_CONTROLS];
    uint32_t post_index, completion_base, completed_index, data_id;

    struct wsd_fence fence[WSD_FENCES];

    unsigned fence_first, fence_count;
    uint32_t control_counter[WSD_CONTROLS];
    struct wsd_write last_data;
    uint64_t recv_source;
    uint32_t recv_key, rq_live;
    uint8_t last_data_valid;
    uint8_t completion_base_valid, notify;
    uint8_t busy[WSD_CONTROLS], configured, failed, installed;
    uint16_t qpn_next; /* DPA-private lane index + 1; zero ends the bucket chain. */
    uint8_t wait_armed[2];
    struct peer_write_fence_stats fence_stats; /* Uses existing tail padding. */
};

_Static_assert(sizeof(struct wsd_lane) == 640, "fence diagnostics must not grow a lane");
_Static_assert(sizeof(struct wsd_fence_snapshot) == 64, "one snapshot cache line per context");

struct wsd_device {
    uint64_t engine_kind, buffers, base_device, cq, cq_device;
    uint64_t control_head, data_head, resume_id;
    struct peer_dpa_work pending;
    struct wsd_request pending_request[PEER_DPA_WORK_CAP];
    uint64_t published_seq, notified_epoch;
    uint64_t event_cq;
    uint32_t event_wait, event_cq_capacity, event_flush_left;
    uint32_t cq_capacity, flush_left, count, cursor, owner_worker, cookie;
    uint8_t paused, notify_pending, fence_stats_enabled;
    uint16_t active[WSD_LANES];
    uint16_t qpn_heads[WSD_QPN_BUCKETS];
    struct wsd_lane lane[WSD_LANES];
};

_Static_assert(sizeof(struct wsd_request) == 64, "one cache line per descriptor");
_Static_assert(_Alignof(struct wsd_request) == 64, "descriptor cache line alignment");
_Static_assert(offsetof(struct wsd_request, ticket) == 0, "publication ticket ABI");
_Static_assert(offsetof(struct wsd_request, data) == 32, "32B header plus 32B payload");
_Static_assert(offsetof(struct wsd_shared, data_credit) % 64 == 0, "consumer cache line");
_Static_assert(offsetof(struct wsd_shared, data) % 64 == 0, "producer cache line");

_Static_assert(offsetof(struct wsd_device, engine_kind) == 0, "engine dispatch ABI");
#endif
