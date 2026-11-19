#ifndef DMESH_WRITE_IMM_DPA_H
#define DMESH_WRITE_IMM_DPA_H

#include "peer_config.h"
#include "peer_dpa_abi.h"
#include "peer_write_protocol.h"
/* DATA-ID completion ABI. Descriptor/SQ bounds are queue capacities, never
 * frame lifetime slots. A fence records one signalled batch, not each DATA.
 * Rebuild ARM and device together when changing this private ABI. */
#define WIM_CREDIT 0u
#define WIM_BOOT 1u
#define WIM_RECV 2u
#define WIM_CONTROLS 3u
static inline unsigned wim_hello_size(void)
{
    return PEER_WRITE_IMM_HELLO;
}

enum {
    WIM_INSTALL = 1,
    WIM_REMOVE,
    WIM_RESUME,
    WIM_STOP,
    WIM_RECEIVE,
    WIM_SEND_BOOT,
    WIM_CONFIGURE,
    WIM_DATA,
    WIM_HEAD,
    WIM_FENCE_SNAPSHOT
};

/* One cache line per immutable submission: 32B header + 32B operation payload.
 * ticket publishes the context FIFO; sequence also identifies per-QP controls.
 * Remote metadata is installed once by CONFIGURE, in the same FIFO as DATA.
 * Private ARM->DPA ABI; never exported to a Host/Pod. */
struct wim_request {
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
            uint64_t base;
            uint32_t key, reserved[5];
        } config;

        struct {
            uint64_t data_fence; /* REMOVE must first import this context ticket. */
            uint64_t reserved[3];
        } control;
    };
} __attribute__((aligned(64)));

static inline unsigned wim_control_slot(unsigned op)
{
    switch (op) {
    case WIM_HEAD:
        return WIM_CREDIT;
    case WIM_SEND_BOOT:
        return WIM_BOOT;
    case WIM_RECEIVE:
        return WIM_RECV;
    default:
        return WIM_CONTROLS;
    }
}

struct wim_binding {
    uint64_t qp, device;
    uint32_t connection;
    uint32_t qpn, generation, capacity;
} __attribute__((aligned(64)));

struct wim_done {
    uint64_t id;
    uint32_t bytes, reserved;
};

struct wim_status {
    uint64_t consumed, tail, head, ack;
    uint32_t completed_id, reserved; /* Completed DATA prefix, not consumed descriptors. */
    uint32_t error, generation;
    uint64_t data_posts, tail_sets, head_sets, send_cqes;
    uint64_t recv_cqes, rq_posts;
    struct wim_done done[WIM_CONTROLS];
} __attribute__((aligned(64)));

struct wim_ring {
    struct wim_request entry[WIM_RING];
};

/* Separate cache lines and single writers: ARM publishes a wait request,
 * DPA publishes a sequence only AFTER status writeback completes. */
struct wim_wait {
    uint64_t epoch, seen;
} __attribute__((aligned(64)));

struct wim_publication {
    uint64_t sequence;
} __attribute__((aligned(64)));

struct wim_data_credit {
    uint64_t consumed;
} __attribute__((aligned(64)));

/* Batch-weighted observations immediately after adding a fence, before CQ
 * retirement. This is not a time-weighted mean or a hardware queue depth.
 * Histogram ranges: 1, 2-4, 5-8, 9-16, 17+. Snapshot/reset bounds counters. */
struct peer_write_fence_stats {
    uint64_t samples, depth_sum, full_stalls;
    uint32_t histogram[5], peak;
};

_Static_assert(sizeof(struct peer_write_fence_stats) == 48, "compact fence diagnostics");

/* Only DPA writes this frozen snapshot. It writes back BEFORE the existing
 * control ACK; ARM reads it before issuing another snapshot request. */
struct wim_fence_snapshot {
    uint32_t lane, generation, depth, reserved;
    struct peer_write_fence_stats stats;
} __attribute__((aligned(64)));

struct wim_shared {
    uint64_t control_consumed, control_ack, stopped, notification;
    struct wim_wait wait;
    struct wim_publication publication;
    struct wim_request control[WIM_RING];
    struct wim_binding binding[WIM_LANES];
    /* One ARM producer / DPA consumer FIFO for every QP in this context. */
    struct wim_data_credit data_credit;
    struct wim_ring data;
    struct wim_status status[WIM_LANES];
    struct wim_fence_snapshot fence_snapshot;
} __attribute__((aligned(64)));

_Static_assert(sizeof(struct wim_fence_snapshot) == 64, "one snapshot cache line per context");

_Static_assert(sizeof(struct wim_request) == 64, "one cache line per descriptor");
_Static_assert(_Alignof(struct wim_request) == 64, "descriptor cache line alignment");
_Static_assert(offsetof(struct wim_request, ticket) == 0, "publication ticket ABI");
_Static_assert(offsetof(struct wim_request, data) == 32, "32B header plus 32B payload");
_Static_assert(offsetof(struct wim_shared, data_credit) % 64 == 0, "consumer cache line");
_Static_assert(offsetof(struct wim_shared, data) % 64 == 0, "producer cache line");

/* Every attached peer context runs this one engine. */
unsigned peer_write_imm_dpa_drain(struct peer_dpa_thread_arg *, unsigned context,
                                  uint64_t state, unsigned budget);

/* ARM setup and progress API. SDK and owner types are pointer-only here so
 * the common header also compiles for the DPA target. */
struct peer_dpa_runtime;
struct peer_verbs_endpoint;
struct peer_pair_transport_config;
struct peer_pair_transport;
struct ibv_context;
struct ibv_recv_wr;
struct ibv_send_wr;
struct ibv_wc;
struct peer_rdma_device;
struct peer_wim;
struct peer_wim_qp;
int peer_wim_new(struct peer_rdma_device *,
                     const struct peer_dpa_runtime *,
                     unsigned eu,
                     struct peer_wim **);
int peer_wim_free(struct peer_wim *);
int peer_wim_qp_new(struct peer_wim *,
                    unsigned lane,
                    uint32_t generation,
                    uint32_t capacity,
                    uint32_t gid,
                    uint16_t mtu,
                    struct peer_verbs_endpoint *,
                    struct peer_wim_qp **);
struct dpa_rdma_counter_msg;
void peer_wim_counter_message(const struct dpa_rdma_counter_msg *);
/* Data-owner wait hooks; PE notification must be armed before arm_worker,
 * followed by a final transport scan. Cancel on every wait/scan exit. */
int peer_wim_arm_worker(unsigned worker);
void peer_wim_disarm_worker(unsigned worker);
int peer_wim_register(struct peer_wim *, void *, size_t, uint32_t);
int peer_wim_unregister(struct peer_wim *, uint32_t);
int peer_wim_activate(struct peer_wim_qp *, const struct peer_verbs_endpoint *, int);
unsigned peer_wim_hello_size(const struct peer_wim_qp *);
int peer_wim_configure_native(
    struct peer_wim_qp *, uint64_t, uint32_t, uint32_t);
int peer_wim_destroy(struct peer_wim_qp *);
/* Entries includes any descriptor reserved by a currently held frame lease. */
int peer_wim_admit(struct peer_wim_qp *, unsigned entries);
int peer_wim_data(struct peer_wim_qp *, uint64_t, uint64_t, uint32_t, uint32_t);
int peer_wim_head(struct peer_wim_qp *, uint64_t, uint64_t);
int peer_wim_recv(struct peer_wim_qp *, const struct ibv_recv_wr *);
int peer_wim_send(struct peer_wim_qp *, const struct ibv_send_wr *);
int peer_wim_poll(struct peer_wim *, int max, struct ibv_wc *);
void peer_wim_debug(struct peer_wim_qp *);
/* RX tail, returned remote head, local completed byte end, consumed requests. */
int peer_wim_counters(struct peer_wim_qp *, uint64_t *, uint64_t *, uint64_t *, uint64_t *);
int peer_pair_write_imm_dpa_new(const struct peer_pair_transport_config *,
                                struct ibv_context *,
                                uint8_t,
                                uint32_t,
                                uint16_t,
                                uint32_t,
                                const struct peer_dpa_runtime *,
                                struct peer_pair_transport **,
                                char *,
                                size_t);
#endif
