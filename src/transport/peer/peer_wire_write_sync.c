/* WRITE + DOCA Sync Event carrier with ARM and native DPA adapters, plus
 * native DPA WRITE_WITH_IMM sharing its framing/source/lease machinery. See
 * design/ARM_WRITE_SYNC.md for their separate publication contracts.
 * Shared framing/source/lease rules intentionally match WRITE_WITH_IMM while
 * using full 64-bit logical progress. Only the Sync mode eliminates
 * steady-state receive CQEs; the IMM DPA owns its RQ/CQ entirely. */
#define _GNU_SOURCE
#include "peer_wire_write_sync.h"
#include "peer_sync_event.h"
#include "peer_write_sync_dpa.h"
#include "peer_dpa_placement.h"
#include "peer_arena_config.h"
#ifndef DMESH_NATIVE_PACKED_TX
#define DMESH_NATIVE_PACKED_TX 0
#endif
#include "peer_write_tx_ring.h"
#include "peer_write_imm_counter.h"
#include "peer_write_bootstrap.h"
#if DMESH_DPA_RDMA_OFFLOAD
#include "peer_dpa_ring.h"
#endif
#include "peer_security_wire.h"
#include "live_slots.h"
#include "peer_write_credit.h"
#include "peer_wait_diag.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <time.h>
#include <unistd.h>

#define WSY_CONNECTIONS PEER_RDMA_LANES
/* ARM verbs reserves bounded SQ work; DATA lifetime is a byte range. */
#define WSY_DATA_WR 16u
#define WSY_RQ 1u
#define WSY_SQ (2u * WSY_DATA_WR + 2u)
#define WSY_CQ_DEPTH (WSY_CONNECTIONS * (WSY_SQ + WSY_RQ))
#define WSY_BUDGET 128u
/* 23 epoch bits + 7 connection bits + 32 low counter bits, plus 2 kind bits. */
#define WSY_EPOCH_MAX 0x7fffffu
_Static_assert(WSY_CONNECTIONS <= DMESH_LIVE_SLOTS_MAX, "live connection list capacity");

enum {
    WSY_DATA,
    WSY_RECV,
    WSY_CREDIT,
    WSY_BOOT
};

enum {
    WSY_UNUSED,
    WSY_INIT,
    WSY_CONNECTING,
    WSY_READY,
    WSY_DEAD
};

struct wsy_ctx;

struct wsy_conn {
    struct wsy_ctx *ctx;
    struct ibv_qp *qp;
    struct peer_wsd_qp *dpa_qp;
    struct ibv_mr *ring_mr, *scratch_mr;
    struct peer_sync_events *events;
    uint8_t *ring, *scratch;
    uint8_t *tx_ring;
    struct ibv_mr *tx_ring_mr;
    struct peer_write_tx_ring tx_source;
    uint64_t tx_source_posts;
    struct peer_verbs_endpoint endpoint;
    uint32_t index, epoch, peer_qpn, remote_capacity, remote_key;
    uint64_t remote_base, remote_generation, lease_serial;
    uint64_t remote_tail_event, remote_head_event;
    uint32_t remote_event_key;
    uint64_t tx_tail, tx_head, rx_tail, rx_head, credit_sent;
    uint64_t activated_ns, cq_end, dpa_consumed;
    uint8_t rq_posted[WSY_RQ];
    unsigned state, tx_inflight;
    int used, leased, peer_ready, boot_busy, credit_busy;
};

struct wsy_ctx {
    struct ibv_context *verbs; /* borrowed */
    struct ibv_pd *pd;         /* borrowed from real DOCA device */
    struct peer_sync_engine *sync;
    struct peer_wsd *dpa;
    struct ibv_comp_channel *cc;
    struct ibv_cq *cq;
    struct ibv_mr *arena_mr;
    void *arena;
    size_t arena_len;
    uint32_t capacity, gid_index, epoch;
    uint16_t mtu;
    uint8_t port, gid[16];
    int epfd, continuation, armed, dead, immediate;
    struct peer_wait_diag diag;
    uint64_t diag_last_cq_ns, diag_last_dpa_ns;
    struct dmesh_live_slots active;
    struct wsy_conn conns[WSY_CONNECTIONS];
};

static unsigned wsy_hello_size(const struct wsy_conn *c)
{
    return peer_write_hello_size(c->ctx->immediate, !!c->ctx->dpa);
}

static int wsy_modify_qp(struct wsy_conn *c, struct ibv_qp_attr *a, int mask)
{
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa)
        return peer_wsd_modify(c->dpa_qp, a, mask);
#endif
    return ibv_modify_qp(c->qp, a, mask);
}

static int wsy_submit(struct wsy_conn *c, struct ibv_send_wr *wr)
{
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa)
        return peer_wsd_send(c->dpa_qp, wr);
#endif
    struct ibv_send_wr *bad;
    return ibv_post_send(c->qp, wr, &bad);
}

static void wsy_fail(struct wsy_conn *c, const char *reason)
{
    if (c->state != WSY_DEAD)
        fprintf(stderr,
                "%s fault reason=%s lane=%u epoch=%u qpn=%u\n",
                c->ctx->immediate ? "PEER_WRITE_IMM_DPA" : "PEER_WRITE_SYNC",
                reason,
                c->index,
                c->epoch,
                c->endpoint.qpn);

    c->state = WSY_DEAD;
}

static uint64_t wsy_now(void)
{
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t))
        abort();
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static void wsy_put(uint8_t *p, uint64_t v, unsigned n)
{
    for (unsigned i = 0; i < n; i++)
        p[i] = (uint8_t)(v >> (8u * (n - i - 1u)));
}

static uint64_t wsy_get(const uint8_t *p, unsigned n)
{
    uint64_t v = 0;
    for (unsigned i = 0; i < n; i++)
        v = (v << 8) | p[i];
    return v;
}

static int wsy_capacity(uint32_t c)
{
    long page = sysconf(_SC_PAGESIZE);
    return page > 0 && c >= PEER_WIRE_FRAME_BYTES && c <= PEER_WRITE_SYNC_RING_MAX &&
           !(c & (c - 1u)) && c % (uint64_t)page == 0;
}

static enum ibv_mtu wsy_mtu(uint16_t n)
{
    switch (n) {
    case 256:
        return IBV_MTU_256;

    case 512:
        return IBV_MTU_512;

    case 1024:
        return IBV_MTU_1024;

    case 2048:
        return IBV_MTU_2048;

    case 4096:
        return IBV_MTU_4096;

    default:
        return 0;
    }
}

static uint64_t wsy_id(const struct wsy_conn *c, unsigned kind, uint32_t low)
{
    return (uint64_t)kind << 62 | (uint64_t)c->epoch << 39 | (uint64_t)c->index << 32 | low;
}

static uint8_t *wsy_tx(struct wsy_conn *c)
{
    return c->tx_ring + c->tx_tail % c->ctx->capacity;
}

static uint8_t *wsy_boot(struct wsy_conn *c)
{
    return c->scratch;
}

static uint64_t wsy_tx_space(const struct wsy_conn *c)
{
    uint64_t remote = c->remote_capacity - (c->tx_tail - c->tx_head);
    uint64_t local = peer_write_tx_ring_space(&c->tx_source, c->ctx->capacity);
    return remote < local ? remote : local;
}

static int wsy_tx_admit(struct wsy_conn *c)
{
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa) {
        int admit = peer_wsd_admit(c->dpa_qp, 1);
        if (admit < 0)
            wsy_fail(c, "descriptor-admission");
        return admit;
    }
#endif
    return c->tx_inflight < WSY_DATA_WR;
}

static int wsy_map_buffer(uint8_t **out, size_t bytes)
{
    int fd = memfd_create("dpumesh-write-ring", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0)
        return -1;
    if (ftruncate(fd, (off_t)bytes) ||
        fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL)) {
        close(fd);
        return -1;
    }

    void *base = mmap(NULL, 2 * bytes, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return -1;
    }

    int ok =
        mmap(base, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) != MAP_FAILED &&
        mmap((uint8_t *)base + bytes,
             bytes,
             PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_FIXED,
             fd,
             0) != MAP_FAILED;
    close(fd);

    if (!ok) {
        munmap(base, 2 * bytes);
        return -1;
    }

    *out = base;

    return 0;
}

static int wsy_post_recv(struct wsy_conn *c, unsigned slot)
{
    /* Exactly one protected SEND bootstrap. No RQ is used after setup. */
    struct ibv_sge s = {.addr = (uintptr_t)(wsy_boot(c) + (slot + 1u) * wsy_hello_size(c)),
                        .length = wsy_hello_size(c),
                        .lkey = c->scratch_mr->lkey};
    struct ibv_recv_wr wr = {.wr_id = wsy_id(c, WSY_RECV, slot), .sg_list = &s, .num_sge = 1};
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa) {
        if (peer_wsd_recv(c->dpa_qp, &wr))
            return -1;
    } else
#endif
    {
        struct ibv_recv_wr *bad;
        if (ibv_post_recv(c->qp, &wr, &bad))
            return -1;
    }
    peer_wait_diag_add(&c->ctx->diag, PWD_rq_posts, 1);
    c->rq_posted[slot] = 1;

    return 0;
}

static int wsy_hello_encode(struct wsy_conn *c)
{
    uint8_t *p = wsy_boot(c);
    memset(p, 0, wsy_hello_size(c));
    memcpy(p, "DMWS", 4);
    wsy_put(p + 4, 4, 2); /* dmesh4/1; reject legacy DATA codecs before use. */
    wsy_put(p + 6, 1, 2); /* v1, native little-endian atomic backing, 64-bit counters */
    wsy_put(p + 8, c->ctx->capacity, 4);
    wsy_put(p + 12, c->ring_mr->rkey, 4);
    wsy_put(p + 16, (uintptr_t)c->ring, 8);
    wsy_put(p + 24, c->epoch, 8);
    wsy_put(p + 32, c->endpoint.qpn, 4);
    wsy_put(p + 36, PEER_WIRE_FRAME_BYTES, 4);
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa) {
        wsy_put(p + 6, c->ctx->immediate ? 3 : 2, 2);
        return peer_wsd_event_export(c->dpa_qp, p + 64, wsy_hello_size(c) - 64);
    }
#endif
    wsy_put(p + 40, peer_sync_events_address(c->events, 0), 8);
    wsy_put(p + 48, peer_sync_events_address(c->events, 1), 8);
    wsy_put(p + 56, peer_sync_events_key(c->events), 4);

    return 0;
}

static int wsy_hello_decode(struct wsy_conn *c, const uint8_t *p)
{
    uint32_t capacity = (uint32_t)wsy_get(p + 8, 4);
    uint64_t base = wsy_get(p + 16, 8), generation = wsy_get(p + 24, 8);
#if DMESH_NATIVE_PACKED_TX
    if (capacity != c->ctx->capacity)
        return -1; /* identical TX/RX offsets */
#endif
    if (c->peer_ready || memcmp(p, "DMWS", 4) ||
        wsy_get(p + 4, 2) != 4u ||
        wsy_get(p + 6, 2) != (c->ctx->immediate ? 3u : (c->ctx->dpa ? 2u : 1u)) ||
        !wsy_capacity(capacity) || !base || base > UINT64_MAX - 2ull * capacity || !generation ||
        wsy_get(p + 32, 4) != c->peer_qpn || wsy_get(p + 36, 4) != PEER_WIRE_FRAME_BYTES)
        return -1;
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa) {
        for (unsigned i = 40; i < 64; i++)
            if (p[i])
                return -1;

        if (peer_wsd_configure_native(c->dpa_qp,
                                      base,
                                      (uint32_t)wsy_get(p + 12, 4),
                                      capacity,
                                      p + 64,
                                      wsy_hello_size(c) - 64))
            return -1;

        c->remote_capacity = capacity;
        c->remote_base = base;
        c->remote_key = (uint32_t)wsy_get(p + 12, 4);
        c->remote_generation = generation;
        c->peer_ready = 1;
        return 0;
    }
#endif
    uint64_t tail_event = wsy_get(p + 40, 8), head_event = wsy_get(p + 48, 8);
    if (!tail_event || !head_event || (tail_event & 7) || (head_event & 7) ||
        tail_event > UINT64_MAX - 8 || head_event > UINT64_MAX - 8 || tail_event == head_event)
        return -1;

    for (unsigned i = 60; i < wsy_hello_size(c); i++)
        if (p[i])
            return -1;

    c->remote_tail_event = tail_event;
    c->remote_head_event = head_event;
    c->remote_event_key = (uint32_t)wsy_get(p + 56, 4);
    c->remote_capacity = capacity;
    c->remote_key = (uint32_t)wsy_get(p + 12, 4);
    c->remote_base = base;
    c->remote_generation = generation;
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa &&
        peer_wsd_configure(
            c->dpa_qp, base, c->remote_key, capacity, tail_event, head_event, c->remote_event_key))
        return -1;
#endif
    c->peer_ready = 1;

    return 0;
}

/* Validate framing here only to recover byte-ring record boundaries. Pair,
 * stream, sequence and authorization validation remains in peer_pair_wire. */
static size_t wsy_frame_size(const uint8_t *p)
{
    return peer_data_frame_size(p);
}

static int wsy_ready(struct wsy_conn *c)
{
    if (c->ctx->dead || c->state == WSY_DEAD)
        return -1;
    if (c->state == WSY_READY)
        return 1;
    if (c->state == WSY_CONNECTING && wsy_now() - c->activated_ns >= 5000000000ull) {
#if DMESH_DPA_RDMA_OFFLOAD
        if (c->ctx->dpa)
            peer_wsd_debug(c->dpa_qp);
#endif
        wsy_fail(c, "bootstrap-timeout");
        return -1;
    }

    return 0;
}

static int wsy_credit(struct wsy_conn *c, int force)
{
    if (c->state != WSY_READY || c->credit_busy || c->credit_sent == c->rx_head)
        return 0;
    if (!force && c->rx_head - c->credit_sent < peer_write_credit_batch_bytes(c->ctx->capacity))
        return 0;

#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa) {
        /* Encoding may hold a frame lease across progress calls. Leave its
         * descriptor available so a credit cannot steal a promised commit. */
        int admit = peer_wsd_admit(c->dpa_qp, 1u + !!c->leased);
        if (admit < 0)
            wsy_fail(c, "credit-admission");
        if (admit <= 0)
            return admit;
    }
#endif

    struct ibv_sge result = {.addr = (uintptr_t)(c->scratch + PEER_WRITE_ARM_SYNC_RESULTS + 8u),
                             .length = 8,
                             .lkey = c->scratch_mr->lkey};
    struct ibv_send_wr wr = {.wr_id = wsy_id(c, WSY_CREDIT, 0),
                             .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
                             .send_flags = IBV_SEND_SIGNALED,
                             .sg_list = &result,
                             .num_sge = 1,
                             .wr.atomic = {.remote_addr = c->remote_head_event,
                                           .rkey = c->remote_event_key,
                                           .compare_add = c->rx_head - c->credit_sent}};
    int rc;
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa)
        rc = peer_wsd_head(c->dpa_qp, wr.wr_id, c->rx_head - c->credit_sent);
    else
#endif
        rc = wsy_submit(c, &wr);
    if (rc) {
        wsy_fail(c, "credit-post");
        return -1;
    }

    peer_wait_diag_add(&c->ctx->diag, PWD_credit_post, 1);
    c->credit_sent = c->rx_head;
    c->credit_busy = 1;

    return 1;
}

static int wsy_arena(struct wsy_ctx *t, const struct peer_wire_payload *p)
{
    if (t->arena_mr)
        return t->arena == p->region && t->arena_len == p->region_len ? 0 : -1;

    t->arena_mr = ibv_reg_mr(t->pd, p->region, p->region_len, IBV_ACCESS_LOCAL_WRITE);
    if (!t->arena_mr)
        return -1;
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa && peer_wsd_register(t->dpa, p->region, p->region_len, t->arena_mr->lkey))
        return -1;
#endif
    t->arena = p->region;
    t->arena_len = p->region_len;

    return 0;
}

static int wsy_post_data(struct wsy_conn *c, size_t bytes, const struct peer_wire_payload *p)
{
    if (c->tx_source.tail != c->tx_tail)
        goto fail;
    if (p && (bytes < PEER_WIRE_HEADER_BYTES ||
              !peer_wire_frame_payload_valid(p, bytes - PEER_WIRE_HEADER_BYTES)))
        goto fail;

    const uint8_t *source = p ? p->tx_header : wsy_tx(c);
    if (bytes < PEER_WIRE_HEADER_BYTES || bytes > PEER_WIRE_FRAME_BYTES ||
        bytes > wsy_tx_space(c) || UINT64_MAX - c->tx_tail < bytes ||
        wsy_frame_size(source) != bytes || (p && wsy_arena(c->ctx, p)))
        goto fail;

    uint64_t tail = c->tx_tail + bytes;
    struct ibv_sge s = {.addr = (uintptr_t)source,
                        .length = (uint32_t)bytes,
                        .lkey = p ? c->ctx->arena_mr->lkey : c->tx_ring_mr->lkey};
    struct ibv_send_wr wr = {.wr_id = wsy_id(c, WSY_DATA, (uint32_t)tail),
                             .sg_list = &s,
                             .num_sge = 1,
                             .opcode = IBV_WR_RDMA_WRITE};
    struct ibv_sge result = {
        .addr = (uintptr_t)(c->scratch + PEER_WRITE_ARM_SYNC_RESULTS),
        .length = 8, .lkey = c->scratch_mr->lkey};
    struct ibv_send_wr signal = {.wr_id = wsy_id(c, WSY_DATA, (uint32_t)tail),
                                 .opcode = IBV_WR_ATOMIC_FETCH_AND_ADD,
                                 .send_flags = IBV_SEND_SIGNALED,
                                 .sg_list = &result,
                                 .num_sge = 1,
                                 .wr.atomic = {.remote_addr = c->remote_tail_event,
                                               .rkey = c->remote_event_key,
                                               .compare_add = bytes}};

    /* Non-relaxed RC WRITE -> atomic event on the SAME QP. The signalled
     * atomic completion also fences the preceding source WRITE.
     * One writer per event; no publication holes or 64-bit modular wrap. */
    wr.next = &signal;
    wr.wr.rdma.remote_addr = c->remote_base + c->tx_tail % c->remote_capacity;
    wr.wr.rdma.rkey = c->remote_key;

    if (p &&
        peer_write_tx_ring_hold(&c->tx_source, tail, p->owner, p->cookie, p->retain, p->release))
        goto fail;

    /* A linked post can accept WRITE and reject its signal. Keep every
     * borrowed source until a successful QP destroy even on partial failure;
     * do not publish a later tail across the resulting hole. */
    int rc;
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa)
        rc = peer_wsd_data(c->dpa_qp, tail, s.addr, s.lkey, s.length);
    else
#endif
        rc = wsy_submit(c, &wr);
    if (rc)
        goto fail;
    if (peer_write_tx_ring_post(&c->tx_source, (uint32_t)bytes, c->ctx->capacity))
        goto fail;

    c->tx_source_posts++;
    if (!c->ctx->dpa)
        c->tx_inflight++;
    peer_wait_diag_add(&c->ctx->diag, PWD_tx_data_post, 1);
    c->tx_tail = tail;
    return 1;

fail:
    wsy_fail(c, "data-validation-or-post");

    return -1;
}

static int wsy_send(void *opaque, const void *bytes, size_t len)
{
    struct wsy_conn *c = opaque;
    int ready = wsy_ready(c);
    if (ready <= 0)
        return ready;
    if (!bytes || len < PEER_WIRE_HEADER_BYTES || len > PEER_WIRE_FRAME_BYTES)
        return -1;
    if (c->leased || len > wsy_tx_space(c)) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_ring, !c->leased);
        return 0;
    }

    int admit = wsy_tx_admit(c);
    if (admit <= 0) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_slot, admit == 0);
        return admit;
    }

    memcpy(wsy_tx(c), bytes, len);
    return wsy_post_data(c, len, NULL);
}

static int wsy_reserve(void *opaque, struct peer_wire_frame_lease *out)
{
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    struct wsy_conn *c = opaque;
    int ready = wsy_ready(c);
    if (ready <= 0)
        return ready;

    uint64_t space = wsy_tx_space(c);

    /* frame_reserve is called BEFORE the stateful encoder knows its record
     * size. Offer the usual full body capacity or would-block; a short lease
     * could turn ordinary byte-credit pressure into a fatal codec failure. */
    if (c->leased || space < PEER_WIRE_FRAME_BYTES) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_ring, !c->leased);
        return 0;
    }
    if (c->lease_serial == UINT64_MAX) {
        c->state = WSY_DEAD;
        return -1;
    }

    int admit = wsy_tx_admit(c);
    if (admit <= 0) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_slot, admit == 0);
        return admit;
    }

    c->leased = 1;
    c->lease_serial++;
    *out = (struct peer_wire_frame_lease){.header = wsy_tx(c),
                                          .body = wsy_tx(c) + PEER_WIRE_HEADER_BYTES,
                                          .body_cap = PEER_WIRE_BODY_BYTES,
                                          .token = {.conn_epoch = c->epoch,
                                                    .lease_generation = c->lease_serial,
                                                    .slot = 0,
                                                    .kind = PEER_WIRE_FRAME_TX}};

    return 1;
}

static int wsy_lease_valid(struct wsy_conn *c, const struct peer_wire_frame_lease *l)
{
    return c->leased && l->token.slot == 0 && l->token.conn_epoch == c->epoch &&
           l->token.kind == PEER_WIRE_FRAME_TX && l->token.lease_generation == c->lease_serial &&
           l->header == wsy_tx(c) && l->body == wsy_tx(c) + PEER_WIRE_HEADER_BYTES;
}

static int wsy_cancel(void *opaque, struct peer_wire_frame_lease *l)
{
    struct wsy_conn *c = opaque;
    if (!l || !wsy_lease_valid(c, l))
        return -1;

    c->leased = 0;
    memset(l, 0, sizeof(*l));

    return 0;
}

static int wsy_commit(void *opaque,
                      struct peer_wire_frame_lease *l,
                      size_t body,
                      const struct peer_wire_payload *p)
{
    struct wsy_conn *c = opaque;
    if (!l || !wsy_lease_valid(c, l))
        return -1;

    int valid = body <= l->body_cap && body <= PEER_WIRE_BODY_BYTES &&
                (!p || peer_wire_frame_payload_valid(p, body));
    c->leased = 0;
    memset(l, 0, sizeof(*l));

    if (!valid || wsy_ready(c) != 1) {
        wsy_fail(c, "frame-commit");
        return -1;
    }

    return wsy_post_data(c, body + PEER_WIRE_HEADER_BYTES, p);
}

static long wsy_recv(void *opaque, void *out, size_t cap)
{
    struct wsy_conn *c = opaque;
    int ready = wsy_ready(c);
    if (ready <= 0)
        return ready;

    uint64_t available = c->rx_tail - c->rx_head;
    if (!available) {
        peer_wait_diag_add(&c->ctx->diag, PWD_rx_empty, 1);
        return 0;
    }
    if (available < PEER_WIRE_HEADER_BYTES)
        goto fail;

    const uint8_t *frame = c->ring + c->rx_head % c->ctx->capacity;
    size_t bytes = wsy_frame_size(frame);
    if (!bytes || bytes > available || !out || bytes > cap)
        goto fail;

    /* Draft RX adapter: COPY before returning ring byte credit. Downstream
     * receives its existing channel/arena storage, never a borrowed ring view.
     * Adding zero-copy requires a ring lease + Host-DMA mmap source adapter. */
    memcpy(out, frame, bytes);
    c->rx_head += bytes;

    if (wsy_credit(c, 0) < 0)
        return -1;

    return (long)bytes;

fail:
    wsy_fail(c, "rx-framing");

    return -1;
}

static void wsy_completion(struct wsy_ctx *t, const struct ibv_wc *wc)
{
    unsigned index = (unsigned)(wc->wr_id >> 32) & 0x7fu;
    /* DATA carries a low32 byte end; RECV carries an RQ index. */
    uint32_t low = (uint32_t)wc->wr_id;
    uint32_t epoch = (uint32_t)(wc->wr_id >> 39) & WSY_EPOCH_MAX;
    unsigned kind = (unsigned)(wc->wr_id >> 62);
    if (index >= WSY_CONNECTIONS)
        return;

    struct wsy_conn *c = &t->conns[index];
    if (!c->used || c->epoch != epoch) {
        peer_wait_diag_add(&t->diag, PWD_cq_stale, 1);
        return; /* retired generation CQE */
    }
    if (wc->status != IBV_WC_SUCCESS || wc->qp_num != c->endpoint.qpn)
        goto fail;
    if (c->state == WSY_DEAD)
        return; /* failed WRs keep source holds until destroy */

    peer_wait_diag_add(&t->diag,
                       kind == WSY_DATA     ? PWD_cq_data_done
                       : kind == WSY_CREDIT ? PWD_cq_credit_done
                       : kind == WSY_BOOT   ? PWD_cq_boot_done
                                            : (wc->opcode == IBV_WC_RECV ? PWD_cq_rx_boot
                                               : !wc->byte_len           ? PWD_cq_rx_credit
                                                                         : PWD_cq_rx_data),
                       1);

    if (kind == WSY_DATA) {
        uint64_t end;
        if (t->dpa || !c->tx_inflight || wc->opcode != IBV_WC_FETCH_ADD ||
            peer_write_imm_extend(c->cq_end, low, c->tx_tail - c->cq_end, &end) != 1)
            goto fail;
        c->cq_end = end;
        c->tx_inflight--;
    } else if (kind == WSY_CREDIT) {
        if (low || !c->credit_busy || wc->opcode != (t->dpa ? IBV_WC_RDMA_WRITE : IBV_WC_FETCH_ADD))
            goto fail;

        c->credit_busy = 0;
    } else if (kind == WSY_BOOT) {
        if (low || !c->boot_busy || wc->opcode != IBV_WC_SEND)
            goto fail;

        c->boot_busy = 0;
    } else {
        if (low >= WSY_RQ || !c->rq_posted[low] || wc->opcode != IBV_WC_RECV ||
            c->state != WSY_CONNECTING || wc->byte_len != wsy_hello_size(c) ||
            (wc->wc_flags & IBV_WC_WITH_IMM) ||
            wsy_hello_decode(c, wsy_boot(c) + (low + 1u) * wsy_hello_size(c)))
            goto fail;

        c->rq_posted[low] = 0;
    }

    /* The local HELLO SEND must finish before ARM Sync may reuse its TX slot
     * for atomic results. Peer readiness alone is insufficient. */
    if (c->state == WSY_CONNECTING && c->peer_ready && !c->boot_busy)
        c->state = WSY_READY;
    return;

fail:
    peer_wait_diag_add(&t->diag, PWD_cq_error, 1);
    peer_wait_diag_note(
        &t->diag, "cq_error", 1, wc->wr_id, wc->status, wc->qp_num, wc->vendor_err, 0, 0);

    /* On error only wr_id/status/qp_num/vendor_err are defined by verbs. */
    if (c->state != WSY_DEAD)
        fprintf(
            stderr,
            "PEER_WRITE_SYNC CQ fault lane=%u epoch=%u kind=%u low=%u status=%u vendor=%u state=%u opcode=%u bytes=%u hello_bytes=%u peer_ready=%d boot_busy=%d rq_posted=%u\n",
            c->index,
            c->epoch,
            kind,
            low,
            (unsigned)wc->status,
            wc->vendor_err,
            c->state,
            wc->status == IBV_WC_SUCCESS ? (unsigned)wc->opcode : 0,
            wc->status == IBV_WC_SUCCESS ? wc->byte_len : 0,
            wsy_hello_size(c),
            c->peer_ready,
            c->boot_busy,
            low < WSY_RQ ? c->rq_posted[low] : 0);

    c->state = WSY_DEAD;
}

static void wsy_continue(struct wsy_ctx *t)
{
    peer_wait_diag_add(&t->diag, PWD_continuations, 1);

    uint64_t one = 1;
    ssize_t n;

    do {
        n = write(t->continuation, &one, sizeof(one));
    } while (n < 0 && errno == EINTR);

    if (n < 0 && errno != EAGAIN)
        t->dead = 1;
}

static void wsy_drain_events(struct wsy_ctx *t)
{
    for (;;) {
        struct ibv_cq *cq;
        void *cookie;
        if (ibv_get_cq_event(t->cc, &cq, &cookie)) {
            if (errno == EINTR)
                continue;

            if (errno != EAGAIN && errno != EWOULDBLOCK)
                t->dead = 1;
            break;
        }

        peer_wait_diag_add(&t->diag, PWD_channel_events, 1);
        peer_wait_diag_note(&t->diag, "cq_event", 0, t->armed, cq == t->cq, cookie == t, 0, 0, 0);
        ibv_ack_cq_events(cq, 1);

        if (cq != t->cq || cookie != t)
            t->dead = 1;
        t->armed = 0;
    }
}

static int wsy_poll(struct wsy_ctx *t)
{
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa && t->diag.enabled && wsy_now() - t->diag_last_dpa_ns >= 1000000000ull) {
        t->diag_last_dpa_ns = wsy_now();
        for (unsigned k = 0; k < t->active.count; k++) {
            struct wsy_conn *c = &t->conns[t->active.ids[k]];
            fprintf(
                stderr,
                "WRITE_SYNC_DPA_FRONT lane=%u state=%u tx=%llu/%llu rx=%llu/%llu ready=%d boot=%d\n",
                c->index,
                c->state,
                (unsigned long long)c->tx_head,
                (unsigned long long)c->tx_tail,
                (unsigned long long)c->rx_head,
                (unsigned long long)c->rx_tail,
                c->peer_ready,
                c->boot_busy);
            peer_wsd_debug(c->dpa_qp);
        }
    }
#endif
    int sync_progress = t->dpa ? 0 : peer_sync_engine_progress(t->sync);
    if (sync_progress < 0)
        t->dead = 1;

    int moved = sync_progress > 0;
    unsigned consumed = 0;

    while (!t->dead && consumed < WSY_BUDGET) {
        struct ibv_wc wc[PEER_RDMA_CQ_BATCH];
        unsigned remaining = WSY_BUDGET - consumed;
        int want = remaining < PEER_RDMA_CQ_BATCH ? (int)remaining : PEER_RDMA_CQ_BATCH;
        uint64_t stamp = peer_wait_diag_begin(&t->diag);
        int n;
#if DMESH_DPA_RDMA_OFFLOAD
        if (t->dpa)
            n = peer_wsd_poll(t->dpa, want, wc);
        else
#endif
            n = ibv_poll_cq(t->cq, want, wc);
        peer_wait_diag_end(&t->diag, PWD_CQ_POLL, stamp);
        peer_wait_diag_add(&t->diag, PWD_cq_poll_calls, 1);
        peer_wait_diag_add(&t->diag, PWD_cq_poll_empty, n == 0);

        if (n > 0 && t->diag.enabled) {
            uint64_t now = PEER_WAIT_DIAG_NOW();
            uint64_t gap = t->diag_last_cq_ns ? now - t->diag_last_cq_ns : 0;
            t->diag_last_cq_ns = now;
            peer_wait_diag_add(&t->diag, PWD_cq_completions, (unsigned)n);
            peer_wait_diag_note(&t->diag,
                                "cq_batch",
                                gap >= 100000,
                                (unsigned)n,
                                gap,
                                t->armed,
                                wc[0].wr_id,
                                wc[0].status,
                                0);
        }
        if (n < 0) {
            t->dead = 1;
            break;
        }
        if (!n)
            break;

        moved = 1;
        consumed += (unsigned)n;

        for (int i = 0; i < n; i++)
            wsy_completion(t, &wc[i]);
    }

    if (!t->dpa && consumed == WSY_BUDGET)
        wsy_continue(t); /* unread CQEs need another visit */

    for (unsigned k = 0; !t->dead && k < t->active.count; k++) {
        struct wsy_conn *c = &t->conns[t->active.ids[k]];
        if (!c->used)
            continue;

        uint64_t tail, head, completed = c->cq_end, consumed_requests = c->dpa_consumed;
        int counter_rc;
#if DMESH_DPA_RDMA_OFFLOAD
        if (t->dpa) {
            if (!c->dpa_qp || c->state == WSY_DEAD)
                continue;

            /* Completion polling visits only pending control operations.
             * This existing counter pass also catches faults on idle QPs
             * and during bootstrap, even with no control completion pending. */
            counter_rc = peer_wsd_counters(c->dpa_qp, &tail, &head, &completed, &consumed_requests);
            if (counter_rc) {
                wsy_fail(c, "dpa-status");
                moved = 1;
                continue;
            }
            if (wsy_ready(c) != 1)
                continue;
        } else
#endif
        {
            if (wsy_ready(c) != 1)
                continue;
            counter_rc = peer_sync_events_get(c->events, &tail, &head);
        }
        if (counter_rc || consumed_requests < c->dpa_consumed || tail < c->rx_tail ||
            tail < c->rx_head || tail - c->rx_head > t->capacity || head < c->tx_head ||
            head > c->tx_tail) {
            wsy_fail(c, "event-counter-range");
            moved = 1;
            continue;
        }

        /* Descriptor consumption can unblock an already encoded frame before
         * any local DATA CQE. Surface that progress to the final idle rescan. */
        if (consumed_requests != c->dpa_consumed) {
            c->dpa_consumed = consumed_requests;
            moved = 1;
        }

        if (completed != c->tx_source.completed) {
            if (peer_write_tx_ring_done(&c->tx_source, completed)) {
                wsy_fail(c, "local-completion-range");
                moved = 1;
                continue;
            }
            moved = 1;
        }

        peer_wait_diag_add(&t->diag, PWD_tail_changes, tail != c->rx_tail);
        peer_wait_diag_add(&t->diag, PWD_head_changes, head != c->tx_head);

        if (tail != c->rx_tail || head != c->tx_head)
            moved = 1;

        /* Coherent ARM system memory; event observation precedes payload
         * loads. This CPU barrier complements, rather than substitutes for,
         * ordered non-relaxed WRITE -> event publication on the RC QP. */
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        c->rx_tail = tail;
        c->tx_head = head;

        int rc = wsy_credit(c, 0);
        if (rc)
            moved = 1;
    }

    return moved || t->dead;
}

static int wsy_progress(void *opaque, void **accepted, int max, int *naccepted)
{
    struct wsy_ctx *t = opaque;
    (void)accepted;
    (void)max;
    *naccepted = 0;
    peer_wait_diag_add(&t->diag, PWD_progress_calls, 1);

    if (t->dead)
        return 0;
    if (t->dpa)
        return wsy_poll(t); /* MsgQ wake is owned by existing worker PE */

    /* Active work uses userspace CQ/counter polling. Drain old channel hints
     * and arm interrupts ONLY after that work is exhausted. A second full
     * poll after arming prevents sleeping past an arrival at this boundary. */
    int moved = wsy_poll(t);
    if (moved || t->dead) {
        peer_wait_diag_add(&t->diag, PWD_progress_moved, 1);
        return 1;
    }

    peer_wait_diag_add(&t->diag, PWD_idle_boundary, 1);

    /* One shared readiness check replaces empty reads of every event FD.
     * CQ contents are still polled below even without an interrupt hint. */
    struct epoll_event events[3];
    int ready = epoll_wait(t->epfd, events, 3, 0);
    peer_wait_diag_add(&t->diag, PWD_epoll_ready, ready > 0);
    peer_wait_diag_add(&t->diag, PWD_epoll_empty, ready == 0);

    unsigned hints = 0;
    if (ready < 0 && errno != EINTR)
        t->dead = 1;

    for (int i = 0; i < ready; i++) {
        if (events[i].events & (EPOLLERR | EPOLLHUP))
            t->dead = 1;

        int fd = events[i].data.fd;
        hints |= fd == t->cc->fd ? 1u : fd == t->continuation ? 2u : 4u;

        if (fd == t->continuation) {
            uint64_t wake;
            ssize_t nread;

            do {
                nread = read(fd, &wake, sizeof(wake));
            } while (nread < 0 && errno == EINTR);

            if (nread < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
                t->dead = 1;
        } else if (fd == t->cc->fd)
            wsy_drain_events(t);
        else if (fd == peer_sync_engine_fd(t->sync))
            peer_sync_engine_notification(t->sync);
    }

    if (!t->dead && !t->armed) {
        uint64_t stamp = peer_wait_diag_begin(&t->diag);
        peer_wait_diag_add(&t->diag, PWD_cq_arms, 1);

        if (ibv_req_notify_cq(t->cq, 0))
            t->dead = 1;
        else
            t->armed = 1;

        peer_wait_diag_end(&t->diag, PWD_CQ_ARM, stamp);
    } else
        peer_wait_diag_add(&t->diag, PWD_cq_arm_skip, 1);

    if (!t->dead && peer_sync_engine_arm(t->sync))
        t->dead = 1;
    moved = wsy_poll(t) || t->dead;
    peer_wait_diag_add(&t->diag, moved ? PWD_progress_moved : PWD_idle_return, 1);
    peer_wait_diag_note(&t->diag, "idle_scan", 0, hints, t->armed, moved, t->active.count, 0, 0);

    if (!moved && t->diag.enabled) {
        uint64_t posted = 0, reserved = 0, rx_bytes = 0, tx_bytes = 0, rq = 0, credit = 0;
        for (unsigned k = 0; k < t->active.count; k++) {
            struct wsy_conn *c = &t->conns[t->active.ids[k]];
            posted += c->tx_tail - c->tx_source.completed;
            reserved += !!c->leased;

            for (unsigned j = 0; j < WSY_RQ; j++)
                rq += c->rq_posted[j];

            rx_bytes += c->rx_tail - c->rx_head;
            tx_bytes += c->tx_tail - c->tx_head;
            credit += !!c->credit_busy;
        }

        peer_wait_diag_note(
            &t->diag, "idle_state", 0, posted, reserved, rx_bytes, tx_bytes, rq, credit);
    }

    return moved;
}

int peer_write_sync_close(void *opaque)
{
    struct wsy_conn *c = opaque;
    if (!c || !c->used)
        return -1;

    c->state = WSY_DEAD;

    /* Each new ring has a fresh QP/MR. Destroy fences local DMA and invalidates
     * the remote QP path before rkey/backing reuse. Failed fences quarantine
     * ALL dependent storage; a timeout never authorizes reuse. */
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->dpa_qp) {
        if (peer_wsd_destroy(c->dpa_qp))
            return -1;

        c->dpa_qp = NULL;
    }
#endif
    if (c->qp) {
        if (ibv_destroy_qp(c->qp))
            return -1;

        c->qp = NULL;
    }

    if (c->events) {
        if (peer_sync_events_free(c->events))
            return -1;

        c->events = NULL;
    }

    peer_write_tx_ring_fenced(&c->tx_source);

    if (c->ring_mr) {
        if (ibv_dereg_mr(c->ring_mr))
            return -1;

        c->ring_mr = NULL;
    }
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa && c->scratch_mr && peer_wsd_unregister(c->ctx->dpa, c->scratch_mr->lkey))
        return -1;
#endif
    if (c->scratch_mr) {
        if (ibv_dereg_mr(c->scratch_mr))
            return -1;

        c->scratch_mr = NULL;
    }
#if DMESH_DPA_RDMA_OFFLOAD
    if (c->ctx->dpa && c->tx_ring_mr && peer_wsd_unregister(c->ctx->dpa, c->tx_ring_mr->lkey))
        return -1;
#endif
    if (c->tx_ring_mr) {
        if (ibv_dereg_mr(c->tx_ring_mr))
            return -1;

        c->tx_ring_mr = NULL;
    }
    if (c->tx_ring) {
        munmap(c->tx_ring, 2ull * c->ctx->capacity);
        c->tx_ring = NULL;
#if DMESH_NATIVE_PACKED_TX
        printf(
            "WRITE_SYNC_TX_RING_RECEIPT bytes=%u virtual_bytes=%llu same_offset=1 data_posts=%llu posted_bytes=%llu completed_bytes=%llu outstanding=%u qp_fenced=1 mappings_destroyed=1\n",
            c->ctx->capacity,
            2ull * c->ctx->capacity,
            (unsigned long long)c->tx_source_posts,
            (unsigned long long)c->tx_source.tail,
            (unsigned long long)c->tx_source.completed,
            (unsigned)(c->tx_source.completed != c->tx_source.tail));
#endif
    }
    if (c->ring)
        munmap(c->ring, 2ull * c->ctx->capacity);
    free(c->scratch);
    dmesh_live_remove(&c->ctx->active, c->index);
    memset(c, 0, sizeof(*c));

    return 0;
}

static void wsy_close(void *opaque)
{
    (void)peer_write_sync_close(opaque);
}

static int wsy_established(void *opaque)
{
    return wsy_ready(opaque);
}

static int wsy_faulted(void *opaque)
{
    return wsy_ready(opaque) < 0;
}

static int wsy_epfd(void *opaque)
{
    return ((struct wsy_ctx *)opaque)->epfd;
}

static void wsy_ctx_free(void *opaque)
{
    struct wsy_ctx *t = opaque;
    if (!t)
        return;

    for (unsigned i = 0; i < WSY_CONNECTIONS; i++)
        if (t->conns[i].used && peer_write_sync_close(&t->conns[i]))
            return;

    /* Retrieved channel events must be ACKed before CQ destruction. Drain
     * even after a context fault; no QP/device access remains at this point. */
    if (t->cc && t->cq)
        wsy_drain_events(t);
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa && t->arena_mr && peer_wsd_unregister(t->dpa, t->arena_mr->lkey))
        return;
#endif
    if (t->arena_mr) {
        if (ibv_dereg_mr(t->arena_mr))
            return;

        t->arena_mr = NULL;
    }

    if (t->cq) {
        if (ibv_destroy_cq(t->cq))
            return;

        t->cq = NULL;
    }

    if (t->cc) {
        if (ibv_destroy_comp_channel(t->cc))
            return;

        t->cc = NULL;
    }
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa) {
        if (peer_wsd_free(t->dpa))
            return;

        t->dpa = NULL;
    }
#endif
    if (t->sync) {
        if (peer_sync_engine_free(t->sync))
            return;

        t->sync = NULL;
        t->pd = NULL;
    }

    if (t->continuation >= 0)
        close(t->continuation);
    if (t->epfd >= 0)
        close(t->epfd);
    peer_wait_diag_dump(&t->diag, stderr);
    peer_wait_diag_free(&t->diag);
    free(t);
}
static const struct peer_wire_ops WSY_OPS = {.progress = wsy_progress,
                                             .send_msg = wsy_send,
                                             .recv_msg = wsy_recv,
                                             .established = wsy_established,
                                             .faulted = wsy_faulted,
                                             .close = wsy_close,
                                             .epfd = wsy_epfd,
                                             .ctx_free = wsy_ctx_free,
                                             .frame_reserve = wsy_reserve,
                                             .frame_commit = wsy_commit,
                                             .frame_cancel = wsy_cancel};

static int wsy_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int wsy_epoll(int epfd, int fd)
{
    struct epoll_event ev = {.events = EPOLLIN, .data = {.fd = fd}};
    return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

static int wsy_new(struct ibv_context *verbs,
                   uint8_t port,
                   uint32_t gid,
                   uint16_t mtu,
                   uint32_t capacity,
                   const struct peer_dpa_runtime *runtime,
                   unsigned eu,
                   int immediate,
                   const struct peer_wire_ops **ops,
                   void **out,
                   char *error,
                   size_t error_len)
{
    if (!ops || !out)
        return -1;

    *ops = NULL;
    *out = NULL;

    if (error && error_len)
        error[0] = 0;

    struct ibv_port_attr attr;
    struct ibv_gid_entry entry;
    static const uint8_t prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (!verbs || !port || gid > 255 || !wsy_mtu(mtu) || !wsy_capacity(capacity) ||
        ibv_query_port(verbs, port, &attr) || attr.state != IBV_PORT_ACTIVE ||
        attr.link_layer != IBV_LINK_LAYER_ETHERNET || attr.active_mtu < wsy_mtu(mtu) ||
        ibv_query_gid_ex(verbs, port, gid, &entry, 0) || entry.gid_type != IBV_GID_TYPE_ROCE_V2 ||
        memcmp(entry.gid.raw, prefix, sizeof(prefix))) {
        if (error && error_len)
            snprintf(
                error,
                error_len,
                "WRITE_SYNC needs active IPv4 RoCEv2, valid MTU and page-aligned power-of-two ring (64KiB..16MiB)");
        return -1;
    }

    struct wsy_ctx *t = calloc(1, sizeof(*t));
    if (!t) {
        if (error && error_len)
            snprintf(error, error_len, "WRITE_SYNC context allocation: %s", strerror(errno));
        return -1;
    }

    peer_wait_diag_init(&t->diag, immediate ? "peer_wire_write_imm_dpa" : "peer_wire_write_sync");
    t->immediate = immediate;
    t->verbs = verbs;
    t->port = port;
    t->gid_index = gid;
    t->mtu = mtu;
    t->capacity = capacity;
    t->epfd = t->continuation = -1;
    memcpy(t->gid, entry.gid.raw, 16);

    const uint16_t endian = 1;
    if (*(const uint8_t *)&endian != 1)
        goto fail;
#if DMESH_DPA_RDMA_OFFLOAD
    if (runtime) {
        if (peer_sync_engine_new_dpa(ibv_get_device_name(verbs->device), runtime->dpa, &t->sync))
            goto fail;
    } else
#else
    (void)runtime;
    (void)eu;
#endif
        if (peer_sync_engine_new(ibv_get_device_name(verbs->device), &t->sync))
        goto fail;

    t->pd = peer_sync_engine_pd(t->sync);

    /* All registrations, QPs, CQs and channels must use this SAME context.
     * The caller's verbs handle identifies the device and remains borrowed. */
    t->verbs = t->pd->context;
#if DMESH_DPA_RDMA_OFFLOAD
    if (runtime) {
        int rc = immediate ? peer_wsd_new_imm(t->sync, runtime, eu, &t->dpa)
                           : peer_wsd_new(t->sync, runtime, eu, &t->dpa);
        if (rc)
            goto fail;

        *ops = &WSY_OPS;
        *out = t;
        return 0;
    }
#endif
    t->cc = ibv_create_comp_channel(t->verbs);
    if (!t->cc || wsy_nonblock(t->cc->fd))
        goto fail;

    t->cq = ibv_create_cq(t->verbs, WSY_CQ_DEPTH, t, t->cc, 0);
    if (!t->cq || ibv_req_notify_cq(t->cq, 0))
        goto fail;

    t->armed = 1;
    t->epfd = epoll_create1(EPOLL_CLOEXEC);
    t->continuation = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (t->epfd < 0 || t->continuation < 0 || wsy_epoll(t->epfd, t->cc->fd) ||
        wsy_epoll(t->epfd, t->continuation) || wsy_epoll(t->epfd, peer_sync_engine_fd(t->sync)))
        goto fail;

    *ops = &WSY_OPS;
    *out = t;

    if (error && error_len)
        error[0] = 0;
    return 0;

fail:
    if (error && error_len)
        snprintf(error,
                 error_len,
                 "WRITE_SYNC resource/capability setup failed (see stage diagnostics); errno=%d",
                 errno);
    wsy_ctx_free(t);

    return -1;
}

int peer_write_sync_prepare(void *opaque, void **out, struct peer_verbs_endpoint *ep)
{
    if (!out || !ep)
        return -1;

    *out = NULL;
    memset(ep, 0, sizeof(*ep));

    struct wsy_ctx *t = opaque;
    if (!t || t->dead || t->epoch == WSY_EPOCH_MAX)
        return -1;

    struct wsy_conn *c = NULL;
    for (unsigned i = 0; i < WSY_CONNECTIONS; i++)
        if (!t->conns[i].used) {
            c = &t->conns[i];
            break;
        }

    if (!c)
        return -1;

    c->ctx = t;
    c->used = 1;
    c->index = (uint32_t)(c - t->conns);
    c->epoch = ++t->epoch;
    c->state = WSY_INIT;
    dmesh_live_add(&t->active, c->index);

    const char *stage = "alias-mapping";
    if (wsy_map_buffer(&c->ring, t->capacity))
        goto fail;
    stage = "tx-ring-allocation";

    if (wsy_map_buffer(&c->tx_ring, t->capacity))
        goto fail;

    stage = "tx-ring-registration";
    c->tx_ring_mr = ibv_reg_mr(t->pd, c->tx_ring, 2ull * t->capacity, IBV_ACCESS_LOCAL_WRITE);
    if (!c->tx_ring_mr)
        goto fail;
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa && peer_wsd_register(t->dpa, c->tx_ring, 2ull * t->capacity, c->tx_ring_mr->lkey))
        goto fail;
#endif
    stage = "scratch-allocation";
    unsigned scratch_bytes = peer_write_scratch_size(t->immediate, !!t->dpa);
    c->scratch = aligned_alloc(64, scratch_bytes);
    if (!c->scratch)
        goto fail;

    memset(c->scratch, 0, scratch_bytes);
    stage = "alias-remote-write-registration";
    c->ring_mr = ibv_reg_mr(
        t->pd, c->ring, 2ull * t->capacity, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!c->ring_mr)
        goto fail;

    stage = "sync-event-create";
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa) {
        /* Native event creation belongs to the native RDMA connection. */
    } else
#endif
        if (peer_sync_events_new(t->sync, &c->events))
        goto fail;

    stage = "scratch-registration";
    c->scratch_mr = ibv_reg_mr(t->pd, c->scratch, scratch_bytes, IBV_ACCESS_LOCAL_WRITE);
    if (!c->scratch_mr)
        goto fail;

    struct ibv_qp_init_attr attr = {
        .send_cq = t->cq,
        .recv_cq = t->cq,
        .qp_type = IBV_QPT_RC,
        .cap = {
            .max_send_wr = WSY_SQ, .max_recv_wr = WSY_RQ, .max_send_sge = 1, .max_recv_sge = 1}};
    stage = "QP-create";
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa) {
        if (peer_wsd_register(t->dpa, c->scratch, scratch_bytes, c->scratch_mr->lkey) ||
            peer_wsd_qp_new(t->dpa,
                            c->index,
                            c->epoch,
                            t->capacity,
                            t->gid_index,
                            t->mtu,
                            &c->endpoint,
                            &c->dpa_qp))
            goto fail;

        c->endpoint.mtu = t->mtu;
        memcpy(c->endpoint.gid, t->gid, 16);

        if (wsy_post_recv(c, 0))
            goto fail;

        *ep = c->endpoint;
        *out = c;
        return 0;
    } else
#endif
    {
        c->qp = ibv_create_qp(t->pd, &attr);
        if (!c->qp)
            goto fail;
    }

    struct ibv_qp_attr init = {.qp_state = IBV_QPS_INIT,
                               .port_num = t->port,
                               .qp_access_flags =
                                   IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC};
    stage = "QP-init";

    if (wsy_modify_qp(
            c, &init, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
        goto fail;

    stage = "notification-RQ";

    for (unsigned i = 0; i < WSY_RQ; i++)
        if (wsy_post_recv(c, i))
            goto fail;

    stage = "PSN-random";

    uint32_t psn;
    ssize_t n;

    do {
        n = getrandom(&psn, sizeof(psn), 0);
    } while (n < 0 && errno == EINTR);

    if (n != sizeof(psn))
        goto fail;

    uint32_t qpn;
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa)
        qpn = peer_wsd_qpn(c->dpa_qp);
    else
#endif
        qpn = c->qp->qp_num;
    c->endpoint = (struct peer_verbs_endpoint){.qpn = qpn, .psn = psn & 0xffffff, .mtu = t->mtu};
    memcpy(c->endpoint.gid, t->gid, 16);
    *ep = c->endpoint;
    *out = c;
    return 0;

fail:
    fprintf(stderr, "PEER_WRITE_SYNC prepare failed stage=%s errno=%d\n", stage, errno);

    if (peer_write_sync_close(c))
        *out = c;

    return -1;
}

int peer_write_sync_prepare_pair(void *ctx,
                                 const struct dmesh_peer_pair *pair,
                                 void **out,
                                 struct peer_verbs_endpoint *ep)
{
    return pair ? peer_write_sync_prepare(ctx, out, ep) : -1;
}

int peer_write_sync_activate(void *opaque, const struct peer_verbs_endpoint *ep, int protected)
{
    struct wsy_conn *c = opaque;
    static const uint8_t prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (!c || !c->used || c->ctx->dead || c->state != WSY_INIT || protected != 1 || !ep ||
        !ep->qpn || ep->qpn > 0xffffff || ep->psn > 0xffffff || ep->mtu != c->endpoint.mtu ||
        memcmp(ep->gid, prefix, 12))
        return -1;

    struct wsy_ctx *t = c->ctx;
#if DMESH_DPA_RDMA_OFFLOAD
    if (t->dpa) {
        if (peer_wsd_activate(c->dpa_qp, ep, protected))
            goto fail;
    } else
#endif
    {
        if (ep->native.bytes)
            goto fail;

        struct ibv_qp_attr a = {
            .qp_state = IBV_QPS_RTR,
            .path_mtu = wsy_mtu(ep->mtu),
            .dest_qp_num = ep->qpn,
            .rq_psn = ep->psn,
            .min_rnr_timer = 12,
            .max_dest_rd_atomic = 16,
            .ah_attr = {.is_global = 1,
                        .port_num = t->port,
                        .grh = {.sgid_index = (uint8_t)t->gid_index, .hop_limit = 64}}};
        memcpy(a.ah_attr.grh.dgid.raw, ep->gid, 16);

        if (wsy_modify_qp(c,
                          &a,
                          IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                              IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
            goto fail;

        memset(&a, 0, sizeof(a));
        a.qp_state = IBV_QPS_RTS;
        a.sq_psn = c->endpoint.psn;
        a.timeout = 14;
        a.retry_cnt = a.rnr_retry = 7;
        a.max_rd_atomic = 16;

        if (wsy_modify_qp(c,
                          &a,
                          IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                              IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
            goto fail;
    }
    c->state = WSY_CONNECTING;
    c->activated_ns = wsy_now();
    c->peer_qpn = ep->qpn;

    if (wsy_hello_encode(c))
        goto fail;

    struct ibv_sge s = {
        .addr = (uintptr_t)wsy_boot(c), .length = wsy_hello_size(c), .lkey = c->scratch_mr->lkey};
    struct ibv_send_wr wr = {.wr_id = wsy_id(c, WSY_BOOT, 0),
                             .sg_list = &s,
                             .num_sge = 1,
                             .opcode = IBV_WR_SEND,
                             .send_flags = IBV_SEND_SIGNALED};

    /* Only after the manager's RX/TX IPsec install barrier. No rkey/address is
     * added to the existing TLS wire format, exposed to a Pod, or logged.
     * Both opt-in peers must validate this fixed canonical bootstrap first. */
    if (wsy_submit(c, &wr))
        goto fail;

    c->boot_busy = 1;
    return 0;

fail:
    c->state = WSY_DEAD;

    return -1;
}

int peer_write_sync_send_drained(void *opaque)
{
    struct wsy_conn *c = opaque;
    if (!c || !c->used || wsy_ready(c) < 0)
        return -1;

    /* QUIESCE must flush the final partial watermark even with no new DATA.
     * The owning worker still progresses the reserved credit submission/completion. */
    if (wsy_credit(c, 1) < 0)
        return -1;
    if (c->leased || c->boot_busy || c->credit_busy || c->credit_sent != c->rx_head)
        return 0;

    if (c->tx_source.completed != c->tx_tail || c->tx_inflight)
        return 0;

    return 1;
}

int peer_wire_write_sync_new(struct ibv_context *verbs,
                             uint8_t port,
                             uint32_t gid,
                             uint16_t mtu,
                             uint32_t bytes,
                             const struct peer_wire_ops **ops,
                             void **out,
                             char *error,
                             size_t error_len)
{
    return wsy_new(verbs, port, gid, mtu, bytes, NULL, 0, 0, ops, out, error, error_len);
}

int peer_pair_write_sync_new(const struct peer_pair_transport_config *cfg,
                             struct ibv_context *verbs,
                             uint8_t port,
                             uint32_t gid,
                             uint16_t mtu,
                             uint32_t bytes,
                             struct peer_pair_transport **out,
                             char *error,
                             size_t error_len)
{
    if (!cfg || !out)
        return -1;

    *out = NULL;

    struct peer_pair_driver d = {.prepare = peer_write_sync_prepare,
                                 .prepare_pair = peer_write_sync_prepare_pair,
                                 .activate = peer_write_sync_activate,
                                 .send_drained = peer_write_sync_send_drained,
                                 .close = peer_write_sync_close};

    /* Keep progress enabled with zero live QPs: retired CQEs/channel events
     * can still need draining. Skipping them would leave the wait FD readable
     * and spin the ARM worker after the last pair closes. Idle progress is 0. */
    if (peer_wire_write_sync_new(verbs, port, gid, mtu, bytes, &d.wire, &d.ctx, error, error_len))
        return -1;

    struct peer_pair_transport_config copy = *cfg;
    copy.rx_pool = NULL; /* explicit copied RX adapter, not proxy arena RX */

    if (!peer_pair_transport_new_driver(&copy, &d, out))
        return 0;

    d.wire->ctx_free(d.ctx);

    return -1;
}

#if DMESH_DPA_RDMA_OFFLOAD
static int wsy_dpa_driver(struct ibv_context *verbs,
                          uint8_t port,
                          uint32_t gid,
                          uint16_t mtu,
                          uint32_t bytes,
                          unsigned eu,
                          const struct peer_dpa_runtime *runtime,
                          struct peer_pair_driver *d,
                          char *error,
                          size_t error_len,
                          int immediate)
{
    *d = (struct peer_pair_driver){.prepare = peer_write_sync_prepare,
                                   .prepare_pair = peer_write_sync_prepare_pair,
                                   .activate = peer_write_sync_activate,
                                   .send_drained = peer_write_sync_send_drained,
                                   .close = peer_write_sync_close};
    return wsy_new(
        verbs, port, gid, mtu, bytes, runtime, eu, immediate, &d->wire, &d->ctx, error, error_len);
}

static int wsy_pair_dpa_new(const struct peer_pair_transport_config *cfg,
                            struct ibv_context *verbs,
                            uint8_t port,
                            uint32_t gid,
                            uint16_t mtu,
                            uint32_t bytes,
                            const struct peer_dpa_runtime *rt,
                            struct peer_pair_transport **out,
                            char *error,
                            size_t error_len,
                            int immediate)
{
    if (!cfg || !rt || !out)
        return -1;

    *out = NULL;

    struct peer_pair_driver d = {0};
    if (rt->select_group) {
        struct peer_pair_driver children[PEER_MAX_DPA_EU] = {0};
        unsigned built = 0;
        if (!rt->workers || rt->workers > DPA_RDMA_CONTEXTS || !rt->rdma_threads ||
            rt->rdma_first >= PEER_MAX_DPA_EU || rt->rdma_threads > PEER_MAX_DPA_EU - rt->rdma_first ||
            cfg->worker >= rt->workers || rt->owner_worker != cfg->worker)
            return -1;

        unsigned count =
            peer_dpa_child_count(rt->rdma_first, rt->rdma_threads, rt->workers, rt->owner_worker);
        for (; built < count; built++) {
            unsigned eu = peer_dpa_child_eu(
                rt->rdma_first, rt->rdma_threads, rt->workers, rt->owner_worker, built);
            if (wsy_dpa_driver(verbs,
                               port,
                               gid,
                               mtu,
                               bytes,
                               eu,
                               rt,
                               &children[built],
                               error,
                               error_len,
                               immediate))
                break;
        }

        if (built != count ||
            peer_pair_driver_group_new(children, count, rt->select_group, rt->select_ctx, &d)) {
            for (unsigned i = 0; i < built; i++)
                children[i].wire->ctx_free(children[i].ctx);
            return -1;
        }
    } else if (wsy_dpa_driver(
                   verbs, port, gid, mtu, bytes, cfg->worker, rt, &d, error, error_len, immediate))
        return -1;

    struct peer_pair_transport_config copy = *cfg;
    copy.rx_pool = NULL;

    if (!peer_pair_transport_new_driver(&copy, &d, out))
        return 0;

    d.wire->ctx_free(d.ctx);

    return -1;
}

int peer_pair_write_sync_dpa_new(const struct peer_pair_transport_config *cfg,
                                 struct ibv_context *verbs,
                                 uint8_t port,
                                 uint32_t gid,
                                 uint16_t mtu,
                                 uint32_t bytes,
                                 const struct peer_dpa_runtime *rt,
                                 struct peer_pair_transport **out,
                                 char *error,
                                 size_t error_len)
{
    return wsy_pair_dpa_new(cfg, verbs, port, gid, mtu, bytes, rt, out, error, error_len, 0);
}

int peer_pair_write_imm_dpa_new(const struct peer_pair_transport_config *cfg,
                                struct ibv_context *verbs,
                                uint8_t port,
                                uint32_t gid,
                                uint16_t mtu,
                                uint32_t bytes,
                                const struct peer_dpa_runtime *rt,
                                struct peer_pair_transport **out,
                                char *error,
                                size_t error_len)
{
    return wsy_pair_dpa_new(cfg, verbs, port, gid, mtu, bytes, rt, out, error, error_len, 1);
}
#endif
