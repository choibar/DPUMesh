/* SEND carrier for authenticated, manually configured RC pair lanes.
 * The control plane supplies endpoints and installs protection before RTS.
 * One owner worker drives the shared completion queue, TX leases and arena RX
 * refills. QP destruction fences all source and receive-buffer reclamation. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "peer_wire.h"
#include "peer_pair_transport.h"
#include "peer_verbs_common.h"
#include "peer_identity.h"
#include "peer_config.h"

#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/random.h>
#include <unistd.h>

/* One authenticated lane per remote peer node on this worker. */
#define RDMA_CONN_MAX PEER_RDMA_LANES
/* Send depth absorbs completion latency. The layer above offers the next frame
 * as soon as this one is accepted, and accepted here means posted, not
 * delivered — with a single slot every frame would wait on the previous one's
 * completion. */
#define RDMA_SEND_RING PEER_RDMA_SEND_SLOTS
/* Absorb CPU-window bursts while CQ drain/reposting catches up. Local TX
 * completion does not guarantee the peer has reposted its receive WQE. */
#define RDMA_RECV_RING PEER_RDMA_RECV_SLOTS
#define RDMA_REFILL_WORDS ((RDMA_RECV_RING + 63u) / 64u)
#define RDMA_BUFFER_BYTES (PEER_RDMA_TX_SCRATCH_BUFFER_BYTES + PEER_RDMA_RX_BUFFER_BYTES)
#define RDMA_CQ_DEPTH ((int)(RDMA_CONN_MAX * (RDMA_SEND_RING + RDMA_RECV_RING)))
#define RDMA_POLL_BATCH PEER_RDMA_CQ_BATCH

enum {
    RC_FREE = 0,
    RC_CONNECTING,
    RC_READY,
    RC_DEAD
};

enum {
    TX_FREE,
    TX_RESERVED,
    TX_POSTED
};

enum {
    RX_EMPTY,
    RX_POSTED,
    RX_READY,
    RX_LEASED,
    RX_HELD
};

static atomic_uint_fast64_t lease_epoch_next = 1;
struct rdma_ctx;

struct rdma_conn {
    struct rdma_ctx *ctx;
    struct ibv_mr *mr;
    struct ibv_qp *qp;
    struct peer_verbs_endpoint local_endpoint;
    uint8_t *buf; /* the send ring, then the receive ring */
    uint32_t index;
    uint32_t epoch;
    int state;
    uint8_t in_use;
    uint8_t send_busy[RDMA_SEND_RING]; /* TX state */
    struct peer_wire_payload payload[RDMA_SEND_RING];
    uint8_t recv_state[RDMA_RECV_RING];
    int arena_rx;
    /* Owner-worker-only refill membership; payload ownership stays in rx_payload. */
    uint64_t refill_slots[RDMA_REFILL_WORDS];
    uint32_t refill_count;
    uint8_t refill_queued;
    struct rdma_conn *refill_prev, *refill_next;
    char rx_uid[DMESH_POD_UID_MAX];
    struct peer_wire_payload rx_payload[RDMA_RECV_RING];
    uint64_t lease_epoch;
    uint64_t tx_generation[RDMA_SEND_RING];
    uint64_t rx_generation[RDMA_RECV_RING];
    uint8_t tx_leased, rx_leased;
    uint32_t rx_lease_slot, rx_lease_len;
    uint32_t send_next; /* where the free-slot scan starts */
    /* Receives that completed and have not been handed up yet, oldest first. */
    uint16_t rq_slot[RDMA_RECV_RING];
    uint32_t rq_len[RDMA_RECV_RING];
    uint32_t rq_head;
    uint32_t rq_count;
};

struct rdma_ctx {
    int epfd;
    struct ibv_context *verbs;
    struct ibv_pd *pd;
    struct peer_wire_rx_pool rx_pool;
    struct ibv_mr *arena_mr;
    void *arena;
    size_t arena_len;
    struct ibv_comp_channel *cc;
    struct ibv_cq *cq;
    uint8_t armed;
    uint8_t dead;
    uint8_t verbs_port;
    uint32_t gid_index;
    uint16_t mtu;
    uint8_t gid[16];
    uint32_t epoch_next;
    struct rdma_conn *refill_head, *refill_tail;
    struct rdma_conn conns[RDMA_CONN_MAX];
};

/* Only the connection's ARM worker mutates this list and its slot bitmap.
 * Keep a blocked connection linked for the next progress pass, without
 * repeatedly adding it when another RX frame is returned. */
static void refill_unlink(struct rdma_conn *c)
{
    if (!c->refill_queued)
        return;

    struct rdma_ctx *ctx = c->ctx;
    if (c->refill_prev)
        c->refill_prev->refill_next = c->refill_next;
    else
        ctx->refill_head = c->refill_next;

    if (c->refill_next)
        c->refill_next->refill_prev = c->refill_prev;
    else
        ctx->refill_tail = c->refill_prev;

    c->refill_prev = c->refill_next = NULL;
    c->refill_queued = 0;
}

static void rdma_mark_dead(struct rdma_conn *c)
{
    refill_unlink(c);
    memset(c->refill_slots, 0, sizeof(c->refill_slots));
    c->refill_count = 0;
    c->state = RC_DEAD;
}

static void refill_enqueue(struct rdma_conn *c, unsigned slot)
{
    if (c->state == RC_DEAD)
        return; /* A late frame release must not revive a failed connection. */

    uint64_t bit = UINT64_C(1) << (slot % 64u);
    if (!(c->refill_slots[slot / 64u] & bit)) {
        c->refill_slots[slot / 64u] |= bit;
        c->refill_count++;
    }

    if (c->refill_queued)
        return;

    struct rdma_ctx *ctx = c->ctx;
    c->refill_prev = ctx->refill_tail;
    c->refill_next = NULL;
    if (ctx->refill_tail)
        ctx->refill_tail->refill_next = c;
    else
        ctx->refill_head = c;

    ctx->refill_tail = c;
    c->refill_queued = 1;
}

/* ---- work-request identity --------------------------------------------- */

/* Completions outlive the connection that posted them: one queue destroyed
 * while its work is still in a shared completion queue leaves entries behind.
 * A slot index alone would name a struct a later peer now owns, so the epoch
 * the slot carried when it posted rides along and a stale completion is
 * recognised and dropped. */
#define WR_KIND_SHIFT 63
#define WR_EPOCH_SHIFT 32
#define WR_CONN_SHIFT 16
#define WR_EPOCH_MASK 0x7fffffffu

static uint64_t wr_pack(int recv, uint32_t epoch, uint32_t conn, uint32_t slot)
{
    return ((uint64_t)(recv & 1) << WR_KIND_SHIFT) |
           ((uint64_t)(epoch & WR_EPOCH_MASK) << WR_EPOCH_SHIFT) |
           ((uint64_t)(conn & 0xffffu) << WR_CONN_SHIFT) | (uint64_t)(slot & 0xffffu);
}

/* ---- slots -------------------------------------------------------------- */

static uint8_t *send_slot(struct rdma_conn *c, uint32_t slot)
{
    return c->buf + (size_t)slot * PEER_RDMA_TX_SCRATCH_BYTES;
}

static uint8_t *recv_slot(struct rdma_conn *c, uint32_t slot)
{
    return c->buf + (size_t)RDMA_SEND_RING * PEER_RDMA_TX_SCRATCH_BYTES +
           (size_t)slot * PEER_RDMA_RX_SLOT_BYTES;
}

static uint8_t *header_slot(struct rdma_conn *c, uint32_t slot)
{
    return send_slot(c, slot);
}

static int arena_register(struct rdma_ctx *, const struct peer_wire_payload *);

static int rx_pool_set(void *opaque, const struct peer_wire_rx_pool *pool)
{
    struct rdma_ctx *ctx = opaque;
    if (!ctx || !pool || !pool->acquire || !pool->exclusive)
        return -1;

    for (unsigned i = 0; i < RDMA_CONN_MAX; i++)
        if (ctx->conns[i].in_use)
            return -1;

    ctx->rx_pool = *pool;

    return 0;
}

static void release_payload(struct rdma_conn *c, uint32_t slot)
{
    struct peer_wire_payload p = c->payload[slot];
    memset(&c->payload[slot], 0, sizeof(c->payload[slot]));

    if (p.release)
        p.release(p.owner, p.cookie);
}

static struct rdma_conn *rdma_slot_take(struct rdma_ctx *ctx)
{
    for (uint32_t i = 0; i < RDMA_CONN_MAX; i++) {
        struct rdma_conn *c = &ctx->conns[i];
        if (c->in_use)
            continue;
        if (ctx->epoch_next == WR_EPOCH_MASK) {
            ctx->dead = 1; /* Never let old CQ epochs name a replacement. */
            return NULL;
        }

        uint64_t lease_epoch = atomic_load(&lease_epoch_next);
        for (;;) {
            if (lease_epoch == UINT64_MAX)
                return NULL;
            if (atomic_compare_exchange_weak(&lease_epoch_next, &lease_epoch, lease_epoch + 1))
                break;
        }

        uint8_t *buf = malloc(RDMA_BUFFER_BYTES);
        if (!buf)
            return NULL;

        memset(c, 0, sizeof(*c));
        c->ctx = ctx;
        c->index = i;
        c->epoch = (++ctx->epoch_next) & WR_EPOCH_MASK;
        c->lease_epoch = lease_epoch;
        c->buf = buf;
        c->in_use = 1;
        c->state = RC_CONNECTING;

        return c;
    }
    return NULL;
}

static void rdma_release(struct rdma_conn *c)
{
    if (!c->in_use)
        return;

    rdma_mark_dead(c);

    if (c->qp && ibv_destroy_qp(c->qp) != 0)
        return; /* Failed fence: retain MR and memory, including slot identity. */

    c->qp = NULL;
    if (c->mr && ibv_dereg_mr(c->mr) != 0)
        return;
    c->mr = NULL;

    /* QP destruction fenced every NIC reader, including failed SENDs. */
    for (uint32_t i = 0; i < RDMA_SEND_RING; i++)
        release_payload(c, i);

    /* arena RX arena holds survive a failed QP fence. Consumer holds independently
     * outlive this QP and registration; proxy arena DMA mapping remains live. */
    for (unsigned i = 0; i < RDMA_RECV_RING; i++) {
        struct peer_wire_payload *p = &c->rx_payload[i];
        if (p->release)
            p->release(p->owner, p->cookie);
    }

    free(c->buf);
    memset(c, 0, sizeof(*c));
}

/* ---- queue pair --------------------------------------------------------- */

static int rdma_post_recv(struct rdma_conn *c, uint32_t slot)
{
    struct ibv_sge sge = {
        .addr = (uintptr_t)recv_slot(c, slot),
        .length = PEER_RDMA_RX_SLOT_BYTES,
        .lkey = c->mr->lkey,
    };
    if (c->arena_rx) {
        struct peer_wire_payload *p = &c->rx_payload[slot];
        if (!peer_wire_payload_valid(p, PEER_WIRE_FRAME_BYTES) || arena_register(c->ctx, p))
            return -1;

        sge = (struct ibv_sge){.addr = (uintptr_t)p->data,
                               .length = PEER_WIRE_FRAME_BYTES,
                               .lkey = c->ctx->arena_mr->lkey};
    }

    struct ibv_recv_wr wr = {
        .wr_id = wr_pack(1, c->epoch, c->index, slot),
        .sg_list = &sge,
        .num_sge = 1,
    };
    struct ibv_recv_wr *bad = NULL;
    struct ibv_qp *qp = c->qp;
    if (ibv_post_recv(qp, &wr, &bad) != 0)
        return -1;

    c->recv_state[slot] = RX_POSTED;

    return 0;
}

/* ---- transfer ----------------------------------------------------------- */

static int rdma_tx_reserve(void *wc, struct peer_wire_tx_lease *out)
{
    struct rdma_conn *c = wc;
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    if (!c || !c->in_use || c->state == RC_DEAD)
        return -1;
    if (c->state != RC_READY || c->tx_leased)
        return 0;

    for (uint32_t i = 0; i < RDMA_SEND_RING; i++) {
        uint32_t slot = (c->send_next + i) % RDMA_SEND_RING;
        if (c->send_busy[slot] != TX_FREE)
            continue;
        if (c->tx_generation[slot] == UINT64_MAX) {
            rdma_mark_dead(c);
            return -1;
        }

        c->send_busy[slot] = TX_RESERVED;
        c->tx_leased = 1;
        *out = (struct peer_wire_tx_lease){
            .data = send_slot(c, slot),
            .cap = PEER_RDMA_TX_SCRATCH_BYTES,
            .token = {c->lease_epoch, ++c->tx_generation[slot], slot, PEER_WIRE_TX},
        };

        return 1;
    }

    return 0;
}

static int valid_tx_kind(struct rdma_conn *c, const struct peer_wire_tx_lease *l, unsigned kind)
{
    return c && c->in_use && l && l->token.kind == kind && l->token.conn_epoch == c->lease_epoch &&
           l->token.slot < RDMA_SEND_RING && c->tx_leased == kind &&
           c->send_busy[l->token.slot] == TX_RESERVED &&
           c->tx_generation[l->token.slot] == l->token.lease_generation &&
           l->data == send_slot(c, l->token.slot) && l->cap == PEER_RDMA_TX_SCRATCH_BYTES;
}

static int valid_tx(struct rdma_conn *c, const struct peer_wire_tx_lease *l)
{
    return valid_tx_kind(c, l, PEER_WIRE_TX);
}

static int rdma_tx_cancel(void *wc, struct peer_wire_tx_lease *l)
{
    struct rdma_conn *c = wc;
    if (!valid_tx(c, l))
        return -1;

    c->send_busy[l->token.slot] = TX_FREE;
    c->tx_leased = 0;
    memset(l, 0, sizeof(*l));

    return c->state == RC_DEAD ? -1 : 0;
}

static int rdma_tx_commit(void *wc, struct peer_wire_tx_lease *l, size_t len)
{
    struct rdma_conn *c = wc;
    if (!valid_tx(c, l))
        return -1;

    uint32_t slot = l->token.slot;
    c->tx_leased = 0;
    memset(l, 0, sizeof(*l));

    if (!len || len > PEER_RDMA_TX_SCRATCH_BYTES || c->state != RC_READY) {
        c->send_busy[slot] = TX_FREE;
        rdma_mark_dead(c);
        return -1;
    }

    struct ibv_sge sge = {
        .addr = (uintptr_t)send_slot(c, slot),
        .length = (uint32_t)len,
        .lkey = c->mr->lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = wr_pack(0, c->epoch, c->index, slot),
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    struct ibv_send_wr *bad = NULL;
    struct ibv_qp *qp = c->qp;
    if (ibv_post_send(qp, &wr, &bad) != 0) {
        c->send_busy[slot] = TX_FREE;
        rdma_mark_dead(c);
        return -1;
    }

    c->send_busy[slot] = TX_POSTED;
    c->send_next = (slot + 1) % RDMA_SEND_RING;

    return 1;
}

static int rdma_frame_reserve(void *wc, struct peer_wire_frame_lease *out)
{
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    struct peer_wire_tx_lease l;
    int rc = rdma_tx_reserve(wc, &l);
    if (rc != 1)
        return rc;

    struct rdma_conn *c = wc;
    c->tx_leased = l.token.kind = PEER_WIRE_FRAME_TX;
    *out = (struct peer_wire_frame_lease){.header = header_slot(c, l.token.slot),
                                          .body = l.data + PEER_WIRE_HEADER_BYTES,
                                          .body_cap =
                                              PEER_RDMA_TX_SCRATCH_BYTES - PEER_WIRE_HEADER_BYTES,
                                          .token = l.token};

    return 1;
}

static int valid_frame(struct rdma_conn *c, const struct peer_wire_frame_lease *l)
{
    if (!c || !l)
        return 0;

    struct peer_wire_tx_lease raw = {
        .data = l->header, .cap = PEER_RDMA_TX_SCRATCH_BYTES, .token = l->token};
    return valid_tx_kind(c, &raw, PEER_WIRE_FRAME_TX) &&
           l->body == l->header + PEER_WIRE_HEADER_BYTES &&
           l->body_cap == PEER_RDMA_TX_SCRATCH_BYTES - PEER_WIRE_HEADER_BYTES;
}

static int rdma_frame_cancel(void *wc, struct peer_wire_frame_lease *l)
{
    struct rdma_conn *c = wc;
    if (!valid_frame(c, l))
        return -1;

    c->send_busy[l->token.slot] = TX_FREE;
    c->tx_leased = 0;
    memset(l, 0, sizeof(*l));

    return c->state == RC_DEAD ? -1 : 0;
}

static int arena_register(struct rdma_ctx *ctx, const struct peer_wire_payload *p)
{
    if (ctx->arena_mr)
        return ctx->arena == p->region && ctx->arena_len == p->region_len ? 0 : -1;

    /* Each owner context registers the stable proxy arena in its own QP PD.
     * Transport teardown must precede proxy arena destruction. */
    struct ibv_mr *mr = ibv_reg_mr(ctx->pd, p->region, p->region_len, IBV_ACCESS_LOCAL_WRITE);
    if (!mr)
        return -1;

    ctx->arena_mr = mr;
    ctx->arena = p->region;
    ctx->arena_len = p->region_len;

    return 0;
}

static int rdma_frame_commit(void *wc,
                             struct peer_wire_frame_lease *l,
                             size_t len,
                             const struct peer_wire_payload *p)
{
    struct rdma_conn *c = wc;
    if (!valid_frame(c, l))
        return -1;

    unsigned slot = l->token.slot;
    int ok = c->state == RC_READY &&
             len <= (p ? PEER_WIRE_FRAME_BYTES - PEER_WIRE_HEADER_BYTES : l->body_cap) &&
             (!p || (peer_wire_frame_payload_valid(p, len) && !arena_register(c->ctx, p)));
    c->tx_leased = 0;
    memset(l, 0, sizeof(*l));

    if (!ok) {
        c->send_busy[slot] = TX_FREE;
        rdma_mark_dead(c);
        return -1;
    }

    struct ibv_sge sge = {.addr = p ? (uintptr_t)p->tx_header : (uintptr_t)header_slot(c, slot),
                          .length = PEER_WIRE_HEADER_BYTES + (uint32_t)len,
                          .lkey = p ? c->ctx->arena_mr->lkey : c->mr->lkey};
    struct ibv_send_wr wr = {.wr_id = wr_pack(0, c->epoch, c->index, slot),
                             .sg_list = &sge,
                             .num_sge = 1,
                             .opcode = IBV_WR_SEND,
                             .send_flags = IBV_SEND_SIGNALED},
                       *bad = NULL;
    if (p) {
        p->retain(p->owner, p->cookie);
        c->payload[slot] = *p;
    }
    if (ibv_post_send(c->qp, &wr, &bad)) {
        release_payload(c, slot);
        c->send_busy[slot] = TX_FREE;
        rdma_mark_dead(c);
        return -1;
    }

    c->send_busy[slot] = TX_POSTED;
    c->send_next = (slot + 1) % RDMA_SEND_RING;

    return 1;
}

static int rdma_rx_acquire(void *wc, struct peer_wire_rx_lease *out)
{
    struct rdma_conn *c = wc;
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    if (!c || !c->in_use || c->state == RC_DEAD)
        return -1;
    if (c->arena_rx)
        return -1;
    if (c->rx_leased || !c->rq_count)
        return 0;

    uint32_t slot = c->rq_slot[c->rq_head], len = c->rq_len[c->rq_head];
    if (slot >= RDMA_RECV_RING || c->recv_state[slot] != RX_READY || !len ||
        len > PEER_RDMA_RX_SLOT_BYTES || c->rx_generation[slot] == UINT64_MAX) {
        rdma_mark_dead(c);
        return -1;
    }

    c->recv_state[slot] = RX_LEASED;
    c->rx_leased = 1;
    *out = (struct peer_wire_rx_lease){
        .data = recv_slot(c, slot),
        .len = len,
        .token = {c->lease_epoch, ++c->rx_generation[slot], slot, PEER_WIRE_RX},
    };
    c->rx_lease_slot = slot;
    c->rx_lease_len = len;
    c->rq_head = (c->rq_head + 1) % RDMA_RECV_RING;
    c->rq_count--;

    return 1;
}

static int rdma_rx_release(void *wc, struct peer_wire_rx_lease *l)
{
    struct rdma_conn *c = wc;
    if (!c || !c->in_use || !l || l->token.kind != PEER_WIRE_RX ||
        l->token.conn_epoch != c->lease_epoch || l->token.slot >= RDMA_RECV_RING || !c->rx_leased ||
        c->recv_state[l->token.slot] != RX_LEASED ||
        c->rx_generation[l->token.slot] != l->token.lease_generation ||
        l->data != recv_slot(c, l->token.slot) || c->rx_lease_slot != l->token.slot ||
        c->rx_lease_len != l->len)
        return -1;

    uint32_t slot = l->token.slot;
    memset(l, 0, sizeof(*l));
    c->rx_leased = 0;
    c->rx_lease_len = 0;
    c->recv_state[slot] = RX_EMPTY;

    if (c->state == RC_DEAD || rdma_post_recv(c, slot) != 0) {
        rdma_mark_dead(c);
        return -1;
    }

    return 0;
}

static int frame_acquire(void *opaque, struct peer_wire_rx_frame *out)
{
    struct rdma_conn *c = opaque;
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    if (!c || !c->in_use || c->state == RC_DEAD)
        return -1;
    if (!c->arena_rx)
        return -2;
    if (c->rx_leased || !c->rq_count)
        return 0;

    unsigned slot = c->rq_slot[c->rq_head], len = c->rq_len[c->rq_head];
    if (slot >= RDMA_RECV_RING || c->recv_state[slot] != RX_READY || len < PEER_WIRE_HEADER_BYTES ||
        len > PEER_WIRE_FRAME_BYTES || c->rx_generation[slot] == UINT64_MAX) {
        rdma_mark_dead(c);
        return -1;
    }

    *out = (struct peer_wire_rx_frame){
        .header = c->rx_payload[slot].data,
        .payload = c->rx_payload[slot],
        .body_len = len - PEER_WIRE_HEADER_BYTES,
        .token = {c->lease_epoch, ++c->rx_generation[slot], slot, PEER_WIRE_RX}};
    out->payload.data += PEER_WIRE_HEADER_BYTES;
    out->payload.len = PEER_WIRE_BODY_BYTES;
    c->rx_leased = 1;
    c->rx_lease_slot = slot;
    c->rx_lease_len = len;
    c->recv_state[slot] = RX_LEASED;
    c->rq_head = (c->rq_head + 1) % RDMA_RECV_RING;
    c->rq_count--;

    return 1;
}

static int frame_release(void *opaque, struct peer_wire_rx_frame *f)
{
    struct rdma_conn *c = opaque;
    if (!c || !f || !c->in_use || !c->arena_rx || !c->rx_leased || f->token.kind != PEER_WIRE_RX ||
        f->token.conn_epoch != c->lease_epoch || f->token.slot >= RDMA_RECV_RING ||
        f->token.slot != c->rx_lease_slot)
        return -1;

    unsigned slot = f->token.slot;
    if (c->recv_state[slot] != RX_LEASED || f->token.lease_generation != c->rx_generation[slot] ||
        f->header != c->rx_payload[slot].data ||
        f->payload.data != c->rx_payload[slot].data + PEER_WIRE_HEADER_BYTES ||
        f->payload.cookie != c->rx_payload[slot].cookie ||
        f->body_len + PEER_WIRE_HEADER_BYTES != c->rx_lease_len)
        return -1;

    c->recv_state[slot] = RX_HELD;
    refill_enqueue(c, slot);
    c->rx_leased = 0;
    memset(f, 0, sizeof(*f));

    return 0;
}

static int refill_receives(struct rdma_conn *c)
{
    if (!c->refill_count)
        return 0;

    int moved = 0;
    for (unsigned word = 0; word < RDMA_REFILL_WORDS; word++) {
        uint64_t slots = c->refill_slots[word];
        while (slots) {
            unsigned slot = word * 64u + (unsigned)__builtin_ctzll(slots);
            uint64_t bit = UINT64_C(1) << (slot % 64u);
            slots &= slots - 1;

            int rc = peer_wire_rx_refill(&c->ctx->rx_pool, c->rx_uid, &c->rx_payload[slot]);
            if (!rc)
                continue; /* Retain the bit until a later pass can repost. */
            if (rc < 0 || rdma_post_recv(c, slot)) {
                rdma_mark_dead(c);
                return -1;
            }

            c->refill_slots[word] &= ~bit;
            c->refill_count--;
            moved = 1;
        }
    }

    if (!c->refill_count)
        refill_unlink(c);

    return moved;
}

static int progress_refills(struct rdma_ctx *ctx)
{
    int moved = 0;
    struct rdma_conn *c = ctx->refill_head;
    while (c) {
        /* Refill success or failure can unlink this connection. Visit each
         * queued connection once; exhaustion must not block its successors. */
        struct rdma_conn *next = c->refill_next;
        if (refill_receives(c))
            moved = 1;
        c = next;
    }

    return moved;
}

/* Whole-message senders keep copy-in ownership and message ordering. */
static int rdma_send_msg(void *wc, const void *buf, size_t len)
{
    if (!wc || !buf || !len || len > PEER_RDMA_TX_SCRATCH_BYTES)
        return -1;

    struct peer_wire_tx_lease l;
    int r = rdma_tx_reserve(wc, &l);
    if (r != 1)
        return r;

    memcpy(l.data, buf, len);

    return rdma_tx_commit(wc, &l, len);
}

static long rdma_recv_msg(void *wc, void *buf, size_t cap)
{
    struct peer_wire_rx_lease l;
    int r = rdma_rx_acquire(wc, &l);
    if (r != 1)
        return r;
    if (!buf || l.len > cap) {
        rdma_mark_dead(wc);
        (void)rdma_rx_release(wc, &l);
        return -1;
    }

    size_t len = l.len;
    memcpy(buf, l.data, len);

    return rdma_rx_release(wc, &l) == 0 ? (long)len : -1;
}

/* ---- completions -------------------------------------------------------- */

/* Consume whatever notifications are queued. They carry no information beyond
 * "look at the queue", which `progress` is about to do anyway; what matters is
 * that they leave the descriptor, since one left unread keeps a level-triggered
 * event loop awake. */
static void rdma_drain_comp(struct rdma_ctx *ctx)
{
    unsigned acked = 0;
    for (;;) {
        struct ibv_cq *cq = NULL;
        void *cookie = NULL;
        if (ibv_get_cq_event(ctx->cc, &cq, &cookie) != 0)
            break;

        acked++;
        ctx->armed = 0;
    }

    if (acked)
        ibv_ack_cq_events(ctx->cq, acked); /* one lock for the whole batch */
}

static int rdma_poll_cq(struct rdma_ctx *ctx)
{
    struct ibv_wc wcs[RDMA_POLL_BATCH];
    int progressed = 0;

    for (;;) {
        int n = ibv_poll_cq(ctx->cq, RDMA_POLL_BATCH, wcs);
        if (n < 0) {
            ctx->dead = 1;

            for (uint32_t i = 0; i < RDMA_CONN_MAX; i++)
                if (ctx->conns[i].in_use)
                    rdma_mark_dead(&ctx->conns[i]);
            return 1;
        }
        if (n == 0)
            break;

        for (int i = 0; i < n; i++) {
            uint64_t id = wcs[i].wr_id;
            uint32_t ci = (uint32_t)((id >> WR_CONN_SHIFT) & 0xffffu);
            uint32_t slot = (uint32_t)(id & 0xffffu);
            uint32_t epoch = (uint32_t)((id >> WR_EPOCH_SHIFT) & WR_EPOCH_MASK);
            int recv = (int)((id >> WR_KIND_SHIFT) & 1);
            if (ci >= RDMA_CONN_MAX)
                continue;

            struct rdma_conn *c = &ctx->conns[ci];
            if (!c->in_use || c->epoch != epoch)
                continue; /* the slot belongs to a later connection */

            progressed = 1;

            if (wcs[i].status != IBV_WC_SUCCESS) {
                rdma_mark_dead(c);
                continue;
            }

            if (recv) {
                if (slot >= RDMA_RECV_RING || wcs[i].byte_len == 0 ||
                    wcs[i].byte_len >
                        (c->arena_rx ? PEER_WIRE_FRAME_BYTES : PEER_RDMA_RX_SLOT_BYTES) ||
                    c->rq_count >= RDMA_RECV_RING || c->recv_state[slot] != RX_POSTED) {
                    rdma_mark_dead(c);
                    continue;
                }

                uint32_t tail = (c->rq_head + c->rq_count) % RDMA_RECV_RING;
                c->rq_slot[tail] = (uint16_t)slot;
                c->rq_len[tail] = wcs[i].byte_len;
                c->rq_count++;
                c->recv_state[slot] = RX_READY;
            } else if (slot < RDMA_SEND_RING && c->send_busy[slot] == TX_POSTED) {
                c->send_busy[slot] = TX_FREE;
                release_payload(c, slot);
            } else {
                rdma_mark_dead(c);
            }
        }

        if (n < RDMA_POLL_BATCH)
            break;
    }

    return progressed;
}

/* ---- ops ---------------------------------------------------------------- */

static int rdma_progress(void *wctx, void **accepted, int max, int *n_accepted)
{
    struct rdma_ctx *ctx = wctx;
    (void)accepted;
    (void)max;
    if (n_accepted)
        *n_accepted = 0;
    if (!ctx)
        return 0;

    rdma_drain_comp(ctx);
    /* Arm before polling so an arrival at the idle boundary cannot be lost. */
    if (!ctx->armed && ibv_req_notify_cq(ctx->cq, 0) == 0)
        ctx->armed = 1;
    int progressed = rdma_poll_cq(ctx);
    progressed |= progress_refills(ctx);
    return progressed;
}

static int rdma_established(void *wc)
{
    struct rdma_conn *c = wc;
    return c && c->in_use && c->state == RC_READY;
}

static int rdma_faulted(void *wc)
{
    struct rdma_conn *c = wc;
    return !c || !c->in_use || c->state == RC_DEAD;
}

static void rdma_close(void *wc)
{
    struct rdma_conn *c = wc;
    if (c)
        rdma_release(c);
}

static int rdma_epfd(void *wctx)
{
    struct rdma_ctx *ctx = wctx;
    return ctx ? ctx->epfd : -1;
}

static void rdma_ctx_free(void *wctx)
{
    struct rdma_ctx *ctx = wctx;
    if (!ctx)
        return;

    ctx->dead = 1;

    for (uint32_t i = 0; i < RDMA_CONN_MAX; i++)
        rdma_release(&ctx->conns[i]);

    /* A manual QP/MR destroy failure leaves device access possible. Keep the
     * owning PD/CQ/context and buffers until an explicit retry succeeds. */
    for (uint32_t i = 0; i < RDMA_CONN_MAX; i++)
        if (ctx->conns[i].in_use)
            return;

    if (ctx->arena_mr) {
        if (ibv_dereg_mr(ctx->arena_mr))
            return; /* retain registration owner on failure */

        ctx->arena_mr = NULL;
    }

    /* Destroying QPs flushes their work requests and may produce one last CQ
     * notification. A CQ destroy waits for every delivered event to be
     * acknowledged, so consume and acknowledge those before tearing it down. */
    if (ctx->cc && ctx->cq) {
        rdma_drain_comp(ctx);
        (void)rdma_poll_cq(ctx);
        rdma_drain_comp(ctx); /* an allowed extra event may have no WC */
    }

    if (ctx->cq)
        ibv_destroy_cq(ctx->cq);
    if (ctx->cc)
        ibv_destroy_comp_channel(ctx->cc);
    if (ctx->pd)
        ibv_dealloc_pd(ctx->pd);
    if (ctx->epfd >= 0)
        close(ctx->epfd);
    free(ctx);
}

static const struct peer_wire_ops RDMA_OPS = {
    .progress = rdma_progress,
    .send_msg = rdma_send_msg,
    .recv_msg = rdma_recv_msg,
    .established = rdma_established,
    .faulted = rdma_faulted,
    .close = rdma_close,
    .epfd = rdma_epfd,
    .ctx_free = rdma_ctx_free,
    .tx_reserve = rdma_tx_reserve,
    .tx_commit = rdma_tx_commit,
    .tx_cancel = rdma_tx_cancel,
    .rx_acquire = rdma_rx_acquire,
    .rx_release = rdma_rx_release,
    .frame_reserve = rdma_frame_reserve,
    .frame_commit = rdma_frame_commit,
    .frame_cancel = rdma_frame_cancel,
    .rx_pool_set = rx_pool_set,
    .frame_acquire = frame_acquire,
    .frame_release = frame_release,
};

/* ---- construction ------------------------------------------------------- */

/* Manual RC setup for a TLS-negotiated lane. Shares the existing RDMA ring,
 * lease, completion and stale-WR fences. Device context is BORROWED; one owner
 * worker gets its own PD/CQ. No CM listen/connect or network discovery occurs.
 * The supplied GID must currently be IPv4 RoCEv2 on an active Ethernet port. */
static int peer_wire_verbs_new(struct ibv_context *verbs,
                        uint8_t port,
                        uint32_t gid_index,
                        uint16_t mtu,
                        const struct peer_wire_ops **ops,
                        void **wctx,
                        char *error,
                        size_t error_len)
{
    if (!ops || !wctx)
        return -1;

    *ops = NULL;
    *wctx = NULL;

    if (error && error_len)
        error[0] = 0;

    struct ibv_port_attr attr;
    struct ibv_gid_entry entry;
    static const uint8_t ipv4_prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (!verbs || !port || gid_index > 255 || !peer_verbs_mtu(mtu) ||
        ibv_query_port(verbs, port, &attr) || attr.state != IBV_PORT_ACTIVE ||
        attr.link_layer != IBV_LINK_LAYER_ETHERNET || attr.active_mtu < peer_verbs_mtu(mtu) ||
        ibv_query_gid_ex(verbs, port, gid_index, &entry, 0) ||
        entry.gid_type != IBV_GID_TYPE_ROCE_V2 || memcmp(entry.gid.raw, ipv4_prefix, 12)) {
        if (error && error_len)
            snprintf(
                error, error_len, "manual RDMA requires active IPv4 RoCEv2 GID and valid path MTU");
        return -1;
    }

    struct rdma_ctx *ctx = calloc(1, sizeof(*ctx));
    if (!ctx)
        return -1;

    ctx->epfd = -1;
    ctx->verbs = verbs;
    ctx->verbs_port = port;
    ctx->gid_index = gid_index;
    ctx->mtu = mtu;
    memcpy(ctx->gid, entry.gid.raw, 16);
    ctx->pd = ibv_alloc_pd(verbs);
    if (!ctx->pd)
        goto fail;

    ctx->cc = ibv_create_comp_channel(verbs);
    if (!ctx->cc || peer_verbs_nonblock(ctx->cc->fd))
        goto fail;

    ctx->cq = ibv_create_cq(verbs, RDMA_CQ_DEPTH, ctx, ctx->cc, 0);
    if (!ctx->cq || ibv_req_notify_cq(ctx->cq, 0))
        goto fail;

    ctx->armed = 1;
    ctx->epfd = epoll_create1(EPOLL_CLOEXEC);
    if (ctx->epfd < 0 || peer_verbs_epoll_add(ctx->epfd, ctx->cc->fd))
        goto fail;

    *ops = &RDMA_OPS;
    *wctx = ctx;
    return 0;

fail:
    if (error && error_len)
        snprintf(error, error_len, "manual RDMA resource setup: %s", strerror(errno));
    rdma_ctx_free(ctx);

    return -1;
}

static int verbs_prepare(void *wctx, const char *uid, void **wc, struct peer_verbs_endpoint *local)
{
    if (!wc || !local)
        return -1;

    *wc = NULL;
    memset(local, 0, sizeof(*local));

    struct rdma_ctx *ctx = wctx;
    if (!ctx || ctx->dead)
        return -1;

    struct rdma_conn *c = rdma_slot_take(ctx);
    if (!c)
        return -1;

    c->arena_rx = uid && ctx->rx_pool.acquire;
    if (c->arena_rx) {
        snprintf(c->rx_uid, sizeof(c->rx_uid), "%s", uid);
        for (unsigned i = 0; i < RDMA_RECV_RING; i++)
            if (peer_wire_rx_refill(&ctx->rx_pool, c->rx_uid, &c->rx_payload[i]) != 1)
                goto fail;
    }

    /* Give the owner a cleanup handle even on a failure whose destroy cannot
     * complete. A NULL output always means no resources remain owned. */
    struct ibv_qp_init_attr attr = {.send_cq = ctx->cq,
                                    .recv_cq = ctx->cq,
                                    .qp_type = IBV_QPT_RC,
                                    .cap = {.max_send_wr = RDMA_SEND_RING,
                                            .max_recv_wr = RDMA_RECV_RING,
                                            .max_send_sge = 1,
                                            .max_recv_sge = 1}};
    c->qp = ibv_create_qp(ctx->pd, &attr);
    if (!c->qp)
        goto fail;

    struct ibv_qp_attr init = {.qp_state = IBV_QPS_INIT,
                               .port_num = ctx->verbs_port,
                               .pkey_index = 0,
                               .qp_access_flags = 0};
    if (ibv_modify_qp(c->qp,
                      &init,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
        goto fail;

    c->mr = ibv_reg_mr(ctx->pd, c->buf, RDMA_BUFFER_BYTES, IBV_ACCESS_LOCAL_WRITE);
    if (!c->mr)
        goto fail;

    for (unsigned i = 0; i < RDMA_RECV_RING; i++)
        if (rdma_post_recv(c, i))
            goto fail;

    uint32_t psn;
    ssize_t n;

    do {
        n = getrandom(&psn, sizeof(psn), 0);
    } while (n < 0 && errno == EINTR);

    if (n != sizeof(psn))
        goto fail;

    c->local_endpoint.qpn = c->qp->qp_num;
    c->local_endpoint.psn = psn & 0xffffff;
    c->local_endpoint.mtu = ctx->mtu;
    memcpy(c->local_endpoint.gid, ctx->gid, 16);
    c->state = RC_CONNECTING;
    *wc = c;
    *local = c->local_endpoint;
    return 0;

fail:
    rdma_release(c);

    if (c->in_use)
        *wc = c;

    return -1;
}

/* QP remains INIT with receives posted; no remote access keys are exchanged. */
static int peer_wire_verbs_prepare(void *ctx, void **out, struct peer_verbs_endpoint *ep)
{
    return verbs_prepare(ctx, NULL, out, ep);
}

static int peer_wire_verbs_prepare_pair(void *ctx,
                                 const struct dmesh_peer_pair *pair,
                                 void **out,
                                 struct peer_verbs_endpoint *ep)
{
    return pair ? verbs_prepare(ctx, pair->local_uid, out, ep) : -1;
}

/* Call ONLY after both RX and TX hardware paths have completed installation.
 * protection_ready is the control owner's barrier result, never a wire claim.
 * RTR may emit automatic responses, so this guard precedes both RTR and RTS. */
static int peer_wire_verbs_activate(void *wc, const struct peer_verbs_endpoint *remote, int protected)
{
    struct rdma_conn *c = wc;
    static const uint8_t ipv4_prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (!c || !c->in_use || c->ctx->dead || c->state != RC_CONNECTING ||
        !c->qp || !remote || protected != 1 || !remote->qpn || remote->qpn > 0xffffff ||
        remote->psn > 0xffffff || !peer_verbs_mtu(remote->mtu) || remote->mtu != c->local_endpoint.mtu ||
        memcmp(remote->gid, ipv4_prefix, 12))
        return -1;

    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = peer_verbs_mtu(remote->mtu),
        .dest_qp_num = remote->qpn,
        .rq_psn = remote->psn,
        .max_dest_rd_atomic = 0,
        .min_rnr_timer = PEER_RDMA_MIN_RNR_TIMER,
        .ah_attr = {.is_global = 1,
                    .port_num = c->ctx->verbs_port,
                    .grh = {.sgid_index = (uint8_t)c->ctx->gid_index, .hop_limit = 64}}};
    memcpy(attr.ah_attr.grh.dgid.raw, remote->gid, 16);

    if (ibv_modify_qp(c->qp,
                      &attr,
                      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                          IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
        goto fail;

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = c->local_endpoint.psn;
    attr.timeout = PEER_RDMA_ACK_TIMEOUT;
    attr.retry_cnt = PEER_RDMA_RETRY_COUNT;
    attr.rnr_retry = PEER_RDMA_RNR_RETRY;
    attr.max_rd_atomic = 0;

    if (ibv_modify_qp(c->qp,
                      &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
        goto fail;

    c->state = RC_READY;
    return 0;

fail:
    rdma_mark_dead(c);

    return -1;
}

/* Does not include application STREAM_ACK or host DMA; the worker must fence
 * those separately. 1 means every SEND completion and TX lease has retired. */
static int peer_wire_verbs_send_drained(void *wc)
{
    struct rdma_conn *c = wc;
    if (!c || !c->in_use || c->state == RC_DEAD)
        return -1;
    if (c->tx_leased)
        return 0;

    for (unsigned i = 0; i < RDMA_SEND_RING; i++)
        if (c->send_busy[i] != TX_FREE)
            return 0;

    return 1;
}

/* On failure retains QP/MR/memory for reconciliation; never frees live DMA
 * memory because a verbs destroy call failed. Caller keeps the context alive. */
static int peer_wire_verbs_close(void *wc)
{
    struct rdma_conn *c = wc;
    if (!c || !c->in_use)
        return -1;

    rdma_release(c);

    return c->in_use ? -1 : 0;
}

int peer_pair_transport_new(const struct peer_pair_transport_config *cfg,
                            struct ibv_context *verbs,
                            uint8_t port,
                            uint32_t gid,
                            uint16_t mtu,
                            struct peer_pair_transport **out,
                            char *error,
                            size_t error_len)
{
    if (!out)
        return -1;

    *out = NULL;

    struct peer_pair_driver d = {.prepare = peer_wire_verbs_prepare,
                                 .prepare_pair = peer_wire_verbs_prepare_pair,
                                 .activate = peer_wire_verbs_activate,
                                 .send_drained = peer_wire_verbs_send_drained,
                                 .close = peer_wire_verbs_close};
    if (peer_wire_verbs_new(verbs, port, gid, mtu, &d.wire, &d.ctx, error, error_len))
        return -1;
    if (!peer_pair_transport_new_driver(cfg, &d, out))
        return 0;

    d.wire->ctx_free(d.ctx);

    return -1;
}
