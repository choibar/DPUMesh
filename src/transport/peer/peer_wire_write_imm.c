#define _GNU_SOURCE
#include "peer_wire_write_imm.h"
#include "peer_write_imm_counter.h"
#include "peer_write_bootstrap.h"
#include "peer_write_tx_ring.h"
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

#define WIM_CONNECTIONS PEER_RDMA_LANES
#define WIM_DATA_WR 16u
#define WIM_RQ PEER_VERBS_RECV_SLOTS
#define WIM_HELLO PEER_WRITE_IMM_HELLO
/* DATA cannot consume the credit or bootstrap SQ entries. Each direction has
 * its own ring. Credit uses a zero-byte WRITE_WITH_IMM, never ring DATA space. */
#define WIM_SQ (WIM_DATA_WR + 2u)
#define WIM_CQ_DEPTH (WIM_CONNECTIONS * (WIM_SQ + 128u)) /* fixed CQ budget */
#define WIM_BUDGET 128u
#define WIM_SCRATCH (2u * WIM_HELLO)
#define WIM_EPOCH_MAX 0x7fffffu
_Static_assert(WIM_CONNECTIONS <= DMESH_LIVE_SLOTS_MAX, "live connection list capacity");

enum {
    WIM_DATA,
    WIM_RECV,
    WIM_CREDIT,
    WIM_BOOT
};

enum {
    WIM_UNUSED,
    WIM_INIT,
    WIM_CONNECTING,
    WIM_READY,
    WIM_DEAD
};

struct wim_ctx;

struct wim_conn {
    struct wim_ctx *ctx;
    struct ibv_qp *qp;
    struct ibv_mr *ring_mr, *scratch_mr, *tx_ring_mr;
    uint8_t *ring, *scratch, *tx_ring;
    struct peer_write_tx_ring tx_source;
    struct peer_verbs_endpoint endpoint;
    uint32_t index, epoch, peer_qpn, remote_capacity, remote_key;
    uint64_t remote_base, remote_generation, lease_serial;
    uint64_t tx_tail, tx_head, rx_tail, rx_head, credit_sent;
    uint64_t activated_ns, cq_end;
    uint8_t rq_posted[WIM_RQ];
    unsigned state, tx_inflight;
    int used, leased, peer_ready, boot_busy, credit_busy;
};

struct wim_ctx {
    struct ibv_context *verbs; /* borrowed */
    struct ibv_pd *pd;
    struct ibv_comp_channel *cc;
    struct ibv_cq *cq;
    struct ibv_mr *arena_mr;
    void *arena;
    size_t arena_len;
    uint32_t capacity, gid_index, epoch;
    uint16_t mtu;
    uint8_t port, gid[16];
    int epfd, continuation, armed, dead;
    struct peer_wait_diag diag;
    uint64_t diag_last_cq_ns;
    struct dmesh_live_slots active;
    struct wim_conn conns[WIM_CONNECTIONS];
};

static void wim_fail(struct wim_conn *c, const char *reason)
{
    if (c->state != WIM_DEAD)
        fprintf(stderr,
                "PEER_WRITE_IMM fault reason=%s lane=%u epoch=%u qpn=%u\n",
                reason,
                c->index,
                c->epoch,
                c->endpoint.qpn);

    c->state = WIM_DEAD;
}

static uint64_t wim_now(void)
{
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t))
        abort();
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static void wim_put(uint8_t *p, uint64_t v, unsigned n)
{
    for (unsigned i = 0; i < n; i++)
        p[i] = (uint8_t)(v >> (8u * (n - i - 1u)));
}

static uint64_t wim_get(const uint8_t *p, unsigned n)
{
    uint64_t v = 0;
    for (unsigned i = 0; i < n; i++)
        v = (v << 8) | p[i];
    return v;
}

static int wim_capacity(uint32_t c)
{
    long page = sysconf(_SC_PAGESIZE);
    return page > 0 && c >= PEER_WIRE_FRAME_BYTES && c <= PEER_WRITE_IMM_RING_MAX &&
           !(c & (c - 1u)) && c % (uint64_t)page == 0;
}

static enum ibv_mtu wim_mtu(uint16_t n)
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

static uint64_t wim_id(const struct wim_conn *c, unsigned kind, uint32_t low)
{
    return (uint64_t)kind << 62 | (uint64_t)c->epoch << 39 | (uint64_t)c->index << 32 | low;
}

static uint8_t *wim_tx(struct wim_conn *c)
{
    return c->tx_ring + c->tx_tail % c->ctx->capacity;
}

static uint8_t *wim_boot(struct wim_conn *c)
{
    return c->scratch;
}

static uint64_t wim_tx_space(const struct wim_conn *c)
{
    uint64_t remote = c->remote_capacity - (c->tx_tail - c->tx_head);
    uint64_t local = peer_write_tx_ring_space(&c->tx_source, c->ctx->capacity);
    return remote < local ? remote : local;
}

static int wim_map_buffer(uint8_t **out, size_t bytes)
{
    int fd = memfd_create("dpumesh-write-imm", MFD_CLOEXEC | MFD_ALLOW_SEALING);
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

static int wim_post_recv(struct wim_conn *c, unsigned slot)
{
    /* Only the first RQ entry has a buffer for the protected SEND bootstrap.
     * WRITE_WITH_IMM consumes an RQ WQE but needs no SGE. Repost on CQ,
     * independently of consumer progress; ring byte credit protects payload. */
    int bootstrap = slot == 0 && !c->peer_ready;
    struct ibv_sge s = {.addr = (uintptr_t)(wim_boot(c) + WIM_HELLO),
                        .length = WIM_HELLO,
                        .lkey = c->scratch_mr->lkey};
    struct ibv_recv_wr wr = {.wr_id = wim_id(c, WIM_RECV, slot),
                             .sg_list = bootstrap ? &s : NULL, .num_sge = bootstrap ? 1 : 0}, *bad;
    if (ibv_post_recv(c->qp, &wr, &bad))
        return -1;

    peer_wait_diag_add(&c->ctx->diag, PWD_rq_posts, 1);
    c->rq_posted[slot] = 1;

    return 0;
}

static void wim_hello_encode(struct wim_conn *c)
{
    uint8_t *p = wim_boot(c);
    memset(p, 0, WIM_HELLO);
    memcpy(p, "DMWI", 4);
    wim_put(p + 4, 4, 2); /* dmesh4/1 */
    wim_put(p + 6, 1, 2); /* v1, full 32-bit counters */
    wim_put(p + 8, c->ctx->capacity, 4);
    wim_put(p + 12, c->ring_mr->rkey, 4);
    wim_put(p + 16, (uintptr_t)c->ring, 8);
    wim_put(p + 24, c->epoch, 8);
    wim_put(p + 32, c->endpoint.qpn, 4);
    wim_put(p + 36, PEER_WIRE_FRAME_BYTES, 4);
}

static int wim_hello_decode(struct wim_conn *c, const uint8_t *p)
{
    uint32_t capacity = (uint32_t)wim_get(p + 8, 4);
    uint64_t base = wim_get(p + 16, 8), generation = wim_get(p + 24, 8);
    if (c->peer_ready || memcmp(p, "DMWI", 4) || wim_get(p + 4, 2) != 4 || wim_get(p + 6, 2) != 1 ||
        !wim_capacity(capacity) || !base || base > UINT64_MAX - 2ull * capacity || !generation ||
        wim_get(p + 32, 4) != c->peer_qpn || wim_get(p + 36, 4) != PEER_WIRE_FRAME_BYTES)
        return -1;

    for (unsigned i = 40; i < WIM_HELLO; i++)
        if (p[i])
            return -1;

    c->remote_capacity = capacity;
    c->remote_key = (uint32_t)wim_get(p + 12, 4);
    c->remote_base = base;
    c->remote_generation = generation;
    c->peer_ready = 1;

    return 0;
}

/* Validate framing here only to recover byte-ring record boundaries. Pair,
 * stream, sequence and authorization validation remains in peer_pair_wire. */
static size_t wim_frame_size(const uint8_t *p)
{
    return peer_data_frame_size(p);
}

static int wim_ready(struct wim_conn *c)
{
    if (c->ctx->dead || c->state == WIM_DEAD)
        return -1;
    if (c->state == WIM_READY)
        return 1;
    if (c->state == WIM_CONNECTING && wim_now() - c->activated_ns >= 5000000000ull) {
        wim_fail(c, "bootstrap-timeout");
        return -1;
    }

    return 0;
}

static int wim_credit(struct wim_conn *c, int force)
{
    if (c->state != WIM_READY || c->credit_busy || c->credit_sent == c->rx_head)
        return 0;
    if (!force && c->rx_head - c->credit_sent < peer_write_credit_batch_bytes(c->ctx->capacity))
        return 0;

    struct ibv_send_wr wr = {.wr_id = wim_id(c, WIM_CREDIT, 0),
                             .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
                             .send_flags = IBV_SEND_SIGNALED,
                             .imm_data = htonl((uint32_t)c->rx_head)},
                       *bad;
    wr.wr.rdma.remote_addr = c->remote_base;
    wr.wr.rdma.rkey = c->remote_key;

    if (ibv_post_send(c->qp, &wr, &bad)) {
        wim_fail(c, "credit-post");
        return -1;
    }

    peer_wait_diag_add(&c->ctx->diag, PWD_credit_post, 1);
    c->credit_sent = c->rx_head;
    c->credit_busy = 1;

    return 1;
}

static int wim_arena(struct wim_ctx *t, const struct peer_wire_payload *p)
{
    if (t->arena_mr)
        return t->arena == p->region && t->arena_len == p->region_len ? 0 : -1;

    t->arena_mr = ibv_reg_mr(t->pd, p->region, p->region_len, IBV_ACCESS_LOCAL_WRITE);
    if (!t->arena_mr)
        return -1;

    t->arena = p->region;
    t->arena_len = p->region_len;

    return 0;
}

static int wim_post_data(struct wim_conn *c, size_t bytes, const struct peer_wire_payload *p)
{
    if (c->tx_source.tail != c->tx_tail)
        goto fail;
    if (p && (bytes < PEER_WIRE_HEADER_BYTES ||
              !peer_wire_frame_payload_valid(p, bytes - PEER_WIRE_HEADER_BYTES)))
        goto fail;

    const uint8_t *source = p ? p->tx_header : wim_tx(c);
    if (bytes < PEER_WIRE_HEADER_BYTES || bytes > PEER_WIRE_FRAME_BYTES ||
        bytes > wim_tx_space(c) || UINT64_MAX - c->tx_tail < bytes ||
        wim_frame_size(source) != bytes || (p && wim_arena(c->ctx, p)))
        goto fail;

    uint64_t tail = c->tx_tail + bytes;
    struct ibv_sge s = {.addr = (uintptr_t)source,
                        .length = (uint32_t)bytes,
                        .lkey = p ? c->ctx->arena_mr->lkey : c->tx_ring_mr->lkey};
    struct ibv_send_wr wr = {.wr_id = wim_id(c, WIM_DATA, (uint32_t)tail),
                             .sg_list = &s,
                             .num_sge = 1,
                             .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
                             .send_flags = IBV_SEND_SIGNALED,
                             .imm_data = htonl((uint32_t)tail)},
                       *bad;
    wr.wr.rdma.remote_addr = c->remote_base + c->tx_tail % c->remote_capacity;
    wr.wr.rdma.rkey = c->remote_key;

    if (p &&
        peer_write_tx_ring_hold(&c->tx_source, tail, p->owner, p->cookie, p->retain, p->release))
        goto fail;

    /* No relaxed-ordering MR flag. One WR writes the complete record and its
     * immediate produces the receiver CQE. ARM reads only after that CQE.
     * The source is retained separately until LOCAL WRITE CQ or QP destroy.
     * CPU acquire/release alone would not establish NIC visibility; the RC
     * completion/coherent ARM memory contract still needs BF3 validation. */
    if (ibv_post_send(c->qp, &wr, &bad))
        goto fail;
    if (peer_write_tx_ring_post(&c->tx_source, (uint32_t)bytes, c->ctx->capacity))
        goto fail;
    c->tx_inflight++;

    peer_wait_diag_add(&c->ctx->diag, PWD_tx_data_post, 1);
    c->tx_tail = tail;
    return 1;

fail:
    wim_fail(c, "data-validation-or-post");

    return -1;
}

static int wim_send(void *opaque, const void *bytes, size_t len)
{
    struct wim_conn *c = opaque;
    int ready = wim_ready(c);
    if (ready <= 0)
        return ready;
    if (!bytes || len < PEER_WIRE_HEADER_BYTES || len > PEER_WIRE_FRAME_BYTES)
        return -1;
    if (c->leased || len > wim_tx_space(c)) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_ring, !c->leased);
        return 0;
    }

    if (c->tx_inflight == WIM_DATA_WR) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_slot, 1);
        return 0;
    }

    memcpy(wim_tx(c), bytes, len);
    return wim_post_data(c, len, NULL);
}

static int wim_reserve(void *opaque, struct peer_wire_frame_lease *out)
{
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    struct wim_conn *c = opaque;
    int ready = wim_ready(c);
    if (ready <= 0)
        return ready;

    uint64_t space = wim_tx_space(c);

    /* frame_reserve is called BEFORE the stateful encoder knows its record
     * size. Offer the usual full body capacity or would-block; a short lease
     * could turn ordinary byte-credit pressure into a fatal codec failure. */
    if (c->leased || space < PEER_WIRE_FRAME_BYTES) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_ring, !c->leased);
        return 0;
    }
    if (c->lease_serial == UINT64_MAX) {
        c->state = WIM_DEAD;
        return -1;
    }

    if (c->tx_inflight == WIM_DATA_WR) {
        peer_wait_diag_add(&c->ctx->diag, PWD_tx_no_slot, 1);
        return 0;
    }

    c->leased = 1;
    c->lease_serial++;
    *out = (struct peer_wire_frame_lease){.header = wim_tx(c),
                                          .body = wim_tx(c) + PEER_WIRE_HEADER_BYTES,
                                          .body_cap = PEER_WIRE_BODY_BYTES,
                                          .token = {.conn_epoch = c->epoch,
                                                    .lease_generation = c->lease_serial,
                                                    .slot = 0,
                                                    .kind = PEER_WIRE_FRAME_TX}};

    return 1;
}

static int wim_lease_valid(struct wim_conn *c, const struct peer_wire_frame_lease *l)
{
    return c->leased && l->token.slot == 0 && l->token.conn_epoch == c->epoch &&
           l->token.kind == PEER_WIRE_FRAME_TX && l->token.lease_generation == c->lease_serial &&
           l->header == wim_tx(c) && l->body == wim_tx(c) + PEER_WIRE_HEADER_BYTES;
}

static int wim_cancel(void *opaque, struct peer_wire_frame_lease *l)
{
    struct wim_conn *c = opaque;
    if (!l || !wim_lease_valid(c, l))
        return -1;

    c->leased = 0;
    memset(l, 0, sizeof(*l));

    return 0;
}

static int wim_commit(void *opaque,
                      struct peer_wire_frame_lease *l,
                      size_t body,
                      const struct peer_wire_payload *p)
{
    struct wim_conn *c = opaque;
    if (!l || !wim_lease_valid(c, l))
        return -1;

    int valid = body <= l->body_cap && body <= PEER_WIRE_BODY_BYTES &&
                (!p || peer_wire_frame_payload_valid(p, body));
    c->leased = 0;
    memset(l, 0, sizeof(*l));

    if (!valid || wim_ready(c) != 1) {
        wim_fail(c, "frame-commit");
        return -1;
    }

    return wim_post_data(c, body + PEER_WIRE_HEADER_BYTES, p);
}

static long wim_recv(void *opaque, void *out, size_t cap)
{
    struct wim_conn *c = opaque;
    int ready = wim_ready(c);
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
    size_t bytes = wim_frame_size(frame);
    if (!bytes || bytes > available || !out || bytes > cap)
        goto fail;

    /* Draft RX adapter: COPY before returning ring byte credit. Downstream
     * receives its existing channel/arena storage, never a borrowed ring view.
     * Adding zero-copy requires a ring lease + Host-DMA mmap source adapter. */
    memcpy(out, frame, bytes);
    c->rx_head += bytes;

    if (wim_credit(c, 0) < 0)
        return -1;

    return (long)bytes;

fail:
    wim_fail(c, "rx-framing");

    return -1;
}

static void wim_completion(struct wim_ctx *t, const struct ibv_wc *wc)
{
    unsigned index = (unsigned)(wc->wr_id >> 32) & 0x7fu;
    /* DATA carries a low32 byte end; RECV carries an RQ index. */
    uint32_t low = (uint32_t)wc->wr_id;
    uint32_t epoch = (uint32_t)(wc->wr_id >> 39) & WIM_EPOCH_MAX;
    unsigned kind = (unsigned)(wc->wr_id >> 62);
    if (index >= WIM_CONNECTIONS)
        return;

    struct wim_conn *c = &t->conns[index];
    if (!c->used || c->epoch != epoch) {
        peer_wait_diag_add(&t->diag, PWD_cq_stale, 1);
        return; /* retired generation CQE */
    }
    if (wc->status != IBV_WC_SUCCESS || wc->qp_num != c->endpoint.qpn)
        goto fail;
    if (c->state == WIM_DEAD)
        return; /* failed WRs keep source holds until destroy */

    peer_wait_diag_add(&t->diag,
                       kind == WIM_DATA     ? PWD_cq_data_done
                       : kind == WIM_CREDIT ? PWD_cq_credit_done
                       : kind == WIM_BOOT   ? PWD_cq_boot_done
                                            : (wc->opcode == IBV_WC_RECV ? PWD_cq_rx_boot
                                               : !wc->byte_len           ? PWD_cq_rx_credit
                                                                         : PWD_cq_rx_data),
                       1);

    if (kind == WIM_DATA) {
        uint64_t end;
        if (!c->tx_inflight || wc->opcode != IBV_WC_RDMA_WRITE ||
            peer_write_imm_extend(c->cq_end, low, c->tx_tail - c->cq_end, &end) != 1)
            goto fail;
        c->cq_end = end;
        c->tx_inflight--;
    } else if (kind == WIM_CREDIT) {
        if (low || !c->credit_busy || wc->opcode != IBV_WC_RDMA_WRITE)
            goto fail;

        c->credit_busy = 0;
    } else if (kind == WIM_BOOT) {
        if (low || !c->boot_busy || wc->opcode != IBV_WC_SEND)
            goto fail;

        c->boot_busy = 0;
    } else {
        if (low >= WIM_RQ || !c->rq_posted[low] ||
            (wc->opcode != IBV_WC_RECV && wc->opcode != IBV_WC_RECV_RDMA_WITH_IMM))
            goto fail;

        c->rq_posted[low] = 0;

        if (wc->opcode == IBV_WC_RECV) {
            if (low || c->state != WIM_CONNECTING || wc->byte_len != WIM_HELLO ||
                (wc->wc_flags & IBV_WC_WITH_IMM) ||
                wim_hello_decode(c, wim_boot(c) + WIM_HELLO))
                goto fail;
        } else {
            if (!c->peer_ready || !(wc->wc_flags & IBV_WC_WITH_IMM))
                goto fail;

            uint64_t next;
            uint32_t low = ntohl(wc->imm_data);
            if (!wc->byte_len) {
                if (peer_write_imm_extend(c->tx_head, low, c->tx_tail - c->tx_head, &next) < 0)
                    goto fail;

                c->tx_head = next;
            } else {
                if (wc->byte_len < PEER_WIRE_HEADER_BYTES || wc->byte_len > PEER_WIRE_FRAME_BYTES ||
                    peer_write_imm_extend(
                        c->rx_tail, low, t->capacity - (c->rx_tail - c->rx_head), &next) != 1 ||
                    next - c->rx_tail != wc->byte_len)
                    goto fail;

                c->rx_tail = next;
            }
        }
        if (wim_post_recv(c, low))
            goto fail;
    }

    if (c->state == WIM_CONNECTING && c->peer_ready && !c->boot_busy)
        c->state = WIM_READY;
    return;

fail:
    peer_wait_diag_add(&t->diag, PWD_cq_error, 1);
    peer_wait_diag_note(
        &t->diag, "cq_error", 1, wc->wr_id, wc->status, wc->qp_num, wc->vendor_err, 0, 0);

    /* On error only wr_id/status/qp_num/vendor_err are defined by verbs. */
    if (c->state != WIM_DEAD)
        fprintf(stderr,
                "PEER_WRITE_IMM CQ fault lane=%u epoch=%u kind=%u low=%u status=%u vendor=%u\n",
                c->index,
                c->epoch,
                kind,
                low,
                (unsigned)wc->status,
                wc->vendor_err);

    c->state = WIM_DEAD;
}

static void wim_continue(struct wim_ctx *t)
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

static void wim_drain_events(struct wim_ctx *t)
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

static int wim_poll(struct wim_ctx *t)
{
    int moved = 0;
    unsigned consumed = 0;

    while (!t->dead && consumed < WIM_BUDGET) {
        struct ibv_wc wc[PEER_RDMA_CQ_BATCH];
        unsigned remaining = WIM_BUDGET - consumed;
        int want = remaining < PEER_RDMA_CQ_BATCH ? (int)remaining : PEER_RDMA_CQ_BATCH;
        uint64_t stamp = peer_wait_diag_begin(&t->diag);
        int n = ibv_poll_cq(t->cq, want, wc);
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
            wim_completion(t, &wc[i]);
    }

    if (consumed == WIM_BUDGET)
        wim_continue(t); /* unread CQEs need another visit */

    for (unsigned k = 0; !t->dead && k < t->active.count; k++) {
        struct wim_conn *c = &t->conns[t->active.ids[k]];
        if (!c->used || wim_ready(c) != 1)
            continue;

        /* Fold all CQEs observed in this poll into one completed byte range. */
        if (c->cq_end != c->tx_source.completed) {
            if (peer_write_tx_ring_done(&c->tx_source, c->cq_end)) {
                wim_fail(c, "local-completion-range");
                moved = 1;
                continue;
            }
            moved = 1;
        }

        int rc = wim_credit(c, 0);
        if (rc)
            moved = 1;
    }

    return moved || t->dead;
}

static int wim_progress(void *opaque, void **accepted, int max, int *naccepted)
{
    struct wim_ctx *t = opaque;
    (void)accepted;
    (void)max;
    *naccepted = 0;
    peer_wait_diag_add(&t->diag, PWD_progress_calls, 1);

    if (t->dead)
        return 0;

    /* Active work uses userspace CQ/counter polling. Drain old channel hints
     * and arm interrupts ONLY after that work is exhausted. A second full
     * poll after arming prevents sleeping past an arrival at this boundary. */
    int moved = wim_poll(t);
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
            wim_drain_events(t);
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

    moved = wim_poll(t) || t->dead;
    peer_wait_diag_add(&t->diag, moved ? PWD_progress_moved : PWD_idle_return, 1);
    peer_wait_diag_note(&t->diag, "idle_scan", 0, hints, t->armed, moved, t->active.count, 0, 0);

    if (!moved && t->diag.enabled) {
        uint64_t posted = 0, reserved = 0, rx_bytes = 0, tx_bytes = 0, rq = 0, credit = 0;
        for (unsigned k = 0; k < t->active.count; k++) {
            struct wim_conn *c = &t->conns[t->active.ids[k]];
            posted += c->tx_tail - c->tx_source.completed;
            reserved += !!c->leased;

            for (unsigned j = 0; j < WIM_RQ; j++)
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

int peer_write_imm_close(void *opaque)
{
    struct wim_conn *c = opaque;
    if (!c || !c->used)
        return -1;

    c->state = WIM_DEAD;

    /* Each new ring has a fresh QP/MR. Destroy fences local DMA and invalidates
     * the remote QP path before rkey/backing reuse. Failed fences quarantine
     * ALL dependent storage; a timeout never authorizes reuse. */
    if (c->qp) {
        if (ibv_destroy_qp(c->qp))
            return -1;

        c->qp = NULL;
    }

    peer_write_tx_ring_fenced(&c->tx_source);

    if (c->ring_mr) {
        if (ibv_dereg_mr(c->ring_mr))
            return -1;

        c->ring_mr = NULL;
    }

    if (c->scratch_mr) {
        if (ibv_dereg_mr(c->scratch_mr))
            return -1;

        c->scratch_mr = NULL;
    }

    if (c->ring)
        munmap(c->ring, 2ull * c->ctx->capacity);
    if (c->tx_ring_mr) {
        if (ibv_dereg_mr(c->tx_ring_mr))
            return -1;
        c->tx_ring_mr = NULL;
    }
    if (c->tx_ring) {
        munmap(c->tx_ring, 2ull * c->ctx->capacity);
        c->tx_ring = NULL;
    }

    free(c->scratch);
    dmesh_live_remove(&c->ctx->active, c->index);
    memset(c, 0, sizeof(*c));

    return 0;
}

static void wim_close(void *opaque)
{
    (void)peer_write_imm_close(opaque);
}

static int wim_established(void *opaque)
{
    return wim_ready(opaque);
}

static int wim_faulted(void *opaque)
{
    return wim_ready(opaque) < 0;
}

static int wim_epfd(void *opaque)
{
    return ((struct wim_ctx *)opaque)->epfd;
}

static void wim_ctx_free(void *opaque)
{
    struct wim_ctx *t = opaque;
    if (!t)
        return;

    for (unsigned i = 0; i < WIM_CONNECTIONS; i++)
        if (t->conns[i].used && peer_write_imm_close(&t->conns[i]))
            return;

    /* Retrieved channel events must be ACKed before CQ destruction. Drain
     * even after a context fault; no QP/device access remains at this point. */
    if (t->cc && t->cq)
        wim_drain_events(t);
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

    if (t->pd) {
        if (ibv_dealloc_pd(t->pd))
            return;

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
static const struct peer_wire_ops WIM_OPS = {.progress = wim_progress,
                                             .send_msg = wim_send,
                                             .recv_msg = wim_recv,
                                             .established = wim_established,
                                             .faulted = wim_faulted,
                                             .close = wim_close,
                                             .epfd = wim_epfd,
                                             .ctx_free = wim_ctx_free,
                                             .frame_reserve = wim_reserve,
                                             .frame_commit = wim_commit,
                                             .frame_cancel = wim_cancel};

static int wim_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int wim_epoll(int epfd, int fd)
{
    struct epoll_event ev = {.events = EPOLLIN, .data = {.fd = fd}};
    return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

int peer_wire_write_imm_new(struct ibv_context *verbs,
                            uint8_t port,
                            uint32_t gid,
                            uint16_t mtu,
                            uint32_t capacity,
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
    if (!verbs || !port || gid > 255 || !wim_mtu(mtu) || !wim_capacity(capacity) ||
        ibv_query_port(verbs, port, &attr) || attr.state != IBV_PORT_ACTIVE ||
        attr.link_layer != IBV_LINK_LAYER_ETHERNET || attr.active_mtu < wim_mtu(mtu) ||
        ibv_query_gid_ex(verbs, port, gid, &entry, 0) || entry.gid_type != IBV_GID_TYPE_ROCE_V2 ||
        memcmp(entry.gid.raw, prefix, sizeof(prefix))) {
        if (error && error_len)
            snprintf(
                error,
                error_len,
                "WRITE_IMM needs active IPv4 RoCEv2, valid MTU and page-aligned power-of-two ring (64KiB..16MiB)");
        return -1;
    }

    struct wim_ctx *t = calloc(1, sizeof(*t));
    if (!t) {
        if (error && error_len)
            snprintf(error, error_len, "WRITE_IMM context allocation: %s", strerror(errno));
        return -1;
    }

    peer_wait_diag_init(&t->diag, "peer_wire_write_imm");
    t->verbs = verbs;
    t->port = port;
    t->gid_index = gid;
    t->mtu = mtu;
    t->capacity = capacity;
    t->epfd = t->continuation = -1;
    memcpy(t->gid, entry.gid.raw, 16);
    t->pd = ibv_alloc_pd(verbs);
    if (!t->pd)
        goto fail;

    t->cc = ibv_create_comp_channel(verbs);
    if (!t->cc || wim_nonblock(t->cc->fd))
        goto fail;

    t->cq = ibv_create_cq(verbs, WIM_CQ_DEPTH, t, t->cc, 0);
    if (!t->cq || ibv_req_notify_cq(t->cq, 0))
        goto fail;

    t->armed = 1;
    t->epfd = epoll_create1(EPOLL_CLOEXEC);
    t->continuation = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (t->epfd < 0 || t->continuation < 0 || wim_epoll(t->epfd, t->cc->fd) ||
        wim_epoll(t->epfd, t->continuation))
        goto fail;

    *ops = &WIM_OPS;
    *out = t;

    if (error && error_len)
        error[0] = 0;
    return 0;

fail:
    if (error && error_len)
        snprintf(error, error_len, "WRITE_IMM resource setup: %s", strerror(errno));
    wim_ctx_free(t);

    return -1;
}

int peer_write_imm_prepare(void *opaque, void **out, struct peer_verbs_endpoint *ep)
{
    if (!out || !ep)
        return -1;

    *out = NULL;
    memset(ep, 0, sizeof(*ep));

    struct wim_ctx *t = opaque;
    if (!t || t->dead || t->epoch == WIM_EPOCH_MAX)
        return -1;

    struct wim_conn *c = NULL;
    for (unsigned i = 0; i < WIM_CONNECTIONS; i++)
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
    c->state = WIM_INIT;
    dmesh_live_add(&t->active, c->index);

    const char *stage = "alias-mapping";
    if (wim_map_buffer(&c->ring, t->capacity))
        goto fail;

    stage = "scratch-allocation";
    if (wim_map_buffer(&c->tx_ring, t->capacity))
        goto fail;
    c->tx_ring_mr = ibv_reg_mr(t->pd, c->tx_ring, 2ull * t->capacity, IBV_ACCESS_LOCAL_WRITE);
    if (!c->tx_ring_mr)
        goto fail;

    c->scratch = aligned_alloc(64, WIM_SCRATCH);
    if (!c->scratch)
        goto fail;

    memset(c->scratch, 0, WIM_SCRATCH);
    stage = "alias-remote-write-registration";
    c->ring_mr = ibv_reg_mr(
        t->pd, c->ring, 2ull * t->capacity, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!c->ring_mr)
        goto fail;

    stage = "scratch-registration";
    c->scratch_mr = ibv_reg_mr(t->pd, c->scratch, WIM_SCRATCH, IBV_ACCESS_LOCAL_WRITE);
    if (!c->scratch_mr)
        goto fail;

    struct ibv_qp_init_attr attr = {
        .send_cq = t->cq,
        .recv_cq = t->cq,
        .qp_type = IBV_QPT_RC,
        .cap = {
            .max_send_wr = WIM_SQ, .max_recv_wr = WIM_RQ, .max_send_sge = 1, .max_recv_sge = 1}};
    stage = "QP-create";
    c->qp = ibv_create_qp(t->pd, &attr);
    if (!c->qp)
        goto fail;

    struct ibv_qp_attr init = {
        .qp_state = IBV_QPS_INIT, .port_num = t->port, .qp_access_flags = IBV_ACCESS_REMOTE_WRITE};
    stage = "QP-init";

    if (ibv_modify_qp(
            c->qp, &init, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
        goto fail;

    stage = "notification-RQ";

    for (unsigned i = 0; i < WIM_RQ; i++)
        if (wim_post_recv(c, i))
            goto fail;

    stage = "PSN-random";

    uint32_t psn;
    ssize_t n;

    do {
        n = getrandom(&psn, sizeof(psn), 0);
    } while (n < 0 && errno == EINTR);

    if (n != sizeof(psn))
        goto fail;

    c->endpoint =
        (struct peer_verbs_endpoint){.qpn = c->qp->qp_num, .psn = psn & 0xffffff, .mtu = t->mtu};
    memcpy(c->endpoint.gid, t->gid, 16);
    *ep = c->endpoint;
    *out = c;
    return 0;

fail:
    fprintf(stderr, "PEER_WRITE_IMM prepare failed stage=%s errno=%d\n", stage, errno);

    if (peer_write_imm_close(c))
        *out = c;

    return -1;
}

int peer_write_imm_prepare_pair(void *ctx,
                                const struct dmesh_peer_pair *pair,
                                void **out,
                                struct peer_verbs_endpoint *ep)
{
    return pair ? peer_write_imm_prepare(ctx, out, ep) : -1;
}

int peer_write_imm_activate(void *opaque, const struct peer_verbs_endpoint *ep, int protected)
{
    struct wim_conn *c = opaque;
    static const uint8_t prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (!c || !c->used || c->ctx->dead || c->state != WIM_INIT || protected != 1 || !ep ||
        !ep->qpn || ep->qpn > 0xffffff || ep->psn > 0xffffff || ep->mtu != c->endpoint.mtu ||
        memcmp(ep->gid, prefix, 12))
        return -1;

    struct wim_ctx *t = c->ctx;
    struct ibv_qp_attr a = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = wim_mtu(ep->mtu),
        .dest_qp_num = ep->qpn,
        .rq_psn = ep->psn,
        .min_rnr_timer = PEER_RDMA_MIN_RNR_TIMER,
        .ah_attr = {.is_global = 1,
                    .port_num = t->port,
                    .grh = {.sgid_index = (uint8_t)t->gid_index, .hop_limit = 64}}};
    memcpy(a.ah_attr.grh.dgid.raw, ep->gid, 16);

    if (ibv_modify_qp(c->qp,
                      &a,
                      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                          IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
        goto fail;

    memset(&a, 0, sizeof(a));
    a.qp_state = IBV_QPS_RTS;
    a.sq_psn = c->endpoint.psn;
    a.timeout = 14;
    a.retry_cnt = a.rnr_retry = 7;

    if (ibv_modify_qp(c->qp,
                      &a,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
        goto fail;

    c->state = WIM_CONNECTING;
    c->activated_ns = wim_now();
    c->peer_qpn = ep->qpn;
    wim_hello_encode(c);

    struct ibv_sge s = {
        .addr = (uintptr_t)wim_boot(c), .length = WIM_HELLO, .lkey = c->scratch_mr->lkey};
    struct ibv_send_wr wr = {.wr_id = wim_id(c, WIM_BOOT, 0),
                             .sg_list = &s,
                             .num_sge = 1,
                             .opcode = IBV_WR_SEND,
                             .send_flags = IBV_SEND_SIGNALED},
                       *bad;

    /* Only after the manager's RX/TX IPsec install barrier. No rkey/address is
     * added to the existing TLS wire format, exposed to a Pod, or logged.
     * Both opt-in peers must validate this fixed canonical bootstrap first. */
    if (ibv_post_send(c->qp, &wr, &bad))
        goto fail;

    c->boot_busy = 1;
    return 0;

fail:
    c->state = WIM_DEAD;

    return -1;
}

int peer_write_imm_send_drained(void *opaque)
{
    struct wim_conn *c = opaque;
    if (!c || !c->used || wim_ready(c) < 0)
        return -1;

    /* QUIESCE must flush the final partial watermark even with no new DATA.
     * The owning worker still progresses the reserved credit submission/completion. */
    if (wim_credit(c, 1) < 0)
        return -1;
    if (c->leased || c->boot_busy || c->credit_busy || c->credit_sent != c->rx_head)
        return 0;

    if (c->tx_source.completed != c->tx_tail || c->tx_inflight)
        return 0;

    return 1;
}

int peer_pair_write_imm_new(const struct peer_pair_transport_config *cfg,
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

    struct peer_pair_driver d = {.prepare = peer_write_imm_prepare,
                                 .prepare_pair = peer_write_imm_prepare_pair,
                                 .activate = peer_write_imm_activate,
                                 .send_drained = peer_write_imm_send_drained,
                                 .close = peer_write_imm_close};

    /* Keep progress enabled with zero live QPs: retired CQEs/channel events
     * can still need draining. Skipping them would leave the wait FD readable
     * and spin the ARM worker after the last pair closes. Idle progress is 0. */
    if (peer_wire_write_imm_new(verbs, port, gid, mtu, bytes, &d.wire, &d.ctx, error, error_len))
        return -1;

    struct peer_pair_transport_config copy = *cfg;
    copy.rx_pool = NULL; /* explicit copied RX adapter, not proxy arena RX */

    if (!peer_pair_transport_new_driver(&copy, &d, out))
        return 0;

    d.wire->ctx_free(d.ctx);

    return -1;
}
