/* WRITE_WITH_IMM + DPA carrier. ARM publishes descriptors; DPA owns NIC WQEs and CQEs. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "peer_write_imm_dpa.h"
#include "peer_imm_native.h"
#include "peer_dpa_owner.h"
#include "peer_config.h"
#include "peer_write_protocol.h"

#include "peer_control_protocol.h"
#include "peer_profile.h"
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

/* ARM setup helpers private to this carrier. */
static inline enum ibv_mtu peer_verbs_mtu(uint16_t bytes)
{
    switch (bytes) {
    case 256: return IBV_MTU_256;
    case 512: return IBV_MTU_512;
    case 1024: return IBV_MTU_1024;
    case 2048: return IBV_MTU_2048;
    case 4096: return IBV_MTU_4096;
    default: return 0;
    }
}

static inline uint64_t peer_verbs_now_ns(void)
{
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t))
        abort();
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static inline int peer_write_capacity_valid(uint32_t capacity)
{
    long page = sysconf(_SC_PAGESIZE);
    return page > 0 && peer_write_capacity_in_range(capacity) && capacity % (uint64_t)page == 0;
}

/* Two adjacent mappings of one sealed backing object make a wrapped record
 * contiguous. The caller unregisters memory before unmapping 2 * bytes. */
static inline int peer_write_map_buffer(uint8_t **out, size_t bytes, const char *name)
{
    int fd = memfd_create(name, MFD_CLOEXEC | MFD_ALLOW_SEALING);
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
    int ok = mmap(base, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) != MAP_FAILED &&
        mmap((uint8_t *)base + bytes, bytes, PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_FIXED, fd, 0) != MAP_FAILED;
    close(fd);
    if (!ok) {
        munmap(base, 2 * bytes);
        return -1;
    }
    *out = base;
    return 0;
}

/* ---- TX byte ranges and source ownership ------------------------------ */

/* Logical byte positions never wrap. Physical addresses use position % capacity.
 * A completion certifies the whole submitted prefix [completed, end), not an
 * individual frame. Only borrowed arena sources need ownership records. */
struct peer_write_tx_hold {
    struct peer_write_tx_hold *next;
    uint64_t end;
    void *owner, *cookie;
    void (*retain)(void *, void *);
    void (*release)(void *, void *);
};

struct peer_write_tx_ring {
    uint64_t tail, completed;
    struct peer_write_tx_hold *first, *last;
    unsigned holds;
};

static inline uint64_t peer_write_tx_ring_space(const struct peer_write_tx_ring *r,
                                                uint32_t capacity)
{
    return capacity - (r->tail - r->completed);
}

/* Reserve the source reference BEFORE posting: a partial/failed submission is
 * quarantined until QP destruction. Adjacent uses of the same arena owner share
 * one retained reference, extended through the last byte that needs it. */
static inline int peer_write_tx_ring_hold(struct peer_write_tx_ring *r,
                                          uint64_t end,
                                          void *owner,
                                          void *cookie,
                                          void (*retain)(void *, void *),
                                          void (*release)(void *, void *))
{
    if (end <= r->tail || !retain || !release)
        return -1;

    struct peer_write_tx_hold *h = r->last;
    if (h && h->end == r->tail && h->owner == owner && h->cookie == cookie && h->retain == retain &&
        h->release == release) {
        h->end = end;
        return 0;
    }

    h = malloc(sizeof(*h));
    if (!h)
        return -1;

    *h = (struct peer_write_tx_hold){
        .end = end, .owner = owner, .cookie = cookie, .retain = retain, .release = release};
    retain(owner, cookie);
    if (r->last)
        r->last->next = h;
    else
        r->first = h;
    r->last = h;
    r->holds++;
    return 0;
}

static inline int
peer_write_tx_ring_post(struct peer_write_tx_ring *r, uint32_t bytes, uint32_t capacity)
{
    if (!bytes || r->completed > r->tail || r->tail - r->completed > capacity ||
        bytes > peer_write_tx_ring_space(r, capacity) || UINT64_MAX - r->tail < bytes)
        return -1;

    r->tail += bytes;
    return 0;
}

static inline int peer_write_tx_ring_done(struct peer_write_tx_ring *r, uint64_t end)
{
    if (end < r->completed || end > r->tail)
        return -1;

    r->completed = end;
    while (r->first && r->first->end <= end) {
        struct peer_write_tx_hold *h = r->first;
        r->first = h->next;
        if (!r->first)
            r->last = NULL;
        r->holds--;
        h->release(h->owner, h->cookie);
        free(h);
    }
    return 0;
}

/* Call only AFTER a successful QP destruction fence, including partial posts
 * whose source hold extends past the last successfully submitted tail. */
static inline void peer_write_tx_ring_fenced(struct peer_write_tx_ring *r)
{
    while (r->first) {
        struct peer_write_tx_hold *h = r->first;
        r->first = h->next;
        h->release(h->owner, h->cookie);
        free(h);
    }
    r->last = NULL;
    r->holds = 0;
    /* Destruction authorizes releasing sources, but may have aborted WRs.
     * Preserve the last CQ-certified end for teardown diagnostics. */
}

#define WIM_CARRIER_CONNECTIONS PEER_RDMA_LANES
/* One ARM bootstrap buffer; the DPA owns the notification receive queue. */
#define WIM_CARRIER_RQ 1u
_Static_assert(WIM_CARRIER_CONNECTIONS <= DMESH_LIVE_SLOTS_MAX, "live connection list capacity");

struct wim_carrier_ctx;

struct wim_carrier_conn {
    struct wim_carrier_ctx *ctx;
    struct peer_wim_qp *dpa_qp;
    struct ibv_mr *ring_mr, *scratch_mr;
    uint8_t *ring, *scratch;
    uint8_t *tx_ring;
    struct ibv_mr *tx_ring_mr;
    struct peer_write_tx_ring tx_source;
    struct peer_verbs_endpoint endpoint;
    uint32_t index, epoch, peer_qpn, remote_capacity;
    uint64_t lease_serial;
    uint64_t tx_tail, tx_head, rx_tail, rx_head, credit_sent;
    uint64_t activated_ns, dpa_consumed;
    uint8_t rq_posted[WIM_CARRIER_RQ];
    unsigned state;
    int used, leased, peer_ready, boot_busy, credit_busy;
};

struct wim_carrier_ctx {
    struct ibv_pd *pd;         /* borrowed from real DOCA device */
    struct peer_rdma_device *device;
    struct peer_wim *dpa;
    struct ibv_mr *arena_mr;
    void *arena;
    size_t arena_len;
    uint32_t capacity, gid_index, epoch;
    uint16_t mtu;
    uint8_t gid[16];
    int dead;
    struct peer_profile profile;
    uint64_t profile_last_cq_ns, profile_last_dpa_ns;
    struct dmesh_live_slots active;
    struct wim_carrier_conn conns[WIM_CARRIER_CONNECTIONS];
};

static int wim_carrier_retire(void *);

static int wim_carrier_submit(struct wim_carrier_conn *c, struct ibv_send_wr *wr)
{
    return peer_wim_send(c->dpa_qp, wr);
}

static void wim_carrier_fail(struct wim_carrier_conn *c, const char *reason)
{
    if (c->state != PEER_WRITE_DEAD)
        fprintf(stderr,
                "PEER_WRITE_IMM_DPA fault reason=%s lane=%u epoch=%u qpn=%u\n",
                reason,
                c->index,
                c->epoch,
                c->endpoint.qpn);

    c->state = PEER_WRITE_DEAD;
}

static uint64_t wim_carrier_id(const struct wim_carrier_conn *c, unsigned kind, uint32_t low)
{
    return peer_write_wr_id(kind, c->epoch, c->index, low);
}

static uint8_t *wim_carrier_tx(struct wim_carrier_conn *c)
{
    return c->tx_ring + c->tx_tail % c->ctx->capacity;
}

static uint8_t *wim_carrier_boot(struct wim_carrier_conn *c)
{
    return c->scratch;
}

static uint64_t wim_carrier_tx_space(const struct wim_carrier_conn *c)
{
    uint64_t remote = c->remote_capacity - (c->tx_tail - c->tx_head);
    uint64_t local = peer_write_tx_ring_space(&c->tx_source, c->ctx->capacity);
    return remote < local ? remote : local;
}

static int wim_carrier_tx_admit(struct wim_carrier_conn *c)
{
    int admit = peer_wim_admit(c->dpa_qp, 1);
    if (admit < 0)
        wim_carrier_fail(c, "descriptor-admission");
    return admit;
}

static int wim_carrier_post_recv(struct wim_carrier_conn *c, unsigned slot)
{
    /* The DPA posts this bootstrap buffer and the zero-byte IMM RQ entries. */
    struct ibv_sge s = {.addr = (uintptr_t)(wim_carrier_boot(c) + (slot + 1u) * PEER_WRITE_IMM_HELLO),
                        .length = PEER_WRITE_IMM_HELLO,
                        .lkey = c->scratch_mr->lkey};
    struct ibv_recv_wr wr = {.wr_id = wim_carrier_id(c, PEER_WRITE_RECV, slot), .sg_list = &s, .num_sge = 1};
    if (peer_wim_recv(c->dpa_qp, &wr))
        return -1;

    peer_profile_add(&c->ctx->profile, PEER_PROFILE_rq_posts, 1);
    c->rq_posted[slot] = 1;

    return 0;
}

static int wim_carrier_hello_encode(struct wim_carrier_conn *c)
{
    struct peer_write_hello h = {.capacity = c->ctx->capacity, .key = c->ring_mr->rkey,
        .base = (uintptr_t)c->ring, .generation = c->epoch, .qpn = c->endpoint.qpn};
    peer_write_hello_encode(wim_carrier_boot(c), &h);
    return 0;
}

static int wim_carrier_hello_decode(struct wim_carrier_conn *c, const uint8_t *p)
{
    struct peer_write_hello h;
    if (c->peer_ready || peer_write_hello_decode(p, c->peer_qpn, &h) ||
        !peer_write_capacity_valid(h.capacity))
        return -1;
    for (unsigned i = PEER_WRITE_HELLO_HEADER; i < 64; i++)
        if (p[i])
            return -1;

    if (peer_wim_configure_native(c->dpa_qp,
                                  h.base,
                                  h.key,
                                  h.capacity))
        return -1;

    c->remote_capacity = h.capacity;
    c->peer_ready = 1;
    return 0;

}

static int wim_carrier_ready(struct wim_carrier_conn *c)
{
    if (c->ctx->dead || c->state == PEER_WRITE_DEAD)
        return -1;
    if (c->state == PEER_WRITE_READY)
        return 1;
    if (c->state == PEER_WRITE_CONNECTING && peer_verbs_now_ns() - c->activated_ns >= PEER_WRITE_BOOTSTRAP_TIMEOUT_NS) {
        peer_wim_debug(c->dpa_qp);
        wim_carrier_fail(c, "bootstrap-timeout");
        return -1;
    }

    return 0;
}

static int wim_carrier_credit(struct wim_carrier_conn *c, int force)
{
    if (c->state != PEER_WRITE_READY || c->credit_busy || c->credit_sent == c->rx_head)
        return 0;
    if (!force && c->rx_head - c->credit_sent < peer_write_credit_batch_bytes(c->ctx->capacity))
        return 0;

    /* Encoding may hold a frame lease across progress calls. Leave its
     * descriptor available so a credit cannot steal a promised commit. */
    int admit = peer_wim_admit(c->dpa_qp, 1u + !!c->leased);
    if (admit < 0)
        wim_carrier_fail(c, "credit-admission");
    if (admit <= 0)
        return admit;

    int rc = peer_wim_head(c->dpa_qp, wim_carrier_id(c, PEER_WRITE_CREDIT, 0),
                           c->rx_head - c->credit_sent);
    if (rc) {
        wim_carrier_fail(c, "credit-post");
        return -1;
    }

    peer_profile_add(&c->ctx->profile, PEER_PROFILE_credit_post, 1);
    c->credit_sent = c->rx_head;
    c->credit_busy = 1;

    return 1;
}

static int wim_carrier_arena(struct wim_carrier_ctx *t, const struct peer_carrier_payload *p)
{
    if (t->arena_mr)
        return t->arena == p->region && t->arena_len == p->region_len ? 0 : -1;

    t->arena_mr = ibv_reg_mr(t->pd, p->region, p->region_len, IBV_ACCESS_LOCAL_WRITE);
    if (!t->arena_mr)
        return -1;
    if (peer_wim_register(t->dpa, p->region, p->region_len, t->arena_mr->lkey))
        return -1;
    t->arena = p->region;
    t->arena_len = p->region_len;

    return 0;
}

static int wim_carrier_post_data(struct wim_carrier_conn *c, size_t bytes, const struct peer_carrier_payload *p)
{
    if (c->tx_source.tail != c->tx_tail)
        goto fail;
    if (p && (bytes < PEER_CARRIER_HEADER_BYTES ||
              !peer_carrier_frame_payload_valid(p, bytes - PEER_CARRIER_HEADER_BYTES)))
        goto fail;

    const uint8_t *source = p ? p->tx_header : wim_carrier_tx(c);
    if (bytes < PEER_CARRIER_HEADER_BYTES || bytes > PEER_CARRIER_FRAME_BYTES ||
        bytes > wim_carrier_tx_space(c) || UINT64_MAX - c->tx_tail < bytes ||
        peer_data_frame_size(source) != bytes || (p && wim_carrier_arena(c->ctx, p)))
        goto fail;

    uint64_t tail = c->tx_tail + bytes;
    struct ibv_sge s = {.addr = (uintptr_t)source,
                        .length = (uint32_t)bytes,
                        .lkey = p ? c->ctx->arena_mr->lkey : c->tx_ring_mr->lkey};
    if (p &&
        peer_write_tx_ring_hold(&c->tx_source, tail, p->owner, p->cookie, p->retain, p->release))
        goto fail;

    /* Retain the borrowed source before publishing a descriptor. On failure,
     * release it only after destroying the native QP fences device access. */
    int rc = peer_wim_data(c->dpa_qp, tail, s.addr, s.lkey, s.length);
    if (rc)
        goto fail;
    if (peer_write_tx_ring_post(&c->tx_source, (uint32_t)bytes, c->ctx->capacity))
        goto fail;

    peer_profile_add(&c->ctx->profile, PEER_PROFILE_tx_data_post, 1);
    c->tx_tail = tail;
    return 1;

fail:
    wim_carrier_fail(c, "data-validation-or-post");

    return -1;
}

static int wim_carrier_send(void *opaque, const void *bytes, size_t len)
{
    struct wim_carrier_conn *c = opaque;
    int ready = wim_carrier_ready(c);
    if (ready <= 0)
        return ready;
    if (!bytes || len < PEER_CARRIER_HEADER_BYTES || len > PEER_CARRIER_FRAME_BYTES)
        return -1;
    if (c->leased || len > wim_carrier_tx_space(c)) {
        peer_profile_add(&c->ctx->profile, PEER_PROFILE_tx_no_ring, !c->leased);
        return 0;
    }

    int admit = wim_carrier_tx_admit(c);
    if (admit <= 0) {
        peer_profile_add(&c->ctx->profile, PEER_PROFILE_tx_no_slot, admit == 0);
        return admit;
    }

    memcpy(wim_carrier_tx(c), bytes, len);
    return wim_carrier_post_data(c, len, NULL);
}

static int wim_carrier_reserve(void *opaque, struct peer_carrier_frame_lease *out)
{
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    struct wim_carrier_conn *c = opaque;
    int ready = wim_carrier_ready(c);
    if (ready <= 0)
        return ready;

    uint64_t space = wim_carrier_tx_space(c);

    /* frame_reserve is called BEFORE the stateful encoder knows its record
     * size. Offer the usual full body capacity or would-block; a short lease
     * could turn ordinary byte-credit pressure into a fatal codec failure. */
    if (c->leased || space < PEER_CARRIER_FRAME_BYTES) {
        peer_profile_add(&c->ctx->profile, PEER_PROFILE_tx_no_ring, !c->leased);
        return 0;
    }
    if (c->lease_serial == UINT64_MAX) {
        c->state = PEER_WRITE_DEAD;
        return -1;
    }

    int admit = wim_carrier_tx_admit(c);
    if (admit <= 0) {
        peer_profile_add(&c->ctx->profile, PEER_PROFILE_tx_no_slot, admit == 0);
        return admit;
    }

    c->leased = 1;
    c->lease_serial++;
    *out = (struct peer_carrier_frame_lease){.header = wim_carrier_tx(c),
                                          .body = wim_carrier_tx(c) + PEER_CARRIER_HEADER_BYTES,
                                          .body_cap = PEER_CARRIER_BODY_BYTES,
                                          .token = {.conn_epoch = c->epoch,
                                                    .lease_generation = c->lease_serial,
                                                    .slot = 0,
                                                    .kind = PEER_CARRIER_FRAME_TX}};

    return 1;
}

static int wim_carrier_lease_valid(struct wim_carrier_conn *c, const struct peer_carrier_frame_lease *l)
{
    return c->leased && l->token.slot == 0 && l->token.conn_epoch == c->epoch &&
           l->token.kind == PEER_CARRIER_FRAME_TX && l->token.lease_generation == c->lease_serial &&
           l->header == wim_carrier_tx(c) && l->body == wim_carrier_tx(c) + PEER_CARRIER_HEADER_BYTES;
}

static int wim_carrier_cancel(void *opaque, struct peer_carrier_frame_lease *l)
{
    struct wim_carrier_conn *c = opaque;
    if (!l || !wim_carrier_lease_valid(c, l))
        return -1;

    c->leased = 0;
    memset(l, 0, sizeof(*l));

    return 0;
}

static int wim_carrier_commit(void *opaque,
                      struct peer_carrier_frame_lease *l,
                      size_t body,
                      const struct peer_carrier_payload *p)
{
    struct wim_carrier_conn *c = opaque;
    if (!l || !wim_carrier_lease_valid(c, l))
        return -1;

    int valid = body <= l->body_cap && body <= PEER_CARRIER_BODY_BYTES &&
                (!p || peer_carrier_frame_payload_valid(p, body));
    c->leased = 0;
    memset(l, 0, sizeof(*l));

    if (!valid || wim_carrier_ready(c) != 1) {
        wim_carrier_fail(c, "frame-commit");
        return -1;
    }

    return wim_carrier_post_data(c, body + PEER_CARRIER_HEADER_BYTES, p);
}

static long wim_carrier_recv(void *opaque, void *out, size_t cap)
{
    struct wim_carrier_conn *c = opaque;
    int ready = wim_carrier_ready(c);
    if (ready <= 0)
        return ready;

    uint64_t available = c->rx_tail - c->rx_head;
    if (!available) {
        peer_profile_add(&c->ctx->profile, PEER_PROFILE_rx_empty, 1);
        return 0;
    }
    if (available < PEER_CARRIER_HEADER_BYTES)
        goto fail;

    const uint8_t *frame = c->ring + c->rx_head % c->ctx->capacity;
    size_t bytes = peer_data_frame_size(frame);
    if (!bytes || bytes > available || !out || bytes > cap)
        goto fail;

    /* Draft RX adapter: COPY before returning ring byte credit. Downstream
     * receives its existing channel/arena storage, never a borrowed ring view.
     * Adding zero-copy requires a ring lease + Host-DMA mmap source adapter. */
    memcpy(out, frame, bytes);
    c->rx_head += bytes;

    if (wim_carrier_credit(c, 0) < 0)
        return -1;

    return (long)bytes;

fail:
    wim_carrier_fail(c, "rx-framing");

    return -1;
}

static void wim_carrier_completion(struct wim_carrier_ctx *t, const struct ibv_wc *wc)
{
    unsigned index = peer_write_wr_index(wc->wr_id);
    /* DATA carries a low32 byte end; RECV carries an RQ index. */
    uint32_t low = (uint32_t)wc->wr_id;
    uint32_t epoch = peer_write_wr_epoch(wc->wr_id);
    unsigned kind = peer_write_wr_kind(wc->wr_id);
    if (index >= WIM_CARRIER_CONNECTIONS)
        return;

    struct wim_carrier_conn *c = &t->conns[index];
    if (!c->used || c->epoch != epoch) {
        peer_profile_add(&t->profile, PEER_PROFILE_cq_stale, 1);
        return; /* retired generation CQE */
    }
    if (wc->status != IBV_WC_SUCCESS || wc->qp_num != c->endpoint.qpn)
        goto fail;
    if (c->state == PEER_WRITE_DEAD)
        return; /* failed WRs keep source holds until destroy */

    peer_profile_add(&t->profile,
                       kind == PEER_WRITE_DATA     ? PEER_PROFILE_cq_data_done
                       : kind == PEER_WRITE_CREDIT ? PEER_PROFILE_cq_credit_done
                       : kind == PEER_WRITE_BOOT   ? PEER_PROFILE_cq_boot_done
                                            : (wc->opcode == IBV_WC_RECV ? PEER_PROFILE_cq_rx_boot
                                               : !wc->byte_len           ? PEER_PROFILE_cq_rx_credit
                                                                         : PEER_PROFILE_cq_rx_data),
                       1);

    if (kind == PEER_WRITE_DATA) {
        /* DPA DATA completions are cumulative counters, never control CQEs. */
        goto fail;
    } else if (kind == PEER_WRITE_CREDIT) {
        if (low || !c->credit_busy || wc->opcode != IBV_WC_RDMA_WRITE)
            goto fail;

        c->credit_busy = 0;
    } else if (kind == PEER_WRITE_BOOT) {
        if (low || !c->boot_busy || wc->opcode != IBV_WC_SEND)
            goto fail;

        c->boot_busy = 0;
    } else {
        if (low >= WIM_CARRIER_RQ || !c->rq_posted[low] || wc->opcode != IBV_WC_RECV ||
            c->state != PEER_WRITE_CONNECTING || wc->byte_len != PEER_WRITE_IMM_HELLO ||
            (wc->wc_flags & IBV_WC_WITH_IMM) ||
            wim_carrier_hello_decode(c, wim_carrier_boot(c) + (low + 1u) * PEER_WRITE_IMM_HELLO))
            goto fail;

        c->rq_posted[low] = 0;
    }

    /* Admit data only after validating the peer HELLO and completing our SEND. */
    if (c->state == PEER_WRITE_CONNECTING && c->peer_ready && !c->boot_busy)
        c->state = PEER_WRITE_READY;
    return;

fail:
    peer_profile_add(&t->profile, PEER_PROFILE_cq_error, 1);
    peer_profile_note(
        &t->profile, "cq_error", 1, wc->wr_id, wc->status, wc->qp_num, wc->vendor_err, 0, 0);

    /* On error only wr_id/status/qp_num/vendor_err are defined by verbs. */
    if (c->state != PEER_WRITE_DEAD)
        fprintf(
            stderr,
            "PEER_WRITE_IMM_DPA CQ fault lane=%u epoch=%u kind=%u low=%u status=%u vendor=%u state=%u opcode=%u bytes=%u hello_bytes=%u peer_ready=%d boot_busy=%d rq_posted=%u\n",
            c->index,
            c->epoch,
            kind,
            low,
            (unsigned)wc->status,
            wc->vendor_err,
            c->state,
            wc->status == IBV_WC_SUCCESS ? (unsigned)wc->opcode : 0,
            wc->status == IBV_WC_SUCCESS ? wc->byte_len : 0,
            PEER_WRITE_IMM_HELLO,
            c->peer_ready,
            c->boot_busy,
            low < WIM_CARRIER_RQ ? c->rq_posted[low] : 0);

    c->state = PEER_WRITE_DEAD;
}

static int wim_carrier_poll(struct wim_carrier_ctx *t)
{
    if (t->profile.enabled && peer_verbs_now_ns() - t->profile_last_dpa_ns >= 1000000000ull) {
        t->profile_last_dpa_ns = peer_verbs_now_ns();
        for (unsigned k = 0; k < t->active.count; k++) {
            struct wim_carrier_conn *c = &t->conns[t->active.ids[k]];
            fprintf(
                stderr,
                "WRITE_IMM_DPA_FRONT lane=%u state=%u tx=%llu/%llu rx=%llu/%llu ready=%d boot=%d\n",
                c->index,
                c->state,
                (unsigned long long)c->tx_head,
                (unsigned long long)c->tx_tail,
                (unsigned long long)c->rx_head,
                (unsigned long long)c->rx_tail,
                c->peer_ready,
                c->boot_busy);
            peer_wim_debug(c->dpa_qp);
        }
    }
    int moved = 0;
    unsigned consumed = 0;

    while (!t->dead && consumed < PEER_WRITE_BUDGET) {
        struct ibv_wc wc[PEER_RDMA_CQ_BATCH];
        unsigned remaining = PEER_WRITE_BUDGET - consumed;
        int want = remaining < PEER_RDMA_CQ_BATCH ? (int)remaining : PEER_RDMA_CQ_BATCH;
        uint64_t stamp = peer_profile_begin(&t->profile);
        int n = peer_wim_poll(t->dpa, want, wc);
        peer_profile_end(&t->profile, PEER_PROFILE_CQ_POLL, stamp);
        peer_profile_add(&t->profile, PEER_PROFILE_cq_poll_calls, 1);
        peer_profile_add(&t->profile, PEER_PROFILE_cq_poll_empty, n == 0);

        if (n > 0 && t->profile.enabled) {
            uint64_t now = PEER_PROFILE_NOW();
            uint64_t gap = t->profile_last_cq_ns ? now - t->profile_last_cq_ns : 0;
            t->profile_last_cq_ns = now;
            peer_profile_add(&t->profile, PEER_PROFILE_cq_completions, (unsigned)n);
            peer_profile_note(&t->profile,
                                "cq_batch",
                                gap >= 100000,
                                (unsigned)n,
                                gap,
                                0,
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
            wim_carrier_completion(t, &wc[i]);
    }

    for (unsigned k = 0; !t->dead && k < t->active.count; k++) {
        struct wim_carrier_conn *c = &t->conns[t->active.ids[k]];
        if (!c->used)
            continue;

        uint64_t tail, head, completed = c->tx_source.completed, consumed_requests = c->dpa_consumed;
        int counter_rc;
        if (!c->dpa_qp || c->state == PEER_WRITE_DEAD)
            continue;

        /* Completion polling visits only pending control operations.
         * This existing counter pass also catches faults on idle QPs
         * and during bootstrap, even with no control completion pending. */
        counter_rc = peer_wim_counters(c->dpa_qp, &tail, &head, &completed, &consumed_requests);
        if (counter_rc) {
            wim_carrier_fail(c, "dpa-status");
            moved = 1;
            continue;
        }
        if (wim_carrier_ready(c) != 1)
            continue;

        if (consumed_requests < c->dpa_consumed || tail < c->rx_tail ||
            tail < c->rx_head || tail - c->rx_head > t->capacity || head < c->tx_head ||
            head > c->tx_tail) {
            wim_carrier_fail(c, "dpa-counter-range");
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
                wim_carrier_fail(c, "local-completion-range");
                moved = 1;
                continue;
            }
            moved = 1;
        }

        peer_profile_add(&t->profile, PEER_PROFILE_tail_changes, tail != c->rx_tail);
        peer_profile_add(&t->profile, PEER_PROFILE_head_changes, head != c->tx_head);

        if (tail != c->rx_tail || head != c->tx_head)
            moved = 1;

        /* DPA publishes status after observing the ordered WRITE_IMM CQE.
         * Acquire its counter publication before loading the ring payload. */
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
        c->rx_tail = tail;
        c->tx_head = head;

        int rc = wim_carrier_credit(c, 0);
        if (rc)
            moved = 1;
    }

    return moved || t->dead;
}

static int wim_carrier_progress(void *opaque, void **accepted, int max, int *naccepted)
{
    struct wim_carrier_ctx *t = opaque;
    (void)accepted;
    (void)max;
    *naccepted = 0;
    peer_profile_add(&t->profile, PEER_PROFILE_progress_calls, 1);
    if (t->dead)
        return 0;
    /* The worker PE owns the DPA MsgQ wake and the final arm/scan boundary. */
    return wim_carrier_poll(t);
}

static int wim_carrier_retire(void *opaque)
{
    struct wim_carrier_conn *c = opaque;
    if (!c || !c->used)
        return -1;

    c->state = PEER_WRITE_DEAD;

    /* Each new ring has a fresh QP/MR. Destroy fences local DMA and invalidates
     * the remote QP path before rkey/backing reuse. Failed fences quarantine
     * ALL dependent storage; a timeout never authorizes reuse. */
    if (c->dpa_qp) {
        if (peer_wim_destroy(c->dpa_qp))
            return -1;

        c->dpa_qp = NULL;
    }

    peer_write_tx_ring_fenced(&c->tx_source);

    if (c->ring_mr) {
        if (ibv_dereg_mr(c->ring_mr))
            return -1;

        c->ring_mr = NULL;
    }
    if (c->ctx->dpa && c->scratch_mr && peer_wim_unregister(c->ctx->dpa, c->scratch_mr->lkey))
        return -1;
    if (c->scratch_mr) {
        if (ibv_dereg_mr(c->scratch_mr))
            return -1;

        c->scratch_mr = NULL;
    }
    if (c->ctx->dpa && c->tx_ring_mr && peer_wim_unregister(c->ctx->dpa, c->tx_ring_mr->lkey))
        return -1;
    if (c->tx_ring_mr) {
        if (ibv_dereg_mr(c->tx_ring_mr))
            return -1;

        c->tx_ring_mr = NULL;
    }
    if (c->tx_ring) {
        munmap(c->tx_ring, 2ull * c->ctx->capacity);
        c->tx_ring = NULL;
    }
    if (c->ring)
        munmap(c->ring, 2ull * c->ctx->capacity);
    free(c->scratch);
    dmesh_live_remove(&c->ctx->active, c->index);
    memset(c, 0, sizeof(*c));

    return 0;
}

static void wim_carrier_close(void *opaque)
{
    (void)wim_carrier_retire(opaque);
}

static int wim_carrier_established(void *opaque)
{
    return wim_carrier_ready(opaque);
}

static int wim_carrier_faulted(void *opaque)
{
    return wim_carrier_ready(opaque) < 0;
}

static int wim_carrier_epfd(void *opaque)
{
    (void)opaque;
    return -1; /* Readiness is provided by the worker's DPA owner. */
}

static void wim_carrier_ctx_free(void *opaque)
{
    struct wim_carrier_ctx *t = opaque;
    if (!t)
        return;

    for (unsigned i = 0; i < WIM_CARRIER_CONNECTIONS; i++)
        if (t->conns[i].used && wim_carrier_retire(&t->conns[i]))
            return;

    if (t->dpa && t->arena_mr && peer_wim_unregister(t->dpa, t->arena_mr->lkey))
        return;
    if (t->arena_mr) {
        if (ibv_dereg_mr(t->arena_mr))
            return;

        t->arena_mr = NULL;
    }

    if (peer_wim_free(t->dpa))
        return;

    t->dpa = NULL;

    if (t->device) {
        if (peer_rdma_device_free(t->device))
            return;

        t->device = NULL;
        t->pd = NULL;
    }

    peer_profile_dump(&t->profile, stderr);
    peer_profile_free(&t->profile);
    free(t);
}
static const struct peer_carrier_ops WIM_CARRIER_OPS = {.progress = wim_carrier_progress,
                                             .send_msg = wim_carrier_send,
                                             .recv_msg = wim_carrier_recv,
                                             .established = wim_carrier_established,
                                             .faulted = wim_carrier_faulted,
                                             .close = wim_carrier_close,
                                             .epfd = wim_carrier_epfd,
                                             .ctx_free = wim_carrier_ctx_free,
                                             .frame_reserve = wim_carrier_reserve,
                                             .frame_commit = wim_carrier_commit,
                                             .frame_cancel = wim_carrier_cancel};

static int wim_carrier_new(struct ibv_context *verbs,
                   uint8_t port,
                   uint32_t gid,
                   uint16_t mtu,
                   uint32_t capacity,
                   const struct peer_dpa_runtime *runtime,
                   unsigned eu,
                   const struct peer_carrier_ops **ops,
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
    if (!runtime || !verbs || !port || gid > 255 || !peer_verbs_mtu(mtu) || !peer_write_capacity_valid(capacity) ||
        ibv_query_port(verbs, port, &attr) || attr.state != IBV_PORT_ACTIVE ||
        attr.link_layer != IBV_LINK_LAYER_ETHERNET || attr.active_mtu < peer_verbs_mtu(mtu) ||
        ibv_query_gid_ex(verbs, port, gid, &entry, 0) || entry.gid_type != IBV_GID_TYPE_ROCE_V2 ||
        memcmp(entry.gid.raw, prefix, sizeof(prefix))) {
        if (error && error_len)
            snprintf(
                error,
                error_len,
                "WRITE_IMM_DPA needs active IPv4 RoCEv2, valid MTU and page-aligned power-of-two ring (64KiB..16MiB)");
        return -1;
    }

    struct wim_carrier_ctx *t = calloc(1, sizeof(*t));
    if (!t) {
        if (error && error_len)
            snprintf(error, error_len, "WRITE_IMM_DPA context allocation: %s", strerror(errno));
        return -1;
    }

    peer_profile_init(&t->profile, "peer_carrier_write_imm_dpa");
    t->gid_index = gid;
    t->mtu = mtu;
    t->capacity = capacity;
    memcpy(t->gid, entry.gid.raw, 16);

    const uint16_t endian = 1;
    if (*(const uint8_t *)&endian != 1)
        goto fail;
    if (peer_rdma_device_new(ibv_get_device_name(verbs->device), runtime->dpa, &t->device))
        goto fail;

    t->pd = peer_rdma_device_pd(t->device);

    /* Registrations and native DPA QPs use the same DOCA device PD. */
    int rc = peer_wim_new(t->device, runtime, eu, &t->dpa);
    if (rc)
        goto fail;

    *ops = &WIM_CARRIER_OPS;
    *out = t;
    return 0;

fail:
    if (error && error_len)
        snprintf(error,
                 error_len,
                 "WRITE_IMM_DPA resource/capability setup failed (see stage diagnostics); errno=%d",
                 errno);
    wim_carrier_ctx_free(t);

    return -1;
}

static int wim_carrier_prepare(void *opaque, void **out, struct peer_verbs_endpoint *ep)
{
    if (!out || !ep)
        return -1;

    *out = NULL;
    memset(ep, 0, sizeof(*ep));

    struct wim_carrier_ctx *t = opaque;
    if (!t || t->dead || t->epoch == PEER_WRITE_EPOCH_MAX)
        return -1;

    struct wim_carrier_conn *c = NULL;
    for (unsigned i = 0; i < WIM_CARRIER_CONNECTIONS; i++)
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
    c->state = PEER_WRITE_INIT;
    dmesh_live_add(&t->active, c->index);

    const char *stage = "alias-mapping";
    if (peer_write_map_buffer(&c->ring, t->capacity, "dpumesh-write-ring"))
        goto fail;
    stage = "tx-ring-allocation";

    if (peer_write_map_buffer(&c->tx_ring, t->capacity, "dpumesh-write-ring"))
        goto fail;

    stage = "tx-ring-registration";
    c->tx_ring_mr = ibv_reg_mr(t->pd, c->tx_ring, 2ull * t->capacity, IBV_ACCESS_LOCAL_WRITE);
    if (!c->tx_ring_mr)
        goto fail;
    if (peer_wim_register(t->dpa, c->tx_ring, 2ull * t->capacity, c->tx_ring_mr->lkey))
        goto fail;
    stage = "scratch-allocation";
    unsigned scratch_bytes = 2u * PEER_WRITE_IMM_HELLO;
    c->scratch = aligned_alloc(64, scratch_bytes);
    if (!c->scratch)
        goto fail;

    memset(c->scratch, 0, scratch_bytes);
    stage = "alias-remote-write-registration";
    c->ring_mr = ibv_reg_mr(
        t->pd, c->ring, 2ull * t->capacity, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!c->ring_mr)
        goto fail;

    stage = "scratch-registration";
    c->scratch_mr = ibv_reg_mr(t->pd, c->scratch, scratch_bytes, IBV_ACCESS_LOCAL_WRITE);
    if (!c->scratch_mr)
        goto fail;

    stage = "QP-create";
    if (peer_wim_register(t->dpa, c->scratch, scratch_bytes, c->scratch_mr->lkey) ||
        peer_wim_qp_new(t->dpa,
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

    if (wim_carrier_post_recv(c, 0))
        goto fail;

    *ep = c->endpoint;
    *out = c;
    return 0;

fail:
    fprintf(stderr, "PEER_WRITE_IMM_DPA prepare failed stage=%s errno=%d\n", stage, errno);

    if (wim_carrier_retire(c))
        *out = c;

    return -1;
}

static int wim_carrier_prepare_pair(void *ctx,
                                 const struct dmesh_peer_pair *pair,
                                 void **out,
                                 struct peer_verbs_endpoint *ep)
{
    return pair ? wim_carrier_prepare(ctx, out, ep) : -1;
}

static int wim_carrier_activate(void *opaque, const struct peer_verbs_endpoint *ep, int protected)
{
    struct wim_carrier_conn *c = opaque;
    static const uint8_t prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
    if (!c || !c->used || c->ctx->dead || c->state != PEER_WRITE_INIT || protected != 1 || !ep ||
        !ep->qpn || ep->qpn > 0xffffff || ep->psn > 0xffffff || ep->mtu != c->endpoint.mtu ||
        memcmp(ep->gid, prefix, 12))
        return -1;

    if (peer_wim_activate(c->dpa_qp, ep, protected))
        goto fail;

    c->state = PEER_WRITE_CONNECTING;
    c->activated_ns = peer_verbs_now_ns();
    c->peer_qpn = ep->qpn;

    if (wim_carrier_hello_encode(c))
        goto fail;

    struct ibv_sge s = {
        .addr = (uintptr_t)wim_carrier_boot(c), .length = PEER_WRITE_IMM_HELLO, .lkey = c->scratch_mr->lkey};
    struct ibv_send_wr wr = {.wr_id = wim_carrier_id(c, PEER_WRITE_BOOT, 0),
                             .sg_list = &s,
                             .num_sge = 1,
                             .opcode = IBV_WR_SEND,
                             .send_flags = IBV_SEND_SIGNALED};

    /* Only after the manager's RX/TX IPsec install barrier. No rkey/address is
     * added to the existing TLS wire format, exposed to a Pod, or logged.
     * Both peers must validate this fixed canonical bootstrap first. */
    if (wim_carrier_submit(c, &wr))
        goto fail;

    c->boot_busy = 1;
    return 0;

fail:
    c->state = PEER_WRITE_DEAD;

    return -1;
}

static int wim_carrier_send_drained(void *opaque)
{
    struct wim_carrier_conn *c = opaque;
    if (!c || !c->used || wim_carrier_ready(c) < 0)
        return -1;

    /* QUIESCE must flush the final partial watermark even with no new DATA.
     * The owning worker still progresses the reserved credit submission/completion. */
    if (wim_carrier_credit(c, 1) < 0)
        return -1;
    if (c->leased || c->boot_busy || c->credit_busy || c->credit_sent != c->rx_head)
        return 0;

    if (c->tx_source.completed != c->tx_tail)
        return 0;

    return 1;
}

static int wim_carrier_dpa_driver(struct ibv_context *verbs,
                          uint8_t port,
                          uint32_t gid,
                          uint16_t mtu,
                          uint32_t bytes,
                          unsigned eu,
                          const struct peer_dpa_runtime *runtime,
                          struct peer_pair_driver *d,
                          char *error,
                          size_t error_len)
{
    *d = (struct peer_pair_driver){.prepare = wim_carrier_prepare,
                                   .prepare_pair = wim_carrier_prepare_pair,
                                   .activate = wim_carrier_activate,
                                   .send_drained = wim_carrier_send_drained,
                                   .close = wim_carrier_retire};
    return wim_carrier_new(
        verbs, port, gid, mtu, bytes, runtime, eu, &d->carrier, &d->ctx, error, error_len);
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
            if (wim_carrier_dpa_driver(verbs,
                               port,
                               gid,
                               mtu,
                               bytes,
                               eu,
                               rt,
                               &children[built],
                               error,
                               error_len))
                break;
        }

        if (built != count ||
            peer_pair_driver_group_new(children, count, rt->select_group, rt->select_ctx, &d)) {
            for (unsigned i = 0; i < built; i++)
                children[i].carrier->ctx_free(children[i].ctx);
            return -1;
        }
    } else if (wim_carrier_dpa_driver(
                   verbs, port, gid, mtu, bytes, cfg->worker, rt, &d, error, error_len))
        return -1;

    struct peer_pair_transport_config copy = *cfg;
    copy.rx_pool = NULL;

    if (!peer_pair_transport_new_driver(&copy, &d, out))
        return 0;

    d.carrier->ctx_free(d.ctx);

    return -1;
}
