#include "../common/dpa_runtime.h"
/* ARM control half of the descriptor-driven DPA peer driver. No per-WR RPC.
 * Each assigned (worker, RDMA EU) owns one control ring/CQ and one shared data ring.
 * A QP never changes its DPA owner. Host DMA runs on a disjoint EU pool.
 * Existing peer manager authorizes the pair, installs inline IPsec rules,
 * then calls activate. Pair DATA borrows registered ARM arena memory; header
 * slots and payload holds remain owned through SEND CQ or QP destruction.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/random.h>
#include <time.h>
#include <stdatomic.h>
#include <doca_buf_array.h>
#include <infiniband/verbs.h>
#include <doca_dev.h>
#include <doca_dpa.h>
#include <doca_mmap.h>
#include <doca_log.h>
#include <doca_rdma_bridge.h>
#include <doca_verbs.h>
#include <doca_verbs_bridge.h>
#include <doca_version.h>
#include "peer_pair_dpa.h"
#include "peer_dpa_ring.h"
#include "peer_dpa_owner.h"
#include "peer_profile.h"

_Static_assert(PEER_DPA_LANES <= DMESH_LIVE_SLOTS_MAX, "live list capacity");
_Static_assert(PEER_RDMA_SEND_SLOTS <= 65535, "posted TX slot index capacity");

#define LIMIT PEER_DPA_LANES
static atomic_uint_fast64_t frame_epoch_next = 1;
struct ctx;

struct arm_delay {
    uint64_t count, ns, max_ns;
};

struct conn_profile {
    uint64_t seen_head, pub_seq[PEER_DPA_RING_CAP], pub_ns[PEER_DPA_RING_CAP];
    uint64_t rx_acquired_ns[PEER_RDMA_RECV_SLOTS], rx_released_ns[PEER_RDMA_RECV_SLOTS];
    uint64_t send, recv, rx_delivered, ring_full, pool_wait;
    struct arm_delay credit, completion, rx_hold, repost;
};

struct conn {
    struct ctx *ctx;
    struct conn_profile *profile;
    struct doca_verbs_qp *qp;
    struct doca_mmap *map;
    doca_dpa_dev_verbs_qp_t qph;
    doca_dpa_dev_mmap_t key;
    uint8_t *buf;
    uint32_t generation, index, tx_next;
    uint64_t request_tail;
    uint64_t tx_token[PEER_RDMA_SEND_SLOTS], tx_since[PEER_RDMA_SEND_SLOTS], rx_sequence;
    uint8_t tx_busy[PEER_RDMA_SEND_SLOTS];
    uint64_t tx_posted[PEER_RDMA_TX_BITMAP_WORDS]; /* owner-only; excludes reservations */
    uint64_t lease_epoch, lease_generation[PEER_RDMA_SEND_SLOTS];
    int tx_leased;
    int arena_rx, rx_view;
    char rx_uid[DMESH_POD_UID_MAX];
    uint64_t rx_reposted;
    uint8_t rx_pending[PEER_RDMA_RECV_SLOTS];
    struct peer_wire_payload rx_payload[PEER_RDMA_RECV_SLOTS];
    struct peer_wire_payload payload[PEER_RDMA_SEND_SLOTS];
    int installed;
    int used, active, dead;
    struct peer_verbs_endpoint endpoint;
};

struct ctx {
    struct arm_path_profile profile;
    struct doca_dev *pf, *sf;
    struct doca_dpa *base, *extended;
    struct doca_verbs_context *verbs;
#if DOCA_VERSION_MAJOR == 3 && DOCA_VERSION_MINOR >= 5
    struct doca_verbs_pd *pd;
#endif
    doca_dpa_dev_t devh;
    struct doca_dpa_completion *cq;
    doca_dpa_dev_completion_t cqh;
    unsigned cq_capacity;
    int cq_paused;
    struct ibv_context *ib;
    uint8_t port;
    uint32_t gid;
    uint16_t mtu;
    struct conn conns[LIMIT];
    struct dmesh_live_slots live;
    struct peer_dpa_shared *shared;
    struct doca_mmap *ring_map;
    struct doca_buf_arr *ring_array;
    uint64_t device_state, tail, data_tail, token;
    uint64_t profile_ticket, profile_last, profile_requested_ns;
    int attached;
    unsigned eu;
    int (*cancel_attach)(void *, unsigned, uint64_t);
    void *attach_ctx;
    uint32_t generations[LIMIT];
    /* The proxy arena outlives this driver. Register once on this SF/PD;
     * never reuse the proxy's PF mmap key for a native SF SEND. */
    struct peer_wire_rx_pool rx_pool;
    struct doca_mmap *arena_map;
    void *arena;
    size_t arena_len;
    doca_dpa_dev_mmap_t arena_key;
};

/* Owner-only bitmap: visit posted SENDs without scanning every TX slot. */
static int tx_has_posted(const struct conn *c)
{
    for (unsigned word = 0; word < PEER_RDMA_TX_BITMAP_WORDS; word++)
        if (c->tx_posted[word])
            return 1;

    return 0;
}

static void tx_mark_posted(struct conn *c, unsigned slot)
{
    c->tx_posted[slot / 64u] |= UINT64_C(1) << (slot % 64u);
}

static void tx_clear_posted(struct conn *c, unsigned slot)
{
    c->tx_posted[slot / 64u] &= ~(UINT64_C(1) << (slot % 64u));
}

static uint8_t *header_slot(struct conn *c, unsigned slot)
{
    return c->buf + (size_t)slot * PEER_RDMA_TX_SCRATCH_BYTES;
}

static int arena_register(struct ctx *, const struct peer_wire_payload *);

static int rx_pool_set(void *opaque, const struct peer_wire_rx_pool *pool)
{
    struct ctx *t = opaque;
    if (!t || !pool || !pool->acquire || !pool->exclusive)
        return -1;
    if (t->live.count)
        return -1;

    t->rx_pool = *pool;

    return 0;
}

static int rx_refill(struct conn *c, unsigned slot)
{
    struct peer_wire_payload *p = &c->rx_payload[slot];
    int rc = peer_wire_rx_refill(&c->ctx->rx_pool, c->rx_uid, p);
    if (rc != 1)
        return rc;
    if (arena_register(c->ctx, p))
        return -1;

    return 1;
}

static void release_payload(struct conn *c, unsigned slot)
{
    struct peer_wire_payload p = c->payload[slot];
    memset(&c->payload[slot], 0, sizeof(c->payload[slot]));

    if (p.release)
        p.release(p.owner, p.cookie);
}

static int check(doca_error_t e, const char *op)
{
    if (e == DOCA_SUCCESS)
        return 0;

    fprintf(stderr, "DPA_FAIL %s: %s\n", op, doca_error_get_descr(e));
    return -1;
}

#define TRY(x)                                                                                     \
    do {                                                                                           \
        if (check((x), #x))                                                                        \
            goto fail;                                                                             \
    } while (0)

static int device(const char *name, struct doca_dev **out)
{
    struct doca_devinfo **list;
    uint32_t n;
    if (check(doca_devinfo_create_list(&list, &n), "device list"))
        return -1;

    int rc = -1;
    for (uint32_t i = 0; i < n; i++) {
        char ibname[DOCA_DEVINFO_IBDEV_NAME_SIZE] = {0};
        if (doca_devinfo_get_ibdev_name(list[i], ibname, sizeof(ibname)) == DOCA_SUCCESS &&
            !strcmp(name, ibname)) {
            rc = check(doca_dev_open(list[i], out), "device open");
            break;
        }
    }

    doca_devinfo_destroy_list(list);

    return rc;
}

static int modify(struct conn *c, struct ibv_qp_attr *ib, int mask)
{
    struct doca_verbs_qp_attr *a = NULL;
    struct doca_verbs_ah_attr *ah = NULL;
    int rc = -1;
    TRY(doca_verbs_qp_attr_create(&a));
    TRY(doca_verbs_ah_attr_create(c->ctx->verbs, &ah));
    TRY(doca_verbs_qp_attr_set_ah_attr(a, ah));
    TRY(doca_verbs_bridge_qp_attr_set(a, ib, mask));
    TRY(doca_verbs_qp_modify(c->qp, a, doca_verbs_bridge_get_qp_attr_mask(mask)));
    rc = 0;

fail:
    if (a)
        doca_verbs_qp_attr_destroy(a);

    if (ah)
        doca_verbs_ah_attr_destroy(ah);

    return rc;
}

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static uint64_t load64(const volatile uint64_t *p)
{
    return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

static uint32_t load32(const uint32_t *p)
{
    return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

/* Payload/table/descriptor stores precede release publication. ARM never
 * writes DPA-owned head/status cache lines. The device invalidates its window
 * before reading and writebacks completions before ARM acquires them. */
static void delay_add(struct arm_delay *d, uint64_t ns)
{
    d->count++;
    d->ns += ns;

    if (ns > d->max_ns)
        d->max_ns = ns;
}

/* This measures ARM publication -> ARM observation of DPA ring credit, not
 * DPA post time. No device timestamp is read or compared to an ARM clock. */
static void profile_credit(struct conn *c, uint64_t head)
{
    struct conn_profile *p = c->profile;
    if (!p || head < p->seen_head || head > c->request_tail)
        return;
    if (head == p->seen_head)
        return;

    uint64_t now = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
    for (unsigned slot = 0; slot < PEER_DPA_RING_CAP; slot++) {
        if (p->pub_seq[slot] > p->seen_head && p->pub_seq[slot] <= head) {
            delay_add(&p->credit, now - p->pub_ns[slot]);
            p->pub_seq[slot] = 0;
        }
    }

    p->seen_head = head;
}

static void profile_lane(struct conn *c, int final)
{
    if (!c->profile)
        return;

    struct conn_profile *p = c->profile;
    uint64_t now = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC), oldest = 0;
    unsigned tx_pending = 0;

    for (unsigned i = 0; i < PEER_RDMA_SEND_SLOTS; i++) {
        if (c->tx_busy[i] != 1)
            continue;

        tx_pending++;

        uint64_t age = now - c->tx_since[i];
        if (age > oldest)
            oldest = age;
    }

    uint64_t head = load64(&c->ctx->shared->status[c->index].request_consumed);
    fprintf(stderr,
            "DPA_ARM_LANE worker=%d eu=%u lane=%u generation=%u qpn=%u final=%d"
            " mono_ns=%" PRIu64 " dead=%d send=%" PRIu64 " recv=%" PRIu64 " rx_delivered=%" PRIu64
            " ring_full=%" PRIu64 " pool_wait=%" PRIu64 " head=%" PRIu64 " tail=%" PRIu64
            " tx_pending=%u oldest_tx_ns=%" PRIu64 " rx_repost_pending=%" PRIu64
            " credit_samples=%" PRIu64 " credit_ns=%" PRIu64 " credit_max_ns=%" PRIu64
            " tx_done_count=%" PRIu64 " tx_done_ns=%" PRIu64 " tx_done_max_ns=%" PRIu64
            " rx_hold_count=%" PRIu64 " rx_hold_ns=%" PRIu64 " rx_hold_max_ns=%" PRIu64
            " repost_count=%" PRIu64 " repost_ns=%" PRIu64 " repost_max_ns=%" PRIu64 "\n",
            c->ctx->profile.worker,
            c->ctx->eu,
            c->index,
            c->generation,
            c->endpoint.qpn,
            final,
            now,
            c->dead,
            p->send,
            p->recv,
            p->rx_delivered,
            p->ring_full,
            p->pool_wait,
            head,
            c->request_tail,
            tx_pending,
            oldest,
            c->arena_rx ? c->rx_sequence - c->rx_reposted : 0,
            p->credit.count,
            p->credit.ns,
            p->credit.max_ns,
            p->completion.count,
            p->completion.ns,
            p->completion.max_ns,
            p->rx_hold.count,
            p->rx_hold.ns,
            p->rx_hold.max_ns,
            p->repost.count,
            p->repost.ns,
            p->repost.max_ns);
}

static void profile_context(struct ctx *t, int force)
{
    if (!arm_profile_report(&t->profile, "rdma", force))
        return;

    if (t->profile.handoff_enabled && t->shared) {
        struct peer_dpa_profile_shared *q = &t->shared->profile;
        uint64_t ack = load64(&q->ack);
        if (ack == t->profile_ticket && ack && ack != t->profile_last) {
            /* DPA freezes this response until we publish the next request.
             * Acquire ack pairs with data-writeback -> ack-writeback. */
            const struct peer_dpa_profile_data snap = q->data;
            static const char *const counts[] = {"passes",
                                                 "empty_context",
                                                 "idle_pass",
                                                 "send",
                                                 "recv",
                                                 "send_cqe",
                                                 "recv_cqe",
                                                 "stale_cqe",
                                                 "commit_send",
                                                 "commit_recv",
                                                 "writeback",
                                                 "notify_attempt",
                                                 "notify_sent",
                                                 "notify_no_credit",
                                                 "notify_unavailable"};
            static const char *const stages[] = {"pass",
                                                 "invalidate",
                                                 "requests",
                                                 "post_send",
                                                 "post_recv",
                                                 "commit",
                                                 "cq",
                                                 "cq_to_writeback",
                                                 "writeback",
                                                 "notify"};
            _Static_assert(sizeof(stages) / sizeof(stages[0]) == PEER_PROF_STAGES,
                           "device stage labels");
            _Static_assert(sizeof(counts) / sizeof(counts[0]) == PEER_PROF_COUNTERS,
                           "device counter labels");
            flockfile(stderr);
            fprintf(stderr,
                    "DPA_DEVICE_PROFILE worker=%d eu=%u snapshot=%" PRIu64 " final=%d"
                    " requested_mono_ns=%" PRIu64 " observed_mono_ns=%" PRIu64,
                    t->profile.worker,
                    t->eu,
                    ack,
                    force,
                    t->profile_requested_ns,
                    t->profile.reported_ns);

            for (unsigned i = 0; i < PEER_PROF_COUNTERS; i++)
                fprintf(stderr, " %s=%" PRIu64, counts[i], snap.count[i]);

            fputc('\n', stderr);
            funlockfile(stderr);

            for (unsigned i = 0; i < PEER_PROF_STAGES; i++) {
                const struct peer_dpa_profile_time *p = &snap.stage[i];
                if (!p->calls)
                    continue;

                fprintf(stderr,
                        "DPA_DEVICE_STAGE worker=%d eu=%u snapshot=%" PRIu64
                        " stage=%s unit=eu_cycles sample_every=%u calls=%" PRIu64
                        " samples=%" PRIu64 " cycles=%" PRIu64 " max_cycles=%" PRIu64 "\n",
                        t->profile.worker,
                        t->eu,
                        ack,
                        stages[i],
                        t->profile.sample_every,
                        p->calls,
                        p->samples,
                        p->cycles,
                        p->max_cycles);
            }

            t->profile_last = ack;
        }

        /* At most one request in flight; never wait or send a MsgQ to force
         * a sample. Final teardown may report the last available snapshot. */
        if (!force && (!t->profile_ticket || t->profile_last == t->profile_ticket) &&
            t->profile_ticket != UINT64_MAX) {
            t->profile_requested_ns = t->profile.reported_ns;
            __atomic_store_n(&q->request, ++t->profile_ticket, __ATOMIC_RELEASE);
        }
    }

    for (unsigned k = 0; k < t->live.count; k++)
        profile_lane(&t->conns[t->live.ids[k]], force);
}

static int enqueue_impl(struct ctx *t,
                        struct conn *c,
                        uint8_t op,
                        uint32_t slot,
                        uint32_t bytes,
                        const struct peer_dpa_buffer *source,
                        uint64_t *token)
{
    int data = peer_dpa_data_op(op);
    int needs_buffer = op == PEER_DPA_SEND_FRAME || (op == PEER_DPA_RECV && c && c->arena_rx);
    if ((data && !c) || (needs_buffer != (source != NULL)))
        return -1;

    struct peer_dpa_request_ring *ring = data ? &t->shared->data : NULL;
    uint64_t *tail = data ? &t->data_tail : &t->tail;
    uint64_t head =
        load64(data ? &ring->consumed.consumer_head : &t->shared->consumed.consumer_head);
    if (*tail < head || *tail - head > PEER_DPA_RING_CAP || *tail == UINT64_MAX ||
        t->token == UINT64_MAX)
        return -1;

    if (data)
        profile_credit(c, load64(&t->shared->status[c->index].request_consumed));
    if (*tail - head == PEER_DPA_RING_CAP) {
        arm_profile_count(&t->profile, ARM_CNT_RING_FULL);

        if (data && c->profile)
            c->profile->ring_full++;
        return 0;
    }

    struct peer_dma_desc *d = &(data ? ring->requests : t->shared->requests)[*tail % PEER_DPA_RING_CAP];

    /* addr is a validated slot index on this private ring, never a raw
     * address supplied by a Host. The installed table defines the stride. */
    struct peer_dma_desc v = {.addr = slot, .size = bytes, .mmap = c ? c->key : 0};

    /* Same ARM owner publishes all data before REMOVE. Its release ticket
     * carries the final data tail; DPA drains through it before retiring QP. */
    if (op == PEER_DPA_REMOVE && c)
        v.addr = c->request_tail;

    struct peer_dpa_meta m = {.op = op,
                              .version = PEER_DPA_ABI,
                              .lane_id = c ? c->index : 0,
                              .generation = c ? c->generation : 0,
                              .qpn = c ? c->endpoint.qpn : 0,
                              .token = ++t->token};
    memcpy(v.reserved, &m, sizeof(m));

    if (source)
        peer_dpa_buffer_set(&v, source);

    /* Never clear or bulk-write the live publication word while reusing a
     * slot. A stale ticket cannot equal the next monotonically increasing one. */
    memcpy(d, &v, offsetof(struct peer_dma_desc, publish_seq));

    if (data && c->profile && *tail % t->profile.sample_every == 0) {
        unsigned slot = *tail % PEER_DPA_RING_CAP;
        c->profile->pub_seq[slot] = *tail + 1;
        c->profile->pub_ns[slot] = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
    }

    __atomic_store_n(&d->publish_seq, ++*tail, __ATOMIC_RELEASE);
    if (data)
        c->request_tail = *tail;
    arm_profile_count(&t->profile,
                      !data                 ? ARM_CNT_CONTROL
                      : op == PEER_DPA_RECV ? ARM_CNT_RECV
                                            : ARM_CNT_SEND);

    if (data && c->profile) {
        if (op == PEER_DPA_RECV)
            c->profile->recv++;
        else
            c->profile->send++;
    }

    *token = m.token;

    return 1;
}

static int enqueue(struct ctx *t,
                   struct conn *c,
                   uint8_t op,
                   uint32_t slot,
                   uint32_t bytes,
                   const struct peer_dpa_buffer *source,
                   uint64_t *token)
{
    struct arm_profile_stamp stamp = arm_profile_begin(&t->profile, ARM_PROF_ENQUEUE);
    int rc = enqueue_impl(t, c, op, slot, bytes, source, token);
    arm_profile_end(&t->profile, ARM_PROF_ENQUEUE, stamp, rc);

    return rc;
}

static int control(struct ctx *t, struct conn *c, uint8_t op)
{
    uint64_t token = 0, deadline = now_ns() + 5000000000ull;
    int rc;

    while ((rc = enqueue(t, c, op, 0, 0, NULL, &token)) == 0 && now_ns() < deadline)
        usleep(50);

    if (rc != 1)
        return -1;

    do {
        uint64_t ack = c ? load64(&t->shared->status[c->index].control_ack)
                         : load64(op == PEER_DPA_STOP ? &t->shared->stopped.consumer_head
                                                      : &t->shared->cq_control.consumer_head);
        if (ack == token)
            return 0;

        usleep(50);
    } while (now_ns() < deadline);

    fprintf(
        stderr,
        "DPA_RING control timeout op=%u lane=%u token=%llu head=%llu tail=%llu ack=%llu error=%u DPA=%s; retain mappings\n",
        op,
        c ? c->index : 0,
        (unsigned long long)token,
        (unsigned long long)t->shared->consumed.consumer_head,
        (unsigned long long)t->tail,
        (unsigned long long)(c ? load64(&t->shared->status[c->index].control_ack) : 0),
        c ? load32(&t->shared->status[c->index].error) : 0,
        doca_error_get_descr(DMESH_DPA_CALL(doca_dpa_peek_at_last_error(t->base))));

    return -1;
}

static int close_qp(void *opaque)
{
    struct conn *c = opaque;
    if (!c || !c->used)
        return 0;

    /* Shared CQ destruction/cleanup cannot race the DPA CQ reader. REMOVE
     * pauses that reader and unlinks the QPN before ACK. After QP destroy
     * fences NIC DMA, RESUME drains all pre-destroy CQEs before ACK; only
     * then may a new QP reuse this QPN or ARM free the payload. A timeout
     * retains resources. This protocol has one ARM control owner per ctx. */
    if (c->installed) {
        if (control(c->ctx, c, PEER_DPA_REMOVE))
            return -1;

        c->installed = 0;
        c->ctx->cq_paused = 1;
    }

    if (c->qp && !c->ctx->cq_paused) {
        if (control(c->ctx, NULL, PEER_DPA_CQ_PAUSE))
            return -1;

        c->ctx->cq_paused = 1;
    }

    if (c->qp) {
        if (check(doca_verbs_qp_destroy(c->qp), "destroy QP"))
            return -1;

        c->qp = NULL;
    }

    if (c->ctx->cq_paused) {
        if (control(c->ctx, NULL, PEER_DPA_CQ_RESUME))
            return -1;

        c->ctx->cq_paused = 0;
    }

    /* Also fences errored SENDs, whose CQ status never authorizes early reuse. */
    for (unsigned i = 0; i < PEER_RDMA_SEND_SLOTS; i++)
        release_payload(c, i);

    if (c->map) {
        if (check(doca_mmap_destroy(c->map), "destroy mmap"))
            return -1;

        c->map = NULL;
    }

    for (unsigned i = 0; i < PEER_RDMA_RECV_SLOTS; i++) {
        struct peer_wire_payload *p = &c->rx_payload[i];
        if (p->release)
            p->release(p->owner, p->cookie);
    }

    profile_lane(c, 1);
    free(c->profile);

    /* Retain live membership through REMOVE, QP/CQ fencing and failed cleanup.
     * Only a fully retired handle may be removed and its slot reused. */
    dmesh_live_remove(&c->ctx->live, c->index);
    free(c->buf);
    memset(c, 0, sizeof(*c));

    return 0;
}

static int prepare_impl(void *opaque, const char *uid, void **out, struct peer_verbs_endpoint *ep)
{
    struct ctx *t = opaque;
    struct conn *c = NULL;
    struct doca_verbs_qp_init_attr *init = NULL;
    *out = NULL;

    for (unsigned i = 0; i < LIMIT; i++)
        if (!t->conns[i].used) {
            c = &t->conns[i];
            break;
        }

    if (!c)
        return -1;

    c->index = (uint32_t)(c - t->conns);
    if (t->generations[c->index] == UINT32_MAX)
        return -1;

    c->generation = ++t->generations[c->index];
    c->used = 1;
    c->ctx = t;
    dmesh_live_add(&t->live, c->index);

    if (t->profile.enabled) {
        c->profile = calloc(1, sizeof(*c->profile));
        if (!c->profile)
            goto fail;
    }

    uint64_t epoch = atomic_load(&frame_epoch_next);
    for (;;) {
        if (epoch == UINT64_MAX)
            goto fail;
        if (atomic_compare_exchange_weak(&frame_epoch_next, &epoch, epoch + 1))
            break;
    }

    c->lease_epoch = epoch;
    c->arena_rx = uid && t->rx_pool.acquire;
    if (c->arena_rx) {
        snprintf(c->rx_uid, sizeof(c->rx_uid), "%s", uid);
        for (unsigned i = 0; i < PEER_RDMA_RECV_SLOTS; i++) {
            if (rx_refill(c, i) != 1)
                goto fail;

            t->shared->rx_sources[c->index][i] =
                (struct peer_dpa_rx_source){.addr = (uintptr_t)c->rx_payload[i].data,
                                            .len = PEER_WIRE_FRAME_BYTES,
                                            .key = t->arena_key};
        }
    }

    size_t bytes =
        PEER_RDMA_TX_SCRATCH_BUFFER_BYTES + (size_t)PEER_RDMA_RECV_SLOTS * PEER_DPA_BUFFER_SIZE;
    c->buf = aligned_alloc(64, bytes);
    if (!c->buf)
        goto fail;

    memset(c->buf, 0, bytes);
    TRY(doca_mmap_create(&c->map));
    TRY(doca_mmap_set_memrange(c->map, c->buf, bytes));
    TRY(doca_mmap_set_permissions(c->map, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE));
    TRY(doca_mmap_add_dev(c->map, t->sf));
    TRY(doca_mmap_start(c->map));

    /* Native DPA SGE use of this mmap key was validated on DOCA 3.1/BF3 by
     * matching CQEs and receiver bytes. That does not establish compatibility
     * for Host PCI-exported memory or a different SF/PD. */
    TRY(doca_mmap_dev_get_dpa_handle(c->map, t->sf, &c->key));
    TRY(doca_verbs_qp_init_attr_create(&init));
    TRY(doca_verbs_qp_init_attr_set_qp_type(init, DOCA_VERBS_QP_TYPE_RC));
    TRY(doca_verbs_qp_init_attr_set_sq_wr(init, PEER_RDMA_SEND_SLOTS));
    TRY(doca_verbs_qp_init_attr_set_rq_wr(init, PEER_RDMA_RECV_SLOTS));
    TRY(doca_verbs_qp_init_attr_set_send_max_sges(init, 1));
    TRY(doca_verbs_qp_init_attr_set_receive_max_sges(init, 1));
    TRY(doca_verbs_qp_init_attr_set_external_datapath_en(init, 1));
    TRY(doca_verbs_qp_init_attr_set_dpa(init, t->extended));
#if DOCA_VERSION_MAJOR == 3 && DOCA_VERSION_MINOR >= 5
    /* 3.5 requires an explicit PD even with set_dpa(); 3.1 rejects that
     * combination. Import the SF device's existing PD so mmap mkeys and QPs
     * share a domain. Never allocate an unrelated PD on the base PF. */
    TRY(doca_verbs_qp_init_attr_set_pd(init, t->pd));
#endif
    TRY(doca_verbs_qp_init_attr_set_send_dpa_completion(init, t->cq));
    TRY(doca_verbs_qp_init_attr_set_receive_dpa_completion(init, t->cq));
    TRY(doca_verbs_qp_create(t->verbs, init, &c->qp));
    TRY(doca_verbs_qp_get_dpa_handle(c->qp, t->extended, &c->qph));

    struct ibv_qp_attr a = {.qp_state = IBV_QPS_INIT, .port_num = t->port};
    if (modify(c, &a, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
        goto fail;

    struct ibv_gid_entry gid;
    if (ibv_query_gid_ex(t->ib, t->port, t->gid, &gid, 0) || gid.gid_type != IBV_GID_TYPE_ROCE_V2)
        goto fail;

    uint32_t psn;
    if (getrandom(&psn, sizeof(psn), 0) != sizeof(psn))
        goto fail;

    c->endpoint.qpn = doca_verbs_qp_get_qpn(c->qp);
    c->endpoint.psn = psn & 0xffffff;
    c->endpoint.mtu = t->mtu;
    memcpy(c->endpoint.gid, gid.gid.raw, 16);
    t->shared->table[c->index] =
        (struct peer_dpa_binding){.qp = c->qph,
                                  .cq = t->cqh,
                                  .device = t->devh,
                                  .tx_addr = (uintptr_t)c->buf,
                                  .rx_addr = (uintptr_t)c->buf + PEER_RDMA_TX_SCRATCH_BUFFER_BYTES,
                                  .mkey = c->key,
                                  .capacity = PEER_DPA_BUFFER_SIZE,
                                  .rx_frame_bytes = c->arena_rx ? PEER_WIRE_FRAME_BYTES : 0,
                                  .qpn = c->endpoint.qpn,
                                  .generation = c->generation};

    /* Mark before waiting: a timed-out INSTALL can still execute later. */
    c->installed = 1;

    if (control(t, c, PEER_DPA_INSTALL))
        goto fail;

    doca_verbs_qp_init_attr_destroy(init);
    *out = c;
    *ep = c->endpoint;
    fprintf(
        stderr,
        "DPA_RING prepared lane=%u generation=%u qpn=%u tx_slots=%u rx_slots=%u slot_bytes=%u rx_slot_bytes=%u abi=%u\n",
        c->index,
        c->generation,
        ep->qpn,
        PEER_RDMA_SEND_SLOTS,
        PEER_RDMA_RECV_SLOTS,
        PEER_DPA_TX_BUFFER_SIZE,
        PEER_DPA_BUFFER_SIZE,
        PEER_DPA_ABI);
    return 0;

fail:
    if (init)
        doca_verbs_qp_init_attr_destroy(init);

    if (close_qp(c))
        *out = c;

    return -1;
}

static int prepare(void *ctx, void **out, struct peer_verbs_endpoint *ep)
{
    return prepare_impl(ctx, NULL, out, ep);
}

static int prepare_pair(void *ctx,
                        const struct dmesh_peer_pair *pair,
                        void **out,
                        struct peer_verbs_endpoint *ep)
{
    return pair ? prepare_impl(ctx, pair->local_uid, out, ep) : -1;
}

/* Repost strictly in RQ order: DPA maps receive CQEs to that same FIFO.
 * A full request ring retains the replacement and retries publication later. */
static int refill_receives_impl(struct conn *c)
{
    if (!c->arena_rx || c->dead)
        return 0;

    int moved = 0;
    while (c->rx_reposted < c->rx_sequence) {
        unsigned slot = c->rx_reposted % PEER_RDMA_RECV_SLOTS;
        if (!c->rx_pending[slot]) {
            c->dead = 1;
            return -1;
        }

        int rc = rx_refill(c, slot);
        if (!rc) {
            arm_profile_count(&c->ctx->profile, ARM_CNT_RX_POOL_WAIT);

            if (c->profile)
                c->profile->pool_wait++;
            break;
        }
        if (rc < 0) {
            c->dead = 1;
            return -1;
        }

        uint64_t token;
        const struct peer_dpa_buffer source = {.addr = (uintptr_t)c->rx_payload[slot].data,
                                               .key = c->ctx->arena_key};
        rc = enqueue(c->ctx, c, PEER_DPA_RECV, slot, PEER_WIRE_FRAME_BYTES, &source, &token);
        if (!rc)
            break;
        if (rc < 0) {
            c->dead = 1;
            return -1;
        }

        if (c->profile && c->profile->rx_released_ns[slot]) {
            delay_add(&c->profile->repost,
                      ARM_PROFILE_CLOCK(CLOCK_MONOTONIC) - c->profile->rx_released_ns[slot]);
            c->profile->rx_released_ns[slot] = 0;
        }

        arm_profile_count(&c->ctx->profile, ARM_CNT_RX_REPOSTED);
        c->rx_pending[slot] = 0;
        c->rx_reposted++;
        moved = 1;
    }

    return moved;
}

static int refill_receives(struct conn *c)
{
    struct arm_profile_stamp stamp = arm_profile_begin(&c->ctx->profile, ARM_PROF_RX_REFILL);
    int rc = refill_receives_impl(c);
    arm_profile_end(&c->ctx->profile, ARM_PROF_RX_REFILL, stamp, rc);

    return rc;
}

static int activate(void *opaque, const struct peer_verbs_endpoint *remote, int protected)
{
    struct conn *c = opaque;
    struct ctx *t = c->ctx;
    if (protected != 1 || !remote || remote->mtu != t->mtu || c->dead || c->active)
        return -1;

    enum ibv_mtu mtu = t->mtu == 256    ? IBV_MTU_256
                       : t->mtu == 512  ? IBV_MTU_512
                       : t->mtu == 1024 ? IBV_MTU_1024
                       : t->mtu == 2048 ? IBV_MTU_2048
                                        : IBV_MTU_4096;
    struct ibv_qp_attr a = {.qp_state = IBV_QPS_RTR,
                            .path_mtu = mtu,
                            .dest_qp_num = remote->qpn,
                            .rq_psn = remote->psn,
                            .min_rnr_timer = PEER_RDMA_MIN_RNR_TIMER,
                            .ah_attr = {.is_global = 1,
                                        .port_num = t->port,
                                        .grh = {.sgid_index = (uint8_t)t->gid, .hop_limit = 64}}};
    memcpy(a.ah_attr.grh.dgid.raw, remote->gid, 16);

    if (modify(c,
               &a,
               IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                   IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
        return -1;

    memset(&a, 0, sizeof(a));
    a.qp_state = IBV_QPS_RTS;
    a.sq_psn = c->endpoint.psn;
    a.timeout = PEER_RDMA_ACK_TIMEOUT;
    a.retry_cnt = PEER_RDMA_RETRY_COUNT;
    a.rnr_retry = PEER_RDMA_RNR_RETRY;

    if (modify(c,
               &a,
               IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                   IBV_QP_MAX_QP_RD_ATOMIC))
        return -1;

    /* Existing manager calls activate only after both IPsec directions are
     * installed. ENABLE publishes permission after RTR/RTS; application sends
     * are still gated by the existing association GRANT barrier. */
    if (control(t, c, PEER_DPA_ENABLE))
        return -1;

    c->active = 1;

    return 0;
}

static int progress_impl(void *opaque, void **accepted, int max, int *n)
{
    struct ctx *t = opaque;
    int moved = 0;
    (void)accepted;
    (void)max;
    *n = 0;

    for (unsigned k = 0; k < t->live.count; k++) {
        unsigned i = t->live.ids[k];
        struct conn *c = &t->conns[i];
        if (!c->used || !c->installed || c->dead)
            continue;

        arm_profile_count(&t->profile, ARM_CNT_LANES_SCANNED);

        if (c->profile)
            profile_credit(c, load64(&t->shared->status[i].request_consumed));

        struct peer_dpa_status *s = &t->shared->status[i];
        if (load32(&s->error) || load32(&s->generation) != c->generation) {
            c->dead = 1;
            moved = 1;
            continue;
        }

        uint64_t now = tx_has_posted(c) ? now_ns() : 0;

        /* A reservation has no CQ token yet. Inspect only accepted SENDs;
         * acquire-loading their exact token still fences payload retirement. */
        for (unsigned word = 0; word < PEER_RDMA_TX_BITMAP_WORDS; word++) {
            uint64_t posted = c->tx_posted[word];
            while (posted) {
                unsigned slot = word * 64u + (unsigned)__builtin_ctzll(posted);
                posted &= posted - 1;
                arm_profile_count(&t->profile, ARM_CNT_TX_SCANNED);
                arm_profile_count(&t->profile, ARM_CNT_TX_PENDING);

                if (load64(&s->tx[slot].sequence) == c->tx_token[slot]) {
                    arm_profile_count(&t->profile, ARM_CNT_TX_DONE);

                    if (c->profile)
                        delay_add(&c->profile->completion, now - c->tx_since[slot]);
                    tx_clear_posted(c, slot);
                    c->tx_busy[slot] = 0;
                    release_payload(c, slot);
                    moved = 1;
                } else if (now - c->tx_since[slot] > 10000000000ull) {
                    fprintf(stderr, "DPA_RING SEND timeout lane=%u slot=%u\n", i, slot);
                    c->dead = 1;
                    moved = 1;
                }
            }
        }

        if (refill_receives(c))
            moved = 1;

        unsigned rx_slot = (unsigned)(c->rx_sequence % PEER_RDMA_RECV_SLOTS);
        if (load64(&s->rx[rx_slot].sequence) > c->rx_sequence)
            moved = 1;
    }

    return moved;
}

static int progress(void *opaque, void **accepted, int max, int *n)
{
    struct ctx *t = opaque;
    struct arm_profile_stamp stamp = arm_profile_begin(&t->profile, ARM_PROF_PROGRESS);
    int rc = progress_impl(opaque, accepted, max, n);
    arm_profile_end(&t->profile, ARM_PROF_PROGRESS, stamp, rc);
    arm_profile_count(&t->profile, rc ? ARM_CNT_PROGRESS_MOVED : ARM_CNT_PROGRESS_EMPTY);

    return rc;
}

static int send_msg(void *opaque, const void *buf, size_t len)
{
    /* Packed benchmark sources must use SEND_FRAME's explicit address/key;
     * the device's legacy SEND path retains its production slot stride. */
    if (DMESH_NATIVE_PACKED_SEND_TX)
        return -1;

    struct conn *c = opaque;
    if (c->dead || !c->active || !buf || !len || len > PEER_DPA_TX_BUFFER_SIZE)
        return -1;
    if (c->tx_leased)
        return 0;

    unsigned slot;
    for (slot = 0; slot < PEER_RDMA_SEND_SLOTS; slot++) {
        unsigned candidate = (c->tx_next + slot) % PEER_RDMA_SEND_SLOTS;
        if (!c->tx_busy[candidate]) {
            slot = candidate;
            break;
        }
    }

    if (slot == PEER_RDMA_SEND_SLOTS)
        return 0;

    /* Only a free slot may be overwritten. Ring acceptance is not SEND
     * completion; this slot stays owned until its exact token is observed. */
    memcpy(c->buf + (size_t)slot * PEER_RDMA_TX_SCRATCH_BYTES, buf, len);

    int rc = enqueue(c->ctx, c, PEER_DPA_SEND, slot, (uint32_t)len, NULL, &c->tx_token[slot]);
    if (rc == 1) {
        c->tx_busy[slot] = 1;
        tx_mark_posted(c, slot);
        c->tx_since[slot] = now_ns();
        c->tx_next = (slot + 1) % PEER_RDMA_SEND_SLOTS;
    } else if (rc < 0)
        c->dead = 1;

    return rc;
}

static int frame_reserve_impl(void *opaque, struct peer_wire_frame_lease *out)
{
    struct conn *c = opaque;
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    if (!c || !c->used || c->dead || !c->active)
        return -1;
    if (c->tx_leased) {
        arm_profile_count(&c->ctx->profile, ARM_CNT_TX_LEASED);
        return 0;
    }

    uint64_t head = load64(&c->ctx->shared->data.consumed.consumer_head);
    if (head > c->ctx->data_tail || c->ctx->data_tail - head > PEER_DPA_RING_CAP)
        return -1;
    if (c->ctx->data_tail - head == PEER_DPA_RING_CAP) {
        arm_profile_count(&c->ctx->profile, ARM_CNT_RING_FULL);

        if (c->profile)
            c->profile->ring_full++;
        return 0;
    }

    for (unsigned i = 0; i < PEER_RDMA_SEND_SLOTS; i++) {
        unsigned slot = (c->tx_next + i) % PEER_RDMA_SEND_SLOTS;
        if (c->tx_busy[slot])
            continue;
        if (c->lease_generation[slot] == UINT64_MAX) {
            c->dead = 1;
            return -1;
        }

        c->tx_busy[slot] = 2;
        c->tx_leased = 1;
        *out = (struct peer_wire_frame_lease){
            .header = header_slot(c, slot),
            .body = header_slot(c, slot) + PEER_WIRE_HEADER_BYTES,
            .body_cap = PEER_RDMA_TX_SCRATCH_BYTES - PEER_WIRE_HEADER_BYTES,
            .token = {c->lease_epoch, ++c->lease_generation[slot], slot, PEER_WIRE_FRAME_TX}};

        return 1;
    }

    arm_profile_count(&c->ctx->profile, ARM_CNT_TX_FULL);

    return 0;
}

static int frame_reserve(void *opaque, struct peer_wire_frame_lease *out)
{
    struct conn *c = opaque;
    if (!c)
        return -1;

    struct arm_profile_stamp stamp = arm_profile_begin(&c->ctx->profile, ARM_PROF_TX_RESERVE);
    int rc = frame_reserve_impl(opaque, out);
    arm_profile_end(&c->ctx->profile, ARM_PROF_TX_RESERVE, stamp, rc);

    return rc;
}

static int valid_frame(struct conn *c, const struct peer_wire_frame_lease *l)
{
    return c && c->used && l && l->token.kind == PEER_WIRE_FRAME_TX &&
           l->token.conn_epoch == c->lease_epoch && l->token.slot < PEER_RDMA_SEND_SLOTS &&
           c->tx_leased && c->tx_busy[l->token.slot] == 2 &&
           c->lease_generation[l->token.slot] == l->token.lease_generation &&
           l->header == header_slot(c, l->token.slot) &&
           l->body == l->header + PEER_WIRE_HEADER_BYTES &&
           l->body_cap == PEER_RDMA_TX_SCRATCH_BYTES - PEER_WIRE_HEADER_BYTES;
}

static int frame_cancel(void *opaque, struct peer_wire_frame_lease *l)
{
    struct conn *c = opaque;
    if (!valid_frame(c, l))
        return -1;

    c->tx_busy[l->token.slot] = 0;
    c->tx_leased = 0;
    memset(l, 0, sizeof(*l));

    return c->dead ? -1 : 0;
}

static int arena_register(struct ctx *t, const struct peer_wire_payload *p)
{
    if (t->arena_map)
        return t->arena == p->region && t->arena_len == p->region_len ? 0 : -1;

    struct doca_mmap *map = NULL;
    doca_dpa_dev_mmap_t key;
    if (check(doca_mmap_create(&map), "arena mmap create") ||
        check(doca_mmap_set_memrange(map, p->region, p->region_len), "arena range") ||
        check(doca_mmap_set_permissions(map, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE),
              "arena permissions") ||
        check(doca_mmap_add_dev(map, t->sf), "arena SF") ||
        check(doca_mmap_start(map), "arena mmap start") ||
        check(doca_mmap_dev_get_dpa_handle(map, t->sf, &key), "arena mkey")) {
        if (map && check(doca_mmap_destroy(map), "failed arena mmap destroy"))
            abort();
        return -1;
    }

    /* Uses the same native-SF mmap-key contract as the existing slot mapping.
     * BF3 NIC/ARM coherence and native SF registration still require hardware
     * validation; CPU mocks cannot establish visibility on a different SDK. */
    t->arena_map = map;
    t->arena = p->region;
    t->arena_len = p->region_len;
    t->arena_key = key;

    return 0;
}

static int frame_commit_impl(void *opaque,
                             struct peer_wire_frame_lease *l,
                             size_t len,
                             const struct peer_wire_payload *p)
{
    struct conn *c = opaque;
    if (!valid_frame(c, l))
        return -1;

    unsigned slot = l->token.slot;
    int ok = !c->dead && c->active &&
             len <= (p ? PEER_WIRE_FRAME_BYTES - PEER_WIRE_HEADER_BYTES : l->body_cap) &&
             (!p || (peer_wire_frame_payload_valid(p, len) && !arena_register(c->ctx, p)));
    c->tx_leased = 0;
    memset(l, 0, sizeof(*l));

    if (!ok) {
        c->tx_busy[slot] = 0;
        c->dead = 1;
        return -1;
    }

    const struct peer_dpa_buffer source = {.addr = p ? (uintptr_t)p->tx_header
                                                     : (uintptr_t)header_slot(c, slot),
                                           .key = p ? c->ctx->arena_key : c->key};
    if (p) {
        p->retain(p->owner, p->cookie);
        c->payload[slot] = *p;
    }

    /* Descriptor release publishes header/payload and the inline address/key. A
     * reservation has checked ring credit; no worker progress is interleaved
     * with production encode/commit. Any lost credit is a terminal failure,
     * never a partially accepted stateful frame or an unowned queued SEND. */
    int rc = enqueue(c->ctx,
                     c,
                     PEER_DPA_SEND_FRAME,
                     slot,
                     (uint32_t)len + PEER_WIRE_HEADER_BYTES,
                     &source,
                     &c->tx_token[slot]);
    if (rc != 1) {
        release_payload(c, slot);
        c->tx_busy[slot] = 0;
        c->dead = 1;
        return -1;
    }

    c->tx_busy[slot] = 1;
    tx_mark_posted(c, slot);
    c->tx_since[slot] = now_ns();
    c->tx_next = (slot + 1) % PEER_RDMA_SEND_SLOTS;

    return 1;
}

static int frame_commit(void *opaque,
                        struct peer_wire_frame_lease *l,
                        size_t len,
                        const struct peer_wire_payload *p)
{
    struct conn *c = opaque;
    if (!c)
        return -1;

    struct arm_profile_stamp stamp = arm_profile_begin(&c->ctx->profile, ARM_PROF_TX_COMMIT);
    int rc = frame_commit_impl(opaque, l, len, p);
    arm_profile_end(&c->ctx->profile, ARM_PROF_TX_COMMIT, stamp, rc);

    return rc;
}

static int frame_acquire_impl(void *opaque, struct peer_wire_rx_frame *out)
{
    struct conn *c = opaque;
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    if (!c || !c->used || c->dead || !c->active)
        return -1;
    if (!c->arena_rx)
        return -2;

    unsigned slot = c->rx_sequence % PEER_RDMA_RECV_SLOTS;
    if (c->rx_view || c->rx_pending[slot])
        return 0;

    struct peer_dpa_slot_status *s = &c->ctx->shared->status[c->index].rx[slot];
    uint64_t sequence = load64(&s->sequence);
    if (sequence <= c->rx_sequence)
        return 0;

    uint32_t n = load32(&s->bytes);
    if (c->rx_sequence == UINT64_MAX || sequence != c->rx_sequence + 1 ||
        n < PEER_WIRE_HEADER_BYTES || n > PEER_WIRE_FRAME_BYTES) {
        c->dead = 1;
        return -1;
    }

    arm_profile_count(&c->ctx->profile, ARM_CNT_RX_DELIVERED);

    if (c->profile) {
        c->profile->rx_delivered++;
        c->profile->rx_acquired_ns[slot] = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
    }

    c->rx_view = 1;
    *out = (struct peer_wire_rx_frame){.header = c->rx_payload[slot].data,
                                       .payload = c->rx_payload[slot],
                                       .body_len = n - PEER_WIRE_HEADER_BYTES,
                                       .token = {c->lease_epoch, sequence, slot, PEER_WIRE_RX}};
    out->payload.data += PEER_WIRE_HEADER_BYTES;
    out->payload.len = PEER_WIRE_BODY_BYTES;

    return 1;
}

static int frame_acquire(void *opaque, struct peer_wire_rx_frame *out)
{
    struct conn *c = opaque;
    if (!c)
        return -1;

    struct arm_profile_stamp stamp = arm_profile_begin(&c->ctx->profile, ARM_PROF_RX_ACQUIRE);
    int rc = frame_acquire_impl(opaque, out);
    arm_profile_end(&c->ctx->profile, ARM_PROF_RX_ACQUIRE, stamp, rc);

    return rc;
}

static int frame_release_impl(void *opaque, struct peer_wire_rx_frame *f)
{
    struct conn *c = opaque;
    if (!c || !f || !c->used || !c->arena_rx || !c->rx_view || f->token.kind != PEER_WIRE_RX ||
        f->token.conn_epoch != c->lease_epoch || f->token.lease_generation != c->rx_sequence + 1 ||
        f->token.slot != c->rx_sequence % PEER_RDMA_RECV_SLOTS)
        return -1;

    unsigned slot = f->token.slot;
    if (f->header != c->rx_payload[slot].data ||
        f->payload.data != c->rx_payload[slot].data + PEER_WIRE_HEADER_BYTES ||
        f->payload.cookie != c->rx_payload[slot].cookie ||
        f->body_len + PEER_WIRE_HEADER_BYTES !=
            load32(&c->ctx->shared->status[c->index].rx[slot].bytes))
        return -1;

    if (c->profile) {
        uint64_t now = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
        if (c->profile->rx_acquired_ns[slot])
            delay_add(&c->profile->rx_hold, now - c->profile->rx_acquired_ns[slot]);
        c->profile->rx_acquired_ns[slot] = 0;
        c->profile->rx_released_ns[slot] = now;
    }

    c->rx_pending[slot] = 1;
    c->rx_sequence++;
    c->rx_view = 0;
    memset(f, 0, sizeof(*f));

    return 0;
}

static int frame_release(void *opaque, struct peer_wire_rx_frame *f)
{
    struct conn *c = opaque;
    if (!c)
        return -1;

    struct arm_profile_stamp stamp = arm_profile_begin(&c->ctx->profile, ARM_PROF_RX_RELEASE);
    int rc = frame_release_impl(opaque, f);
    arm_profile_end(&c->ctx->profile, ARM_PROF_RX_RELEASE, stamp, rc);

    return rc;
}

static long recv_msg(void *opaque, void *buf, size_t cap)
{
    struct conn *c = opaque;
    if (c->dead || !c->active || c->arena_rx)
        return -1;

    unsigned slot = (unsigned)(c->rx_sequence % PEER_RDMA_RECV_SLOTS);
    struct peer_dpa_slot_status *s = &c->ctx->shared->status[c->index].rx[slot];
    uint64_t sequence = load64(&s->sequence);
    if (sequence <= c->rx_sequence)
        return 0;
    if (c->rx_sequence == UINT64_MAX || sequence != c->rx_sequence + 1) {
        c->dead = 1;
        return -1;
    }

    uint32_t n = load32(&s->bytes);
    if (!n || n > PEER_DPA_BUFFER_SIZE) {
        c->dead = 1;
        return -1;
    }
    if (!buf || cap < n) {
        c->dead = 1;
        return -1;
    }

    memcpy(
        buf, c->buf + PEER_RDMA_TX_SCRATCH_BUFFER_BYTES + (size_t)slot * PEER_DPA_BUFFER_SIZE, n);

    uint64_t token;

    /* The copy finishes before release-publishing RECV. On ring backpressure
     * keep ownership and return 0; retrying cannot deliver the frame twice. */
    int rc = enqueue(c->ctx, c, PEER_DPA_RECV, slot, PEER_DPA_BUFFER_SIZE, NULL, &token);
    if (rc != 1) {
        if (rc < 0)
            c->dead = 1;
        return rc;
    }

    c->rx_sequence = sequence;

    return (long)n;
}

static int drained(void *opaque)
{
    struct conn *c = opaque;
    if (c->dead)
        return -1;

    return !tx_has_posted(c) && !c->tx_leased;
}

static int has_live(void *opaque)
{
    return ((struct ctx *)opaque)->live.count != 0;
}

static int faulted(void *c)
{
    return ((struct conn *)c)->dead;
}

static void close_wire(void *c)
{
    ((struct conn *)c)->dead = 1;
}

static int epfd(void *c)
{
    (void)c;
    return -1;
}

static void free_ctx(void *opaque)
{
    struct ctx *t = opaque;
    if (!t)
        return;

    while (t->live.count)
        if (close_qp(&t->conns[t->live.ids[t->live.count - 1]]))
            abort();

    if (t->arena_map && check(doca_mmap_destroy(t->arena_map), "destroy arena registration"))
        abort();
    if (t->attached) {
        int detached =
            t->cancel_attach ? t->cancel_attach(t->attach_ctx, t->eu, t->device_state) : 0;
        if (!detached && control(t, NULL, PEER_DPA_STOP))
            abort();
    }

    if (t->cq && check(DMESH_DPA_CALL(doca_dpa_completion_destroy(t->cq)), "destroy shared CQ"))
        abort();

    if (t->device_state && check(DMESH_DPA_CALL(doca_dpa_mem_free(t->base, t->device_state)), "free DPA state"))
        abort();

    if (t->ring_array && check(doca_buf_arr_destroy(t->ring_array), "destroy ring array"))
        abort();

    if (t->ring_map && check(doca_mmap_destroy(t->ring_map), "destroy ring map"))
        abort();
    profile_context(t, 1);
    free(t->shared);
#if DOCA_VERSION_MAJOR == 3 && DOCA_VERSION_MINOR >= 5
    /* Only the imported wrapper is ours; the underlying PD belongs to sf. */
    if (t->pd && check(doca_verbs_pd_destroy(t->pd), "destroy imported PD"))
        abort();
#endif
    if (t->verbs && check(doca_verbs_context_destroy(t->verbs), "destroy verbs"))
        abort();
    if (t->extended && t->extended != t->base &&
        check(DMESH_DPA_CALL(doca_dpa_destroy(t->extended)), "destroy extended DPA"))
        abort();
    if (t->sf && t->sf != t->pf)
        doca_dev_close(t->sf);
    free(t);
}
static const struct peer_wire_ops OPS = {.progress = progress,
                                         .send_msg = send_msg,
                                         .recv_msg = recv_msg,
                                         .faulted = faulted,
                                         .close = close_wire,
                                         .epfd = epfd,
                                         .ctx_free = free_ctx,
                                         .frame_reserve = frame_reserve,
                                         .frame_commit = frame_commit,
                                         .frame_cancel = frame_cancel,
                                         .rx_pool_set = rx_pool_set,
                                         .frame_acquire = frame_acquire,
                                         .frame_release = frame_release};

static struct ctx *new_ctx(struct ibv_context *ib,
                           uint8_t port,
                           uint32_t gid,
                           uint16_t mtu,
                           unsigned eu,
                           const struct peer_dpa_runtime *runtime)
{
    struct ctx *t = calloc(1, sizeof(*t));
    if (!t)
        return NULL;

    arm_profile_init(&t->profile, (int)runtime->owner_worker, (int)eu);
    t->ib = ib;
    t->port = port;
    t->gid = gid;
    t->mtu = mtu;
    t->pf = runtime->dev;
    t->base = runtime->dpa;
    t->eu = eu;
    t->cancel_attach = runtime->cancel_attach;
    t->attach_ctx = runtime->attach_ctx;

    const char *sf = ibv_get_device_name(ib->device);
    if (device(sf, &t->sf))
        goto fail;

    if (t->sf != t->pf) {
        TRY(DMESH_DPA_CALL(doca_dpa_device_extend(t->base, t->sf, &t->extended)));
    } else
        t->extended = t->base;

    TRY(DMESH_DPA_CALL(doca_dpa_get_dpa_handle(t->extended, &t->devh)));

    /* Use the SF DOCA device's verbs context, not the separately opened
     * caller context. DPA owns QP PD allocation; these contexts must agree. */
    struct ibv_pd *pd;

    TRY(doca_rdma_bridge_get_dev_pd(t->sf, &pd));
    t->ib = pd->context;
    TRY(doca_verbs_bridge_verbs_context_import(t->ib, 0, &t->verbs));
#if DOCA_VERSION_MAJOR == 3 && DOCA_VERSION_MINOR >= 5
    TRY(doca_verbs_bridge_verbs_pd_import(pd, &t->pd));
#endif
    TRY(DMESH_DPA_CALL(doca_dpa_completion_create(t->extended, PEER_DPA_CQ_SLOTS, &t->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_start(t->cq)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(t->cq, &t->cqh)));
    TRY(DMESH_DPA_CALL(doca_dpa_completion_get_queue_size(t->cq, &t->cq_capacity)));

    if (t->cq_capacity < PEER_DPA_LANES * (PEER_RDMA_SEND_SLOTS + PEER_RDMA_RECV_SLOTS))
        goto fail;

    t->shared = aligned_alloc(64, sizeof(*t->shared));
    if (!t->shared)
        goto fail;

    memset(t->shared, 0, sizeof(*t->shared));

    if (t->profile.handoff_enabled) {
        t->shared->profile.request = t->profile_ticket = 1;
        t->profile_requested_ns = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
    }

    TRY(doca_mmap_create(&t->ring_map));
    TRY(doca_mmap_set_memrange(t->ring_map, t->shared, sizeof(*t->shared)));
    TRY(doca_mmap_set_permissions(t->ring_map, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE));
    TRY(doca_mmap_add_dev(t->ring_map, t->pf));
    TRY(doca_mmap_start(t->ring_map));

    TRY(doca_buf_arr_create(1, &t->ring_array));
    TRY(doca_buf_arr_set_target_dpa(t->ring_array, t->base));
    TRY(doca_buf_arr_set_params(t->ring_array, t->ring_map, sizeof(*t->shared), 0));
    TRY(doca_buf_arr_start(t->ring_array));

    /* The nested QP binding has extended (64-byte) alignment; plain calloc
     * only guarantees max_align_t on ARM. Check device allocation too. */
    struct peer_dpa_device_state *state = aligned_alloc(64, sizeof(*state));
    if (!state)
        goto fail;

    memset(state, 0, sizeof(*state));
    state->profile.enabled = t->profile.handoff_enabled;
    state->profile.sample_every = t->profile.sample_every;
    state->cq = t->cqh;
    state->cq_device = t->devh;
    state->cq_capacity = t->cq_capacity;
    state->owner_worker = runtime->owner_worker;

    doca_error_t e = doca_buf_arr_get_dpa_handle(t->ring_array, &state->buffers);

    if (e == DOCA_SUCCESS)
        e = DMESH_DPA_CALL(doca_dpa_get_dpa_handle(t->base, &state->base_device));

    if (e == DOCA_SUCCESS)
        e = DMESH_DPA_CALL(doca_dpa_mem_alloc(t->base, sizeof(*state), &t->device_state));
    if (e == DOCA_SUCCESS && t->device_state % 64)
        e = DOCA_ERROR_INVALID_VALUE;
    if (e == DOCA_SUCCESS)
        e = DMESH_DPA_CALL(doca_dpa_h2d_memcpy(t->base, t->device_state, state, sizeof(*state)));
    free(state);

    if (check(e, "initialize RDMA ring"))
        goto fail;
    if (runtime->attach(runtime->attach_ctx, eu, t->device_state))
        goto fail;

    t->attached = 1;
    fprintf(stderr,
            "DPA_RING backend eu=%u sf=%s shared_cq_slots=%u; no per-WR RPC\n",
            eu,
            sf,
            t->cq_capacity);
    return t;

fail:
    free_ctx(t);

    return NULL;
}

int peer_pair_dpa_driver_new(struct ibv_context *ib,
                             uint8_t port,
                             uint32_t gid,
                             uint16_t mtu,
                             unsigned eu,
                             const struct peer_dpa_runtime *runtime,
                             struct peer_pair_driver *out)
{
    if (!ib || !runtime || !runtime->dpa || !runtime->dev || !runtime->attach || !out || !port ||
        gid > 255 || (mtu != 256 && mtu != 512 && mtu != 1024 && mtu != 2048 && mtu != 4096))
        return -1;

    memset(out, 0, sizeof(*out));

    struct ctx *t = new_ctx(ib, port, gid, mtu, eu, runtime);
    if (!t)
        return -1;

    *out = (struct peer_pair_driver){.wire = &OPS,
                                     .ctx = t,
                                     .prepare = prepare,
                                     .prepare_pair = prepare_pair,
                                     .activate = activate,
                                     .send_drained = drained,
                                     .close = close_qp,
                                     .has_live = has_live};

    return 0;
}

int peer_pair_dpa_new(const struct peer_pair_transport_config *cfg,
                      struct ibv_context *ib,
                      uint8_t port,
                      uint32_t gid,
                      uint16_t mtu,
                      const struct peer_dpa_runtime *runtime,
                      struct peer_pair_transport **out,
                      char *error,
                      size_t error_len)
{
    if (!cfg || !runtime || !out)
        return -1;

    *out = NULL;

    struct peer_pair_driver d = {0};
    if (runtime->select_group) {
        struct peer_pair_driver children[PEER_MAX_DPA_EU] = {0};
        unsigned built = 0;
        if (!runtime->workers || runtime->workers > DPA_RDMA_CONTEXTS || !runtime->rdma_threads ||
            runtime->rdma_first >= PEER_MAX_DPA_EU ||
            runtime->rdma_threads > PEER_MAX_DPA_EU - runtime->rdma_first ||
            cfg->worker >= runtime->workers || runtime->owner_worker != cfg->worker)
            goto fail;

        unsigned count = peer_dpa_child_count(
            runtime->rdma_first, runtime->rdma_threads, runtime->workers, runtime->owner_worker);
        for (; built < count; built++) {
            unsigned eu = peer_dpa_child_eu(runtime->rdma_first,
                                            runtime->rdma_threads,
                                            runtime->workers,
                                            runtime->owner_worker,
                                            built);
            if (peer_pair_dpa_driver_new(ib, port, gid, mtu, eu, runtime, &children[built]))
                break;
        }

        if (built != count ||
            peer_pair_driver_group_new(
                children, count, runtime->select_group, runtime->select_ctx, &d)) {
            for (unsigned i = 0; i < built; i++)
                children[i].wire->ctx_free(children[i].ctx);
            goto fail;
        }

        fprintf(stderr,
                "DPA_RING pool worker=%u rdma_first=%u contexts=%u first_eu=%u\n",
                cfg->worker,
                runtime->rdma_first,
                count,
                peer_dpa_child_eu(runtime->rdma_first,
                                  runtime->rdma_threads,
                                  runtime->workers,
                                  runtime->owner_worker,
                                  0));
    } else if (peer_pair_dpa_driver_new(ib, port, gid, mtu, cfg->worker, runtime, &d)) {
        goto fail;
    }

    {
        if (!peer_pair_transport_new_driver(cfg, &d, out))
            return 0;

        d.wire->ctx_free(d.ctx);
    }

fail:
    if (error && error_len)
        snprintf(error, error_len, "DPA ring backend initialization failed");

    return -1;
}
