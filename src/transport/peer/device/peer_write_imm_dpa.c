#include <doca_dpa_dev.h>
#include <doca_dpa_dev_buf.h>
#include <doca_dpa_dev_rdma.h>
#include <doca_dpa_dev_comch_msgq.h>
#include <dpaintrin.h>
#include "peer_write_imm_dpa_internal.h"
#include "peer_write_protocol.h"
#ifndef DMESH_TEST_SINGLE_WINDOW
#include <doca_version.h>
#if (DOCA_VERSION_MAJOR != 3 || DOCA_VERSION_MINOR != 5 || DOCA_VERSION_PATCH != 98)
#error "Native WRITE_WITH_IMM WQE/counter contract requires DOCA 3.5.0098"
#endif
#endif

/* Only the owning DPA thread updates this index. CQ dispatch visits a bucket's
 * colliding QPNs rather than the active-QP list. REMOVE unlinks before the
 * existing pause/destroy/RESUME drain fence permits hardware QPN reuse. */
static inline unsigned wim_qpn_bucket(uint32_t qpn)
{
    qpn ^= qpn >> 16;
    return (qpn * 2654435761u) & (WIM_QPN_BUCKETS - 1u);
}

static inline unsigned wim_qpn_find(const struct wim_device *s, uint32_t qpn)
{
    uint16_t entry = s->qpn_heads[wim_qpn_bucket(qpn)];

    while (entry) {
        unsigned lane = entry - 1u;
        if (s->lane[lane].binding.qpn == qpn)
            return lane;

        entry = s->lane[lane].qpn_next;
    }

    return WIM_LANES;
}

static inline void wim_qpn_add(struct wim_device *s, unsigned lane)
{
    unsigned bucket = wim_qpn_bucket(s->lane[lane].binding.qpn);
    s->lane[lane].qpn_next = s->qpn_heads[bucket];
    s->qpn_heads[bucket] = (uint16_t)(lane + 1u);
}

static inline void wim_qpn_remove(struct wim_device *s, unsigned lane)
{
    uint16_t *link = &s->qpn_heads[wim_qpn_bucket(s->lane[lane].binding.qpn)];

    while (*link && *link != lane + 1u)
        link = &s->lane[*link - 1u].qpn_next;

    if (*link)
        *link = s->lane[lane].qpn_next;
    s->lane[lane].qpn_next = 0;
}

enum {
    WIM_COMMIT_SEND = 1u,
    WIM_COMMIT_HEAD = 4u
};

struct wim_commit_batch {
    uint16_t lane[PEER_DPA_REQUEST_BATCH];
    uint8_t flags[PEER_DPA_REQUEST_BATCH];
    unsigned count;
};

/* All DATA in one pass form one logical completion range. Physically adjacent
 * sources/destinations also share a WRITE (up to the receive notification cap). */
static void wim_flush_data(struct wim_lane *l)
{
    struct wim_write *r = &l->last_data;
    doca_dpa_dev_rdma_post_write(l->binding.qp,
                                 l->binding.connection,
                                 r->rkey,
                                 r->remote,
                                 r->key,
                                 r->src,
                                 r->bytes,
                                 DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);
    l->post_index++;
    l->last_data_valid = 0;
}

static void wim_commit_lane(struct wim_device *s, unsigned lane, unsigned flags)
{
    struct wim_lane *l = &s->lane[lane];
    struct wim_binding *b = &l->binding;
    doca_dpa_dev_device_set(b->device);

    if (flags & WIM_COMMIT_SEND) {
        uint32_t counter;
        struct wim_write *r = &l->last_data;
        counter = l->post_index++;
        doca_dpa_dev_rdma_post_write_imm(b->qp,
                                         b->connection,
                                         r->rkey,
                                         r->remote,
                                         r->key,
                                         r->src,
                                         r->bytes,
                                         (uint32_t)l->tx_tail,
                                         DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        l->last_data_valid = 0;

        unsigned at = (l->fence_first + l->fence_count++) % WIM_FENCES;
        l->fence[at].counter = counter;
        l->fence[at].data_id = l->data_id;
    }

    if (flags & WIM_COMMIT_HEAD) {
        l->control_counter[WIM_CREDIT] = l->post_index++;
        doca_dpa_dev_rdma_post_write_imm(b->qp,
                                         b->connection,
                                         l->remote_key,
                                         l->remote_base,
                                         l->recv_key,
                                         l->recv_source,
                                         0,
                                         (uint32_t)l->rx_head,
                                         DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);

    }
}

static void
wim_defer_commit(struct wim_device *s, struct wim_commit_batch *b, unsigned lane, unsigned flags)
{
    (void)s;

    unsigned i;
    for (i = 0; i < b->count; i++)
        if (b->lane[i] == lane)
            break;

    if (i == b->count) {
        b->lane[i] = (uint16_t)lane;
        b->flags[i] = 0;
        b->count++;
    }

    b->flags[i] |= (uint8_t)flags;
}

static void
wim_commit_pending(struct wim_device *s, struct wim_commit_batch *b, struct wim_shared *q)
{
    for (unsigned i = 0; i < b->count; i++) {
        wim_commit_lane(s, b->lane[i], b->flags[i]);
        if (s->fence_stats_enabled && (b->flags[i] & WIM_COMMIT_SEND))
            peer_write_fence_stats_sample(&s->lane[b->lane[i]].fence_stats,
                                          s->lane[b->lane[i]].fence_count);
        q->status[b->lane[i]].tail_sets += !!(b->flags[i] & WIM_COMMIT_SEND);
        q->status[b->lane[i]].head_sets += !!(b->flags[i] & WIM_COMMIT_HEAD);
    }

    b->count = 0;
    doca_dpa_dev_device_set(s->base_device);
}

static void wim_fence_snapshot(struct wim_device *s, struct wim_shared *q, unsigned lane)
{
    struct wim_lane *l = &s->lane[lane];
    q->fence_snapshot = (struct wim_fence_snapshot){
        .lane = lane, .generation = l->binding.generation,
        .depth = l->fence_count, .stats = l->fence_stats};
    __builtin_memset(&l->fence_stats, 0, sizeof(l->fence_stats));
    l->fence_stats.peak = l->fence_count;
    /* The control ACK is published later, after the immutable snapshot. */
    __dpa_thread_window_writeback();
}

static void wim_fault(struct wim_lane *l, struct wim_status *o)
{
    l->failed = 1;
    o->error = 1;
}

static void wim_hint(struct peer_dpa_thread_arg *a, struct wim_device *s, struct wim_shared *q)
{
    if (!a->dpa_producer || !a->dpa_producer_comp)
        return;

    /* ARM arms its epoch before the final scan. Check even on idle passes:
     * arming can follow the last status publication. */
    uint64_t epoch = *(volatile uint64_t *)&q->wait.epoch;
    uint64_t seen = *(volatile uint64_t *)&q->wait.seen;
    if (!epoch || epoch == s->notified_epoch || s->published_seq <= seen)
        return;
    if (doca_dpa_dev_comch_producer_is_consumer_empty(a->dpa_producer, a->dpu_consumer_id))
        return;

    for (unsigned n = 0; n < s->count; n++) {
        unsigned i = s->active[n];
        struct wim_lane *l = &s->lane[i];
        if (!l->notify)
            continue;
        if (doca_dpa_dev_comch_producer_is_consumer_empty(a->dpa_producer, a->dpu_consumer_id))
            return;

        struct dpa_rdma_counter_msg m = {.type = DPA_MSG_RDMA_COUNTER,
                                         .version = DPA_RDMA_NOTIFY_VERSION,
                                         .eu_index = (uint8_t)a->eu_index,
                                         .worker = (uint8_t)s->owner_worker,
                                         .cookie = s->cookie,
                                         .generation = l->binding.generation,
                                         .lane = (uint16_t)i,
                                         .tail = l->rx_tail,
                                         .head = l->tx_head};
        doca_dpa_dev_comch_producer_post_send_imm_only(a->dpa_producer,
                                                       a->dpu_consumer_id,
                                                       (const uint8_t *)&m,
                                                       sizeof(m),
                                                       DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        l->notify = 0;
        a->producer_deferred = 0;

        /* One context-wide wake makes ARM scan all published lane state. */
        s->notified_epoch = epoch;
        return;
    }

    if (doca_dpa_dev_comch_producer_is_consumer_empty(a->dpa_producer, a->dpu_consumer_id))
        return;

    struct dpa_rdma_notify_msg m = {.type = DPA_MSG_RDMA_READY,
                                    .version = DPA_RDMA_NOTIFY_VERSION,
                                    .eu_index = (uint8_t)a->eu_index,
                                    .worker = (uint8_t)s->owner_worker};
    doca_dpa_dev_comch_producer_post_send_imm_only(a->dpa_producer,
                                                   a->dpu_consumer_id,
                                                   (const uint8_t *)&m,
                                                   sizeof(m),
                                                   DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
    a->producer_deferred = 0;

    s->notified_epoch = epoch;
}

static int
wim_request(struct wim_lane *l, struct wim_status *o, const struct wim_request *r, unsigned *commit)
{
    if (l->failed)
        return 0;
    if (r->generation != l->binding.generation || !r->sequence)
        goto bad;
    if (r->op == WIM_CONFIGURE) {
        if (l->configured || !r->config.base ||
            r->bytes < 65536 || r->bytes > 16u * 1024u * 1024u || (r->bytes & (r->bytes - 1)) ||
            r->config.base > UINT64_MAX - 2ull * r->bytes)
            goto bad;

        l->remote_base = r->config.base;
        l->remote_key = r->config.key;
        l->remote_capacity = r->bytes;
        l->configured = 1;
        return 1;
    }

    unsigned slot = wim_control_slot(r->op);
    if ((r->op != WIM_DATA && (slot >= WIM_CONTROLS || l->busy[slot])) || !r->data.src ||
        r->data.src > UINT64_MAX - r->bytes)
        goto bad;

    doca_dpa_dev_device_set(l->binding.device);
    if (r->op == WIM_RECEIVE && slot == WIM_RECV &&
        r->bytes == wim_hello_size()) {
        if (l->recv_source)
            goto bad;

        doca_dpa_dev_rdma_post_receive(l->binding.qp, r->data.key, r->data.src, r->bytes);
        o->rq_posts++;

        l->recv_source = r->data.src;
        l->recv_key = r->data.key;

        /* First entry receives the protected SEND hello. Remaining
         * entries consume only WRITE_IMM notifications: no payload
         * RX buffers, and DATA always lands in the remote ring. */
        for (unsigned n = 1; n < WIM_IMM_RQ; n++) {
            doca_dpa_dev_rdma_post_receive(l->binding.qp, r->data.key, r->data.src, 0);
            o->rq_posts++;
        }

        l->rq_live = WIM_IMM_RQ;

    } else if (r->op == WIM_SEND_BOOT && slot == WIM_BOOT &&
               r->bytes == wim_hello_size()) {
        l->control_counter[slot] = l->post_index++;
        doca_dpa_dev_rdma_post_send(l->binding.qp,
                                    l->binding.connection,
                                    r->data.key,
                                    r->data.src,
                                    r->bytes,
                                    DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
    } else if (r->op == WIM_DATA && l->configured) {
        if (r->bytes < PEER_DATA_HEADER_BYTES || r->bytes > 65536 || l->tx_head > l->tx_tail ||
            l->tx_tail - l->tx_head > l->remote_capacity ||
            r->bytes > l->remote_capacity - (l->tx_tail - l->tx_head) ||
            l->tx_tail > UINT64_MAX - r->bytes || r->data.delta != l->tx_tail + r->bytes ||
            r->data.id != l->data_id + 1u)
            goto bad;

        /* CONFIGURE and DATA remain ordered in one per-QP FIFO. Expand
         * the destination locally before checking adjacent WRITE ranges. */
        struct wim_write next = {.src = r->data.src,
                                 .remote = l->remote_base + l->tx_tail % l->remote_capacity,
                                 .key = r->data.key,
                                 .rkey = l->remote_key,
                                 .bytes = r->bytes};
        struct wim_write *p = &l->last_data;
        if (l->last_data_valid && p->key == next.key && p->rkey == next.rkey &&
            p->src + p->bytes == next.src && p->remote + p->bytes == next.remote &&
            next.bytes <= 65536u - p->bytes) {
            p->bytes += r->bytes;
        } else {
            if (l->last_data_valid)
                wim_flush_data(l);
            l->last_data = next;
            l->last_data_valid = 1;
        }

        l->tx_tail += r->bytes;
        l->data_id = r->data.id;
        o->data_posts++;
        *commit = WIM_COMMIT_SEND;
    } else if (r->op == WIM_HEAD && slot == WIM_CREDIT && l->configured) {
        if (!r->data.delta || r->data.delta > UINT64_MAX - l->rx_head ||
            l->rx_head + r->data.delta > l->rx_tail)
            goto bad;

        l->rx_head += r->data.delta;
        *commit = WIM_COMMIT_HEAD;
    } else
        goto bad;

    if (r->op != WIM_DATA) {
        l->id[slot] = r->sequence;
        l->busy[slot] = 1;
    }

    return 1;

bad:
    wim_fault(l, o);

    return 1;
}

unsigned
peer_write_imm_dpa_drain(struct peer_dpa_thread_arg *a, unsigned context, uint64_t ptr, unsigned budget)
{
    struct wim_device *s = (void *)ptr;
    doca_dpa_dev_device_set(s->base_device);

    struct wim_shared *q =
        (void *)doca_dpa_dev_buf_get_external_ptr(doca_dpa_dev_buf_array_get_buf(s->buffers, 0));
    __dpa_thread_window_read_inv();

    unsigned work = 0;
    int dirty = 0;
    unsigned requests = budget < PEER_DPA_REQUEST_BATCH ? budget : PEER_DPA_REQUEST_BATCH;
    unsigned controls = requests < 16u ? requests : 16u;

    /* Sequential control owner: REMOVE waits for all previously published
     * descriptors, pauses CQ, then ACKs. ARM destroys QP before RESUME. */
    while (work < controls) {
        volatile struct wim_request *src = &q->control[s->control_head % WIM_RING];
        if (src->ticket != s->control_head + 1)
            break;

        struct wim_request r = *src; /* immutable after release-published ticket */
        if (r.op == WIM_REMOVE && r.lane < WIM_LANES &&
            (s->data_head < r.control.data_fence || s->pending.head[r.lane]))
            break;

        if (r.op == WIM_INSTALL && r.lane < WIM_LANES && !s->paused && !s->resume_id &&
            !s->lane[r.lane].installed && s->count < WIM_LANES) {
            struct wim_lane *l = &s->lane[r.lane];
            __builtin_memset(l, 0, sizeof(*l));
            l->binding = q->binding[r.lane];
            __builtin_memset(&q->status[r.lane], 0, sizeof(q->status[r.lane]));

            if (!l->binding.qp || !l->binding.device || !l->binding.qpn || !l->binding.generation ||
                l->binding.capacity < 65536 ||
                l->binding.capacity > 16u * 1024u * 1024u ||
                (l->binding.capacity & (l->binding.capacity - 1)) ||
                wim_qpn_find(s, l->binding.qpn) != WIM_LANES) {
                wim_fault(l, &q->status[r.lane]);
            } else {
                l->installed = 1;
                wim_qpn_add(s, r.lane);
                s->active[s->count++] = r.lane;
                q->status[r.lane].generation = l->binding.generation;

            }
        } else if (r.op == WIM_REMOVE && r.lane < WIM_LANES) {
            if (s->fence_stats_enabled)
                wim_fence_snapshot(s, q, r.lane);
            s->paused = 1;
            if (s->lane[r.lane].installed)
                wim_qpn_remove(s, r.lane);
            s->lane[r.lane].installed = 0;

            for (unsigned i = 0; i < s->count; i++)
                if (s->active[i] == r.lane) {
                    s->active[i] = s->active[--s->count];
                    break;
                }
        } else if (r.op == WIM_FENCE_SNAPSHOT && s->fence_stats_enabled &&
                   r.lane < WIM_LANES && s->lane[r.lane].installed &&
                   r.generation == s->lane[r.lane].binding.generation) {
            wim_fence_snapshot(s, q, r.lane);
        } else if (r.op == WIM_RESUME && s->paused && !s->resume_id) {
            s->paused = 0;
            s->resume_id = r.sequence;
            s->flush_left = s->cq_capacity;
        } else if (r.op == WIM_STOP && !s->count && !s->paused && !s->resume_id) {
            if (context == DPA_RDMA_CONTEXTS)
                a->rdma_state = 0;
            else
                a->rdma_states[context] = 0;

            q->stopped = r.sequence;
            __dpa_thread_window_writeback();

            return work + 1;
        } else {
            break;
        }

        s->control_head++;
        q->control_consumed = s->control_head;

        if (r.op != WIM_RESUME)
            q->control_ack = r.sequence;
        dirty = 1;
        work++;
    }

    /* Import at most one pass from the context FIFO. QPN is carried in
     * the immutable header; no active-QP discovery scan is needed.
     * Credit means the immutable descriptor was copied, never NIC completion. */
    peer_dpa_work_init(&s->pending);
    for (unsigned imported = 0;
         imported < requests && s->pending.free && !s->paused && !s->resume_id;
         imported++) {
        volatile struct wim_request *src = &q->data.entry[s->data_head % WIM_RING];
        if (src->ticket != s->data_head + 1)
            break;

        struct wim_request r = *src;
        unsigned lane = wim_qpn_find(s, r.qpn);
        if (lane != WIM_LANES && r.generation == s->lane[lane].binding.generation) {
            unsigned entry = peer_dpa_work_push(&s->pending, lane);
            s->pending_request[entry - 1] = r;
        } else if (lane == WIM_LANES && r.lane < WIM_LANES && s->lane[r.lane].installed &&
                   r.generation == s->lane[r.lane].binding.generation) {
            wim_fault(&s->lane[r.lane], &q->status[r.lane]);
        }
        /* A stale/unknown QPN never dispatches through a reused lane hint. */
        s->data_head++;
        q->data_credit.consumed = s->data_head;
        dirty = 1;
    }

    struct wim_commit_batch commits = {0};
    unsigned visits = s->pending.count;
    while (!s->paused && !s->resume_id && visits-- && work < requests) {
        unsigned i = peer_dpa_work_take(&s->pending);
        struct wim_lane *l = &s->lane[i];
        struct wim_status *o = &q->status[i];
        unsigned consumed = 0;

        while (s->pending.head[i] && consumed < PEER_DPA_QP_VISIT && work < requests) {
            struct wim_request *r = &s->pending_request[s->pending.head[i] - 1];
            /* A stalled QP keeps its private FIFO, while other ready QPs run. */
            if (!l->failed &&
                (l->fence_count == WIM_FENCES ||
                 (uint32_t)(l->post_index - l->completed_index) > WIM_SQ - PEER_DPA_REQUEST_BATCH - 8u)) {
                if (s->fence_stats_enabled && l->fence_count == WIM_FENCES)
                    l->fence_stats.full_stalls++;
                break;
            }

            unsigned commit = 0;
            if (r->lane != i || r->qpn != l->binding.qpn ||
                r->generation != l->binding.generation || r->sequence != l->consumed + 1)
                wim_fault(l, o);
            else if (wim_request(l, o, r, &commit)) {
                dirty = 1;
                l->notify = 1;
            }

            if (commit)
                wim_defer_commit(s, &commits, i, commit);
            l->consumed++;
            o->consumed = l->consumed;
            peer_dpa_work_pop(&s->pending, i);
            work++;
            consumed++;
            dirty = 1;
        }

        if (s->pending.head[i])
            peer_dpa_work_ready(&s->pending, i);
    }

    /* Flush even a partial batch or a lane whose later descriptor faults.
     * CQ polling, status writeback, hints and the next REMOVE all follow it. */
    wim_commit_pending(s, &commits, q);

    unsigned ack = 0;
    if (!s->paused) {
        doca_dpa_dev_device_set(s->cq_device);

        for (; ack < 32 && work < budget; ack++, work++) {
            doca_dpa_dev_completion_element_t e;
            if (!doca_dpa_dev_get_completion(s->cq, &e)) {
                s->flush_left = 0;
                break;
            }

            uint32_t qpn = doca_dpa_dev_completion_element_get_qpn(e);
            unsigned i = wim_qpn_find(s, qpn);
            if (i != WIM_LANES && !s->lane[i].failed) {
                struct wim_lane *l = &s->lane[i];

                unsigned type = doca_dpa_dev_get_completion_type(e);
                uint32_t completed = doca_dpa_dev_rdma_completion_get_wr_index(e);
                struct wim_status *o = &q->status[i];
                if (type == DOCA_DPA_DEV_COMP_RECV_RDMA_WRITE_IMM) {
                    uint32_t bytes = doca_dpa_dev_completion_element_get_received_bytes(e);
                    uint32_t low = doca_dpa_dev_get_completion_immediate(e);
                    uint64_t next;

                    /* CQEs arrive in RC order. A ring window <2^31
                     * bounds every unseen delta, including 32-bit wrap. */
                    int valid =
                        l->recv_source && !l->busy[WIM_RECV] && l->rq_live &&
                        (bytes ? (bytes >= PEER_DATA_HEADER_BYTES && bytes <= 65536 && l->rx_tail >= l->rx_head &&
                                  l->rx_tail - l->rx_head <= l->binding.capacity &&
                                  peer_write_imm_extend(l->rx_tail,
                                                        low,
                                                        l->binding.capacity -
                                                            (l->rx_tail - l->rx_head),
                                                        &next) > 0 &&
                                  next - l->rx_tail >= bytes)
                               : (l->tx_head <= l->tx_tail &&
                                  peer_write_imm_extend(
                                      l->tx_head, low, l->tx_tail - l->tx_head, &next) >= 0));
                    if (!valid)
                        wim_fault(l, o);
                    else {
                        if (bytes)
                            o->tail = l->rx_tail = next;
                        else
                            o->head = l->tx_head = next;

                        o->recv_cqes++;
                        doca_dpa_dev_device_set(l->binding.device);
                        doca_dpa_dev_rdma_receive_ack(l->binding.qp, 1);
                        doca_dpa_dev_rdma_post_receive(
                            l->binding.qp, l->recv_key, l->recv_source, 0);
                        o->rq_posts++;
                        doca_dpa_dev_device_set(s->cq_device);
                    }
                } else if (type == DOCA_DPA_DEV_COMP_RECV_SEND && l->busy[WIM_RECV]) {
                    uint32_t bytes = doca_dpa_dev_completion_element_get_received_bytes(e);
                    if (bytes != wim_hello_size())
                        wim_fault(l, o);
                    else {
                        o->done[WIM_RECV].bytes = bytes;
                        o->done[WIM_RECV].id = l->id[WIM_RECV];
                        l->busy[WIM_RECV] = 0;
                        doca_dpa_dev_device_set(l->binding.device);
                        doca_dpa_dev_rdma_receive_ack(l->binding.qp, 1);

                        doca_dpa_dev_rdma_post_receive(
                            l->binding.qp, l->recv_key, l->recv_source, 0);
                        o->rq_posts++;

                        doca_dpa_dev_device_set(s->cq_device);
                    }
                } else if (type == DOCA_DPA_DEV_COMP_SEND) {
                    o->send_cqes++;

                    if (!l->completion_base_valid && l->busy[WIM_BOOT]) {
                        l->completion_base = completed - l->control_counter[WIM_BOOT];
                        l->completion_base_valid = 1;
                    }

                    uint32_t step = (uint16_t)((uint16_t)(completed - l->completion_base + 1) -
                                               (uint16_t)l->completed_index);
                    uint32_t pending = l->post_index - l->completed_index;
                    if (!l->completion_base_valid || !step || step > pending || pending > WIM_SQ)
                        wim_fault(l, o);
                    else {
                        l->completed_index += step;
                        while (l->fence_count) {
                            unsigned at = l->fence_first;
                            if ((uint32_t)(l->completed_index - l->fence[at].counter - 1) >= WIM_SQ)
                                break;
                            o->completed_id = l->fence[at].data_id;
                            l->fence_first = (at + 1) % WIM_FENCES;
                            l->fence_count--;
                        }

                        for (unsigned slot = 0; slot < WIM_RECV; slot++)
                            if (l->busy[slot] &&
                                (uint32_t)(l->completed_index - l->control_counter[slot] - 1) <
                                    WIM_SQ) {
                                o->done[slot].id = l->id[slot];
                                l->busy[slot] = 0;
                            }
                    }
                } else
                    wim_fault(l, o);

                l->notify = 1;
                dirty = 1;
            }

            if (s->flush_left)
                s->flush_left--;
        }

        if (ack)
            doca_dpa_dev_completion_ack(s->cq, ack);
        if (s->resume_id && !s->flush_left) {
            q->control_ack = s->resume_id;
            s->resume_id = 0;
            dirty = 1;
        }
    }

    doca_dpa_dev_device_set(s->base_device);

    if (!q->notification && a->dpa_producer && a->dpa_producer_comp) {
        q->notification = 1;
        dirty = 1;
    }

    if (dirty) {
        __dpa_thread_window_writeback();

        /* A separate publication after status writeback closes the arm/scan
         * race across cache lines. Saturation fails closed on the ARM side. */
        if (s->published_seq != UINT64_MAX)
            s->published_seq++;
        q->publication.sequence = s->published_seq;
        __dpa_thread_window_writeback();
    }

    wim_hint(a, s, q);

    return work;
}
