#include <doca_dpa_dev.h>
#include <doca_dpa_dev_buf.h>
#include <doca_dpa_dev_rdma.h>
#include <doca_dpa_dev_sync_event.h>
#include <doca_dpa_dev_comch_msgq.h>
#include <dpaintrin.h>
#include "peer_write_sync_dpa_shared.h"
#include "peer_write_sync_dpa_cq.h"
#include "peer_write_imm_counter.h"
#ifndef DMESH_TEST_SINGLE_WINDOW
#include <doca_version.h>
#if (DOCA_VERSION_MAJOR != 3 || DOCA_VERSION_MINOR != 5 || DOCA_VERSION_PATCH != 98)
#error "Native SET WQE/counter contract requires DOCA 3.5.0098"
#endif
#endif

/* Match SEND's context request budget and per-QP visit bound. Keep the
 * touched list local: no deferred NIC work may survive this drain pass. */
#define WSD_REQUEST_BATCH 32u
#define WSD_QP_VISIT 32u

enum {
    WSD_COMMIT_SEND = 1u,
    WSD_COMMIT_RECV = 2u,
    WSD_COMMIT_HEAD = 4u
};

struct wsd_commit_batch {
    uint16_t lane[WSD_REQUEST_BATCH];
    uint8_t flags[WSD_REQUEST_BATCH];
    unsigned count;
};

/* All DATA in one pass form one logical completion range. Physically adjacent
 * sources/destinations also share a WRITE (up to the receive notification cap). */
static void wsd_flush_data(struct wsd_lane *l)
{
    struct wsd_write *r = &l->last_data;
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

static void wsd_commit_lane(struct wsd_device *s, unsigned lane, unsigned flags)
{
    struct wsd_lane *l = &s->lane[lane];
    struct wsd_binding *b = &l->binding;
    doca_dpa_dev_device_set(b->device);

    if (flags & WSD_COMMIT_SEND) {
        uint32_t counter;
        if (b->mode == WSD_IMMEDIATE) {
            struct wsd_write *r = &l->last_data;
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
        } else {
            wsd_flush_data(l);
            /* SDK3.5 SET is a READ fence + signalled inline WRITE. */
            counter = l->post_index + 1;
            l->post_index += 2;
            doca_dpa_dev_rdma_signal_set(b->qp, b->connection, l->tail_addr, l->tx_tail);
        }

        unsigned at = (l->fence_first + l->fence_count++) % WSD_FENCES;
        l->fence[at].counter = counter;
        l->fence[at].data_id = l->data_id;
    }

    if (flags & WSD_COMMIT_HEAD) {
        if (b->mode == WSD_IMMEDIATE) {
            l->control_counter[WSD_CREDIT] = l->post_index++;
            doca_dpa_dev_rdma_post_write_imm(b->qp,
                                             b->connection,
                                             l->remote_key,
                                             l->remote_base,
                                             l->recv_key,
                                             l->recv_source,
                                             0,
                                             (uint32_t)l->rx_head,
                                             DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        } else {
            l->control_counter[WSD_CREDIT] = l->post_index + 1;
            l->post_index += 2;
            doca_dpa_dev_rdma_signal_set(b->qp, b->connection, l->head_addr, l->rx_head);
        }
    }
}

static void
wsd_defer_commit(struct wsd_device *s, struct wsd_commit_batch *b, unsigned lane, unsigned flags)
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
wsd_commit_pending(struct wsd_device *s, struct wsd_commit_batch *b, struct wsd_shared *q)
{
    for (unsigned i = 0; i < b->count; i++) {
        wsd_commit_lane(s, b->lane[i], b->flags[i]);
        if (s->fence_stats_enabled && (b->flags[i] & WSD_COMMIT_SEND))
            peer_write_fence_stats_sample(&s->lane[b->lane[i]].fence_stats,
                                          s->lane[b->lane[i]].fence_count);
        q->status[b->lane[i]].tail_sets += !!(b->flags[i] & WSD_COMMIT_SEND);
        q->status[b->lane[i]].head_sets += !!(b->flags[i] & WSD_COMMIT_HEAD);
    }

    b->count = 0;
    doca_dpa_dev_device_set(s->base_device);
}

static void wsd_fence_snapshot(struct wsd_device *s, struct wsd_shared *q, unsigned lane)
{
    struct wsd_lane *l = &s->lane[lane];
    q->fence_snapshot = (struct wsd_fence_snapshot){
        .lane = lane, .generation = l->binding.generation,
        .depth = l->fence_count, .stats = l->fence_stats};
    __builtin_memset(&l->fence_stats, 0, sizeof(l->fence_stats));
    l->fence_stats.peak = l->fence_count;
    /* The control ACK is published later, after the immutable snapshot. */
    __dpa_thread_window_writeback();
}

static void wsd_fault(struct wsd_lane *l, struct wsd_status *o)
{
    l->failed = 1;
    o->error = 1;
}

/* GET mode and async completions share the same bounded raw32 extension.
 * Async mode reads only the signalled event, then rearms against that exact
 * raw value. An update between GET and post_wait_ne satisfies the new wait. */
static int wsd_event_read(
    struct wsd_device *s, struct wsd_lane *l, struct wsd_status *o, unsigned kind, uint64_t *raw)
{
    doca_dpa_dev_device_set(l->binding.device);
    doca_dpa_dev_sync_event_get(kind ? l->binding.local_head : l->binding.local_tail, raw);
    doca_dpa_dev_device_set(s->base_device);
    o->event_reads++;

    uint64_t previous = kind ? l->tx_head : l->rx_tail;
    uint64_t delta = (uint32_t)((uint32_t)*raw - (uint32_t)previous);
    if (*raw > UINT32_MAX || previous > UINT64_MAX - delta) {
        wsd_fault(l, o);
        return 1;
    }

    uint64_t next = previous + delta;
    if (kind ? next > l->tx_tail : (next < l->rx_head || next - l->rx_head > l->binding.capacity)) {
        wsd_fault(l, o);
        return 1;
    }
    if (next == previous)
        return 0;

    if (kind)
        o->head = l->tx_head = next;
    else
        o->tail = l->rx_tail = next;
    l->notify = 1;
    return 1;
}

static int
wsd_event_arm(struct wsd_device *s, struct wsd_lane *l, struct wsd_status *o, unsigned kind)
{
    uint64_t raw;
    int dirty = wsd_event_read(s, l, o, kind, &raw);
    if (!l->failed) {
        doca_dpa_dev_device_set(l->binding.device);
        doca_dpa_dev_sync_event_post_wait_ne(
            l->binding.wait_ops[kind], kind ? l->binding.local_head : l->binding.local_tail, raw);
        doca_dpa_dev_device_set(s->base_device);
        l->wait_armed[kind] = 1;
        o->event_posts++;
    }
    return dirty;
}

static unsigned
wsd_event_drain(struct wsd_device *s, struct wsd_shared *q, unsigned budget, int *dirty)
{
    unsigned work = 0;
    while (work < 16 && work < budget) {
        doca_dpa_dev_completion_element_t e;
        doca_dpa_dev_device_set(s->cq_device);
        if (!doca_dpa_dev_get_completion(s->event_cq, &e)) {
            s->event_flush_left = 0;
            break;
        }

        uint32_t tag = doca_dpa_dev_get_completion_user_data(e);
        unsigned lane = (tag & 0xffu) >> 1, kind = tag & 1u;
        unsigned type = doca_dpa_dev_get_completion_type(e);
        /* Return CQ credits before rearming the same async context. */
        doca_dpa_dev_completion_ack(s->event_cq, 1);
        doca_dpa_dev_device_set(s->base_device);
        struct wsd_lane *l = lane < WSD_LANES ? &s->lane[lane] : NULL;
        if (l && l->installed && !l->failed && l->binding.mode == WSD_SYNC &&
            tag == (l->binding.wait_tag | kind) && l->wait_armed[kind]) {
            struct wsd_status *o = &q->status[lane];
            l->wait_armed[kind] = 0;
            o->event_completions++;
            *dirty = 1;
            if (type != DOCA_DPA_DEV_COMP_SEND)
                wsd_fault(l, o);
            else
                *dirty |= wsd_event_arm(s, l, o, kind);
        }
        if (s->event_flush_left)
            s->event_flush_left--;
        work++;
    }
    doca_dpa_dev_device_set(s->base_device);
    return work;
}

static void wsd_hint(struct peer_dpa_thread_arg *a, struct wsd_device *s, struct wsd_shared *q)
{
    if (!a->dpa_producer || !a->dpa_producer_comp)
        return;

    uint64_t epoch = 0;
    if (q->wait.enabled) {
        /* Only ARM writes this line. No cross-device atomic RMW is needed.
         * Check even on an idle pass: arm may follow the last dirty pass. */
        epoch = *(volatile uint64_t *)&q->wait.epoch;

        uint64_t seen = *(volatile uint64_t *)&q->wait.seen;
        if (!epoch || epoch == s->notified_epoch || s->published_seq <= seen)
            return;
    } else if (!s->notify_pending)
        return; /* legacy NIC-model fixtures */
    if (doca_dpa_dev_comch_producer_is_consumer_empty(a->dpa_producer, a->dpu_consumer_id))
        return;

    unsigned sent = 0;
    for (unsigned n = 0; n < s->count; n++) {
        unsigned i = s->active[n];
        struct wsd_lane *l = &s->lane[i];
        if (!l->binding.native || !l->notify)
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
        sent++;

        /* ARM scans ALL lane state after this context-wide wake. Keep the
         * counter snapshot for routing/diagnostics; no per-lane wake burst. */
        if (epoch) {
            s->notified_epoch = epoch;
            s->notify_pending = 0;
            return;
        }
    }

    if (sent) {
        s->notify_pending = 0;
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
    s->notify_pending = 0;

    if (epoch)
        s->notified_epoch = epoch;
}

static int
wsd_request(struct wsd_lane *l, struct wsd_status *o, const struct wsd_request *r, unsigned *commit)
{
    if (l->failed)
        return 0;
    if (r->generation != l->binding.generation || !r->sequence)
        goto bad;
    if (r->op == WSD_CONFIGURE) {
        if (l->configured || !r->config.base ||
            (l->binding.mode != WSD_IMMEDIATE && (!r->config.tail_event || !r->config.head_event ||
                                                  r->config.tail_event == r->config.head_event)) ||
            r->bytes < 65536 || r->bytes > 16u * 1024u * 1024u || (r->bytes & (r->bytes - 1)) ||
            r->config.base > UINT64_MAX - 2ull * r->bytes)
            goto bad;

        l->remote_base = r->config.base;
        l->tail_addr = r->config.tail_event;
        l->head_addr = r->config.head_event;
        l->remote_key = r->config.key;
        l->event_key = r->config.event_key;
        l->remote_capacity = r->bytes;
        l->configured = 1;
        return 1;
    }

    unsigned slot = wsd_control_slot(r->op);
    if ((r->op != WSD_DATA && (slot >= WSD_CONTROLS || l->busy[slot])) || !r->data.src ||
        r->data.src > UINT64_MAX - r->bytes)
        goto bad;

    doca_dpa_dev_device_set(l->binding.device);
    {
        if (r->op == WSD_RECEIVE && slot == WSD_RECV &&
            r->bytes == wsd_hello_size(l->binding.mode)) {
            if (l->binding.mode == WSD_IMMEDIATE && l->recv_source)
                goto bad;

            doca_dpa_dev_rdma_post_receive(l->binding.qp, r->data.key, r->data.src, r->bytes);
            o->rq_posts++;

            if (l->binding.mode == WSD_IMMEDIATE) {
                l->recv_source = r->data.src;
                l->recv_key = r->data.key;

                /* First entry receives the protected SEND hello. Remaining
                 * entries consume only WRITE_IMM notifications: no payload
                 * RX buffers, and DATA always lands in the remote ring. */
                for (unsigned n = 1; n < WSD_IMM_RQ; n++) {
                    doca_dpa_dev_rdma_post_receive(l->binding.qp, r->data.key, r->data.src, 0);
                    o->rq_posts++;
                }

                l->rq_live = WSD_IMM_RQ;
            }
        } else if (r->op == WSD_SEND_BOOT && slot == WSD_BOOT &&
                   r->bytes == wsd_hello_size(l->binding.mode)) {
            l->control_counter[slot] = l->post_index++;
            doca_dpa_dev_rdma_post_send(l->binding.qp,
                                        l->binding.connection,
                                        r->data.key,
                                        r->data.src,
                                        r->bytes,
                                        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
        } else if (r->op == WSD_DATA && l->configured) {
            if (r->bytes < PEER_DATA_HEADER_BYTES || r->bytes > 65536 || l->tx_head > l->tx_tail ||
                l->tx_tail - l->tx_head > l->remote_capacity ||
                r->bytes > l->remote_capacity - (l->tx_tail - l->tx_head) ||
                l->tx_tail > UINT64_MAX - r->bytes || r->data.delta != l->tx_tail + r->bytes ||
                r->data.id != l->data_id + 1u)
                goto bad;

            /* CONFIGURE and DATA remain ordered in one per-QP FIFO. Expand
             * the destination locally before checking adjacent WRITE ranges. */
            struct wsd_write next = {.src = r->data.src,
                                     .remote = l->remote_base + l->tx_tail % l->remote_capacity,
                                     .key = r->data.key,
                                     .rkey = l->remote_key,
                                     .bytes = r->bytes};
            struct wsd_write *p = &l->last_data;
            if (l->last_data_valid && p->key == next.key && p->rkey == next.rkey &&
                p->src + p->bytes == next.src && p->remote + p->bytes == next.remote &&
                next.bytes <= 65536u - p->bytes) {
                p->bytes += r->bytes;
            } else {
                if (l->last_data_valid)
                    wsd_flush_data(l);
                l->last_data = next;
                l->last_data_valid = 1;
            }

            l->tx_tail += r->bytes;
            l->data_id = r->data.id;
            o->data_posts++;
            *commit = WSD_COMMIT_SEND;
        } else if (r->op == WSD_HEAD && slot == WSD_CREDIT && l->configured) {
            if (!r->data.delta || r->data.delta > UINT64_MAX - l->rx_head ||
                l->rx_head + r->data.delta > l->rx_tail)
                goto bad;

            l->rx_head += r->data.delta;
            *commit = WSD_COMMIT_HEAD;
        } else
            goto bad;

        if (r->op != WSD_DATA) {
            l->id[slot] = r->sequence;
            l->busy[slot] = 1;
        }

        return 1;
    }

bad:
    wsd_fault(l, o);

    return 1;
}

unsigned
peer_write_sync_dpa_drain(struct peer_dpa_thread_arg *a, unsigned context, uint64_t ptr, unsigned budget)
{
    struct wsd_device *s = (void *)ptr;
    doca_dpa_dev_device_set(s->base_device);

    struct wsd_shared *q =
        (void *)doca_dpa_dev_buf_get_external_ptr(doca_dpa_dev_buf_array_get_buf(s->buffers, 0));
    __dpa_thread_window_read_inv();

    unsigned work = 0;
    int dirty = 0;
    unsigned requests = budget < WSD_REQUEST_BATCH ? budget : WSD_REQUEST_BATCH;
    unsigned controls = requests < 16u ? requests : 16u;

    /* Sequential control owner: REMOVE waits for all previously published
     * descriptors, pauses CQ, then ACKs. ARM destroys QP before RESUME. */
    while (work < controls) {
        volatile struct wsd_request *src = &q->control[s->control_head % WSD_RING];
        if (src->ticket != s->control_head + 1)
            break;

        struct wsd_request r = *src; /* immutable after release-published ticket */
        if (r.op == WSD_REMOVE && r.lane < WSD_LANES &&
            (s->data_head < r.control.data_fence || s->pending.head[r.lane]))
            break;

        if (r.op == WSD_INSTALL && r.lane < WSD_LANES && !s->paused && !s->resume_id &&
            !s->lane[r.lane].installed && s->count < WSD_LANES) {
            struct wsd_lane *l = &s->lane[r.lane];
            __builtin_memset(l, 0, sizeof(*l));
            l->binding = q->binding[r.lane];
            __builtin_memset(&q->status[r.lane], 0, sizeof(q->status[r.lane]));

            if (!l->binding.qp || !l->binding.device || !l->binding.qpn || !l->binding.generation ||
                !l->binding.native || l->binding.capacity < 65536 ||
                l->binding.capacity > 16u * 1024u * 1024u ||
                (l->binding.capacity & (l->binding.capacity - 1)) ||
                (l->binding.mode != WSD_SYNC && l->binding.mode != WSD_IMMEDIATE) ||
                (l->binding.mode != WSD_IMMEDIATE &&
                 (!l->binding.local_tail || !l->binding.local_head)) ||
                (s->event_wait == WSD_EVENT_ASYNC &&
                 (!s->event_cq || l->binding.mode != WSD_SYNC || !l->binding.wait_ops[0] ||
                  !l->binding.wait_ops[1] || !(l->binding.wait_tag >> WSD_WAIT_TAG_SHIFT) ||
                  (l->binding.wait_tag >> WSD_WAIT_TAG_SHIFT) > WSD_WAIT_SERIAL_MAX ||
                  (l->binding.wait_tag & 0xffu) != r.lane * 2u)) ||
                wsd_qpn_find(s, l->binding.qpn) != WSD_LANES) {
                wsd_fault(l, &q->status[r.lane]);
            } else {
                l->installed = 1;
                wsd_qpn_add(s, r.lane);
                s->active[s->count++] = r.lane;
                q->status[r.lane].generation = l->binding.generation;
                if (s->event_wait == WSD_EVENT_ASYNC) {
                    wsd_event_arm(s, l, &q->status[r.lane], 0);
                    if (!l->failed)
                        wsd_event_arm(s, l, &q->status[r.lane], 1);
                }
            }
        } else if (r.op == WSD_REMOVE && r.lane < WSD_LANES) {
            if (s->fence_stats_enabled)
                wsd_fence_snapshot(s, q, r.lane);
            s->paused = 1;
            if (s->lane[r.lane].installed)
                wsd_qpn_remove(s, r.lane);
            s->lane[r.lane].installed = 0;
            s->lane[r.lane].wait_armed[0] = s->lane[r.lane].wait_armed[1] = 0;

            for (unsigned i = 0; i < s->count; i++)
                if (s->active[i] == r.lane) {
                    s->active[i] = s->active[--s->count];
                    break;
                }
        } else if (r.op == WSD_FENCE_SNAPSHOT && s->fence_stats_enabled &&
                   r.lane < WSD_LANES && s->lane[r.lane].installed &&
                   r.generation == s->lane[r.lane].binding.generation) {
            wsd_fence_snapshot(s, q, r.lane);
        } else if (r.op == WSD_RESUME && s->paused && !s->resume_id) {
            s->paused = 0;
            s->resume_id = r.sequence;
            s->flush_left = s->cq_capacity;
            s->event_flush_left = s->event_cq_capacity;
        } else if (r.op == WSD_STOP && !s->count && !s->paused && !s->resume_id) {
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

        if (r.op != WSD_RESUME)
            q->control_ack = r.sequence;
        dirty = 1;
        work++;
    }

    unsigned count = s->count;
    if (s->event_wait == WSD_EVENT_ASYNC) {
        /* Empty event CQ causes no QP scan and no event counter GETs. */
        if (!s->paused)
            work += wsd_event_drain(s, q, budget - work, &dirty);
    } else {
        /* Legacy GET mode remains selectable for direct A/B comparison. */
        for (unsigned n = 0; n < count && work < budget; n++) {
            unsigned i = s->active[(s->cursor + n) % count];
            struct wsd_lane *l = &s->lane[i];
            if (l->failed || l->binding.mode == WSD_IMMEDIATE)
                continue;

            uint64_t raw;
            dirty |= wsd_event_read(s, l, &q->status[i], 0, &raw);
            if (!l->failed)
                dirty |= wsd_event_read(s, l, &q->status[i], 1, &raw);
        }
    }

    /* Import at most one pass from the context FIFO. QPN is carried in
     * the immutable header; no active-QP discovery scan is needed.
     * Credit means the immutable descriptor was copied, never NIC completion. */
    peer_dpa_work_init(&s->pending);
    for (unsigned imported = 0;
         imported < requests && s->pending.free && !s->paused && !s->resume_id;
         imported++) {
        volatile struct wsd_request *src = &q->data.entry[s->data_head % WSD_RING];
        if (src->ticket != s->data_head + 1)
            break;

        struct wsd_request r = *src;
        unsigned lane = wsd_qpn_find(s, r.qpn);
        if (lane != WSD_LANES && r.generation == s->lane[lane].binding.generation) {
            unsigned entry = peer_dpa_work_push(&s->pending, lane);
            s->pending_request[entry - 1] = r;
        } else if (lane == WSD_LANES && r.lane < WSD_LANES && s->lane[r.lane].installed &&
                   r.generation == s->lane[r.lane].binding.generation) {
            wsd_fault(&s->lane[r.lane], &q->status[r.lane]);
        }
        /* A stale/unknown QPN never dispatches through a reused lane hint. */
        s->data_head++;
        q->data_credit.consumed = s->data_head;
        dirty = 1;
    }

    struct wsd_commit_batch commits = {0};
    unsigned visits = s->pending.count;
    while (!s->paused && !s->resume_id && visits-- && work < requests) {
        unsigned i = peer_dpa_work_take(&s->pending);
        struct wsd_lane *l = &s->lane[i];
        struct wsd_status *o = &q->status[i];
        unsigned consumed = 0;

        while (s->pending.head[i] && consumed < WSD_QP_VISIT && work < requests) {
            struct wsd_request *r = &s->pending_request[s->pending.head[i] - 1];
            /* A stalled QP keeps its private FIFO, while other ready QPs run. */
            if (!l->failed &&
                (l->fence_count == WSD_FENCES ||
                 (uint32_t)(l->post_index - l->completed_index) > WSD_SQ - WSD_REQUEST_BATCH - 8u)) {
                if (s->fence_stats_enabled && l->fence_count == WSD_FENCES)
                    l->fence_stats.full_stalls++;
                break;
            }

            unsigned commit = 0;
            if (r->lane != i || r->qpn != l->binding.qpn ||
                r->generation != l->binding.generation || r->sequence != l->consumed + 1)
                wsd_fault(l, o);
            else if (wsd_request(l, o, r, &commit)) {
                dirty = 1;
                l->notify = 1;
            }

            if (commit)
                wsd_defer_commit(s, &commits, i, commit);
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
    wsd_commit_pending(s, &commits, q);

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
            unsigned i = wsd_qpn_find(s, qpn);
            if (i != WSD_LANES && !s->lane[i].failed) {
                struct wsd_lane *l = &s->lane[i];

                unsigned type = doca_dpa_dev_get_completion_type(e);
                uint32_t completed = doca_dpa_dev_rdma_completion_get_wr_index(e);
                struct wsd_status *o = &q->status[i];
                if (type == DOCA_DPA_DEV_COMP_RECV_RDMA_WRITE_IMM &&
                    l->binding.mode == WSD_IMMEDIATE) {
                    uint32_t bytes = doca_dpa_dev_completion_element_get_received_bytes(e);
                    uint32_t low = doca_dpa_dev_get_completion_immediate(e);
                    uint64_t next;

                    /* CQEs arrive in RC order. A ring window <2^31
                     * bounds every unseen delta, including 32-bit wrap. */
                    int valid =
                        l->recv_source && !l->busy[WSD_RECV] && l->rq_live &&
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
                        wsd_fault(l, o);
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
                } else if (type == DOCA_DPA_DEV_COMP_RECV_SEND && l->busy[WSD_RECV]) {
                    uint32_t bytes = doca_dpa_dev_completion_element_get_received_bytes(e);
                    if (bytes != wsd_hello_size(l->binding.mode))
                        wsd_fault(l, o);
                    else {
                        o->done[WSD_RECV].bytes = bytes;
                        o->done[WSD_RECV].id = l->id[WSD_RECV];
                        l->busy[WSD_RECV] = 0;
                        doca_dpa_dev_device_set(l->binding.device);
                        doca_dpa_dev_rdma_receive_ack(l->binding.qp, 1);

                        if (l->binding.mode == WSD_IMMEDIATE) {
                            doca_dpa_dev_rdma_post_receive(
                                l->binding.qp, l->recv_key, l->recv_source, 0);
                            o->rq_posts++;
                        }

                        doca_dpa_dev_device_set(s->cq_device);
                    }
                } else if (type == DOCA_DPA_DEV_COMP_SEND) {
                    o->send_cqes++;

                    if (!l->completion_base_valid && l->busy[WSD_BOOT]) {
                        l->completion_base = completed - l->control_counter[WSD_BOOT];
                        l->completion_base_valid = 1;
                    }

                    uint32_t step = (uint16_t)((uint16_t)(completed - l->completion_base + 1) -
                                               (uint16_t)l->completed_index);
                    uint32_t pending = l->post_index - l->completed_index;
                    if (!l->completion_base_valid || !step || step > pending || pending > WSD_SQ)
                        wsd_fault(l, o);
                    else {
                        l->completed_index += step;
                        while (l->fence_count) {
                            unsigned at = l->fence_first;
                            if ((uint32_t)(l->completed_index - l->fence[at].counter - 1) >= WSD_SQ)
                                break;
                            o->completed_id = l->fence[at].data_id;
                            l->fence_first = (at + 1) % WSD_FENCES;
                            l->fence_count--;
                        }

                        for (unsigned slot = 0; slot < WSD_RECV; slot++)
                            if (l->busy[slot] &&
                                (uint32_t)(l->completed_index - l->control_counter[slot] - 1) <
                                    WSD_SQ) {
                                o->done[slot].id = l->id[slot];
                                l->busy[slot] = 0;
                            }
                    }
                } else
                    wsd_fault(l, o);

                l->notify = 1;
                dirty = 1;
            }

            if (s->flush_left)
                s->flush_left--;
        }

        if (ack)
            doca_dpa_dev_completion_ack(s->cq, ack);
        if (s->resume_id && !s->flush_left && !s->event_flush_left) {
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
        s->notify_pending = 1;
    }

    wsd_hint(a, s, q);

    return work;
}
