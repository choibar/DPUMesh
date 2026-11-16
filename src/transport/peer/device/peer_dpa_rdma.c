#include <doca_dpa_dev.h>
#include <doca_dpa_dev_buf.h>
#include <doca_dpa_dev_verbs.h>
#include <dpaintrin.h>
#include "peer_dpa_ring.h"
#include "peer_dpa_rdma.h"
#include "peer_dpa_cq.h"
#include "peer_dpa_notify.h"
#include "peer_dpa_profile.h"

#ifdef DMESH_DPA_WRITE_SYNC
#include "peer_write_sync_dpa_shared.h"
unsigned peer_write_sync_dpa_drain(struct peer_dpa_thread_arg *, unsigned, uint64_t, unsigned);
#endif

/* A bounded descriptor pass can touch at most this many distinct QPs.
 * Keep only those QPs, rather than scanning all software lanes to commit. */
#define PEER_DPA_REQUEST_BATCH 32u
#define PEER_DPA_QP_VISIT 32u

enum {
    COMMIT_SEND = 1u,
    COMMIT_RECV = 2u
};

struct commit_batch {
    uint16_t lanes[PEER_DPA_REQUEST_BATCH];
    uint8_t flags[PEER_DPA_REQUEST_BATCH];
    unsigned count;
};

static void defer_commit(struct commit_batch *batch, unsigned lane, unsigned flags)
{
#if DMESH_DPA_COMMIT_BATCH
    unsigned i;
    for (i = 0; i < batch->count; i++)
        if (batch->lanes[i] == lane)
            break;

    if (i == batch->count) {
        /* At most one insertion per consumed descriptor; the request budget
         * bounds count, including passes interrupted by control barriers. */
        batch->lanes[i] = (uint16_t)lane;
        batch->flags[i] = 0;
        batch->count++;
    }

    batch->flags[i] |= flags;
#else
    (void)batch;
    (void)lane;
    (void)flags;
#endif
}

static void commit_pending(struct peer_dpa_device_state *s, struct commit_batch *batch)
{
    if (!batch->count)
        return;

    struct peer_dpa_profile_stamp stamp = peer_profile_begin(&s->profile, PEER_PROF_COMMIT);
    for (unsigned i = 0; i < batch->count; i++) {
        struct peer_dpa_lane_state *l = &s->lanes[batch->lanes[i]];

        /* One DPA owner posts and commits. Control requests flush this list
         * BEFORE changing lane lifetime; even a later malformed descriptor
         * must not strand earlier valid WRs. Keep normal commits and their
         * memory fences. Payload/status coherence still uses the window
         * protocol below; a commit is not permission for ARM to reuse slots. */
        doca_dpa_dev_device_set(l->binding.device);

        if (batch->flags[i] & COMMIT_RECV) {
            doca_dpa_dev_verbs_qp_commit_recv(l->binding.qp);
            peer_profile_count(&s->profile, PEER_CNT_COMMIT_RECV);
        }

        if (batch->flags[i] & COMMIT_SEND) {
            doca_dpa_dev_verbs_qp_commit_send(l->binding.qp);
            peer_profile_count(&s->profile, PEER_CNT_COMMIT_SEND);
        }
    }

    batch->count = 0;
    doca_dpa_dev_device_set(s->base_device);
    peer_profile_end(&s->profile, PEER_PROF_COMMIT, stamp);
}

static int rx_source_valid(const struct peer_dpa_rx_source *p)
{
    return p->addr && p->len == 65536u && p->addr <= UINT64_MAX - p->len;
}

static void
post_receive(struct peer_dpa_lane_state *l, unsigned slot, const struct peer_dpa_rx_source *source)
{
    struct doca_dpa_dev_verbs_sge sge = {.addr = l->binding.rx_addr +
                                                 (uint64_t)slot * l->binding.capacity,
                                         .length = l->binding.capacity,
                                         .lkey = l->binding.mkey};
    if (source) {
        /* One registered chunk owns header and body. ARM publishes a replacement
         * only after CQ and all consumer references retire (or a new chunk is
         * acquired); the device must never overwrite a borrowed payload. */
        sge = (struct doca_dpa_dev_verbs_sge){
            .addr = source->addr, .length = source->len, .lkey = source->key};
    }

    struct doca_dpa_dev_verbs_recv_wr wr = {0};
    doca_dpa_dev_verbs_recv_wr_set_sg_list(&wr, &sge);
    doca_dpa_dev_verbs_recv_wr_set_sg_num_sge(&wr, 1);
    l->rx_counter[slot] = doca_dpa_dev_verbs_qp_post_recv_wr(l->binding.qp, &wr);

    /* Reserve immediately, including while this WR awaits the batch commit. */
    l->rx_posted[slot] = 1;
#if !DMESH_DPA_COMMIT_BATCH
    /* Same ownership/fence contract as batching: reserve BEFORE submission. */
    doca_dpa_dev_verbs_qp_commit_recv(l->binding.qp);
#endif
}

static int post_send(struct peer_dpa_lane_state *l,
                     unsigned slot,
                     unsigned bytes,
                     uint64_t token,
                     const struct peer_dpa_buffer *source)
{
    uint64_t addr = l->binding.tx_addr + (uint64_t)slot * PEER_DPA_TX_BUFFER_SIZE;

    /* ARM publishes one immutable contiguous frame, including its headroom.
     * The SEND completion fences both header and payload before reuse. */
    struct doca_dpa_dev_verbs_sge sge = {.addr = source ? source->addr : addr,
                                         .length = bytes,
                                         .lkey = source ? source->key : l->binding.mkey};
    struct doca_dpa_dev_verbs_send_wr wr = {0};
    doca_dpa_dev_verbs_send_wr_set_sg_list(&wr, &sge);
    doca_dpa_dev_verbs_send_wr_set_sg_num_sge(&wr, 1);
    doca_dpa_dev_verbs_send_wr_set_opcode(&wr, DOCA_DPA_DEV_VERBS_SEND_WR_OPCODE_SEND);
    doca_dpa_dev_verbs_send_wr_set_send_flags(&wr, DOCA_DPA_DEV_VERBS_SEND_WR_FLAGS_SIGNALED);

    /* DOCA 3.1 returns 0 for the first valid WQE. Slot ownership bounds
     * SQ/RQ occupancy. Match raw 16-bit CQ counters against live slots rather
     * than assuming one counter increment per WQE (SGE layout may vary).
     * Other SDKs must revalidate this counter/error convention. */
    l->tx_counter[slot] = doca_dpa_dev_verbs_qp_post_send_wr(l->binding.qp, &wr);

    /* A posted-but-uncommitted slot is already owned; only its CQE releases it. */
    l->tx_token[slot] = token;
    l->tx_busy[slot] = 1;

    int tracked = peer_dpa_tx_add(l, slot);
#if !DMESH_DPA_COMMIT_BATCH
    doca_dpa_dev_verbs_qp_commit_send(l->binding.qp);
#endif
    return tracked;
}

/* Returns 0 for a pending data fence, 1 for a consumed request, 2 for STOP.
 * Control and data have separate tickets; only REMOVE can retire a live QP,
 * and its ARM-published tail fences every request accepted by that QP. */
static unsigned apply_request(struct peer_dpa_thread_arg *arg,
                              unsigned context,
                              struct peer_dpa_device_state *s,
                              struct peer_dpa_shared *q,
                              struct peer_dma_desc d,
                              struct commit_batch *batch)
{
    struct peer_dpa_meta m = peer_dpa_metadata(&d);
    if (m.op == PEER_DPA_REMOVE && m.version == PEER_DPA_ABI && m.token &&
        m.lane_id < PEER_DPA_LANES) {
        struct peer_dpa_lane_state *l = &s->lanes[m.lane_id];
        if (l->installed && l->binding.generation == m.generation && l->binding.qpn == m.qpn &&
            (s->data_head < d.addr || s->pending.head[m.lane_id]))
            return 0;
    }

    /* Preserve control FIFO barriers, in particular before REMOVE clears
     * a binding or PAUSE/STOP permits ARM to destroy hardware resources. */
    if (!peer_dpa_data_op(m.op))
        commit_pending(s, batch);
    if (m.op == PEER_DPA_CQ_PAUSE && m.version == PEER_DPA_ABI && m.token) {
        if (!s->cq_resume_token) {
            s->cq_paused = 1;
            q->cq_control.consumer_head = m.token;
        }
    } else if (m.op == PEER_DPA_CQ_RESUME && m.version == PEER_DPA_ABI && m.token) {
        if (s->cq_paused && !s->cq_resume_token) {
            /* ARM destroyed the retired QP while CQ access was paused.
             * No more CQEs from that QP may arrive after this NIC fence.
             * Drain up to the CQ capacity (or until empty): even with
             * other active QPs, every pre-fence CQE must then be consumed.
             * Publish ACK only after that bound; do not reuse its QPN early. */
            s->cq_resume_token = m.token;
            s->cq_flush_left = s->cq_capacity;
            s->cq_paused = 0;
        }
    } else if (m.op == PEER_DPA_STOP && m.version == PEER_DPA_ABI) {
        int occupied = 0;
        for (unsigned i = 0; i < PEER_DPA_LANES; i++)
            occupied |= s->lanes[i].installed;

        if (!occupied && !s->cq_paused && !s->cq_resume_token) {
            /* Clear the only live pointer before publishing ACK. There
             * can be no further q/s accesses after the final writeback. */
            if (context == DPA_RDMA_CONTEXTS)
                arg->rdma_state = 0;
            else
                arg->rdma_states[context] = 0;

            doca_dpa_dev_device_set(s->base_device);

            /* Publish credits from earlier descriptors in this pass too.
             * STOP itself is fenced by stopped, not by the ring head. */
            q->consumed.consumer_head = s->head;
            q->stopped.consumer_head = m.token;
            __dpa_thread_window_writeback();
            return 2;
        }
    } else if (m.lane_id < PEER_DPA_LANES) {
        struct peer_dpa_lane_state *l = &s->lanes[m.lane_id];
        struct peer_dpa_status *out = &q->status[m.lane_id];
        int ok = m.version == PEER_DPA_ABI && m.token && m.generation;
        if (m.op == PEER_DPA_INSTALL && ok && !l->installed) {
            struct peer_dpa_binding b = q->table[m.lane_id];
            ok = b.qp && b.cq && b.device && b.tx_addr && b.rx_addr &&
                 b.generation == m.generation && b.qpn == m.qpn &&
                 b.capacity == PEER_DPA_BUFFER_SIZE && b.qpn &&
                 (!b.rx_frame_bytes || b.rx_frame_bytes == 65536u) && b.cq == s->cq &&
                 b.device == s->cq_device && !s->cq_paused && !s->cq_resume_token &&
                 peer_dpa_qpn_find(s, b.qpn) == PEER_DPA_LANES;
            if (ok && b.rx_frame_bytes) {
                for (unsigned i = 0; i < PEER_RDMA_RECV_SLOTS; i++)
                    if (!rx_source_valid(&q->rx_sources[m.lane_id][i]))
                        ok = 0;
            }

            if (ok) {
                __builtin_memset(l, 0, sizeof(*l));
                l->binding = b;
                l->installed = 1;
                peer_dpa_qpn_add(s, m.lane_id);
                s->data_lanes[s->data_count++] = m.lane_id;
                __builtin_memset(out, 0, sizeof(*out));
                out->generation = b.generation;
                doca_dpa_dev_device_set(b.device);

                for (unsigned slot = 0; slot < PEER_RDMA_RECV_SLOTS; slot++) {
                    post_receive(
                        l, slot, b.rx_frame_bytes ? &q->rx_sources[m.lane_id][slot] : NULL);
                    out->receives++;
                }

                /* All initial RQ entries must reach hardware before INSTALL ACK. */
#if DMESH_DPA_COMMIT_BATCH
                doca_dpa_dev_verbs_qp_commit_recv(l->binding.qp);
#endif
                out->control_ack = m.token;
            }
        } else if (ok && l->installed && l->binding.generation == m.generation &&
                   l->binding.qpn == m.qpn) {
            doca_dpa_dev_device_set(l->binding.device);
            switch (m.op) {
            case PEER_DPA_ENABLE:
                ok = !l->failed;
                if (ok) {
                    l->active = 1;
                    out->control_ack = m.token;
                }
                break;

            case PEER_DPA_SEND:
            case PEER_DPA_SEND_FRAME: {
                ok = !l->failed && l->active && peer_dpa_request_valid(&d, &m, &l->binding) &&
                     !l->tx_busy[d.addr];

                struct peer_dpa_buffer source = {0};
                if (ok && m.op == PEER_DPA_SEND_FRAME) {
                    source = peer_dpa_buffer_get(&d);
                    ok = source.addr && source.addr <= UINT64_MAX - d.size;
                }

                if (ok) {
                    struct peer_dpa_profile_stamp stamp =
                        peer_profile_begin(&s->profile, PEER_PROF_POST_SEND);
                    int tracked = post_send(l,
                                            (unsigned)d.addr,
                                            d.size,
                                            m.token,
                                            m.op == PEER_DPA_SEND_FRAME ? &source : NULL);
                    peer_profile_end(&s->profile, PEER_PROF_POST_SEND, stamp);
                    peer_profile_count(&s->profile, PEER_CNT_SEND);
#if !DMESH_DPA_COMMIT_BATCH
                    peer_profile_count(&s->profile, PEER_CNT_COMMIT_SEND);
#endif
                    defer_commit(batch, m.lane_id, COMMIT_SEND);
                    out->sends++;

                    if (!tracked) {
                        out->error = PEER_DPA_CQ_ERROR;
                        l->failed = 1;
                    }
                }
                break;
            }

            case PEER_DPA_RECV: {
                /* Address/key and size belong to the same published snapshot.
                 * rx_sources is immutable INSTALL metadata, never repost state. */
                struct peer_dpa_buffer buffer = peer_dpa_buffer_get(&d);
                struct peer_dpa_rx_source source = {
                    .addr = buffer.addr, .key = buffer.key, .len = d.size};
                ok = !l->failed && peer_dpa_request_valid(&d, &m, &l->binding) &&
                     !l->rx_posted[d.addr] && l->rx_reposted < l->rx_sequence &&
                     d.addr == l->rx_reposted % PEER_RDMA_RECV_SLOTS;
                if (ok && l->binding.rx_frame_bytes)
                    ok = rx_source_valid(&source);
                if (ok) {
                    struct peer_dpa_profile_stamp stamp =
                        peer_profile_begin(&s->profile, PEER_PROF_POST_RECV);
                    post_receive(l, (unsigned)d.addr, l->binding.rx_frame_bytes ? &source : NULL);
                    peer_profile_end(&s->profile, PEER_PROF_POST_RECV, stamp);
                    peer_profile_count(&s->profile, PEER_CNT_RECV);
#if !DMESH_DPA_COMMIT_BATCH
                    peer_profile_count(&s->profile, PEER_CNT_COMMIT_RECV);
#endif
                    defer_commit(batch, m.lane_id, COMMIT_RECV);
                    l->rx_reposted++;
                    out->receives++;
                }
                break;
            }

            case PEER_DPA_REMOVE:
                /* Stop the single CQ reader before ARM destroys a QP.
                 * The shared CQ itself lives until context STOP. RESUME
                 * must finish the stale-CQE barrier before QPN reuse. */
                s->cq_paused = 1;
                peer_dpa_qpn_remove(s, m.lane_id);

                for (unsigned i = 0; i < s->data_count; i++)
                    if (s->data_lanes[i] == m.lane_id) {
                        s->data_lanes[i] = s->data_lanes[--s->data_count];
                        break;
                    }

                __builtin_memset(l, 0, sizeof(*l));
                out->control_ack = m.token;
                break;

            default:
                ok = 0;
                break;
            }
        } else
            ok = 0;

        if (!ok) {
            if (!out->error)
                out->error = PEER_DPA_BAD_REQUEST;
            l->failed = 1;
        }
    }

    return 1;
}

static unsigned drain_context(struct peer_dpa_thread_arg *arg, unsigned context, unsigned budget)
{
    uint64_t ptr = context == DPA_RDMA_CONTEXTS ? arg->rdma_state : arg->rdma_states[context];
    if (!ptr || !budget)
        return 0;
#ifdef DMESH_DPA_WRITE_SYNC
    if (*(const uint64_t *)(uintptr_t)ptr == WSD_ENGINE)
        return peer_write_sync_dpa_drain(arg, context, ptr, budget);
#endif
    struct peer_dpa_device_state *s = (void *)ptr;
    struct peer_dpa_profile_stamp pass = peer_profile_begin(&s->profile, PEER_PROF_PASS);
    peer_profile_count(&s->profile, PEER_CNT_PASS);

    if (!s->data_count)
        peer_profile_count(&s->profile, PEER_CNT_EMPTY_CONTEXT);
    doca_dpa_dev_device_set(s->base_device);

    struct peer_dpa_shared *q =
        (void *)doca_dpa_dev_buf_get_external_ptr(doca_dpa_dev_buf_array_get_buf(s->buffers, 0));

    /* Single ARM producer release-publishes each ring. DPA invalidates before
     * reading and writebacks its status/credit lines before ARM acquire-loads.
     * NIC payload coherence still assumes BF3 coherent ARM DRAM; no Host PCI
     * payload or cached DPA payload semantics are inferred from this protocol. */
    struct peer_dpa_profile_stamp stamp = peer_profile_begin(&s->profile, PEER_PROF_INVALIDATE);
    __dpa_thread_window_read_inv();
    peer_profile_end(&s->profile, PEER_PROF_INVALIDATE, stamp);
    stamp = peer_profile_begin(&s->profile, PEER_PROF_REQUESTS);

    struct commit_batch batch = {.count = 0};
    int status_dirty = 0;
    unsigned work = 0;
    unsigned requests = budget < PEER_DPA_REQUEST_BATCH ? budget : PEER_DPA_REQUEST_BATCH;

    /* Bound control work so it cannot monopolize the data-ring pass. A
     * pending REMOVE yields to data rather than blocking its own fence. */
    unsigned controls = requests < 16 ? requests : 16;

    while (work < controls) {
        volatile struct peer_dma_desc *src = &q->requests[s->head % PEER_DPA_RING_CAP];
        if (src->publish_seq != s->head + 1)
            break;

        struct peer_dma_desc d = *src;
        struct peer_dpa_meta m = peer_dpa_metadata(&d);
        unsigned result = 1;

        /* Data never enters the context control FIFO. Reject a misplaced
         * request without allowing it to bypass per-QP ordering. */
        if (!peer_dpa_data_op(m.op))
            result = apply_request(arg, context, s, q, d, &batch);
        if (!result)
            break;
        if (result == 2)
            return work + 1; /* state/mapping may already be freed */

        s->head++;
        work++;
    }

    if (work) {
        q->consumed.consumer_head = s->head;
        status_dirty = 1;
    }

    /* Read one context FIFO, then dispatch directly through the QPN hash.
     * A bounded private pool preserves per-QP ordering without inspecting
     * empty lanes. ARM may reuse a descriptor once its copy is retained. */
    peer_dpa_work_init(&s->pending);
    for (unsigned imported = 0; imported < requests && s->pending.free; imported++) {
        volatile struct peer_dma_desc *src = &q->data.requests[s->data_head % PEER_DPA_RING_CAP];
        if (src->publish_seq != s->data_head + 1)
            break;

        struct peer_dma_desc d = *src;
        struct peer_dpa_meta m = peer_dpa_metadata(&d);
        unsigned lane = peer_dpa_qpn_find(s, m.qpn);
        if (lane != PEER_DPA_LANES && m.generation == s->lanes[lane].binding.generation) {
            unsigned entry = peer_dpa_work_push(&s->pending, lane);
            s->pending_request[entry - 1] = d;
        } else if (lane == PEER_DPA_LANES && m.lane_id < PEER_DPA_LANES &&
                   s->lanes[m.lane_id].installed &&
                   m.generation == s->lanes[m.lane_id].binding.generation) {
            /* An unknown QPN with a current lane hint is a bad request.
             * Old generations cannot fault a newly installed lane. */
            s->lanes[m.lane_id].failed = 1;
            q->status[m.lane_id].error = PEER_DPA_BAD_REQUEST;
        }

        s->data_head++;
        q->data.consumed.consumer_head = s->data_head;
        status_dirty = 1;
    }

    unsigned visits = s->pending.count;
    while (visits-- && work < requests) {
        unsigned lane = peer_dpa_work_take(&s->pending);
        struct peer_dpa_lane_state *l = &s->lanes[lane];
        unsigned consumed = 0;

        while (s->pending.head[lane] && consumed < PEER_DPA_QP_VISIT && work < requests) {
            struct peer_dma_desc d = s->pending_request[s->pending.head[lane] - 1];
            struct peer_dpa_meta m = peer_dpa_metadata(&d);
            if (m.lane_id == lane && peer_dpa_data_op(m.op))
                apply_request(arg, context, s, q, d, &batch);
            else {
                q->status[lane].error = PEER_DPA_BAD_REQUEST;
                l->failed = 1;
            }

            l->request_head = d.publish_seq;
            q->status[lane].request_consumed = l->request_head;
            peer_dpa_work_pop(&s->pending, lane);
            consumed++;
            work++;
            status_dirty = 1;
        }

        if (s->pending.head[lane])
            peer_dpa_work_ready(&s->pending, lane);
    }

    /* Flush even a single WR or an exhausted budget before CQ polling/return.
     * Never wait for more descriptors or carry uncommitted WRs across yield. */
    peer_profile_end(&s->profile, PEER_PROF_REQUESTS, stamp);
    commit_pending(s, &batch);
    stamp = peer_profile_begin(&s->profile, PEER_PROF_CQ);

    struct peer_dpa_profile_stamp cq_publish = {0};
    int have_cqe = 0;

    /* One context-wide CQ: no scan of the 256 software lanes. ARM's shared
     * CQ similarly dispatches by identity, but DPA uses QPN + WQE counter. */
    if (!s->cq_paused) {
        doca_dpa_dev_device_set(s->cq_device);

        unsigned cq_count = 0;
        for (unsigned batch = 0; batch < PEER_RDMA_CQ_BATCH && work < budget; batch++) {
            doca_dpa_dev_completion_element_t e;
            if (!doca_dpa_dev_get_completion(s->cq, &e)) {
                s->cq_flush_left = 0;
                break;
            }

            unsigned i = peer_dpa_qpn_find(s, doca_dpa_dev_completion_element_get_qpn(e));
            if (i == PEER_DPA_LANES || s->lanes[i].failed) {
                /* A removed/failed QP must not block healthy QPs in this CQ. */
                s->stale_completions++;
                peer_profile_count(&s->profile, PEER_CNT_STALE_CQE);
            } else {
                if (!have_cqe) {
                    cq_publish = peer_profile_begin(&s->profile, PEER_PROF_CQ_PUBLISH);
                    have_cqe = 1;
                }

                struct peer_dpa_lane_state *l = &s->lanes[i];
                unsigned type = doca_dpa_dev_get_completion_type(e);
                unsigned counter = doca_dpa_dev_completion_element_get_wqe_counter(e);
                struct peer_dpa_status *out = &q->status[i];
                unsigned slot;
                if (type == DOCA_DPA_DEV_COMP_SEND) {
                    peer_profile_count(&s->profile, PEER_CNT_SEND_CQE);
                    slot = peer_dpa_tx_find(l, counter);
                    if (slot == PEER_RDMA_SEND_SLOTS) {
                        out->error = PEER_DPA_CQ_ERROR;
                        l->failed = 1;
                    } else {
                        peer_dpa_tx_remove(l, slot);
                        l->tx_busy[slot] = 0;
                        out->tx[slot].sequence = l->tx_token[slot];
                    }
                } else if (type == DOCA_DPA_DEV_COMP_RECV_SEND) {
                    peer_profile_count(&s->profile, PEER_CNT_RECV_CQE);

                    /* Initial preposts and ARM releases are in FIFO order on this RC
                     * QP. ARM reposts only an exclusive arena chunk or a replacement. */
                    slot = (unsigned)(l->rx_sequence % PEER_RDMA_RECV_SLOTS);

                    uint32_t bytes = doca_dpa_dev_completion_element_get_received_bytes(e);
                    if (!l->rx_posted[slot] || counter != (l->rx_counter[slot] & 0xffff) ||
                        !bytes ||
                        bytes > (l->binding.rx_frame_bytes ? l->binding.rx_frame_bytes
                                                           : l->binding.capacity) ||
                        l->rx_sequence == UINT64_MAX) {
                        out->error = PEER_DPA_CQ_ERROR;
                        l->failed = 1;
                    } else {
                        l->rx_posted[slot] = 0;
                        out->rx[slot].bytes = bytes;
                        out->rx[slot].sequence = ++l->rx_sequence;
                    }
                } else {
                    out->error = PEER_DPA_CQ_ERROR;
                    l->failed = 1;
                }

                out->completions++;
                status_dirty = 1;
            }

            cq_count++;
            work++;

            if (s->cq_flush_left)
                s->cq_flush_left--;
        }

        /* get_completion advances the read cursor; ACK returns NIC credits.
         * Include stale/error CQEs, and ACK before publishing a resume fence.
         * No completion-element pointer survives this bounded pass. */
        if (cq_count)
            doca_dpa_dev_completion_ack(s->cq, cq_count);
        if (s->cq_resume_token && !s->cq_flush_left) {
            q->cq_control.consumer_head = s->cq_resume_token;
            s->cq_resume_token = 0;
            status_dirty = 1; /* An empty CQ can finish RESUME with work == 0. */
        }
    }

    peer_profile_end(&s->profile, PEER_PROF_CQ, stamp);
    doca_dpa_dev_device_set(s->base_device); /* Comch uses the base PF */

    if (!s->notify_published && arg->dpa_producer && arg->dpa_producer_comp) {
        q->notification.consumer_head = PEER_DPA_NOTIFY_MSGQ;
        s->notify_published = 1;
        status_dirty = 1;
    }

    /* Descriptor consumption also returns ring credits: waking only for CQEs
     * could strand an ARM enqueue blocked on a full request ring. Errors and
     * control ACKs are included. An empty pass must still retry a pending hint.
     * STOP above has no live lanes and publishes its synchronous fence last;
     * it must never retry through state that ARM may already have freed. */
    if (work || status_dirty)
        s->notify_pending = 1;

    /* This function owns RDMA publication, including on an empty RESUME pass.
     * An idle retry only re-sends a hint for already-published state. Keep the
     * pre-read invalidate and writeback-before-MsgQ ordering; ARM acquire
     * loads alone cannot make the external DPA window coherent. */
    if (status_dirty) {
        stamp = peer_profile_begin(&s->profile, PEER_PROF_WRITEBACK);
        __dpa_thread_window_writeback();
        peer_profile_end(&s->profile, PEER_PROF_WRITEBACK, stamp);
        peer_profile_count(&s->profile, PEER_CNT_WRITEBACK);
    }

    if (have_cqe)
        peer_profile_end(&s->profile, PEER_PROF_CQ_PUBLISH, cq_publish);
    peer_dpa_notify(arg, s);

    if (!work)
        peer_profile_count(&s->profile, PEER_CNT_IDLE_PASS);
    peer_profile_end(&s->profile, PEER_PROF_PASS, pass);

    /* Debug snapshot writes cannot generate more RDMA hints. STOP returned
     * above: never access diagnostic memory after its lifetime fence. */
    peer_profile_snapshot(&s->profile, &q->profile);

    return work;
}

/* Interleave independent ARM contexts on a dedicated RDMA thread. Each
 * context owns QP data rings and one CQ; neither producer crosses ARM owners.
 * The cursor survives yields and advances even if a context consumes the
 * entire remaining budget. STOP of one context leaves the others running. */
unsigned peer_dpa_drain(struct peer_dpa_thread_arg *arg, unsigned budget)
{
    if (!budget)
        return 0;
    if (!arg->rdma_count)
        return drain_context(arg, DPA_RDMA_CONTEXTS, budget); /* standalone probe */

    unsigned count = arg->rdma_count;
    if (count > DPA_RDMA_CONTEXTS)
        return 0;

    unsigned first = arg->rdma_cursor % count, work = 0;
    for (unsigned visit = 0; visit < count && work < budget; visit++) {
        unsigned context = (first + visit) % count;
        unsigned left = budget - work;
        work += drain_context(arg, context, left < 64 ? left : 64);
        arg->rdma_cursor = (context + 1) % count;
    }

    return work;
}
