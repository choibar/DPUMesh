#ifndef DMESH_DEVICE_PEER_DPA_CQ_H
#define DMESH_DEVICE_PEER_DPA_CQ_H
#include "peer_dpa_ring.h"

/* DPA-private lookup: CQEs identify their local QPN, not the software lane.
 * Chaining only visits colliding active QPNs; an empty CQ does no lane scan.
 * Only the owning DPA thread mutates this table. Retirement is fenced by the
 * pause/destroy/drain protocol; a hardware QPN alone carries no generation. */
static inline unsigned peer_dpa_qpn_bucket(uint32_t qpn)
{
    qpn ^= qpn >> 16;
    return (qpn * 2654435761u) & (PEER_DPA_QPN_BUCKETS - 1u);
}

static inline unsigned peer_dpa_qpn_find(struct peer_dpa_device_state *s, uint32_t qpn)
{
    uint16_t entry = s->qpn_heads[peer_dpa_qpn_bucket(qpn)];
    while (entry) {
        unsigned i = entry - 1u;
        if (s->lanes[i].binding.qpn == qpn)
            return i;

        entry = s->lanes[i].qpn_next;
    }
    return PEER_DPA_LANES;
}

static inline void peer_dpa_qpn_add(struct peer_dpa_device_state *s, unsigned i)
{
    unsigned bucket = peer_dpa_qpn_bucket(s->lanes[i].binding.qpn);
    s->lanes[i].qpn_next = s->qpn_heads[bucket];
    s->qpn_heads[bucket] = (uint16_t)(i + 1u);
}

static inline void peer_dpa_qpn_remove(struct peer_dpa_device_state *s, unsigned i)
{
    uint16_t *link = &s->qpn_heads[peer_dpa_qpn_bucket(s->lanes[i].binding.qpn)];
    while (*link && *link != i + 1u)
        link = &s->lanes[*link - 1u].qpn_next;

    if (*link)
        *link = s->lanes[i].qpn_next;
}

/* CQ counters are raw 16-bit values, not slot IDs or unit-stride indices.
 * Chain only posted SENDs in this QP; zero and wraparound are ordinary keys.
 * The extra state is private to DPA and is reset by INSTALL/REMOVE fencing. */
_Static_assert(PEER_RDMA_SEND_SLOTS <= UINT16_MAX &&
                   !(PEER_RDMA_SEND_SLOTS & (PEER_RDMA_SEND_SLOTS - 1)),
               "TX hash shape");

static inline unsigned peer_dpa_tx_bucket(uint32_t counter)
{
    unsigned key = counter & 0xffffu;
    key ^= key >> 8;
    key ^= key >> 4;

    return key & (PEER_RDMA_SEND_SLOTS - 1);
}

static inline unsigned peer_dpa_tx_find(const struct peer_dpa_lane_state *l, uint32_t counter)
{
    unsigned entry = l->tx_heads[peer_dpa_tx_bucket(counter)];
    while (entry) {
        unsigned slot = entry - 1;
        if ((l->tx_counter[slot] & 0xffffu) == (counter & 0xffffu))
            return slot;

        entry = l->tx_next[slot];
    }
    return PEER_RDMA_SEND_SLOTS;
}

static inline int peer_dpa_tx_add(struct peer_dpa_lane_state *l, unsigned slot)
{
    unsigned bucket = peer_dpa_tx_bucket(l->tx_counter[slot]);
    if (peer_dpa_tx_find(l, l->tx_counter[slot]) != PEER_RDMA_SEND_SLOTS)
        return 0;

    l->tx_next[slot] = l->tx_heads[bucket];
    l->tx_heads[bucket] = (uint16_t)(slot + 1);

    return 1;
}

static inline void peer_dpa_tx_remove(struct peer_dpa_lane_state *l, unsigned slot)
{
    uint16_t *link = &l->tx_heads[peer_dpa_tx_bucket(l->tx_counter[slot])];
    while (*link && *link != slot + 1)
        link = &l->tx_next[*link - 1];

    if (*link)
        *link = l->tx_next[slot];
    l->tx_next[slot] = 0;
}
#endif
