#ifndef DMESH_DEVICE_PEER_WRITE_SYNC_DPA_CQ_H
#define DMESH_DEVICE_PEER_WRITE_SYNC_DPA_CQ_H

#include "peer_write_sync_dpa_shared.h"

/* Only the owning DPA thread updates this index. CQ dispatch visits a bucket's
 * colliding QPNs rather than the active-QP list. REMOVE unlinks before the
 * existing pause/destroy/RESUME drain fence permits hardware QPN reuse. */
static inline unsigned wsd_qpn_bucket(uint32_t qpn)
{
    qpn ^= qpn >> 16;
    return (qpn * 2654435761u) & (WSD_QPN_BUCKETS - 1u);
}

static inline unsigned wsd_qpn_find(const struct wsd_device *s, uint32_t qpn)
{
    uint16_t entry = s->qpn_heads[wsd_qpn_bucket(qpn)];

    while (entry) {
        unsigned lane = entry - 1u;
        if (s->lane[lane].binding.qpn == qpn)
            return lane;

        entry = s->lane[lane].qpn_next;
    }

    return WSD_LANES;
}

static inline void wsd_qpn_add(struct wsd_device *s, unsigned lane)
{
    unsigned bucket = wsd_qpn_bucket(s->lane[lane].binding.qpn);
    s->lane[lane].qpn_next = s->qpn_heads[bucket];
    s->qpn_heads[bucket] = (uint16_t)(lane + 1u);
}

static inline void wsd_qpn_remove(struct wsd_device *s, unsigned lane)
{
    uint16_t *link = &s->qpn_heads[wsd_qpn_bucket(s->lane[lane].binding.qpn)];

    while (*link && *link != lane + 1u)
        link = &s->lane[*link - 1u].qpn_next;

    if (*link)
        *link = s->lane[lane].qpn_next;
    s->lane[lane].qpn_next = 0;
}

#endif
