#ifndef DMESH_DEVICE_PEER_DPA_PROFILE_H
#define DMESH_DEVICE_PEER_DPA_PROFILE_H
#include <dpaintrin.h>
#include "../peer_dpa_profile.h"

struct peer_dpa_profile_stamp {
    uint64_t cycles;
    int sampled;
};

static inline struct peer_dpa_profile_stamp peer_profile_begin(struct peer_dpa_profile_state *p,
                                                               enum peer_dpa_profile_stage stage)
{
    struct peer_dpa_profile_stamp stamp = {0};
    if (!p->enabled)
        return stamp;

    uint64_t call = p->data.stage[stage].calls++;
    if (call % p->sample_every)
        return stamp;

    stamp.cycles = __dpa_thread_cycles();
    stamp.sampled = 1;

    return stamp;
}

static inline void peer_profile_end(struct peer_dpa_profile_state *p,
                                    enum peer_dpa_profile_stage stage,
                                    struct peer_dpa_profile_stamp stamp)
{
    if (!stamp.sampled)
        return;

    uint64_t delta = __dpa_thread_cycles() - stamp.cycles;
    struct peer_dpa_profile_time *t = &p->data.stage[stage];
    t->samples++;
    t->cycles += delta;

    if (delta > t->max_cycles)
        t->max_cycles = delta;
}

static inline void peer_profile_count(struct peer_dpa_profile_state *p,
                                      enum peer_dpa_profile_counter count)
{
    if (p->enabled)
        p->data.count[count]++;
}

static inline void peer_profile_snapshot(struct peer_dpa_profile_state *p,
                                         struct peer_dpa_profile_shared *out)
{
    if (!p->enabled)
        return;

    uint64_t request = *(volatile uint64_t *)&out->request;
    if (!request || request == p->snapshot_ack)
        return;

    out->data = p->data;
    __dpa_thread_window_writeback();
    out->ack = request;
    p->snapshot_ack = request;
    __dpa_thread_window_writeback();
}
#endif
