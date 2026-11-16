#ifndef DMESH_PEER_DPA_PROFILE_H
#define DMESH_PEER_DPA_PROFILE_H
#include <stdint.h>
#include <stddef.h>

/* Optional cumulative diagnostics. All device timings begin/end in ONE
 * drain_context invocation, before any reschedule. Units are raw EU cycles,
 * never ARM nanoseconds or CQ hardware timestamps. Stages are nested. */
enum peer_dpa_profile_stage {
    PEER_PROF_PASS,
    PEER_PROF_INVALIDATE,
    PEER_PROF_REQUESTS,
    PEER_PROF_POST_SEND,
    PEER_PROF_POST_RECV,
    PEER_PROF_COMMIT,
    PEER_PROF_CQ,
    PEER_PROF_CQ_PUBLISH,
    PEER_PROF_WRITEBACK,
    PEER_PROF_NOTIFY,
    PEER_PROF_STAGES
};

enum peer_dpa_profile_counter {
    PEER_CNT_PASS,
    PEER_CNT_EMPTY_CONTEXT,
    PEER_CNT_IDLE_PASS,
    PEER_CNT_SEND,
    PEER_CNT_RECV,
    PEER_CNT_SEND_CQE,
    PEER_CNT_RECV_CQE,
    PEER_CNT_STALE_CQE,
    PEER_CNT_COMMIT_SEND,
    PEER_CNT_COMMIT_RECV,
    PEER_CNT_WRITEBACK,
    PEER_CNT_NOTIFY_ATTEMPT,
    PEER_CNT_NOTIFY_SENT,
    PEER_CNT_NOTIFY_NO_CREDIT,
    PEER_CNT_NOTIFY_UNAVAILABLE,
    PEER_PROF_COUNTERS
};

struct peer_dpa_profile_time {
    uint64_t calls, samples, cycles, max_cycles;
};

struct peer_dpa_profile_data {
    uint64_t count[PEER_PROF_COUNTERS];
    struct peer_dpa_profile_time stage[PEER_PROF_STAGES];
};

struct peer_dpa_profile_state {
    uint32_t enabled, sample_every;
    uint64_t snapshot_ack;
    struct peer_dpa_profile_data data;
};

/* ARM writes request on its own cache line. DPA freezes data for that request,
 * writebacks data BEFORE ack, then leaves it immutable until ARM has read it
 * and publishes a new request. No live-counter memcpy/seqlock race. */
struct peer_dpa_profile_shared {
    uint64_t request;
    uint8_t request_pad[56];
    uint64_t ack;
    uint8_t ack_pad[56];
    struct peer_dpa_profile_data data;
} __attribute__((aligned(64)));

_Static_assert(offsetof(struct peer_dpa_profile_shared, ack) == 64, "profile writer cache lines");
_Static_assert(offsetof(struct peer_dpa_profile_shared, data) == 128, "profile snapshot alignment");
#endif
