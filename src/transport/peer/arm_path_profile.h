#ifndef DMESH_ARM_PATH_PROFILE_H
#define DMESH_ARM_PATH_PROFILE_H

/* Optional diagnostics: counters/timers are owner-thread-only, with one atomic
 * hint timestamp mailbox for cross-worker sampling. ARM clocks remain local.
 * CPU timers are sampled because CLOCK_THREAD_CPUTIME_ID can cost a syscall;
 * event counters remain exact. Nested stage times must not be added. */
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>

enum arm_profile_stage {
    ARM_PROF_ENQUEUE, ARM_PROF_PROGRESS, ARM_PROF_TX_RESERVE, ARM_PROF_TX_COMMIT,
    ARM_PROF_RX_ACQUIRE, ARM_PROF_RX_RELEASE, ARM_PROF_RX_REFILL,
    ARM_PROF_PE, ARM_PROF_BUDGET, ARM_PROF_POLL, ARM_PROF_ARM, ARM_PROF_CLEAR,
    ARM_PROF_RDMA_HINT, ARM_PROF_RDMA_REPOST,
    ARM_PROF_HOST_REQUEST, ARM_PROF_HOST_REPLY, ARM_PROF_STALL_RETRY, ARM_PROF_DMA_PE,
    ARM_PROF_PEER_STEP, ARM_PROF_PEER_CARRIER, ARM_PROF_PEER_CHANNELS, ARM_PROF_DMA_PUMP,
    ARM_PROF_HOST_EMIT, ARM_PROF_ACK_RETRY, ARM_PROF_ACK_RELEASE, ARM_PROF_PENDING,
    ARM_PROF_PAIR_LIVE, ARM_PROF_WIRE_PROGRESS, ARM_PROF_WIRE_RESERVE, ARM_PROF_WIRE_ACQUIRE,
    ARM_PROF_WIRE_COMMIT, ARM_PROF_DMA_SUBMIT, ARM_PROF_DRIVER_DRAIN, ARM_PROF_DRIVER_GAP,
    ARM_PROF_PEER_REQUEST, ARM_PROF_PEER_REPLY, ARM_PROF_STAGES
};
enum arm_profile_counter {
    ARM_CNT_SEND, ARM_CNT_RECV, ARM_CNT_CONTROL, ARM_CNT_RING_FULL,
    ARM_CNT_TX_FULL, ARM_CNT_TX_LEASED, ARM_CNT_LANES_SCANNED,
    ARM_CNT_TX_SCANNED, ARM_CNT_TX_PENDING, ARM_CNT_TX_DONE,
    ARM_CNT_RX_DELIVERED, ARM_CNT_RX_REPOSTED, ARM_CNT_RX_POOL_WAIT,
    ARM_CNT_POLL_REPOST, ARM_CNT_POLL_NO_MSGQ, ARM_CNT_POLL_DEFERRED,
    ARM_CNT_POLL_TRUE, ARM_CNT_PROGRESS_EMPTY, ARM_CNT_PROGRESS_MOVED,
    ARM_CNT_DRAIN_IDLE, ARM_CNT_DRAIN_PENDING, ARM_CNT_DRAIN_MOVED,
    ARM_CNT_NOTIFY_COMPLETION, ARM_CNT_NOTIFY_DMA, ARM_CNT_NOTIFY_WAKE,
    ARM_CNT_RDMA_HINT, ARM_CNT_RDMA_HINT_INVALID, ARM_CNT_RDMA_HINT_LOCAL,
    ARM_CNT_RDMA_HINT_FORWARDED, ARM_CNT_RDMA_HINT_TARGET_RUNNING,
    ARM_CNT_RDMA_HINT_TARGET_PARKED, ARM_CNT_RDMA_HINT_COALESCED,
    ARM_CNT_RDMA_REPOST_OK, ARM_CNT_RDMA_REPOST_DEFERRED,
    ARM_CNT_HOST_REQUEST, ARM_CNT_HOST_REPLY, ARM_CNT_HOST_REQUEST_BYTES,
    ARM_CNT_HOST_REPLY_BYTES, ARM_CNT_HOST_FORWARD_RETRY, ARM_CNT_HOST_FORWARD_ERROR,
    ARM_CNT_PEER_REQUEST, ARM_CNT_PEER_REPLY, ARM_CNT_PEER_REQUEST_BYTES, ARM_CNT_PEER_REPLY_BYTES,
    ARM_CNT_CHANNEL_SCANNED, ARM_CNT_CHANNEL_LIVE, ARM_CNT_CHANNEL_MOVED, ARM_CNT_PAIR_SCANNED,
    ARM_CNT_WIRE_SKIPPED, ARM_CNT_DMA_SUBMIT_OK, ARM_CNT_DMA_SUBMIT_ERROR, ARM_CNT_DMA_BYTES,
    ARM_CNT_DMA_DONE, ARM_CNT_DMA_ERROR, ARM_CNT_DMA_CREDIT_DONE, ARM_CNT_DMA_REV_DONE,
    ARM_CNT_PENDING_DMA, ARM_CNT_PENDING_RETRY, ARM_CNT_PENDING_REBIND, ARM_CNT_PENDING_FIN,
    ARM_CNT_PENDING_PAIR, ARM_CNT_PENDING_CHANNEL, ARM_CNT_PENDING_SCANNED, ARM_PROF_COUNTERS
};
struct arm_profile_time {
    uint64_t calls, samples, wall_ns, cpu_ns, max_wall_ns;
    uint64_t ok, not_ready, errors;
};
struct arm_profile_stamp { uint64_t wall, cpu; int sampled; };
struct arm_path_profile {
    int enabled;
    int handoff_enabled;
    unsigned sample_every;
    int worker, eu;
    uint64_t period_ns, started_ns, reported_ns;
    struct arm_profile_time stage[ARM_PROF_STAGES];
    uint64_t count[ARM_PROF_COUNTERS];
    /* First sampled callback not yet seen by the target's C budget entry.
     * Multiple PE owners can publish; only that target consumes and sums. */
    _Atomic uint64_t hint_first_ns;
    uint64_t hint_budget_count, hint_budget_ns, hint_budget_max_ns;
    struct arm_profile_stamp gap; /* sampled C drain return -> next C drain entry */
};

/* Tests can supply deterministic clocks without any hardware. */
#ifndef ARM_PROFILE_CLOCK
static inline uint64_t arm_profile_clock(clockid_t id)
{
    struct timespec ts;
    if (clock_gettime(id, &ts)) return 0;
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
#define ARM_PROFILE_CLOCK(id) arm_profile_clock(id)
#endif
static inline unsigned arm_profile_env(const char *name, unsigned fallback,
                                        unsigned maximum)
{
    const char *s = getenv(name);
    if (!s || !*s) return fallback;
    char *end;
    unsigned long n = strtoul(s, &end, 10);
    return *end || n < 1 || n > maximum ? fallback : (unsigned)n;
}
static inline void arm_profile_init(struct arm_path_profile *p, int worker, int eu)
{
    memset(p, 0, sizeof(*p));
    atomic_init(&p->hint_first_ns, 0);
    p->worker = worker; p->eu = eu;
    const char *enabled = getenv("DPUMESH_DPA_ARM_PROFILE");
    p->enabled = enabled && !strcmp(enabled, "1");
    if (!p->enabled) return;
    const char *handoff = getenv("DPUMESH_DPA_HANDOFF_PROFILE");
    p->handoff_enabled = handoff && !strcmp(handoff, "1");
    p->sample_every = arm_profile_env("DPUMESH_DPA_ARM_PROFILE_SAMPLE", 64, 1000000);
    p->period_ns = (uint64_t)arm_profile_env("DPUMESH_DPA_ARM_PROFILE_MS", 5000, 3600000) * 1000000;
    p->started_ns = p->reported_ns = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
}
static inline struct arm_profile_stamp arm_profile_begin(struct arm_path_profile *p,
                                                         enum arm_profile_stage stage)
{
    struct arm_profile_stamp s = {0};
    if (!p->enabled) return s;
    uint64_t calls = p->stage[stage].calls++;
    if (calls % p->sample_every) return s;
    s.cpu = ARM_PROFILE_CLOCK(CLOCK_THREAD_CPUTIME_ID);
    s.wall = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
    s.sampled = 1;
    return s;
}
static inline void arm_profile_end(struct arm_path_profile *p, enum arm_profile_stage stage,
                                   struct arm_profile_stamp s, int rc)
{
    if (!p->enabled) return;
    struct arm_profile_time *t = &p->stage[stage];
    if (s.sampled) {
        uint64_t wall = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC) - s.wall;
        uint64_t cpu = ARM_PROFILE_CLOCK(CLOCK_THREAD_CPUTIME_ID) - s.cpu;
        t->samples++; t->wall_ns += wall; t->cpu_ns += cpu;
        if (wall > t->max_wall_ns) t->max_wall_ns = wall;
    }
    if (rc > 0) t->ok++;
    else if (!rc) t->not_ready++;
    else t->errors++;
}
static inline void arm_profile_count(struct arm_path_profile *p,
                                     enum arm_profile_counter counter)
{ if (p->enabled) p->count[counter]++; }

static inline void arm_profile_add(struct arm_path_profile *p,
                                    enum arm_profile_counter counter, uint64_t n)
{ if (p->enabled) p->count[counter] += n; }

/* Includes Rust proxy tasks, scheduler/yield, notification handling and sleep.
 * Wall time is not CPU usage. A final open sample is intentionally not ended
 * by reporting/teardown; it has no next drain entry. */
static inline void arm_profile_gap_enter(struct arm_path_profile *p)
{
    if (p->gap.sampled) {
        arm_profile_end(p, ARM_PROF_DRIVER_GAP, p->gap, 1);
        p->gap.sampled = 0;
    }
}
static inline void arm_profile_gap_leave(struct arm_path_profile *p)
{ p->gap = arm_profile_begin(p, ARM_PROF_DRIVER_GAP); }

static inline int arm_profile_hint_publish(struct arm_path_profile *target, uint64_t ns)
{
    uint64_t empty = 0;
    return atomic_compare_exchange_strong_explicit(&target->hint_first_ns, &empty, ns,
        memory_order_release, memory_order_relaxed);
}
static inline void arm_profile_hint_observe(struct arm_path_profile *p)
{
    if (!p->handoff_enabled) return;
    uint64_t first = atomic_exchange_explicit(&p->hint_first_ns, 0, memory_order_acquire);
    if (!first) return;
    uint64_t delta = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC) - first;
    p->hint_budget_count++; p->hint_budget_ns += delta;
    if (delta > p->hint_budget_max_ns) p->hint_budget_max_ns = delta;
}

/* Force on clean teardown; otherwise call only from a coarse service cadence.
 * Each record is cumulative, so interval deltas can be taken by the collector.
 * Formatting/output happens outside the timed stages. */
static inline int arm_profile_report(struct arm_path_profile *p, const char *scope,
                                     int force)
{
    if (!p->enabled) return 0;
    uint64_t now = ARM_PROFILE_CLOCK(CLOCK_MONOTONIC);
    if (!force && now - p->reported_ns < p->period_ns) return 0;
    p->reported_ns = now;
    static const char *const names[] = {
        "enqueue", "progress", "tx_reserve", "tx_commit", "rx_acquire", "rx_release",
        "rx_refill", "pe", "budget", "poll", "arm", "clear", "rdma_hint", "rdma_msgq_repost",
        "host_request", "host_reply", "stall_retry", "dma_pe", "peer_step", "peer_carrier",
        "peer_channels", "dma_pump", "host_emit", "ack_retry", "ack_release", "pending",
        "pair_live", "wire_progress", "wire_reserve", "wire_acquire", "wire_commit", "dma_submit",
        "driver_drain", "driver_gap", "peer_request", "peer_reply"
    };
    static const char *const counts[] = {
        "send", "recv", "control", "ring_full", "tx_full", "tx_leased", "lanes_scanned",
        "tx_scanned", "tx_pending", "tx_done", "rx_delivered", "rx_reposted", "rx_pool_wait",
        "poll_repost", "poll_no_msgq", "poll_deferred", "poll_true", "progress_empty",
        "progress_moved", "drain_idle", "drain_pending", "drain_moved",
        "notify_completion", "notify_dma", "notify_wake", "rdma_hint", "rdma_hint_invalid",
        "rdma_hint_local", "rdma_hint_forwarded", "rdma_hint_target_running",
        "rdma_hint_target_parked", "rdma_hint_coalesced", "rdma_repost_ok", "rdma_repost_deferred",
        "host_request", "host_reply", "host_request_bytes", "host_reply_bytes",
        "host_forward_retry", "host_forward_error", "peer_request", "peer_reply",
        "peer_request_bytes", "peer_reply_bytes", "channel_scanned", "channel_live",
        "channel_moved", "pair_scanned", "wire_skipped", "dma_submit_ok", "dma_submit_error",
        "dma_bytes", "dma_done", "dma_error", "dma_credit_done", "dma_rev_done", "pending_dma",
        "pending_retry", "pending_rebind", "pending_fin", "pending_pair", "pending_channel",
        "pending_scanned"
    };
    _Static_assert(sizeof(names) / sizeof(names[0]) == ARM_PROF_STAGES, "ARM stage labels");
    _Static_assert(sizeof(counts) / sizeof(counts[0]) == ARM_PROF_COUNTERS, "ARM counter labels");
    /* One stdio lock per line keeps records intact across worker threads. */
    flockfile(stderr);
    fprintf(stderr, "DPA_ARM_PROFILE scope=%s worker=%d eu=%d final=%d mono_ns=%" PRIu64
        " span_ns=%" PRIu64 " sample_every=%u", scope, p->worker, p->eu, force,
        now, now - p->started_ns, p->sample_every);
    for (unsigned i = 0; i < ARM_PROF_COUNTERS; i++)
        fprintf(stderr, " %s=%" PRIu64, counts[i], p->count[i]);
    fputc('\n', stderr);
    funlockfile(stderr);
    if (p->handoff_enabled)
        fprintf(stderr, "DPA_ARM_HINT scope=%s worker=%d mono_ns=%" PRIu64
            " callback_to_budget_samples=%" PRIu64 " callback_to_budget_ns=%" PRIu64
            " callback_to_budget_max_ns=%" PRIu64 "\n", scope, p->worker, now,
            p->hint_budget_count, p->hint_budget_ns, p->hint_budget_max_ns);
    for (unsigned i = 0; i < ARM_PROF_STAGES; i++) {
        const struct arm_profile_time *t = &p->stage[i];
        if (!t->calls) continue;
        fprintf(stderr, "DPA_ARM_STAGE scope=%s worker=%d eu=%d mono_ns=%" PRIu64
            " stage=%s calls=%" PRIu64 " samples=%" PRIu64 " wall_ns=%" PRIu64
            " cpu_ns=%" PRIu64 " max_wall_ns=%" PRIu64 " ok=%" PRIu64
            " not_ready=%" PRIu64 " errors=%" PRIu64 "\n", scope, p->worker, p->eu,
            now, names[i], t->calls, t->samples, t->wall_ns, t->cpu_ns,
            t->max_wall_ns, t->ok, t->not_ready, t->errors);
    }
    return 1;
}
#endif
