#ifndef DMESH_PEER_PROFILE_H
#define DMESH_PEER_PROFILE_H
/* ARM-side profiling for pair/DPA work, WRITE_WITH_IMM and
 * transfer byte-size distributions.
 * Profile state is owner-thread-only.
 * ARM stages sample CPU and wall time; wire stages record wall time and a
 * bounded event trace. Keep the policies independent to preserve their cost
 * and the meaning of the existing reports. */
#include "peer_config.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Tests may override either clock hook independently. */
#ifndef PEER_PROFILE_CLOCK
static inline uint64_t peer_profile_clock(clockid_t id)
{
    struct timespec ts;
    if (clock_gettime(id, &ts))
        return 0;
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
#define PEER_PROFILE_CLOCK(id) peer_profile_clock(id)
#endif

static inline int peer_profile_env_enabled(const char *name)
{
    const char *value = getenv(name);
    return value && !strcmp(value, "1");
}

static inline unsigned peer_profile_env_unsigned(const char *name, unsigned fallback,
                                                  unsigned maximum)
{
    const char *s = getenv(name);
    if (!s || !*s)
        return fallback;
    char *end;
    unsigned long n = strtoul(s, &end, 10);
    return *end || n < 1 || n > maximum ? fallback : (unsigned)n;
}

/* WRITE_WITH_IMM diagnostics. No hot-path I/O or extra NIC queries/polls.
 * Times are CLOCK_MONOTONIC wall time. First anomaly samples survive circular
 * trace overwrite. */
/* Peer I/O counters and progress-stage timings. */
#define PEER_PROFILE_COUNTS(X) \
    X(progress_calls)          \
    X(progress_moved)          \
    X(idle_boundary)           \
    X(idle_return)             \
    X(epoll_ready)             \
    X(epoll_empty)             \
    X(channel_events)          \
    X(cq_arms)                 \
    X(cq_arm_skip)             \
    X(cq_poll_calls)           \
    X(cq_poll_empty)           \
    X(cq_completions)          \
    X(cq_data_done)            \
    X(cq_rx_data)              \
    X(cq_rx_credit)            \
    X(cq_rx_boot)              \
    X(cq_credit_done)          \
    X(cq_boot_done)            \
    X(cq_error)                \
    X(cq_stale)                \
    X(rq_posts)                \
    X(tx_data_post)            \
    X(credit_post)             \
    X(tx_no_slot)              \
    X(tx_no_ring)              \
    X(rx_empty)                \
    X(pe_calls)                \
    X(pe_moved)                \
    X(pe_arms)                 \
    X(pe_arm_skip)             \
    X(pe_notify)               \
    X(wait_submit)             \
    X(tail_changes)            \
    X(head_changes)            \
    X(continuations)
#define PEER_PROFILE_ENUM(n) PEER_PROFILE_##n,

enum peer_profile_counter {
    PEER_PROFILE_COUNTS(PEER_PROFILE_ENUM) PEER_PROFILE_COUNTERS
};

#undef PEER_PROFILE_ENUM

enum peer_profile_stage {
    PEER_PROFILE_CQ_POLL,
    PEER_PROFILE_PE_PROGRESS,
    PEER_PROFILE_CQ_ARM,
    PEER_PROFILE_PE_ARM,
    PEER_PROFILE_STAGES
};

struct peer_profile_record {
    uint64_t seq, mono_ns, v[6];
    const char *event;
};

struct peer_profile {
    int enabled;
    const char *scope;
    uint64_t started_ns, sequence, samples_seen, count[PEER_PROFILE_COUNTERS];

    struct {
        uint64_t calls, wall_ns, max_ns;
    } stage[PEER_PROFILE_STAGES];
    struct peer_profile_record *trace;
    struct peer_profile_record sample[PEER_PROFILE_SAMPLE_CAP];
    unsigned sample_count;
};
#ifndef PEER_PROFILE_NOW
static inline uint64_t peer_profile_now(void)
{
    return PEER_PROFILE_CLOCK(CLOCK_MONOTONIC);
}

#define PEER_PROFILE_NOW() peer_profile_now()
#endif
static inline void peer_profile_init(struct peer_profile *p, const char *scope)
{
    memset(p, 0, sizeof(*p));
    p->scope = scope;

    /* Preserve the external switch and log format used by benchmark tools. */
    if (!peer_profile_env_enabled("DPUMESH_WAIT_DIAG"))
        return;

    p->trace = calloc(PEER_PROFILE_TRACE_CAP, sizeof(*p->trace));
    if (!p->trace) {
        fprintf(stderr, "WAIT_DIAG unavailable=allocation\n");
        return;
    }

    p->enabled = 1;
    p->started_ns = PEER_PROFILE_NOW();
}

static inline void
peer_profile_add(struct peer_profile *p, enum peer_profile_counter key, uint64_t n)
{
    if (p->enabled)
        p->count[key] += n;
}

static inline uint64_t peer_profile_begin(struct peer_profile *p)
{
    return p->enabled ? PEER_PROFILE_NOW() : 0;
}

static inline void
peer_profile_end(struct peer_profile *p, enum peer_profile_stage key, uint64_t start)
{
    if (!p->enabled)
        return;

    uint64_t dt = PEER_PROFILE_NOW() - start;
    p->stage[key].calls++;
    p->stage[key].wall_ns += dt;

    if (dt > p->stage[key].max_ns)
        p->stage[key].max_ns = dt;
}

static inline void peer_profile_note(struct peer_profile *p,
                                       const char *event,
                                       int retain,
                                       uint64_t a,
                                       uint64_t b,
                                       uint64_t c,
                                       uint64_t d,
                                       uint64_t e,
                                       uint64_t f)
{
    if (!p->enabled)
        return;

    struct peer_profile_record r = {.seq = p->sequence++,
                                      .mono_ns = PEER_PROFILE_NOW(),
                                      .event = event,
                                      .v = {a, b, c, d, e, f}};
    p->trace[r.seq % PEER_PROFILE_TRACE_CAP] = r;

    if (retain) {
        p->samples_seen++;
        if (p->sample_count < PEER_PROFILE_SAMPLE_CAP)
            p->sample[p->sample_count++] = r;
    }
}

static inline void peer_profile_record(FILE *out,
                                         const struct peer_profile *p,
                                         const char *kind,
                                         const struct peer_profile_record *r)
{
    fprintf(out,
            "WAIT_DIAG {\"version\":1,\"scope\":\"%s\",\"object\":\"%p\","
            "\"kind\":\"%s\",\"started_ns\":%" PRIu64 ",\"seq\":%" PRIu64 ",\"mono_ns\":%" PRIu64
            ",\"event\":\"%s\",\"v\":[%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
            ",%" PRIu64 "]}\n",
            p->scope,
            (const void *)p,
            kind,
            p->started_ns,
            r->seq,
            r->mono_ns,
            r->event,
            r->v[0],
            r->v[1],
            r->v[2],
            r->v[3],
            r->v[4],
            r->v[5]);
}

/* Only call outside the measured interval or on teardown. */
static inline void peer_profile_dump(struct peer_profile *p, FILE *out)
{
    if (!p->enabled)
        return;

    flockfile(out);
#define PEER_PROFILE_NAME(n) #n,
    static const char *const names[] = {PEER_PROFILE_COUNTS(PEER_PROFILE_NAME)};
#undef PEER_PROFILE_NAME
    static const char *const stages[] = {
        "cq_poll",
        "pe_progress",
        "cq_arm",
        "pe_arm",
    };
    _Static_assert(sizeof(names) / sizeof(names[0]) == PEER_PROFILE_COUNTERS, "counter labels");
    _Static_assert(sizeof(stages) / sizeof(stages[0]) == PEER_PROFILE_STAGES, "stage labels");
    fprintf(out,
            "WAIT_DIAG {\"version\":1,\"scope\":\"%s\",\"object\":\"%p\",\"kind\":\"summary\","
            "\"mono_ns\":%" PRIu64 ",\"started_ns\":%" PRIu64 ",\"trace_total\":%" PRIu64
            ",\"trace_overwritten\":%" PRIu64 ",\"samples_seen\":%" PRIu64 ",\"counts\":{",
            p->scope,
            (void *)p,
            PEER_PROFILE_NOW(),
            p->started_ns,
            p->sequence,
            p->sequence > PEER_PROFILE_TRACE_CAP ? p->sequence - PEER_PROFILE_TRACE_CAP : 0,
            p->samples_seen);

    for (unsigned i = 0; i < PEER_PROFILE_COUNTERS; i++)
        fprintf(out, "%s\"%s\":%" PRIu64, i ? "," : "", names[i], p->count[i]);

    fputs("},\"stages\":{", out);

    for (unsigned i = 0; i < PEER_PROFILE_STAGES; i++)
        fprintf(out,
                "%s\"%s\":{\"calls\":%" PRIu64 ",\"wall_ns\":%" PRIu64 ",\"max_ns\":%" PRIu64 "}",
                i ? "," : "",
                stages[i],
                p->stage[i].calls,
                p->stage[i].wall_ns,
                p->stage[i].max_ns);

    fputs("}}\n", out);

    for (unsigned i = 0; i < p->sample_count; i++)
        peer_profile_record(out, p, "sample", &p->sample[i]);

    uint64_t first = p->sequence > PEER_PROFILE_TRACE_CAP ? p->sequence - PEER_PROFILE_TRACE_CAP : 0;
    for (uint64_t i = first; i < p->sequence; i++)
        peer_profile_record(out, p, "trace", &p->trace[i % PEER_PROFILE_TRACE_CAP]);

    funlockfile(out);
}

static inline void peer_profile_free(struct peer_profile *p)
{
    free(p->trace);
    p->trace = NULL;
    p->enabled = 0;
}

/* Pair/DPA ARM stages. CPU timers are sampled because the thread CPU clock
 * may require a syscall; event counters remain exact. Stage times are nested
 * and must not be added. ARM clocks are local to this node. */
enum arm_profile_stage {
    ARM_PROF_ENQUEUE,
    ARM_PROF_PROGRESS,
    ARM_PROF_TX_RESERVE,
    ARM_PROF_TX_COMMIT,
    ARM_PROF_RX_ACQUIRE,
    ARM_PROF_RX_RELEASE,
    ARM_PROF_RX_REFILL,
    ARM_PROF_PAIR_LIVE,
    ARM_PROF_WIRE_PROGRESS,
    ARM_PROF_WIRE_RESERVE,
    ARM_PROF_WIRE_ACQUIRE,
    ARM_PROF_WIRE_COMMIT,
    ARM_PROF_STAGES
};
enum arm_profile_counter {
    ARM_CNT_SEND,
    ARM_CNT_RECV,
    ARM_CNT_CONTROL,
    ARM_CNT_RING_FULL,
    ARM_CNT_TX_FULL,
    ARM_CNT_TX_LEASED,
    ARM_CNT_LANES_SCANNED,
    ARM_CNT_TX_SCANNED,
    ARM_CNT_TX_PENDING,
    ARM_CNT_TX_DONE,
    ARM_CNT_RX_DELIVERED,
    ARM_CNT_RX_REPOSTED,
    ARM_CNT_RX_POOL_WAIT,
    ARM_CNT_PROGRESS_EMPTY,
    ARM_CNT_PROGRESS_MOVED,
    ARM_CNT_PAIR_SCANNED,
    ARM_CNT_WIRE_SKIPPED,
    ARM_PROF_COUNTERS
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
};

#ifndef ARM_PROFILE_CLOCK
#define ARM_PROFILE_CLOCK(id) PEER_PROFILE_CLOCK(id)
#endif

static inline void arm_profile_init(struct arm_path_profile *p, int worker, int eu)
{
    memset(p, 0, sizeof(*p));
    p->worker = worker; p->eu = eu;
    p->enabled = peer_profile_env_enabled("DPUMESH_DPA_ARM_PROFILE");
    if (!p->enabled) return;
    p->handoff_enabled = peer_profile_env_enabled("DPUMESH_DPA_HANDOFF_PROFILE");
    p->sample_every = peer_profile_env_unsigned("DPUMESH_DPA_ARM_PROFILE_SAMPLE", 64, 1000000);
    p->period_ns = (uint64_t)peer_profile_env_unsigned("DPUMESH_DPA_ARM_PROFILE_MS", 5000, 3600000) * 1000000;
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
        "enqueue",
        "progress",
        "tx_reserve",
        "tx_commit",
        "rx_acquire",
        "rx_release",
        "rx_refill",
        "pair_live",
        "wire_progress",
        "wire_reserve",
        "wire_acquire",
        "wire_commit",
    };
    static const char *const counts[] = {
        "send",
        "recv",
        "control",
        "ring_full",
        "tx_full",
        "tx_leased",
        "lanes_scanned",
        "tx_scanned",
        "tx_pending",
        "tx_done",
        "rx_delivered",
        "rx_reposted",
        "rx_pool_wait",
        "progress_empty",
        "progress_moved",
        "pair_scanned",
        "wire_skipped",
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

/* Transfer sizes, split into DATA/control by the caller. Each histogram bin
 * counts one exact byte size, including the wire header. Cumulative reports
 * preserve DPUMESH_SIZE_PROFILE and the XFER_SIZE log format. Owner-thread-only;
 * recording performs no clock reads or I/O. */
struct transfer_size_profile {
    uint64_t *hist, count, bytes, oversize, sequence, reported_ns;
    uint32_t limit, min, max;
    int worker;
    const char *topic;
};

static inline void transfer_size_init(struct transfer_size_profile *p,
                                      const char *topic, int worker, uint32_t limit)
{
    memset(p, 0, sizeof(*p));
    if (!peer_profile_env_enabled("DPUMESH_SIZE_PROFILE"))
        return;

    p->topic = topic;
    p->worker = worker;
    p->limit = limit;
    p->min = limit + 1;
    p->reported_ns = PEER_PROFILE_CLOCK(CLOCK_MONOTONIC);
    p->hist = calloc((size_t)limit + 1, sizeof(*p->hist));
    if (!p->hist)
        fprintf(stderr, "XFER_SIZE_ERROR topic=%s allocation\n", topic);
}

static inline void transfer_size_record(struct transfer_size_profile *p, uint32_t bytes)
{
    if (!p->hist)
        return;
    if (bytes > p->limit) {
        p->oversize++;
        return;
    }

    p->hist[bytes]++;
    p->count++;
    p->bytes += bytes;
    if (bytes < p->min)
        p->min = bytes;
    if (bytes > p->max)
        p->max = bytes;
}

static inline void transfer_size_line(FILE *out, struct transfer_size_profile *p,
                                      uint64_t now, unsigned part, int end, const char *pairs)
{
    fprintf(out, "XFER_SIZE pid=%ld worker=%d topic=%s seq=%" PRIu64
        " mono_ns=%" PRIu64 " count=%" PRIu64 " bytes=%" PRIu64
        " oversize=%" PRIu64 " part=%u end=%d hist=%s\n",
        (long)getpid(), p->worker, p->topic, p->sequence, now, p->count,
        p->bytes, p->oversize, part, end, *pairs ? pairs : "-");
}

static inline void transfer_size_report_to(struct transfer_size_profile *p, int force, FILE *out)
{
    if (!p->hist)
        return;

    uint64_t now = PEER_PROFILE_CLOCK(CLOCK_MONOTONIC);
    if (!force && now - p->reported_ns < 5000000000ull)
        return;

    p->reported_ns = now;
    p->sequence++;
    char pairs[3200] = {0};
    size_t used = 0;
    unsigned part = 0;
    for (uint32_t i = p->min; i <= p->max; i++) {
        if (!p->hist[i])
            continue;
        if (used > sizeof(pairs) - 64) {
            transfer_size_line(out, p, now, part++, 0, pairs);
            used = 0;
        }
        int n = snprintf(pairs + used, sizeof(pairs) - used, "%s%u:%" PRIu64,
                         used ? "," : "", i, p->hist[i]);
        used += (size_t)n;
    }
    pairs[used] = 0;
    transfer_size_line(out, p, now, part, 1, pairs);
}

static inline void transfer_size_report(struct transfer_size_profile *p, int force)
{
    transfer_size_report_to(p, force, stderr);
}

static inline void transfer_size_destroy(struct transfer_size_profile *p)
{
    free(p->hist);
    p->hist = NULL;
}

#endif
