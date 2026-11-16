#ifndef DMESH_PEER_WAIT_DIAG_H
#define DMESH_PEER_WAIT_DIAG_H
/* Owner-thread-only, opt-in diagnostics. No hot-path I/O and no extra NIC
 * queries/polls. Times are local CLOCK_MONOTONIC wall times, not CPU time.
 * Trace storage is bounded; first anomaly samples survive circular overwrite. */
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define PEER_WAIT_DIAG_COUNTS(X)                                                                   \
    X(progress_calls)                                                                              \
    X(progress_moved) X(idle_boundary) X(idle_return) X(epoll_ready) X(epoll_empty)                \
        X(channel_events) X(cq_arms) X(cq_arm_skip) X(cq_poll_calls) X(cq_poll_empty)              \
            X(cq_completions) X(cq_data_done) X(cq_rx_data) X(cq_rx_credit) X(cq_rx_boot)          \
                X(cq_credit_done) X(cq_boot_done) X(cq_error) X(cq_stale) X(rq_posts)              \
                    X(tx_data_post) X(credit_post) X(tx_no_slot) X(tx_no_ring) X(rx_empty)         \
                        X(pe_calls) X(pe_moved) X(pe_arms) X(pe_arm_skip) X(pe_notify)             \
                            X(wait_submit) X(tail_changes) X(head_changes) X(continuations)        \
                                X(after_timer_moved) X(after_timer_empty) X(wait_attempt)          \
                                    X(gen_skip) X(wait_fd) X(wait_timer) X(wait_intr)              \
                                        X(wait_errors) X(scan_moved) X(scan_empty)                 \
                                            X(control_moved) X(rdma_moved) X(scan_rdma_moved)      \
                                                X(scan_control_moved)
#define PWD_ENUM(n) PWD_##n,

enum peer_wait_diag_counter {
    PEER_WAIT_DIAG_COUNTS(PWD_ENUM) PWD_COUNTERS
};

#undef PWD_ENUM

enum peer_wait_diag_stage {
    PWD_CQ_POLL,
    PWD_PE_PROGRESS,
    PWD_CQ_ARM,
    PWD_PE_ARM,
    PWD_SCAN,
    PWD_PPOLL,
    PWD_CONTROL,
    PWD_STAGES
};

#define PWD_TRACE_CAP 256u
#define PWD_SAMPLE_CAP 32u

struct peer_wait_diag_record {
    uint64_t seq, mono_ns, v[6];
    const char *event;
};

struct peer_wait_diag {
    int enabled;
    const char *scope;
    uint64_t started_ns, sequence, samples_seen, count[PWD_COUNTERS];

    struct {
        uint64_t calls, wall_ns, max_ns;
    } stage[PWD_STAGES];
    struct peer_wait_diag_record *trace;
    struct peer_wait_diag_record sample[PWD_SAMPLE_CAP];
    unsigned sample_count;
};
#ifndef PEER_WAIT_DIAG_NOW
static inline uint64_t peer_wait_diag_now(void)
{
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t))
        return 0;

    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

#define PEER_WAIT_DIAG_NOW() peer_wait_diag_now()
#endif
static inline void peer_wait_diag_init(struct peer_wait_diag *p, const char *scope)
{
    memset(p, 0, sizeof(*p));
    p->scope = scope;

    const char *value = getenv("DPUMESH_WAIT_DIAG");
    if (!value || strcmp(value, "1"))
        return;

    p->trace = calloc(PWD_TRACE_CAP, sizeof(*p->trace));
    if (!p->trace) {
        fprintf(stderr, "WAIT_DIAG unavailable=allocation\n");
        return;
    }

    p->enabled = 1;
    p->started_ns = PEER_WAIT_DIAG_NOW();
}

static inline void
peer_wait_diag_add(struct peer_wait_diag *p, enum peer_wait_diag_counter key, uint64_t n)
{
    if (p->enabled)
        p->count[key] += n;
}

static inline void peer_wait_diag_restart(struct peer_wait_diag *p)
{
    if (!p->enabled)
        return;

    p->started_ns = PEER_WAIT_DIAG_NOW();
    p->sequence = p->samples_seen = 0;
    p->sample_count = 0;
    memset(p->count, 0, sizeof(p->count));
    memset(p->stage, 0, sizeof(p->stage));
}

static inline uint64_t peer_wait_diag_begin(struct peer_wait_diag *p)
{
    return p->enabled ? PEER_WAIT_DIAG_NOW() : 0;
}

static inline void
peer_wait_diag_end(struct peer_wait_diag *p, enum peer_wait_diag_stage key, uint64_t start)
{
    if (!p->enabled)
        return;

    uint64_t dt = PEER_WAIT_DIAG_NOW() - start;
    p->stage[key].calls++;
    p->stage[key].wall_ns += dt;

    if (dt > p->stage[key].max_ns)
        p->stage[key].max_ns = dt;
}

static inline void peer_wait_diag_note(struct peer_wait_diag *p,
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

    struct peer_wait_diag_record r = {.seq = p->sequence++,
                                      .mono_ns = PEER_WAIT_DIAG_NOW(),
                                      .event = event,
                                      .v = {a, b, c, d, e, f}};
    p->trace[r.seq % PWD_TRACE_CAP] = r;

    if (retain) {
        p->samples_seen++;
        if (p->sample_count < PWD_SAMPLE_CAP)
            p->sample[p->sample_count++] = r;
    }
}

static inline void peer_wait_diag_record(FILE *out,
                                         const struct peer_wait_diag *p,
                                         const char *kind,
                                         const struct peer_wait_diag_record *r)
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
static inline void peer_wait_diag_dump(struct peer_wait_diag *p, FILE *out)
{
    if (!p->enabled)
        return;

    flockfile(out);
#define PWD_NAME(n) #n,
    static const char *const names[] = {PEER_WAIT_DIAG_COUNTS(PWD_NAME)};
#undef PWD_NAME
    static const char *const stages[] = {
        "cq_poll", "pe_progress", "cq_arm", "pe_arm", "scan", "ppoll", "control"};
    _Static_assert(sizeof(names) / sizeof(names[0]) == PWD_COUNTERS, "counter labels");
    _Static_assert(sizeof(stages) / sizeof(stages[0]) == PWD_STAGES, "stage labels");
    fprintf(out,
            "WAIT_DIAG {\"version\":1,\"scope\":\"%s\",\"object\":\"%p\",\"kind\":\"summary\","
            "\"mono_ns\":%" PRIu64 ",\"started_ns\":%" PRIu64 ",\"trace_total\":%" PRIu64
            ",\"trace_overwritten\":%" PRIu64 ",\"samples_seen\":%" PRIu64 ",\"counts\":{",
            p->scope,
            (void *)p,
            PEER_WAIT_DIAG_NOW(),
            p->started_ns,
            p->sequence,
            p->sequence > PWD_TRACE_CAP ? p->sequence - PWD_TRACE_CAP : 0,
            p->samples_seen);

    for (unsigned i = 0; i < PWD_COUNTERS; i++)
        fprintf(out, "%s\"%s\":%" PRIu64, i ? "," : "", names[i], p->count[i]);

    fputs("},\"stages\":{", out);

    for (unsigned i = 0; i < PWD_STAGES; i++)
        fprintf(out,
                "%s\"%s\":{\"calls\":%" PRIu64 ",\"wall_ns\":%" PRIu64 ",\"max_ns\":%" PRIu64 "}",
                i ? "," : "",
                stages[i],
                p->stage[i].calls,
                p->stage[i].wall_ns,
                p->stage[i].max_ns);

    fputs("}}\n", out);

    for (unsigned i = 0; i < p->sample_count; i++)
        peer_wait_diag_record(out, p, "sample", &p->sample[i]);

    uint64_t first = p->sequence > PWD_TRACE_CAP ? p->sequence - PWD_TRACE_CAP : 0;
    for (uint64_t i = first; i < p->sequence; i++)
        peer_wait_diag_record(out, p, "trace", &p->trace[i % PWD_TRACE_CAP]);

    funlockfile(out);
}

static inline void peer_wait_diag_free(struct peer_wait_diag *p)
{
    free(p->trace);
    p->trace = NULL;
    p->enabled = 0;
}
#endif
