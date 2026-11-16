#ifndef DMESH_TRANSFER_SIZE_PROFILE_H
#define DMESH_TRANSFER_SIZE_PROFILE_H
/* Optional owner-thread byte histograms. No clocks/logging on record(). */
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

struct transfer_size_profile {
    uint64_t *hist, count, bytes, oversize, sequence, reported_ns;
    uint32_t limit, min, max;
    int worker;
    const char *topic;
};
static inline uint64_t transfer_size_now(void)
{
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t)) return 0;
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}
static inline void transfer_size_init(struct transfer_size_profile *p,
                                      const char *topic, int worker, uint32_t limit)
{
    memset(p, 0, sizeof(*p));
    const char *enabled = getenv("DPUMESH_SIZE_PROFILE");
    if (!enabled || strcmp(enabled, "1")) return;
    p->topic = topic; p->worker = worker; p->limit = limit; p->min = limit + 1;
    p->reported_ns = transfer_size_now();
    p->hist = calloc((size_t)limit + 1, sizeof(*p->hist));
    if (!p->hist) fprintf(stderr, "XFER_SIZE_ERROR topic=%s allocation\n", topic);
}
static inline void transfer_size_record(struct transfer_size_profile *p, uint32_t bytes)
{
    if (!p->hist) return;
    if (bytes > p->limit) { p->oversize++; return; }
    p->hist[bytes]++; p->count++; p->bytes += bytes;
    if (bytes < p->min) p->min = bytes;
    if (bytes > p->max) p->max = bytes;
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
    if (!p->hist) return;
    uint64_t now = transfer_size_now();
    if (!force && now - p->reported_ns < 5000000000ull) return;
    p->reported_ns = now; p->sequence++;
    char pairs[3200] = {0}; size_t used = 0; unsigned part = 0;
    for (uint32_t i = p->min; i <= p->max; i++) {
        if (!p->hist[i]) continue;
        if (used > sizeof(pairs) - 64) {
            transfer_size_line(out, p, now, part++, 0, pairs); used = 0;
        }
        int n = snprintf(pairs + used, sizeof(pairs) - used, "%s%u:%" PRIu64,
                         used ? "," : "", i, p->hist[i]);
        used += (size_t)n;
    }
    pairs[used] = 0;
    transfer_size_line(out, p, now, part, 1, pairs);
}
static inline void transfer_size_report(struct transfer_size_profile *p, int force)
{ transfer_size_report_to(p, force, stderr); }
static inline void transfer_size_destroy(struct transfer_size_profile *p)
{ free(p->hist); p->hist = NULL; }
#endif
