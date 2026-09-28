/*
 * dpumesh_host — the host-side DMA throughput benchmark over the DPUmesh
 * native API (include/dpumesh/dmesh.h, libdpumesh).
 *
 * This is the legacy host_worker.c benchmark role (one connection per thread,
 * fixed-size messages streamed as fast as the transport takes them) ported
 * onto the host library: one channel per process, one EQ + one client QP per
 * thread, dmesh_alloc/dmesh_post_send under the library's credit and tail
 * policy, replies read zero-copy from the RX region. The DPU peer is
 * dpumesh_dpu in the matching mode.
 *
 * Modes (BENCH_MODE):
 *   sink   one-way host -> DPU stream; the DPU counts the completions
 *   echo   the DPU sends every byte back; the round trip of each message is
 *          measured from a timestamp in its first 8 bytes and BENCH_WINDOW
 *          bytes are kept in flight per thread
 *
 * Output: the main thread prints HOST_BENCH_RATE once a second (aggregate of
 * all threads) and HOST_BENCH_DONE at the end, with RTT percentiles in echo
 * mode. "DMA" in the host's numbers is an estimate — a post of S bytes is
 * ceil(S/8064) forward descriptors — that only holds for messages >= 8 KiB;
 * the library coalesces smaller posts, so the DPU's count is the truth.
 *
 * Environment (besides the library's DPUMESH_*):
 *   BENCH_THREADS   1..32        (1)
 *   BENCH_SIZE      bytes >= 8   (8192)
 *   BENCH_DURATION  seconds      (10)
 *   BENCH_MODE      sink | echo  (sink)
 *   BENCH_WINDOW    echo: bytes in flight per thread (262144)
 *   BENCH_SERVICE   service target <host>:<port> (192.0.2.1:9000; dpumesh_dpu
 *                   serves every flow, whatever its address)
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <dpumesh/dmesh.h>

#define MAX_THREADS    32
#define EVENT_BATCH    64
#define POST_BUDGET    64          /* posts per loop pass before polling events */
#define CHANNEL_DESC_MAX  8064u       /* forward descriptor size the channel layer splits at */
#define STAMP_LEN      8           /* echo: send timestamp at the front of each message */
#define HIST_BUCKETS   (1u << 20)  /* RTT histogram, 1 us per bucket */
#define DRAIN_GRACE_S  1.0         /* echo: time given to replies still in flight at the end */
#define WATCHDOG_S     15.0        /* main thread stops workers this long past the duration */

enum { MODE_SINK = 0, MODE_ECHO = 1 };

/* ------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------ */
static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static const char *env_or(const char *name, const char *fallback)
{
    const char *v = getenv(name);
    return v && *v ? v : fallback;
}

static double gbps(uint64_t bytes, double seconds) { return (double)bytes * 8.0 / seconds / 1e9; }

/* Forward descriptors one post of `size` bytes becomes (>= 8 KiB messages only). */
static double descs_per_post(uint32_t size) { return (double)((size + CHANNEL_DESC_MAX - 1) / CHANNEL_DESC_MAX); }

/* Waits for the EQ readiness fd (bounded). Returns poll()'s result. The fd is
 * level-triggered: the next dmesh_poll_eq run to empty settles it. */
static int wait_eq(int fd, int timeout_ms)
{
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    int rc;
    do {
        rc = poll(&pfd, 1, timeout_ms);
    } while (rc < 0 && errno == EINTR);
    return rc;
}

/* ------------------------------------------------------------------------
 * Configuration and per-thread state
 * ------------------------------------------------------------------------ */
struct config {
    int threads, mode;
    uint32_t size, window;
    double duration;
    const char *service;
};

struct worker {
    /* shared, read-only during the run */
    const struct config *cfg;
    dmesh_channel_t *ch;        /* the process's one channel (dmesh.h: one per process) */
    int idx;
    double start_at;            /* barrier: every thread starts sending at this time */
    atomic_int *stop;           /* the main thread's watchdog */

    /* live counters, read by the reporter (monotonic; torn reads are tolerated) */
    _Atomic uint64_t posts, tx_bytes, rx_bytes, rx_msgs, tx_ready;
    _Atomic uint64_t rtt_sum_ns, rtt_cnt;
    _Atomic uint32_t rtt_max_us;
    uint32_t *hist;             /* RTT histogram, 1 us buckets (this thread only) */

    /* results */
    int failed;
    double wall;
};

/* ------------------------------------------------------------------------
 * Echo replies — reframing the byte stream into messages and timing them
 *
 * The stream comes back as RECV fragments of arbitrary size; a message is
 * `size` bytes whose first 8 bytes are the send timestamp. The reframer
 * keeps its place across fragments and records one RTT per completed
 * message.
 * ------------------------------------------------------------------------ */
struct reframer {
    uint32_t off;               /* bytes of the current message already seen */
    uint8_t hdr[STAMP_LEN];     /* the message's first bytes, possibly split across fragments */
};

static void reframer_record_rtt(struct worker *w, const struct reframer *rf)
{
    uint64_t stamp, now, rtt;
    uint32_t us;
    memcpy(&stamp, rf->hdr, sizeof(stamp));
    if (stamp == 0) return;
    now = now_ns();
    if (now <= stamp) return;
    rtt = now - stamp;
    us = (uint32_t)(rtt / 1000);
    atomic_fetch_add(&w->rtt_sum_ns, rtt);
    atomic_fetch_add(&w->rtt_cnt, 1);
    if (us > atomic_load(&w->rtt_max_us)) atomic_store(&w->rtt_max_us, us);
    w->hist[us < HIST_BUCKETS ? us : HIST_BUCKETS - 1]++;
}

static void reframer_feed(struct worker *w, struct reframer *rf, const uint8_t *buf, uint32_t len)
{
    const uint32_t size = w->cfg->size;
    while (len > 0) {
        uint32_t take = size - rf->off;
        if (take > len) take = len;
        if (rf->off < STAMP_LEN) {
            uint32_t h = STAMP_LEN - rf->off < take ? STAMP_LEN - rf->off : take;
            memcpy(rf->hdr + rf->off, buf, h);
        }
        rf->off += take;
        buf += take;
        len -= take;
        if (rf->off == size) {
            reframer_record_rtt(w, rf);
            atomic_fetch_add(&w->rx_msgs, 1);
            rf->off = 0;
        }
    }
}

/* ------------------------------------------------------------------------
 * Worker — one EQ + one QP, post / poll / drain
 * ------------------------------------------------------------------------ */
struct stream {
    dmesh_eq_t *eq;
    dmesh_qp_t *qp;
    int eq_fd;
    struct reframer rf;
    uint64_t inflight;          /* echo: bytes posted but not yet received back */
    int blocked;                /* dmesh_alloc returned EAGAIN; wait for TX_READY */
};

/* Posts messages until the budget, the window (echo) or the library's
 * credits (EAGAIN -> TX_READY) stop it. Returns 1 if anything was posted,
 * -1 on a failure that ends the run. */
static int stream_post(struct worker *w, struct stream *s)
{
    const struct config *cfg = w->cfg;
    int budget = POST_BUDGET, posted = 0;
    if (s->blocked) return 0;
    while (budget-- > 0 && (cfg->mode == MODE_SINK || s->inflight + cfg->size <= cfg->window)) {
        uint8_t *b = dmesh_alloc(s->qp, cfg->size);
        if (!b) {
            if (errno == EAGAIN) { s->blocked = 1; break; }
            fprintf(stderr, "worker %d: dmesh_alloc: %s\n", w->idx, strerror(errno));
            return -1;
        }
        if (cfg->size >= STAMP_LEN) {
            uint64_t stamp = now_ns();
            memcpy(b, &stamp, sizeof(stamp));
        }
        if (dmesh_post_send(s->qp, b, cfg->size) != 0) {
            fprintf(stderr, "worker %d: dmesh_post_send: %s\n", w->idx, strerror(errno));
            return -1;
        }
        atomic_fetch_add(&w->posts, 1);
        atomic_fetch_add(&w->tx_bytes, cfg->size);
        s->inflight += cfg->size;
        posted = 1;
    }
    /* echo: do not let the library's 500 us tail timer add to the RTT */
    if (posted && cfg->mode == MODE_ECHO) dmesh_flush(s->qp);
    return posted;
}

/* Handles one RECV event: counts the bytes, times echoed messages, releases
 * the zero-copy buffer. */
static void stream_recv(struct worker *w, struct stream *s, dmesh_event_t *ev)
{
    atomic_fetch_add(&w->rx_bytes, ev->len);
    if (w->cfg->mode == MODE_ECHO) {
        reframer_feed(w, &s->rf, ev->buf, ev->len);
        s->inflight = s->inflight > ev->len ? s->inflight - ev->len : 0;
    }
    dmesh_release_rx_buffer(w->ch, ev);
}

/* Drains the EQ. Returns 1 if any event was seen, -1 when the stream ended. */
static int stream_poll(struct worker *w, struct stream *s)
{
    dmesh_event_t events[EVENT_BATCH];
    int n, i, seen = 0;
    while ((n = dmesh_poll_eq(s->eq, events, EVENT_BATCH)) > 0) {
        seen = 1;
        for (i = 0; i < n; i++) {
            dmesh_event_t *ev = &events[i];
            switch (ev->type) {
            case DMESH_EVENT_RECV:
                stream_recv(w, s, ev);
                break;
            case DMESH_EVENT_TX_READY:
                atomic_fetch_add(&w->tx_ready, 1);
                s->blocked = 0;
                break;
            case DMESH_EVENT_RECV_FIN:
            case DMESH_EVENT_TX_ERROR:
                fprintf(stderr, "worker %d: stream ended (event %d)\n", w->idx, ev->type);
                return -1;
            default:
                break;
            }
        }
    }
    return seen;
}

/* Echo: retires the replies still in flight when the clock ran out, so the
 * byte counts on both sides match. Bounded by DRAIN_GRACE_S. */
static void stream_drain(struct worker *w, struct stream *s)
{
    double until = now_sec() + DRAIN_GRACE_S;
    while (s->inflight > 0 && now_sec() < until) {
        dmesh_event_t events[EVENT_BATCH];
        int n, i;
        while ((n = dmesh_poll_eq(s->eq, events, EVENT_BATCH)) > 0)
            for (i = 0; i < n; i++)
                if (events[i].type == DMESH_EVENT_RECV) stream_recv(w, s, &events[i]);
        wait_eq(s->eq_fd, 1);
    }
}

static void *worker_main(void *arg)
{
    struct worker *w = arg;
    const struct config *cfg = w->cfg;
    struct stream s = {0};
    double t0, t_end;

    /* setup: this thread's EQ and its client QP to the service */
    s.eq = dmesh_create_eq(w->ch);
    s.qp = s.eq ? dmesh_create_qp(s.eq, cfg->service) : NULL;
    s.eq_fd = s.eq ? dmesh_eq_fd(s.eq) : -1;
    if (!s.eq || !s.qp || s.eq_fd < 0) {
        fprintf(stderr, "worker %d: DPUmesh setup failed: %s\n", w->idx, strerror(errno));
        w->failed = 1;
        goto out;
    }
    if (cfg->size > (uint32_t)dmesh_post_max(w->ch)) {
        fprintf(stderr, "worker %d: BENCH_SIZE %u exceeds post max %d\n", w->idx, cfg->size, dmesh_post_max(w->ch));
        w->failed = 1;
        goto out;
    }

    /* barrier, then the timed loop: post, poll, sleep only when idle */
    while (now_sec() < w->start_at) usleep(100);
    t0 = now_sec();
    t_end = t0 + cfg->duration;
    while (!atomic_load(w->stop) && now_sec() < t_end) {
        int posted = stream_post(w, &s), seen;
        if (posted < 0) { w->failed = 1; break; }
        seen = stream_poll(w, &s);
        if (seen < 0) { w->failed = 1; break; }
        if (!posted && !seen) {
            /* nothing to do until the library signals; bounded so the
             * duration check and a lost TX_READY cannot hang the worker */
            wait_eq(s.eq_fd, 1);
            s.blocked = 0;
        }
    }
    w->wall = now_sec() - t0;
    if (cfg->mode == MODE_ECHO && !w->failed) stream_drain(w, &s);

out:
    dmesh_destroy_qp(s.qp);
    dmesh_destroy_eq(s.eq);
    return NULL;
}

/* ------------------------------------------------------------------------
 * Reporting — the per-second aggregate and the final summary
 * ------------------------------------------------------------------------ */
struct totals {
    uint64_t posts, tx_bytes, rx_bytes, rx_msgs;
};

static void totals_read(const struct worker *w, int threads, struct totals *t)
{
    int i;
    memset(t, 0, sizeof(*t));
    for (i = 0; i < threads; i++) {
        t->posts += atomic_load(&w[i].posts);
        t->tx_bytes += atomic_load(&w[i].tx_bytes);
        t->rx_bytes += atomic_load(&w[i].rx_bytes);
        t->rx_msgs += atomic_load(&w[i].rx_msgs);
    }
}

static void report_rate(const struct config *cfg, const struct totals *cur, const struct totals *prev, double el)
{
    double tx_msgs = (double)(cur->posts - prev->posts) / el;
    if (cur->posts == prev->posts && cur->rx_bytes == prev->rx_bytes) return;
    fprintf(stderr, "HOST_BENCH_RATE tx: %.0f msg/s (%.0f DMA/s, %.2f Gbps)", tx_msgs,
            tx_msgs * descs_per_post(cfg->size), gbps(cur->tx_bytes - prev->tx_bytes, el));
    if (cfg->mode == MODE_ECHO)
        fprintf(stderr, ", rx: %.0f msg/s (%.2f Gbps)", (double)(cur->rx_msgs - prev->rx_msgs) / el,
                gbps(cur->rx_bytes - prev->rx_bytes, el));
    fputc('\n', stderr);
}

static uint32_t percentile(const uint32_t *hist, uint64_t total, double p)
{
    uint64_t target = (uint64_t)((double)total * p), acc = 0;
    uint32_t i;
    for (i = 0; i < HIST_BUCKETS; i++) {
        acc += hist[i];
        if (acc >= target && total) return i;
    }
    return 0;
}

/* Folds every thread's results together and prints HOST_BENCH_DONE.
 * Returns the number of failed threads. */
static int report_final(const struct config *cfg, const struct worker *w)
{
    struct totals t;
    uint64_t rtt_sum = 0, rtt_cnt = 0, tx_ready = 0;
    uint32_t *hist = calloc(HIST_BUCKETS, sizeof(uint32_t)), rtt_max = 0, j;
    double wall = 0;
    int failed = 0, i;

    totals_read(w, cfg->threads, &t);
    for (i = 0; i < cfg->threads; i++) {
        rtt_sum += w[i].rtt_sum_ns;
        rtt_cnt += w[i].rtt_cnt;
        tx_ready += w[i].tx_ready;
        if (w[i].rtt_max_us > rtt_max) rtt_max = w[i].rtt_max_us;
        if (w[i].wall > wall) wall = w[i].wall;
        failed += w[i].failed;
        if (hist) for (j = 0; j < HIST_BUCKETS; j++) hist[j] += w[i].hist[j];
    }
    if (wall <= 0) wall = cfg->duration;

    printf("HOST_BENCH_DONE mode=%s threads=%d size=%u wall_sec=%.2f posts=%llu tx_msg_per_sec=%.0f "
           "tx_dma_per_sec=%.0f tx_gbps=%.2f rx_msg_per_sec=%.0f rx_gbps=%.2f tx_ready_events=%llu failed=%d",
           cfg->mode == MODE_ECHO ? "echo" : "sink", cfg->threads, cfg->size, wall,
           (unsigned long long)t.posts, (double)t.posts / wall, (double)t.posts * descs_per_post(cfg->size) / wall,
           gbps(t.tx_bytes, wall), (double)t.rx_msgs / wall, gbps(t.rx_bytes, wall),
           (unsigned long long)tx_ready, failed);
    if (rtt_cnt && hist)
        printf(" rtt_avg_us=%.1f rtt_p50_us=%u rtt_p99_us=%u rtt_max_us=%u",
               (double)rtt_sum / (double)rtt_cnt / 1e3, percentile(hist, rtt_cnt, 0.50),
               percentile(hist, rtt_cnt, 0.99), rtt_max);
    printf("\n");
    free(hist);
    return failed;
}

/* ------------------------------------------------------------------------
 * main — configuration, one channel, the worker threads, the reporter loop
 * ------------------------------------------------------------------------ */
static int config_read(struct config *cfg)
{
    cfg->threads = atoi(env_or("BENCH_THREADS", "1"));
    cfg->size = (uint32_t)atoi(env_or("BENCH_SIZE", "8192"));
    cfg->duration = atof(env_or("BENCH_DURATION", "10"));
    cfg->mode = strcmp(env_or("BENCH_MODE", "sink"), "echo") == 0 ? MODE_ECHO : MODE_SINK;
    cfg->window = (uint32_t)atoi(env_or("BENCH_WINDOW", "262144"));
    cfg->service = env_or("BENCH_SERVICE", "192.0.2.1:9000");
    if (cfg->threads < 1 || cfg->threads > MAX_THREADS || cfg->size < STAMP_LEN || cfg->duration <= 0) {
        fprintf(stderr, "usage: BENCH_THREADS=1..%d BENCH_SIZE>=%d BENCH_DURATION>0 BENCH_MODE=sink|echo dpumesh_host\n",
                MAX_THREADS, STAMP_LEN);
        return -1;
    }
    if (getenv("DPUMESH_SERVICE")) {
        fprintf(stderr, "unset DPUMESH_SERVICE: dpumesh_host is a client\n");
        return -1;
    }
    return 0;
}

int main(void)
{
    struct config cfg;
    struct worker w[MAX_THREADS];
    pthread_t tids[MAX_THREADS];
    int joined[MAX_THREADS] = {0};
    atomic_int stop = 0;
    struct totals prev, cur;
    dmesh_channel_t *ch;
    double t0, last, start_at;
    int i, failed;

    if (config_read(&cfg) != 0) return 2;

    ch = dmesh_create_channel();
    if (!ch) {
        fprintf(stderr, "dmesh_create_channel: %s\n", strerror(errno));
        return 1;
    }

    /* workers: QP setup takes milliseconds each, so start the clock a bit later */
    memset(w, 0, sizeof(w));
    start_at = now_sec() + 0.5 + 0.05 * cfg.threads;
    for (i = 0; i < cfg.threads; i++) {
        w[i].cfg = &cfg; w[i].ch = ch; w[i].idx = i;
        w[i].start_at = start_at; w[i].stop = &stop;
        w[i].hist = calloc(HIST_BUCKETS, sizeof(uint32_t));
        if (!w[i].hist || pthread_create(&tids[i], NULL, worker_main, &w[i]) != 0) {
            fprintf(stderr, "failed to start worker %d\n", i);
            return 1;
        }
    }
    fprintf(stderr, "dpumesh_host: threads=%d size=%u mode=%s duration=%.0fs window=%u service=%s\n",
            cfg.threads, cfg.size, cfg.mode == MODE_ECHO ? "echo" : "sink", cfg.duration, cfg.window, cfg.service);

    /* reporter: once a second until every worker has finished (or the watchdog fires) */
    t0 = last = now_sec();
    totals_read(w, cfg.threads, &prev);
    for (;;) {
        int alive = 0;
        double now;
        usleep(1000000);
        now = now_sec();
        totals_read(w, cfg.threads, &cur);
        report_rate(&cfg, &cur, &prev, now - last);
        prev = cur;
        last = now;
        for (i = 0; i < cfg.threads; i++) {
            if (!joined[i] && pthread_tryjoin_np(tids[i], NULL) == 0) joined[i] = 1;
            if (!joined[i]) alive++;
        }
        if (alive == 0) break;
        if (now - t0 > cfg.duration + WATCHDOG_S) {
            fprintf(stderr, "watchdog: stopping workers\n");
            atomic_store(&stop, 1);
        }
    }

    failed = report_final(&cfg, w);
    for (i = 0; i < cfg.threads; i++) free(w[i].hist);
    dmesh_destroy_channel(ch);
    return failed ? 1 : 0;
}
