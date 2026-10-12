/*
 * dpumesh_dpu — the DPU-side peer of the DMA benchmark.
 *
 * This is the legacy dpu_worker.c benchmark role ported onto the API the real
 * data plane uses: the shim (linkerd/doca/src/shim.c) over the comch_server.c
 * state machine. It runs one Comch server, the shared DPA pool / consumer PE /
 * DMA engine, and per connection the zero-copy staging reader, the rx
 * consumed position and the reverse sender. The host peer is dpumesh_host.
 *
 * Modes (DMESH_MODE):
 *   sink   count every forward DMA completion (host -> DPU) and drop the bytes
 *   echo   ...and send the same bytes back (DPU -> host). How they go back is
 *          the channel layer's business: a push batch (dpu-dma reverse path) or a descriptor the
 *          host's DPA pulls (host-dpa reverse path, CLIENT / BACKEND_PULL flows).
 *
 * Output, once a second:
 *   TOTAL(N conns): recv: <DMA/s> (<Gbps>), sent: <push/s> (<Gbps>), ...
 * `recv` is the number of forward DMA completions the DPA delivered, the
 * ground truth for "DMAs per second"; `sent` counts reverse batches this
 * program published. A final DPU_BENCH_DONE line summarizes the run.
 *
 * Environment:
 *   DMESH_DEV_PCI        DPU device            (03:00.1)
 *   DMESH_REP_PCI        host representor      (0b:00.1)
 *   DMESH_SERVER_NAME    Comch server name     (DPUMesh0)
 *   DMESH_MODE           sink | echo           (echo)
 *   DMESH_BUSY_POLL      1 = poll the progress engines, 0 = sleep in epoll (0)
 *   DMESH_BENCH_EXIT_OPS exit after this many forward completions (0 = never)
 *
 * Restart the process per benchmark run: process-wide benchmark shutdown
 * still uses _exit(), while individual connections use checked teardown.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

#include <doca_error.h>

#include "comch_server.h" /* dmesh_doca_ctrl_*, enum dmesh_doca_init_state */

/* ------------------------------------------------------------------------
 * Shim API (shim.c ships without a header; these mirror its exports)
 * ------------------------------------------------------------------------ */
struct objects;
int32_t dmesh_doca_init(const char *dev_pci_addr, const char *rep_pci_addr, const char *server_name,
                        struct objects **handle);
int32_t dmesh_doca_data_get_fd(struct objects *objs, int *out_fd);
int32_t dmesh_doca_data_arm(struct objects *objs);
int32_t dmesh_doca_data_clear_and_drain(struct objects *objs, int fd, int budget, int *out_drained);
int32_t dmesh_doca_max_conns(void);
int32_t dmesh_doca_conn_state_get(struct objects *objs, int32_t slot);
int32_t dmesh_doca_conn_readers_detached(struct objects *objs, int32_t slot);
int32_t dmesh_doca_conn_mode_get(struct objects *objs, int32_t slot);
int32_t dmesh_doca_conn_staging_base(struct objects *objs, int32_t slot, const uint8_t **out_base,
                                     size_t *out_len);
int32_t dmesh_doca_conn_recv_pop(struct objects *objs, int32_t slot, uint32_t *out_pos,
                                 uint32_t *out_len);
int32_t dmesh_doca_conn_tx_staging(struct objects *objs, int32_t slot, uintptr_t *out_base,
                                   size_t *out_len);
int32_t dmesh_doca_conn_update_dpu_rx_consumed_pos(struct objects *objs, int32_t slot, uint32_t pos);
int32_t dmesh_doca_conn_send_staged(struct objects *objs, int32_t slot, uint32_t pos, uint32_t len);
void dmesh_doca_stats_get(struct objects *objs, int64_t *sent, int64_t *recv, int64_t *recv_bytes,
                          int64_t *dma_pending, int64_t *dma_dropped);

/* Per-connection states reported by dmesh_doca_conn_state_get (object.h). */
enum { CONN_FREE = 0, CONN_RUNNING = 3, CONN_ERROR = 4, CONN_CLOSING = 6 };

#define DATA_DRAIN_BUDGET 8192   /* consumer-PE events per tick, as in the proxy driver */
#define MAX_SLOTS         64
#define SEG_MAX           8192u  /* one forward completion covers at most this many bytes */
#define IDLE_WAIT_MS      1      /* epoll timeout: the staged sends have no fd of their own */

/* ------------------------------------------------------------------------
 * Small helpers
 * ------------------------------------------------------------------------ */
static volatile sig_atomic_t g_stop;
static void on_signal(int sig) { (void)sig; g_stop = 1; }

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static const char *env_or(const char *name, const char *fallback)
{
    const char *v = getenv(name);
    return v && *v ? v : fallback;
}

static double gbps(uint64_t bytes, double seconds) { return (double)bytes * 8.0 / seconds / 1e9; }

/* ------------------------------------------------------------------------
 * tx_ring — write cursor over a connection's mapped tx_staging
 *
 * Bytes are copied into staging once and later published by run. Both
 * cursors are cumulative; the physical offset is cursor % len, and the
 * unpublished region [publish, write) is never overwritten. This is the C
 * twin of the C++ router's TxRing / the Rust io.rs write side.
 * ------------------------------------------------------------------------ */
struct tx_ring {
    uint8_t *base;
    size_t len;
    uint64_t write, publish;
};

static size_t tx_room(const struct tx_ring *t)
{
    return t->len ? t->len - (size_t)(t->write - t->publish) : 0;
}

/* Copies up to n bytes, wrapping at the end of the region; returns the bytes taken. */
static size_t tx_push(struct tx_ring *t, const uint8_t *data, size_t n)
{
    size_t room = tx_room(t), off, first;
    if (n > room) n = room;
    if (n == 0) return 0;
    off = (size_t)(t->write % t->len);
    first = n < t->len - off ? n : t->len - off;
    memcpy(t->base + off, data, first);
    if (first < n) memcpy(t->base, data + first, n - first);
    t->write += n;
    return n;
}

/* Next contiguous unpublished run (never crossing the wrap). 0 when empty. */
static int tx_next_run(const struct tx_ring *t, uint32_t *pos, uint32_t *len)
{
    size_t off, pending, run;
    if (!t->base || t->write == t->publish) return 0;
    off = (size_t)(t->publish % t->len);
    pending = (size_t)(t->write - t->publish);
    run = pending < t->len - off ? pending : t->len - off;
    *pos = (uint32_t)off;
    *len = (uint32_t)run;
    return 1;
}

/* ------------------------------------------------------------------------
 * slot — one connection served by this worker
 * ------------------------------------------------------------------------ */
struct slot {
    int32_t state;              /* last dmesh_doca_conn_state_get value */
    const uint8_t *rx_base;     /* forward staging (DPA writes, we read) */
    size_t rx_len;
    uint32_t dpu_rx_consumed_pos; /* end offset of the last fully consumed segment */
    int dpu_rx_consumed_pos_changed;
    struct tx_ring tx;          /* reverse staging (we write, the channel layer sends) */
    uint64_t segs, bytes;       /* forward completions consumed by this slot */
    uint64_t pushes, push_bytes;/* reverse batches published (echo) */
};

/* Fetches the staging regions once the connection is RUNNING; both may
 * become available a tick or two apart. */
static void slot_wire(struct objects *objs, int idx, struct slot *s)
{
    if (!s->rx_base) {
        const uint8_t *base = NULL;
        size_t len = 0;
        if (dmesh_doca_conn_staging_base(objs, idx, &base, &len) == DOCA_SUCCESS && base) {
            s->rx_base = base;
            s->rx_len = len;
        }
    }
    if (!s->tx.base) {
        uintptr_t base = 0;
        size_t len = 0;
        if (dmesh_doca_conn_tx_staging(objs, idx, &base, &len) == 0 && base && len) {
            s->tx.base = (uint8_t *)base;
            s->tx.len = len;
        }
    }
}

/* Publishes everything staged for sending. The wire may take less than a
 * run (the push engine holds one <= 8 KiB batch, the descriptor ring may be
 * full): a short return means "retry next tick". */
static void slot_publish(struct objects *objs, int idx, struct slot *s)
{
    uint32_t pos, len;
    while (tx_next_run(&s->tx, &pos, &len)) {
        int32_t rv = dmesh_doca_conn_send_staged(objs, idx, pos, len);
        if (rv <= 0) break;
        s->tx.publish += (uint32_t)rv;
        s->pushes++;
        s->push_bytes += (uint32_t)rv;
        if ((uint32_t)rv < len) break;
    }
}

/* Consumes the forward completions queued for the slot.
 *
 * Echo: a segment is popped only when tx_staging can take it whole, so no
 * byte is ever dropped; when staging is full the DPU RX consumed position stops
 * advancing, the DPA's staging gate holds the host's forward ring, and the
 * backpressure reaches the host application, exactly as on the proxy path.
 * Sink: every segment is popped and only counted. */
static void slot_pump(struct objects *objs, int idx, struct slot *s, int echo)
{
    uint32_t pos, len;
    if (!s->rx_base) return;
    for (;;) {
        if (echo) {
            slot_publish(objs, idx, s);
            if (!s->tx.base || tx_room(&s->tx) < SEG_MAX) break;
        }
        if (dmesh_doca_conn_recv_pop(objs, idx, &pos, &len) != DOCA_SUCCESS) break;
        if ((size_t)pos + len > s->rx_len) {
            fprintf(stderr, "slot %d: recv segment out of range pos=%u len=%u\n", idx, pos, len);
            continue;
        }
        s->segs++;
        s->bytes += len;
        if (echo) tx_push(&s->tx, s->rx_base + pos, len);
        s->dpu_rx_consumed_pos = pos + len;
        s->dpu_rx_consumed_pos_changed = 1;
    }
    if (s->dpu_rx_consumed_pos_changed) {
        s->dpu_rx_consumed_pos_changed = 0;
        dmesh_doca_conn_update_dpu_rx_consumed_pos(objs, idx, s->dpu_rx_consumed_pos);
    }
    if (echo) slot_publish(objs, idx, s);
}

/* Tracks state transitions of every slot: (re)initializes a slot when its
 * connection reaches RUNNING, folds its counters into the aggregate when it
 * closes. Returns the number of RUNNING slots after pumping them. */
static int slots_poll(struct objects *objs, struct slot *slots, int max_conns, int echo,
                      uint64_t *closed_pushes, uint64_t *closed_push_bytes)
{
    int active = 0, i;
    for (i = 0; i < max_conns; i++) {
        struct slot *s = &slots[i];
        int32_t st = dmesh_doca_conn_state_get(objs, i);
        if (st != s->state) {
            if (st == CONN_RUNNING) {
                memset(s, 0, sizeof(*s));
                fprintf(stderr, "slot %d running (mode %d)\n", i, dmesh_doca_conn_mode_get(objs, i));
            } else if (st == CONN_FREE || st == CONN_ERROR || st == CONN_CLOSING) {
                if (s->state == CONN_RUNNING)
                    fprintf(stderr, "slot %d closed: %llu segments, %llu bytes in, %llu pushes out\n", i,
                            (unsigned long long)s->segs, (unsigned long long)s->bytes,
                            (unsigned long long)s->pushes);
                *closed_pushes += s->pushes;
                *closed_push_bytes += s->push_bytes;
                memset(s, 0, sizeof(*s));
            }
            s->state = st;
        }
        if (st == CONN_CLOSING) {
            /* This thread owns every staging pointer and progresses both
             * PEs only in driver_tick(). The transition above discarded all
             * cached pointers and unpublished work before this ACK allows
             * the next tick to release the connection's mapped buffers. */
            int32_t rv = dmesh_doca_conn_readers_detached(objs, i);
            if (rv != DOCA_SUCCESS)
                fprintf(stderr, "slot %d: reader detach failed (%d), retrying\n", i, rv);
            continue;
        }
        if (st != CONN_RUNNING) continue;
        slot_wire(objs, i, s);
        slot_pump(objs, i, s, echo);
        active++;
    }
    return active;
}

/* ------------------------------------------------------------------------
 * Statistics — the per-second TOTAL line and the final summary
 *
 * Forward numbers come from the shim (dmesh_doca_stats_get: recv = forward
 * completions, recv_bytes, dma_pending, dma_dropped); reverse batches are
 * counted here because the shim's `sent` does not cover them.
 * ------------------------------------------------------------------------ */
struct counters {
    int64_t sent, recv, recv_bytes, dma_pending, dma_dropped;   /* shim */
    uint64_t pushes, push_bytes;                                /* ours */
};

struct stats {
    struct counters prev;       /* at the last report */
    struct counters first;      /* at the first report that saw traffic */
    double last;                /* time of the last report */
    double t0;                  /* time traffic was first seen; 0 = never */
    int echo;
};

static void counters_read(struct objects *objs, const struct slot *slots, int max_conns,
                          uint64_t closed_pushes, uint64_t closed_push_bytes, struct counters *c)
{
    int i;
    dmesh_doca_stats_get(objs, &c->sent, &c->recv, &c->recv_bytes, &c->dma_pending, &c->dma_dropped);
    c->pushes = closed_pushes;
    c->push_bytes = closed_push_bytes;
    for (i = 0; i < max_conns; i++) {
        c->pushes += slots[i].pushes;
        c->push_bytes += slots[i].push_bytes;
    }
}

/* Prints the TOTAL line when a second has passed and something moved. */
static void stats_report(struct stats *st, const struct counters *cur, int active, double now)
{
    double el = now - st->last;
    if (el < 1.0) return;
    if (cur->recv != st->prev.recv || cur->pushes != st->prev.pushes) {
        if (st->t0 == 0) { st->t0 = st->last; st->first = st->prev; }
        fprintf(stderr,
                "TOTAL(%d conns): elapsed: %.2f, recv: %.0f DMA/s (%.2f Gbps), sent: %.0f push/s "
                "(%.2f Gbps), dma_pending: %lld, dma_dropped: %lld\n",
                active, el,
                (double)(cur->recv - st->prev.recv) / el,
                gbps((uint64_t)(cur->recv_bytes - st->prev.recv_bytes), el),
                (double)(cur->pushes - st->prev.pushes) / el,
                gbps(cur->push_bytes - st->prev.push_bytes, el),
                (long long)cur->dma_pending, (long long)cur->dma_dropped);
    }
    st->prev = *cur;
    st->last = now;
}

static void stats_final(const struct stats *st, const struct counters *cur, double now)
{
    double wall;
    if (st->t0 == 0) return;
    wall = now - st->t0;
    fprintf(stderr,
            "DPU_BENCH_DONE mode=%s wall_sec=%.2f recv_dma=%lld recv_dma_per_sec=%.0f recv_gbps=%.2f "
            "sent_push=%llu dma_dropped=%lld\n",
            st->echo ? "echo" : "sink", wall,
            (long long)(cur->recv - st->first.recv), (double)(cur->recv - st->first.recv) / wall,
            gbps((uint64_t)(cur->recv_bytes - st->first.recv_bytes), wall),
            (unsigned long long)(cur->pushes - st->first.pushes), (long long)cur->dma_dropped);
}

/* ------------------------------------------------------------------------
 * Driver — bring the infrastructure up, then the tick loop
 *
 * The loop mirrors run_dpu_worker_event_driven() and the router's main:
 * arm both progress engines, drain control, drain data with a budget,
 * advance the connection state machines, serve the slots, report, and
 * sleep in epoll unless busy-polling.
 * ------------------------------------------------------------------------ */
struct driver {
    struct objects *objs;
    enum dmesh_doca_init_state state;
    int ctrl_fd, data_fd, epfd;     /* epfd = -1 when busy-polling */
    int max_conns;
};

static int driver_open(struct driver *d, const char *dev_pci, const char *rep_pci, const char *server, int busy)
{
    d->state = DMESH_DOCA_STATE_SERVER_STARTED;
    d->epfd = -1;
    if (dmesh_doca_init(dev_pci, rep_pci, server, &d->objs) != DOCA_SUCCESS || !d->objs) {
        fprintf(stderr, "dmesh_doca_init failed (dev=%s rep=%s server=%s)\n", dev_pci, rep_pci, server);
        return -1;
    }
    /* shared infrastructure (DPA pool, consumer PE, DMA engine) before serving */
    if (dmesh_doca_ctrl_advance(d->objs, &d->state) != DOCA_SUCCESS || d->state != DMESH_DOCA_STATE_RUNNING) {
        fprintf(stderr, "infrastructure did not reach RUNNING\n");
        return -1;
    }
    if (dmesh_doca_ctrl_get_fd(d->objs, &d->ctrl_fd) != DOCA_SUCCESS ||
        dmesh_doca_data_get_fd(d->objs, &d->data_fd) != DOCA_SUCCESS) {
        fprintf(stderr, "failed to get progress-engine fds\n");
        return -1;
    }
    if (!busy) {
        struct epoll_event ev = { .events = EPOLLIN };
        d->epfd = epoll_create1(0);
        ev.data.fd = d->ctrl_fd;
        if (d->epfd < 0 || epoll_ctl(d->epfd, EPOLL_CTL_ADD, d->ctrl_fd, &ev) != 0) { perror("epoll ctrl"); return -1; }
        ev.data.fd = d->data_fd;
        if (epoll_ctl(d->epfd, EPOLL_CTL_ADD, d->data_fd, &ev) != 0) { perror("epoll data"); return -1; }
    }
    d->max_conns = dmesh_doca_max_conns();
    if (d->max_conns > MAX_SLOTS) d->max_conns = MAX_SLOTS;
    return 0;
}

/* One progress tick. Returns the data events drained, or -1 on a fatal
 * control-path error. Arming first closes the race where an event lands
 * between the drain and the wait. */
static int driver_tick(struct driver *d)
{
    int drained = 0;
    if (d->epfd >= 0 &&
        (dmesh_doca_ctrl_arm(d->objs) != DOCA_SUCCESS || dmesh_doca_data_arm(d->objs) != DOCA_SUCCESS)) {
        fprintf(stderr, "failed to arm progress engines\n");
        return -1;
    }
    if (dmesh_doca_ctrl_drain(d->objs) != DOCA_SUCCESS) {
        fprintf(stderr, "control drain failed\n");
        return -1;
    }
    dmesh_doca_data_clear_and_drain(d->objs, d->data_fd, DATA_DRAIN_BUDGET, &drained);
    if (dmesh_doca_ctrl_advance(d->objs, &d->state) != DOCA_SUCCESS || d->state == DMESH_DOCA_STATE_ERROR) {
        fprintf(stderr, "control advance failed\n");
        return -1;
    }
    return drained;
}

/* Sleeps until a progress engine signals, at most IDLE_WAIT_MS (the
 * control state machine and the staged sends are only driven by ticks). */
static void driver_idle(const struct driver *d, int drained)
{
    struct epoll_event evs[2];
    if (d->epfd < 0 || drained >= DATA_DRAIN_BUDGET) return;
    epoll_wait(d->epfd, evs, 2, IDLE_WAIT_MS);
}

int main(void)
{
    const char *dev_pci = env_or("DMESH_DEV_PCI", "03:00.1");
    const char *rep_pci = env_or("DMESH_REP_PCI", "0b:00.1");
    const char *server = env_or("DMESH_SERVER_NAME", "DPUMesh0");
    const int echo = strcmp(env_or("DMESH_MODE", "echo"), "sink") != 0;
    const int busy = atoi(env_or("DMESH_BUSY_POLL", "0")) != 0;
    const long exit_ops = atol(env_or("DMESH_BENCH_EXIT_OPS", "0"));
    struct driver d = {0};
    struct slot slots[MAX_SLOTS];
    struct stats st = { .echo = echo };
    struct counters cur;
    uint64_t closed_pushes = 0, closed_push_bytes = 0;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    memset(slots, 0, sizeof(slots));

    if (driver_open(&d, dev_pci, rep_pci, server, busy) != 0) return 1;
    fprintf(stderr, "dpumesh_dpu: server=%s dev=%s rep=%s mode=%s poll=%s\n", server, dev_pci, rep_pci,
            echo ? "echo" : "sink", busy ? "busy" : "epoll");
    counters_read(d.objs, slots, d.max_conns, 0, 0, &st.prev);
    st.last = now_sec();

    while (!g_stop) {
        int drained = driver_tick(&d), active;
        double now;
        if (drained < 0) break;
        active = slots_poll(d.objs, slots, d.max_conns, echo, &closed_pushes, &closed_push_bytes);

        now = now_sec();
        if (now - st.last >= 1.0) {
            counters_read(d.objs, slots, d.max_conns, closed_pushes, closed_push_bytes, &cur);
            stats_report(&st, &cur, active, now);
            if (exit_ops > 0 && cur.recv >= exit_ops) {
                fprintf(stderr, "bench: recv target %ld reached (recv=%lld), exiting\n", exit_ops,
                        (long long)cur.recv);
                break;
            }
        }
        driver_idle(&d, drained);
    }

    counters_read(d.objs, slots, d.max_conns, closed_pushes, closed_push_bytes, &cur);
    stats_final(&st, &cur, now_sec());
    fflush(NULL);
    _exit(0);
}
