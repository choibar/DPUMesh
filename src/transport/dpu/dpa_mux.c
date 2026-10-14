/* Flow-multiplexed forward DMA: the ARM side of run_dma_mux (dpa_mux.h). */
#include "dpa_mux.h"

#include <doca_build_config.h>

#ifdef DOCA_ARCH_DPU

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <doca_buf_array.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_dpa.h>
#include <doca_log.h>
#include <doca_mmap.h>

#include "comch_msgq.h"
#include "dma.h"
#include "dpa.h"
#include "dpa_common.h"
#include "object.h"
#include "ring.h"

DOCA_LOG_REGISTER(DPA_MUX);

extern doca_dpa_func_t run_dma_mux;
extern doca_dpa_func_t thread_init_rpc;

#define MUX_MAX_THREADS 16
#define MUX_WATCH_NS 1000000000ull

struct mux_thread {
    struct objects *objs;
    int idx;
    struct dmesh_doca_dpa_thread thread;
    struct dmesh_dpa_endpoint ep;
    struct dmesh_conn *ring_conn[DMESH_MUX_RINGS]; /* NULL = slot free on the ARM side */
    unsigned rings_used;
    uint64_t last_heartbeat, last_credit_waits;
    bool stalled;
    uint64_t stale_msgs, bad_msgs;
};

struct dmesh_mux {
    int n;
    uint32_t next_gen;
    uint64_t stall_us;
    uint64_t next_watch_ns;
    bool stats;                /* DMESH_DPA_MUX_STATS=1: one line per thread per second */
    struct mux_thread t[MUX_MAX_THREADS];
};

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

int dmesh_dpa_mux_threads(void)
{
    static int threads = -1;
    if (threads < 0) {
        const char *env = getenv("DMESH_DPA_MUX");
        int n = env != NULL ? atoi(env) : 0;
        threads = n < 0 ? 0 : n > MUX_MAX_THREADS ? MUX_MAX_THREADS : n;
    }
    return threads;
}

static doca_dpa_dev_uintptr_t ring_addr(const struct mux_thread *t, unsigned r, size_t field)
{
    return t->thread.local_storage + offsetof(struct dpa_mux_ctx, ring) +
           r * sizeof(struct dpa_mux_ring) + field;
}

static doca_error_t ring_set(const struct mux_thread *t, unsigned r, size_t field, uint64_t value)
{
    return DMESH_DPA_CALL(doca_dpa_h2d_memcpy(t->thread.dpa, ring_addr(t, r, field), &value, sizeof(value)));
}

/* Each copy's immediate. The ring slot names the flow; the generation keeps a
 * refilled slot's late messages away from its next flow. Runs inside the
 * worker's consumer PE progress. */
static void mux_received(void *owner, const void *data, uint32_t len)
{
    struct mux_thread *t = owner;
    const struct dmesh_mux_msg *msg = data;
    struct dmesh_conn *conn;

    if (len != sizeof(*msg) || msg->type != DMESH_MUX_MSG_DMA_DONE || msg->ring >= DMESH_MUX_RINGS) {
        t->bad_msgs++;
        return;
    }
    conn = t->ring_conn[msg->ring];
    if (conn == NULL || conn->mux_gen != msg->generation || (long)msg->slot != conn - t->objs->conns) {
        t->stale_msgs++;
        return;
    }
    conn->mux_received++;
    dmesh_doca_conn_received(conn, msg->pos, msg->length, 1);
}

/* Same sequence as a flow's thread (dpa.c, host-dpa in channel.c): create with
 * its own entry and state, attach the msgq pair, return the consumer credits,
 * copy the state in, run, and kick it with a first message. */
static doca_error_t mux_thread_start(struct objects *objs, struct dmesh_mux *mux, int i)
{
    struct mux_thread *t = &mux->t[i];
    struct dmesh_doca_dpa_thread *th = &t->thread;
    struct dpa_mux_ctx *ctx = NULL;
    doca_dpa_dev_completion_t producer_comp;
    doca_dpa_dev_comch_producer_t producer;
    doca_dpa_dev_comch_consumer_t consumer;
    struct comch_msg kick = {0};
    const char *pin = getenv("DMESH_DPA_PIN_EU_BASE");
    uint64_t rpc_ret = 0;
    doca_error_t result;

    t->objs = objs;
    t->idx = i;
    th->dpa = objs->dpa_pool->dpa;
    if (pin != NULL && *pin != '\0') /* same EUs as pool threads 0..n-1, which do not run */
        th->pin_eu = (uint32_t)(atoi(pin) + objs->worker_idx * DPA_THREAD_POOL_SIZE + i + 1);

    result = DMESH_DPA_CALL(doca_dpa_mem_alloc(th->dpa, sizeof(struct dpa_mux_ctx), &th->local_storage));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_create(th->dpa, &th->thread));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_set_func_arg(th->thread, run_dma_mux, 0));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_set_local_storage(th->thread, th->local_storage));
    if (result == DOCA_SUCCESS && th->pin_eu != 0) {
        struct doca_dpa_eu_affinity *affinity = NULL; /* kept for the thread's lifetime */
        result = DMESH_DPA_CALL(doca_dpa_eu_affinity_create(th->dpa, &affinity));
        if (result == DOCA_SUCCESS)
            result = DMESH_DPA_CALL(doca_dpa_eu_affinity_set(affinity, th->pin_eu - 1));
        if (result == DOCA_SUCCESS)
            result = DMESH_DPA_CALL(doca_dpa_thread_set_affinity(th->thread, affinity));
    }
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_start(th->thread));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: create failed: %s", i, doca_error_get_name(result));
        return result;
    }
    th->started = true;

    t->ep.pe = objs->consumer_pe;
    t->ep.dpa_thread = th;
    result = dmesh_dpa_endpoint_init_comch(&t->ep, objs->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: msgq setup failed: %s", i, doca_error_get_name(result));
        return result;
    }
    t->ep.dpa_comch->owner = t;
    t->ep.dpa_comch->raw_received = mux_received;

    result = DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(t->ep.dpa_comch->producer_comp, &producer_comp));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_producer_get_dpa_handle(t->ep.dpa_comch->recv.producer, &producer));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_consumer_get_dpa_handle(t->ep.dpa_comch->send.consumer, &consumer));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: handles: %s", i, doca_error_get_name(result));
        return result;
    }

    ctx = calloc(1, sizeof(*ctx)); /* every slot NONE/IDLE */
    if (ctx == NULL)
        return DOCA_ERROR_NO_MEMORY;
    ctx->producer = producer;
    ctx->producer_comp = producer_comp;
    result = DMESH_DPA_CALL(doca_dpa_rpc(th->dpa, thread_init_rpc, &rpc_ret, consumer,
                                         (uint32_t)CC_DPA_MAX_MSG_NUM, (uint64_t)0));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_h2d_memcpy(th->dpa, th->local_storage, ctx, sizeof(*ctx)));
    free(ctx);
    if (result == DOCA_SUCCESS) {
        th->running = true;
        result = DMESH_DPA_CALL(doca_dpa_thread_run(th->thread));
    }
    /* A run thread starts on its first completion: this message is that kick. */
    if (result == DOCA_SUCCESS)
        result = dmesh_doca_dpa_msgq_send(&t->ep.dpa_comch->send, &kick, sizeof(kick));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: start failed: %s", i, doca_error_get_name(result));
        return result;
    }
    mux->n = i + 1;
    return DOCA_SUCCESS;
}

/* Worker cleanup (objs->release_mux), after every flow has left: stop each
 * thread once its copies completed, then free it. */
static doca_error_t mux_release(struct objects *objs)
{
    struct dmesh_mux *mux = objs->mux;
    doca_error_t result;

    if (mux == NULL)
        return DOCA_SUCCESS;
    for (int i = 0; i < mux->n; i++) {
        struct mux_thread *t = &mux->t[i];
        uint64_t one = 1, stopped = 0;

        if (t->rings_used != 0)
            return DOCA_ERROR_IN_USE;
        if (t->thread.running && !t->thread.quiesced) {
            (void)DMESH_DPA_CALL(doca_dpa_h2d_memcpy(t->thread.dpa,
                t->thread.local_storage + offsetof(struct dpa_mux_ctx, stop), &one, sizeof(one)));
            for (int spins = 0; spins < 100000 && stopped == 0; spins++)
                (void)DMESH_DPA_CALL(doca_dpa_d2h_memcpy(t->thread.dpa, &stopped,
                    t->thread.local_storage + offsetof(struct dpa_mux_ctx, stopped), sizeof(stopped)));
            if (stopped == 0)
                return DOCA_ERROR_TIME_OUT;
            t->thread.quiesced = true;
        }
        result = dmesh_dpa_comch_destroy_checked(&t->thread, &t->ep.dpa_comch, objs->consumer_pe);
        if (result != DOCA_SUCCESS)
            return result;
        result = dmesh_doca_dpa_thread_destroy_checked(&t->thread);
        if (result != DOCA_SUCCESS)
            return result;
    }
    free(mux);
    objs->mux = NULL;
    objs->release_mux = NULL;
    return DOCA_SUCCESS;
}

doca_error_t dmesh_dpa_mux_init(struct objects *objs)
{
    int n = dmesh_dpa_mux_threads();
    const char *stall = getenv("DMESH_DPA_MUX_STALL_MS");
    const char *stats = getenv("DMESH_DPA_MUX_STATS");
    struct dmesh_mux *mux;
    doca_error_t result;

    if (n == 0 || objs->mux != NULL)
        return DOCA_SUCCESS;
    if (objs->dpa_pool == NULL || objs->dpa_pool->dpa == NULL || objs->consumer_pe == NULL)
        return DOCA_ERROR_BAD_STATE;
    mux = calloc(1, sizeof(*mux));
    if (mux == NULL)
        return DOCA_ERROR_NO_MEMORY;
    mux->stall_us = (stall != NULL && atoi(stall) > 0 ? (uint64_t)atoi(stall) : 100) * 1000;
    mux->stats = stats != NULL && atoi(stats) != 0;
    objs->mux = mux;
    objs->release_mux = mux_release;
    for (int i = 0; i < n; i++) {
        result = mux_thread_start(objs, mux, i);
        if (result != DOCA_SUCCESS)
            return result; /* the threads started so far stay for checked cleanup */
    }
    mux->next_watch_ns = now_ns() + MUX_WATCH_NS; /* first sample once every thread has run */
    DOCA_LOG_INFO("DPA mux: %d threads on worker %d, %u rings each", n, objs->worker_idx, DMESH_MUX_RINGS);
    return DOCA_SUCCESS;
}

doca_error_t dmesh_dpa_mux_add(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    struct dmesh_mux *mux = objs->mux;
    struct dpa_mux_ring g = {0};
    doca_dpa_dev_buf_arr_t buf_arr;
    doca_dpa_dev_mmap_t host_mmap, dpu_mmap;
    struct mux_thread *t = NULL;
    unsigned r;
    doca_error_t result;

    if (mux == NULL || conn->mux)
        return DOCA_ERROR_BAD_STATE;
    for (int i = 0; i < mux->n; i++)
        if (mux->t[i].rings_used < DMESH_MUX_RINGS && (t == NULL || mux->t[i].rings_used < t->rings_used))
            t = &mux->t[i];
    if (t == NULL) {
        DOCA_LOG_ERR("DPA mux: all %d threads hold %u rings; raise DMESH_DPA_MUX", mux->n, DMESH_MUX_RINGS);
        return DOCA_ERROR_FULL;
    }
    for (r = 0; r < DMESH_MUX_RINGS && t->ring_conn[r] != NULL; r++) {
    }

    result = DMESH_DPA_CALL(doca_buf_arr_get_dpa_handle(conn->buf_arr, &buf_arr));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->sndbuf.mmap, objs->dev, &host_mmap));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->local_mmap, objs->dev, &dpu_mmap));
    if (result != DOCA_SUCCESS)
        return result;
    if (++mux->next_gen == 0)
        ++mux->next_gen;
    g.buf_arr = buf_arr;
    g.host_mmap = host_mmap;
    g.dpu_mmap = dpu_mmap;
    g.staging_addr = (uint64_t)(uintptr_t)conn->dma_buffer;
    g.buf_size = BUFFER_SIZE;
    g.ring_size = DMA_RING_SIZE;
    g.host_base = (uint64_t)(uintptr_t)conn->sndbuf.buf;
    g.host_len = conn->sndbuf.size;
    g.slot = (uint64_t)(conn - objs->conns);
    g.generation = mux->next_gen;
    g.cmd = DMESH_MUX_CMD_NONE;
    g.state = DMESH_MUX_IDLE;

    /* A slot the previous flow left is CLOSE/CLOSED, which the DPA ignores.
     * NONE first keeps it ignored while the rest changes underneath; OPEN
     * last hands the filled slot over. Each h2d_memcpy completes before the
     * next starts. */
    result = ring_set(t, r, offsetof(struct dpa_mux_ring, cmd), DMESH_MUX_CMD_NONE);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_h2d_memcpy(t->thread.dpa, ring_addr(t, r, 0), &g, sizeof(g)));
    if (result == DOCA_SUCCESS)
        result = ring_set(t, r, offsetof(struct dpa_mux_ring, cmd), DMESH_MUX_CMD_OPEN);
    if (result != DOCA_SUCCESS)
        return result;

    t->ring_conn[r] = conn;
    t->rings_used++;
    conn->mux = true;
    conn->mux_close_sent = false;
    conn->mux_thread = (uint8_t)t->idx;
    conn->mux_ring = (uint8_t)r;
    conn->mux_gen = (uint32_t)g.generation;
    conn->mux_error = 0;
    conn->mux_received = 0;
    conn->rx_consumed_pos_published = 0;
    DOCA_LOG_INFO("Flow slot %ld on DPA mux thread %d ring %u (generation %u)",
                  conn - objs->conns, t->idx, r, conn->mux_gen);
    return DOCA_SUCCESS;
}

doca_error_t dmesh_dpa_mux_del_step(struct dmesh_conn *conn)
{
    struct mux_thread *t;
    struct dpa_mux_ring g;
    doca_error_t result;

    if (!conn->mux)
        return DOCA_SUCCESS;
    t = &conn->objs->mux->t[conn->mux_thread];
    if (!conn->mux_close_sent) {
        result = ring_set(t, conn->mux_ring, offsetof(struct dpa_mux_ring, cmd), DMESH_MUX_CMD_CLOSE);
        if (result != DOCA_SUCCESS)
            return result;
        conn->mux_close_sent = true;
        conn->mux_deadline_ns = now_ns() + DMESH_DPA_QUIESCE_TIMEOUT_MS * 1000000ull;
        return DOCA_ERROR_AGAIN;
    }
    if (dmesh_doca_dpa_failed(t->thread.dpa))
        return DOCA_ERROR_BAD_STATE;
    /* CLOSED: no copy of the ring is left in flight. Every copy that
     * succeeded sent one message; the slot is done once they all arrived
     * (the consumer PE may still hold some). */
    result = DMESH_DPA_CALL(doca_dpa_d2h_memcpy(t->thread.dpa, &g, ring_addr(t, conn->mux_ring, 0), sizeof(g)));
    if (result != DOCA_SUCCESS)
        return result;
    if (g.state == DMESH_MUX_CLOSED && conn->mux_received + g.failed == g.submit)
        return DOCA_SUCCESS;
    if (now_ns() > conn->mux_deadline_ns) {
        DOCA_LOG_ERR("DPA mux thread %d: ring %u (slot %ld) not closed within %d ms (state %llu, "
                     "%llu copies, %llu messages, %llu failed); the ring is kept",
                     t->idx, conn->mux_ring, conn - conn->objs->conns, DMESH_DPA_QUIESCE_TIMEOUT_MS,
                     (unsigned long long)g.state, (unsigned long long)g.submit,
                     (unsigned long long)conn->mux_received, (unsigned long long)g.failed);
        return DOCA_ERROR_TIME_OUT;
    }
    return DOCA_ERROR_AGAIN;
}

void dmesh_dpa_mux_release(struct dmesh_conn *conn)
{
    struct mux_thread *t;

    if (!conn->mux)
        return;
    t = &conn->objs->mux->t[conn->mux_thread];
    if (t->ring_conn[conn->mux_ring] == conn) {
        t->ring_conn[conn->mux_ring] = NULL;
        t->rings_used--;
    }
    conn->mux = conn->mux_close_sent = false;
    conn->mux_error = 0;
}

doca_error_t dmesh_dpa_mux_rx_consumed(struct dmesh_conn *conn, uint32_t pos)
{
    /* After CLOSE the slot no longer takes releases. */
    if (!conn->mux || conn->mux_close_sent)
        return DOCA_SUCCESS;
    return ring_set(&conn->objs->mux->t[conn->mux_thread], conn->mux_ring,
                    offsetof(struct dpa_mux_ring, rx_consumed_pos), pos);
}

void dmesh_dpa_mux_watch(struct objects *objs)
{
    struct dmesh_mux *mux = objs->mux;
    uint64_t now;

    if (mux == NULL)
        return;
    now = now_ns();
    if (now < mux->next_watch_ns)
        return;
    mux->next_watch_ns = now + MUX_WATCH_NS;

    for (int i = 0; i < mux->n; i++) {
        struct mux_thread *t = &mux->t[i];
        /* The whole context (about 3.6 KB): the header and the ring slots. */
        struct dpa_mux_ctx ctx;
        const struct dpa_mux_ctx *h = &ctx;
        const struct dpa_mux_ring *rings = ctx.ring;
        char slots[DMESH_MUX_RINGS * 6 + 1] = "";
        size_t len = 0;
        bool stalled, stopped;

        if (DMESH_DPA_CALL(doca_dpa_d2h_memcpy(t->thread.dpa, &ctx, t->thread.local_storage,
                                               sizeof(ctx))) != DOCA_SUCCESS)
            continue;

        /* A ring the DPA stopped: its flow is closed with an error. */
        for (unsigned r = 0; r < DMESH_MUX_RINGS; r++) {
            struct dmesh_conn *conn = t->ring_conn[r];

            if (conn != NULL && !conn->mux_close_sent && conn->mux_error == 0 &&
                rings[r].state == DMESH_MUX_ERROR && (uint32_t)rings[r].generation == conn->mux_gen) {
                conn->mux_error = rings[r].error != 0 ? (uint32_t)rings[r].error : DMESH_MUX_ERR_COPY;
                DOCA_LOG_ERR("DPA mux thread %d ring %u (slot %ld): forward ring stopped, error %u",
                             i, r, conn - objs->conns, conn->mux_error);
            }
        }

        stopped = h->heartbeat == t->last_heartbeat; /* dmesh_doca_dpa_failed logs a dead context itself */
        (void)dmesh_doca_dpa_failed(t->thread.dpa);
        stalled = h->oldest_age_us > mux->stall_us;
        if (mux->stats)
            DOCA_LOG_INFO("DPA mux thread %d: rings=%u passes/s=%llu credit_waits/s=%llu in_flight=%llu "
                          "oldest_us=%llu err_cqes=%llu bad_cqes=%llu stale_msgs=%llu bad_msgs=%llu",
                          i, t->rings_used, (unsigned long long)(h->heartbeat - t->last_heartbeat),
                          (unsigned long long)(h->credit_waits - t->last_credit_waits),
                          (unsigned long long)(h->fifo_tail - h->fifo_head), (unsigned long long)h->oldest_age_us,
                          (unsigned long long)h->err_cqes, (unsigned long long)h->bad_cqes,
                          (unsigned long long)t->stale_msgs, (unsigned long long)t->bad_msgs);
        t->last_heartbeat = h->heartbeat;
        t->last_credit_waits = h->credit_waits;
        if ((stalled || stopped) && !t->stalled) {
            for (unsigned r = 0; r < DMESH_MUX_RINGS; r++)
                if (t->ring_conn[r] != NULL && len + 6 < sizeof(slots))
                    len += (size_t)snprintf(slots + len, sizeof(slots) - len, "%s%ld", len ? "," : "",
                                            t->ring_conn[r] - objs->conns);
            DOCA_LOG_ERR("DPA mux thread %d %s: oldest copy in flight %llu ms; flows on it: slots %s. "
                         "A copy from a host process that died blocks the thread for good (dpa_mux.h)",
                         i, stopped ? "is not running" : "is stalled",
                         (unsigned long long)(h->oldest_age_us / 1000), len ? slots : "none");
            t->stalled = true;
        } else if (!stalled && !stopped && t->stalled) {
            DOCA_LOG_WARN("DPA mux thread %d is moving again", i);
            t->stalled = false;
        }
    }
}

#else /* !DOCA_ARCH_DPU */

int dmesh_dpa_mux_threads(void) { return 0; }
doca_error_t dmesh_dpa_mux_init(struct objects *objs) { (void)objs; return DOCA_SUCCESS; }
doca_error_t dmesh_dpa_mux_add(struct dmesh_conn *conn) { (void)conn; return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_dpa_mux_del_step(struct dmesh_conn *conn) { (void)conn; return DOCA_SUCCESS; }
void dmesh_dpa_mux_release(struct dmesh_conn *conn) { (void)conn; }
doca_error_t dmesh_dpa_mux_rx_consumed(struct dmesh_conn *conn, uint32_t pos) { (void)conn; (void)pos; return DOCA_SUCCESS; }
void dmesh_dpa_mux_watch(struct objects *objs) { (void)objs; }

#endif
