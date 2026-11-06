/* Pod-multiplexed forward DMA: the ARM side of run_dma_mux (dpa_mux.h). */
#include "dpa_mux.h"

#include <doca_build_config.h>

#ifdef DOCA_ARCH_DPU

#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <doca_buf_array.h>
#include <doca_comch_consumer.h>
#include <doca_comch_msgq.h>
#include <doca_comch_producer.h>
#include <doca_ctx.h>
#include <doca_dpa.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_pe.h>

#include "buffer.h"
#include "dispatcher.h"
#include "dma.h"
#include "dpa.h"
#include "dpa_common.h"
#include "dpa_runtime.h"
#include "object.h"
#include "ring.h"

DOCA_LOG_REGISTER(DPA_MUX);

extern doca_dpa_func_t run_dma_mux;
extern doca_dpa_func_t mux_kick_rpc;

#define MUX_MAX_THREADS 128
#define MUX_WATCH_NS 1000000000ull  /* each thread is checked once per period */
#define MUX_STOP_NS  100000000ull   /* a stopping thread has drained by then */
#define MUX_COUNTERS 6              /* passes, tail reads, visits, copies, credit waits, FIFO full */

/* A worker's consumer of a thread's messages: created with the worker's
 * first ring of that session, destroyed with its last. */
struct pod_consumer {
    struct dmesh_pod_thread *pod;
    struct objects *objs;               /* its consumer PE drives the consumer */
    struct doca_comch_consumer *consumer;
    uint32_t id;
    unsigned rings;
    bool stopping;
    uint64_t stale_msgs, bad_msgs, recv_errors;
};

struct dmesh_pod_thread {
    bool used, retiring;
    int idx;
    uint64_t key, epoch;                /* the session */
    struct dmesh_doca_dpa_thread thread;
    struct doca_dpa_completion *producer_comp;
    bool producer_comp_started;
    struct doca_dpa_notification_completion *notify;
    bool notify_started;
    struct doca_comch_msgq *msgq;
    bool msgq_started;
    struct doca_comch_producer *producer;
    struct doca_mmap *tails_mmap;       /* the host's table, imported */
    struct doca_buf_arr *tails_arr;     /* one buf per line, read by the DPA */
    struct doca_mmap *status_mmap;
    struct dpa_mux_status *status;      /* the DPA publishes its state here */
    struct pod_consumer *cons;          /* one per worker */
    struct dmesh_conn *ring_conn[DMESH_MUX_RINGS]; /* NULL = free */
    uint32_t ring_error[DMESH_MUX_RINGS];          /* the watch's finding, taken by the ring's worker */
    unsigned rings_used;
    uint64_t ctl_seq;
    /* the watch's previous sample */
    uint64_t last_ns, last[MUX_COUNTERS];
    bool stalled;
};

struct dmesh_mux {
    pthread_mutex_t lock;               /* threads, rings, consumers and their SDK calls */
    unsigned workers;
    uint32_t next_gen;
    uint64_t stall_us;
    bool stats;
    uint64_t next_watch_ns;             /* 0 until the first thread started */
    int watch_next;
    struct dmesh_pod_thread t[MUX_MAX_THREADS];
};

static struct dmesh_mux registry = {.lock = PTHREAD_MUTEX_INITIALIZER};

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

/* A worker's consumer slot. Without a dispatcher a session lives on one worker. */
static unsigned worker_of(const struct objects *objs)
{
    return objs->mailbox != NULL ? (unsigned)objs->worker_idx : 0;
}

/* The session: the dispatcher's id and epoch, or the worker-local session. */
static void session_of(const struct dmesh_conn *conn, uint64_t *key, uint64_t *epoch)
{
    if (conn->key.session_id != 0) {
        *key = conn->key.session_id;
        *epoch = conn->key.session_epoch;
    } else {
        *key = (uint64_t)(uintptr_t)conn->session;
        *epoch = conn->session->epoch;
    }
}

static doca_error_t tls_set(struct dmesh_pod_thread *p, size_t offset, uint64_t value)
{
    return DMESH_DPA_CALL(doca_dpa_h2d_memcpy(p->thread.dpa, p->thread.local_storage + offset,
                                              &value, sizeof(value)));
}

static size_t cfg_off(unsigned r, size_t field)
{
    return offsetof(struct dpa_mux_ctx, cfg) + r * sizeof(struct dpa_mux_ring_cfg) + field;
}

static size_t st_off(unsigned r)
{
    return offsetof(struct dpa_mux_ctx, s.st) + r * sizeof(struct dpa_mux_ring_state);
}

/* After a command: the DPA reads the commands again once ctl_seq moved.
 * Registry lock held. */
static doca_error_t ctl_bump(struct dmesh_pod_thread *p)
{
    return tls_set(p, offsetof(struct dpa_mux_ctx, ctl_seq), ++p->ctl_seq);
}

/* Each copy's immediate. The ring names the flow; the generation keeps a
 * refilled ring's late messages away from its next flow. Runs inside the
 * worker's consumer PE progress. Only rings of this worker send here, and
 * their ring_conn changes only on this worker's thread. */
static void pod_received(struct pod_consumer *c, const struct dmesh_mux_msg *msg, uint32_t len)
{
    struct dmesh_conn *conn;

    if (len != sizeof(*msg) || msg->type != DMESH_MUX_MSG_DMA_DONE || msg->ring >= DMESH_MUX_RINGS) {
        c->bad_msgs++;
        return;
    }
    conn = __atomic_load_n(&c->pod->ring_conn[msg->ring], __ATOMIC_ACQUIRE);
    if (conn == NULL || conn->objs != c->objs || conn->mux_gen != msg->generation ||
        (long)msg->slot != conn - c->objs->conns) {
        c->stale_msgs++;
        return;
    }
    conn->mux_received++;
    dmesh_doca_conn_received(conn, msg->pos, msg->length, 1);
}

static void pod_recv_cb(struct doca_comch_consumer_task_post_recv *task, union doca_data task_user_data,
                        union doca_data ctx_user_data)
{
    struct pod_consumer *c = ctx_user_data.ptr;
    struct doca_task *t = doca_comch_consumer_task_post_recv_as_task(task);
    const void *msg = doca_comch_consumer_task_post_recv_get_imm_data(task);

    (void)task_user_data;
    if (msg != NULL)
        pod_received(c, msg, doca_comch_consumer_task_post_recv_get_imm_data_len(task));
    if (c->stopping || doca_task_submit(t) != DOCA_SUCCESS) {
        if (!c->stopping)
            c->recv_errors++;
        doca_task_free(t);
    }
}

static void pod_recv_error_cb(struct doca_comch_consumer_task_post_recv *task, union doca_data task_user_data,
                              union doca_data ctx_user_data)
{
    struct pod_consumer *c = ctx_user_data.ptr;

    (void)task_user_data;
    if (!c->stopping)
        c->recv_errors++;
    doca_task_free(doca_comch_consumer_task_post_recv_as_task(task));
}

/* Stops ctx; with a PE, progresses it until idle (a consumer flushes its
 * receives through its PE). */
static doca_error_t ctx_stop_checked(struct doca_ctx *ctx, struct doca_pe *pe)
{
    enum doca_ctx_states state;
    doca_error_t result = doca_ctx_get_state(ctx, &state);

    if (result != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
        return result;
    if (state != DOCA_CTX_STATE_STOPPING) {
        result = DMESH_DPA_CALL(doca_ctx_stop(ctx));
        if (result != DOCA_SUCCESS && result != DOCA_ERROR_IN_PROGRESS)
            return result;
    }
    for (unsigned spins = 0; spins < 100000; ++spins) {
        result = doca_ctx_get_state(ctx, &state);
        if (result != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
            return result;
        if (pe != NULL)
            (void)doca_pe_progress(pe);
    }
    return DOCA_ERROR_TIME_OUT;
}

/* On the worker's own thread: a consumer on its consumer PE, with a receive
 * posted for every copy the thread can have in flight. */
static doca_error_t consumer_create(struct dmesh_pod_thread *p, struct objects *objs)
{
    struct pod_consumer *c = &p->cons[worker_of(objs)];
    struct doca_ctx *ctx;
    union doca_data data = {.ptr = c};
    doca_error_t result;

    c->pod = p;
    c->objs = objs;
    c->stopping = false;
    result = DMESH_DPA_CALL(doca_comch_msgq_consumer_create(p->msgq, &c->consumer));
    if (result != DOCA_SUCCESS)
        return result;
    ctx = doca_comch_consumer_as_ctx(c->consumer);
    result = doca_comch_consumer_set_imm_data_len(c->consumer, sizeof(struct comch_msg));
    if (result == DOCA_SUCCESS)
        result = doca_ctx_set_user_data(ctx, data);
    if (result == DOCA_SUCCESS)
        result = doca_pe_connect_ctx(objs->consumer_pe, ctx);
    if (result == DOCA_SUCCESS)
        result = doca_comch_consumer_task_post_recv_set_conf(c->consumer, pod_recv_cb, pod_recv_error_cb,
                                                             DMESH_MUX_FIFO);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_ctx_start(ctx));
    for (unsigned i = 0; result == DOCA_SUCCESS && i < DMESH_MUX_FIFO; i++) {
        struct doca_comch_consumer_task_post_recv *task;

        result = doca_comch_consumer_task_post_recv_alloc_init(c->consumer, NULL, &task);
        if (result == DOCA_SUCCESS) {
            result = doca_task_submit(doca_comch_consumer_task_post_recv_as_task(task));
            if (result != DOCA_SUCCESS)
                doca_task_free(doca_comch_consumer_task_post_recv_as_task(task));
        }
    }
    if (result == DOCA_SUCCESS)
        result = doca_comch_consumer_get_id(c->consumer, &c->id);
    if (result != DOCA_SUCCESS)
        DOCA_LOG_ERR("DPA mux thread %d: worker %u consumer: %s", p->idx, worker_of(objs),
                     doca_error_get_name(result));
    return result; /* a partial consumer stays for consumer_destroy */
}

/* On the consumer's worker thread. Registry lock held. */
static doca_error_t consumer_destroy(struct pod_consumer *c)
{
    doca_error_t result;

    if (c->consumer == NULL)
        return DOCA_SUCCESS;
    c->stopping = true;
    result = ctx_stop_checked(doca_comch_consumer_as_ctx(c->consumer), c->objs->consumer_pe);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_consumer_destroy(c->consumer));
    if (result != DOCA_SUCCESS)
        return result;
    c->consumer = NULL;
    return DOCA_SUCCESS;
}

/* Stops and frees a thread whose rings and consumers are gone, from any
 * worker; keeps what could not be freed for the next try (the watch and
 * worker cleanup retry). Registry lock held. */
static doca_error_t pod_retire(struct dmesh_pod_thread *p)
{
    struct dmesh_doca_dpa_thread *th = &p->thread;
    doca_error_t result;

    p->retiring = true;
    for (unsigned w = 0; w < registry.workers; w++)
        if (p->cons[w].consumer != NULL)
            return DOCA_ERROR_IN_USE;
    if (th->running && !th->quiesced) {
        uint64_t deadline = now_ns() + MUX_STOP_NS;

        result = tls_set(p, offsetof(struct dpa_mux_ctx, stop), 1);
        if (result != DOCA_SUCCESS)
            return result;
        while (!__atomic_load_n(&p->status->stopped, __ATOMIC_ACQUIRE) && now_ns() < deadline)
            sched_yield();
        if (!__atomic_load_n(&p->status->stopped, __ATOMIC_ACQUIRE))
            return DOCA_ERROR_TIME_OUT;
        th->quiesced = true;
    }
    if (p->notify != NULL) {
        if (p->notify_started &&
            (result = DMESH_DPA_CALL(doca_dpa_notification_completion_stop(p->notify))) != DOCA_SUCCESS)
            return result;
        p->notify_started = false;
        if ((result = DMESH_DPA_CALL(doca_dpa_notification_completion_destroy(p->notify))) != DOCA_SUCCESS)
            return result;
        p->notify = NULL;
    }
    if (p->producer != NULL) {
        if ((result = ctx_stop_checked(doca_comch_producer_as_ctx(p->producer), NULL)) != DOCA_SUCCESS ||
            (result = DMESH_DPA_CALL(doca_comch_producer_destroy(p->producer))) != DOCA_SUCCESS)
            return result;
        p->producer = NULL;
    }
    if (p->msgq != NULL) {
        if (p->msgq_started && (result = DMESH_DPA_CALL(doca_comch_msgq_stop(p->msgq))) != DOCA_SUCCESS)
            return result;
        p->msgq_started = false;
        if ((result = DMESH_DPA_CALL(doca_comch_msgq_destroy(p->msgq))) != DOCA_SUCCESS)
            return result;
        p->msgq = NULL;
    }
    if (p->producer_comp != NULL) {
        if (p->producer_comp_started &&
            (result = DMESH_DPA_CALL(doca_dpa_completion_stop(p->producer_comp))) != DOCA_SUCCESS)
            return result;
        p->producer_comp_started = false;
        if ((result = DMESH_DPA_CALL(doca_dpa_completion_destroy(p->producer_comp))) != DOCA_SUCCESS)
            return result;
        p->producer_comp = NULL;
    }
    if ((result = dmesh_doca_dpa_thread_destroy_checked(th)) != DOCA_SUCCESS)
        return result;
    if (p->tails_arr != NULL) {
        if ((result = DMESH_DPA_CALL(doca_buf_arr_destroy(p->tails_arr))) != DOCA_SUCCESS)
            return result;
        p->tails_arr = NULL;
    }
    if (p->tails_mmap != NULL) {
        if ((result = doca_mmap_destroy(p->tails_mmap)) != DOCA_SUCCESS)
            return result;
        p->tails_mmap = NULL;
    }
    if (p->status_mmap != NULL) {
        if ((result = destroy_mmap_and_free_buffer(p->status_mmap, p->status)) != DOCA_SUCCESS)
            return result;
        p->status_mmap = NULL;
        p->status = NULL;
    }
    DOCA_LOG_INFO("DPA mux thread %d of session %llx/%llu released", p->idx,
                  (unsigned long long)p->key, (unsigned long long)p->epoch);
    free(p->cons);
    memset(p, 0, sizeof(*p));
    return DOCA_SUCCESS;
}

/* The session's thread, started with its first flow: the tail table mapped
 * for the DPA, the thread, its completions, its producer and this worker's
 * consumer, its state copied in, and a notification for its first run.
 * Registry lock held. */
static doca_error_t pod_start(struct dmesh_pod_thread *p, struct objects *objs,
                              const struct dmesh_tails_export *tails)
{
    struct dmesh_doca_dpa_thread *th = &p->thread;
    struct dpa_mux_ctx *ctx = NULL;
    doca_dpa_dev_completion_t producer_comp;
    doca_dpa_dev_comch_producer_t producer;
    doca_dpa_dev_notification_completion_t notify;
    doca_dpa_dev_buf_arr_t tails_arr;
    doca_dpa_dev_mmap_t status;
    const char *pin = getenv("DMESH_DPA_PIN_EU_BASE");
    uint64_t rpc_ret = 0;
    doca_error_t result;

    th->dpa = objs->dpa_pool->dpa;
    if (pin != NULL && *pin != '\0')
        th->pin_eu = (uint32_t)(atoi(pin) + p->idx + 1);

    /* The tail table: one buf per cache line. */
    result = doca_mmap_create_from_export(NULL, tails->desc, tails->desc_len, objs->dev, &p->tails_mmap);
    if (result == DOCA_SUCCESS)
        result = doca_mmap_start(p->tails_mmap);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_buf_arr_create(DMESH_TAILS_LINES, &p->tails_arr));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_buf_arr_set_target_dpa(p->tails_arr, th->dpa));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_buf_arr_set_params(p->tails_arr, p->tails_mmap,
                                                        DMESH_TAILS_BYTES / DMESH_TAILS_LINES, 0));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_buf_arr_start(p->tails_arr));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_buf_arr_get_dpa_handle(p->tails_arr, &tails_arr));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: tail table: %s", p->idx, doca_error_get_name(result));
        return result;
    }

    /* The ARM's copy of the thread's state, which the DPA writes. */
    result = alloc_buffer_and_set_mmap(&p->status_mmap, objs->dev, (void **)&p->status,
                                       sizeof(struct dpa_mux_status), DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(p->status_mmap, objs->dev, &status));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: status: %s", p->idx, doca_error_get_name(result));
        return result;
    }

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
        DOCA_LOG_ERR("DPA mux thread %d: create failed: %s", p->idx, doca_error_get_name(result));
        return result;
    }
    th->started = true;

    /* Its copies' completions, and the notification that starts it. */
    result = DMESH_DPA_CALL(doca_dpa_completion_create(th->dpa, CC_DPA_MAX_MSG_NUM, &p->producer_comp));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_completion_set_thread(p->producer_comp, th->thread));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_completion_start(p->producer_comp));
    p->producer_comp_started = result == DOCA_SUCCESS;
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_notification_completion_create(th->dpa, th->thread, &p->notify));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_notification_completion_start(p->notify));
    p->notify_started = result == DOCA_SUCCESS;
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_notification_completion_get_dpa_handle(p->notify, &notify));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: completions: %s", p->idx, doca_error_get_name(result));
        return result;
    }

    /* One producer on the DPA and a consumer per worker, this one first. */
    result = DMESH_DPA_CALL(doca_comch_msgq_create(objs->dev, &p->msgq));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_msgq_set_max_num_consumers(p->msgq, registry.workers));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_msgq_set_max_num_producers(p->msgq, 1));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_msgq_set_dpa_producer(p->msgq, th->dpa));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_msgq_start(p->msgq));
    p->msgq_started = result == DOCA_SUCCESS;
    if (result == DOCA_SUCCESS)
        result = consumer_create(p, objs);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_msgq_producer_create(p->msgq, &p->producer));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_ctx_set_datapath_on_dpa(doca_comch_producer_as_ctx(p->producer), th->dpa));
    if (result == DOCA_SUCCESS)
        result = doca_comch_producer_set_dev_max_num_send(p->producer, CC_DPA_MAX_MSG_NUM);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_producer_dpa_completion_attach(p->producer, p->producer_comp));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_ctx_start(doca_comch_producer_as_ctx(p->producer)));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(p->producer_comp, &producer_comp));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_producer_get_dpa_handle(p->producer, &producer));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: msgq: %s", p->idx, doca_error_get_name(result));
        return result;
    }

    ctx = calloc(1, sizeof(*ctx)); /* every ring NONE/IDLE */
    if (ctx == NULL)
        return DOCA_ERROR_NO_MEMORY;
    ctx->producer = producer;
    ctx->producer_comp = producer_comp;
    ctx->tails = tails_arr;
    ctx->status = status;
    ctx->status_addr = (uint64_t)(uintptr_t)p->status;
    result = DMESH_DPA_CALL(doca_dpa_h2d_memcpy(th->dpa, th->local_storage, ctx, sizeof(*ctx)));
    free(ctx);
    if (result == DOCA_SUCCESS) {
        th->running = true;
        result = DMESH_DPA_CALL(doca_dpa_thread_run(th->thread));
    }
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_rpc(th->dpa, mux_kick_rpc, &rpc_ret, notify));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("DPA mux thread %d: start failed: %s", p->idx, doca_error_get_name(result));
        return result;
    }
    if (registry.next_watch_ns == 0) /* the first check once it has run */
        __atomic_store_n(&registry.next_watch_ns, now_ns() + MUX_WATCH_NS, __ATOMIC_RELAXED);
    DOCA_LOG_INFO("DPA mux thread %d serves session %llx/%llu", p->idx,
                  (unsigned long long)p->key, (unsigned long long)p->epoch);
    return DOCA_SUCCESS;
}

/* The session's thread, or a new one. Registry lock held. */
static doca_error_t pod_get(struct dmesh_conn *conn, struct dmesh_pod_thread **out)
{
    struct dmesh_pod_thread *p = NULL;
    uint64_t key, epoch;
    int live = 0;
    doca_error_t result;

    session_of(conn, &key, &epoch);
    for (int i = 0; i < MUX_MAX_THREADS; i++) {
        struct dmesh_pod_thread *t = &registry.t[i];

        if (t->used && !t->retiring && t->key == key && t->epoch == epoch) {
            *out = t;
            return DOCA_SUCCESS;
        }
        live += t->used;
        if (!t->used && p == NULL)
            p = t;
    }
    if (p == NULL || live >= dmesh_dpa_mux_threads()) {
        DOCA_LOG_ERR("DPA mux: %d session threads in use; raise DMESH_DPA_MUX", live);
        return DOCA_ERROR_FULL;
    }
    if (conn->pending_tails == NULL) {
        DOCA_LOG_ERR("DPA mux: session %llx/%llu sent no TAILS", (unsigned long long)key,
                     (unsigned long long)epoch);
        return DOCA_ERROR_NOT_SUPPORTED;
    }
    p->cons = calloc(registry.workers, sizeof(*p->cons));
    if (p->cons == NULL)
        return DOCA_ERROR_NO_MEMORY;
    p->used = true;
    p->idx = (int)(p - registry.t);
    p->key = key;
    p->epoch = epoch;
    result = pod_start(p, conn->objs, conn->pending_tails);
    if (result != DOCA_SUCCESS) {
        (void)consumer_destroy(&p->cons[worker_of(conn->objs)]);
        (void)pod_retire(p); /* what stays is retried by the watch */
        return result;
    }
    *out = p;
    return DOCA_SUCCESS;
}

/* Worker cleanup (objs->release_mux), after every flow has left. */
static doca_error_t mux_release(struct objects *objs)
{
    doca_error_t result = DOCA_SUCCESS;

    pthread_mutex_lock(&registry.lock);
    for (int i = 0; i < MUX_MAX_THREADS && result == DOCA_SUCCESS; i++) {
        struct dmesh_pod_thread *p = &registry.t[i];

        if (!p->used)
            continue;
        if (p->cons[worker_of(objs)].consumer != NULL)
            result = DOCA_ERROR_IN_USE;
        else if (p->retiring)
            result = pod_retire(p);
    }
    pthread_mutex_unlock(&registry.lock);
    if (result != DOCA_SUCCESS)
        return result;
    objs->mux = NULL;
    objs->release_mux = NULL;
    return DOCA_SUCCESS;
}

doca_error_t dmesh_dpa_mux_init(struct objects *objs)
{
    if (dmesh_dpa_mux_threads() == 0 || objs->mux != NULL)
        return DOCA_SUCCESS;
    if (objs->dpa_pool == NULL || objs->dpa_pool->dpa == NULL || objs->consumer_pe == NULL)
        return DOCA_ERROR_BAD_STATE;
    pthread_mutex_lock(&registry.lock);
    if (registry.workers == 0) {
        const char *stall = getenv("DMESH_DPA_MUX_STALL_MS");
        const char *stats = getenv("DMESH_DPA_MUX_STATS");

        registry.workers = dmesh_dispatch_worker_count(objs);
        registry.stall_us = (stall != NULL && atoi(stall) > 0 ? (uint64_t)atoi(stall) : 100) * 1000;
        registry.stats = stats != NULL && atoi(stats) != 0;
    }
    pthread_mutex_unlock(&registry.lock);
    objs->mux = &registry;
    objs->release_mux = mux_release;
    /* The shape start.sh waits for once per worker; the threads are per process. */
    DOCA_LOG_INFO("DPA mux: %d threads on worker %d, one per host session, shared by the process's %u workers",
                  dmesh_dpa_mux_threads(), objs->worker_idx, registry.workers);
    return DOCA_SUCCESS;
}

doca_error_t dmesh_dpa_mux_add(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    struct dpa_mux_ring_cfg g = {0};
    struct dpa_mux_ring_state st = {0}; /* IDLE */
    doca_dpa_dev_buf_arr_t buf_arr;
    doca_dpa_dev_mmap_t host_mmap, dpu_mmap;
    struct dmesh_pod_thread *p;
    struct pod_consumer *c;
    unsigned r = conn->flow_id - 1;
    doca_error_t result;

    if (objs->mux == NULL || conn->mux)
        return DOCA_ERROR_BAD_STATE;
    if (r >= DMESH_MUX_RINGS)
        return DOCA_ERROR_INVALID_VALUE;
    result = DMESH_DPA_CALL(doca_buf_arr_get_dpa_handle(conn->buf_arr, &buf_arr));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->sndbuf.mmap, objs->dev, &host_mmap));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->local_mmap, objs->dev, &dpu_mmap));
    if (result != DOCA_SUCCESS)
        return result;

    pthread_mutex_lock(&registry.lock);
    result = pod_get(conn, &p);
    if (result != DOCA_SUCCESS)
        goto out;
    if (p->ring_conn[r] != NULL) {
        DOCA_LOG_ERR("DPA mux thread %d: ring %u still holds a flow", p->idx, r);
        result = DOCA_ERROR_IN_USE;
        goto out;
    }
    c = &p->cons[worker_of(objs)];
    if (c->consumer == NULL && (result = consumer_create(p, objs)) != DOCA_SUCCESS) {
        (void)consumer_destroy(c);
        if (p->rings_used == 0)
            (void)pod_retire(p);
        goto out;
    }
    if (++registry.next_gen == 0)
        ++registry.next_gen;
    g.buf_arr = buf_arr;
    g.host_mmap = host_mmap;
    g.dpu_mmap = dpu_mmap;
    g.staging_addr = (uint64_t)(uintptr_t)conn->dma_buffer;
    g.buf_size = BUFFER_SIZE;
    g.ring_size = DMA_RING_SIZE;
    g.host_base = (uint64_t)(uintptr_t)conn->sndbuf.buf;
    g.host_len = conn->sndbuf.size;
    g.slot = (uint64_t)(conn - objs->conns);
    g.generation = registry.next_gen;
    g.consumer = c->id;
    g.cmd = DMESH_MUX_CMD_NONE;

    /* A ring the previous flow left is CLOSE/CLOSED, which the DPA ignores.
     * NONE first keeps it ignored while the rest changes underneath; OPEN
     * and the ctl_seq bump last hand the filled ring over. Each h2d_memcpy
     * completes before the next starts. */
    result = tls_set(p, cfg_off(r, offsetof(struct dpa_mux_ring_cfg, cmd)), DMESH_MUX_CMD_NONE);
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_h2d_memcpy(p->thread.dpa, p->thread.local_storage + cfg_off(r, 0),
                                                    &g, sizeof(g)));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_h2d_memcpy(p->thread.dpa, p->thread.local_storage + st_off(r),
                                                    &st, sizeof(st)));
    /* The previous flow's CLOSED record goes before this flow's ring opens;
     * the DPA writes the line again only when this ring stops or closes. */
    memset(&p->status->st[r], 0, sizeof(p->status->st[r]));
    __atomic_thread_fence(__ATOMIC_RELEASE);
    if (result == DOCA_SUCCESS)
        result = tls_set(p, cfg_off(r, offsetof(struct dpa_mux_ring_cfg, cmd)), DMESH_MUX_CMD_OPEN);
    if (result == DOCA_SUCCESS)
        result = ctl_bump(p);
    if (result != DOCA_SUCCESS)
        goto out;

    conn->mux = true;
    conn->mux_close_sent = false;
    conn->mux_pod = p;
    conn->mux_ring = (uint8_t)r;
    conn->mux_gen = (uint32_t)g.generation;
    conn->mux_error = 0;
    conn->mux_received = 0;
    conn->rx_consumed_pos_published = 0;
    p->ring_error[r] = 0;
    __atomic_store_n(&p->ring_conn[r], conn, __ATOMIC_RELEASE);
    p->rings_used++;
    c->rings++;
    DOCA_LOG_INFO("Flow slot %ld (flow %u) on DPA mux thread %d ring %u (generation %u)",
                  conn - objs->conns, conn->flow_id, p->idx, r, conn->mux_gen);
out:
    pthread_mutex_unlock(&registry.lock);
    if (result == DOCA_SUCCESS) {
        free(conn->pending_tails);
        conn->pending_tails = NULL;
    }
    return result;
}

doca_error_t dmesh_dpa_mux_del_step(struct dmesh_conn *conn)
{
    struct dmesh_pod_thread *p = conn->mux_pod;
    struct dpa_mux_ring_state g;
    doca_error_t result;

    if (!conn->mux)
        return DOCA_SUCCESS;
    if (!conn->mux_close_sent) {
        pthread_mutex_lock(&registry.lock);
        result = tls_set(p, cfg_off(conn->mux_ring, offsetof(struct dpa_mux_ring_cfg, cmd)), DMESH_MUX_CMD_CLOSE);
        if (result == DOCA_SUCCESS)
            result = ctl_bump(p);
        pthread_mutex_unlock(&registry.lock);
        if (result != DOCA_SUCCESS)
            return result;
        conn->mux_close_sent = true;
        conn->mux_deadline_ns = now_ns() + DMESH_DPA_QUIESCE_TIMEOUT_MS * 1000000ull;
        return DOCA_ERROR_AGAIN;
    }
    if (dmesh_doca_dpa_failed(p->thread.dpa))
        return DOCA_ERROR_BAD_STATE;
    /* CLOSED: no copy of the ring is left in flight. Every copy that
     * succeeded sent one message; the ring is done once they all arrived
     * (the consumer PE may still hold some). The DPA publishes the line
     * when the ring closes. */
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    memcpy(&g, &p->status->st[conn->mux_ring], sizeof(g));
    if (g.state == DMESH_MUX_CLOSED && conn->mux_received + g.failed == g.submit) {
        struct pod_consumer *c = &p->cons[worker_of(conn->objs)];

        /* The worker's last ring of the session takes its consumer along. */
        result = DOCA_SUCCESS;
        if (c->rings == 1) {
            pthread_mutex_lock(&registry.lock);
            result = consumer_destroy(c);
            pthread_mutex_unlock(&registry.lock);
        }
        return result;
    }
    if (now_ns() > conn->mux_deadline_ns) {
        DOCA_LOG_ERR("DPA mux thread %d: ring %u (slot %ld) not closed within %d ms (state %llu, "
                     "%llu copies, %llu messages, %llu failed); the ring is kept",
                     p->idx, conn->mux_ring, conn - conn->objs->conns, DMESH_DPA_QUIESCE_TIMEOUT_MS,
                     (unsigned long long)g.state, (unsigned long long)g.submit,
                     (unsigned long long)conn->mux_received, (unsigned long long)g.failed);
        return DOCA_ERROR_TIME_OUT;
    }
    return DOCA_ERROR_AGAIN;
}

void dmesh_dpa_mux_release(struct dmesh_conn *conn)
{
    struct dmesh_pod_thread *p = conn->mux_pod;

    if (!conn->mux)
        return;
    pthread_mutex_lock(&registry.lock);
    if (p->ring_conn[conn->mux_ring] == conn) {
        __atomic_store_n(&p->ring_conn[conn->mux_ring], NULL, __ATOMIC_RELEASE);
        p->rings_used--;
        p->cons[worker_of(conn->objs)].rings--;
        if (p->rings_used == 0 && pod_retire(p) != DOCA_SUCCESS)
            DOCA_LOG_WARN("DPA mux thread %d: release is retried by the watch", p->idx);
    }
    pthread_mutex_unlock(&registry.lock);
    conn->mux = conn->mux_close_sent = false;
    conn->mux_pod = NULL;
    conn->mux_error = 0;
}

doca_error_t dmesh_dpa_mux_rx_consumed(struct dmesh_conn *conn, uint32_t pos)
{
    /* After CLOSE the ring no longer takes releases. */
    if (!conn->mux || conn->mux_close_sent)
        return DOCA_SUCCESS;
    return tls_set(conn->mux_pod, cfg_off(conn->mux_ring, offsetof(struct dpa_mux_ring_cfg, rx_consumed_pos)),
                   pos);
}

uint32_t dmesh_dpa_mux_error(struct dmesh_conn *conn)
{
    if (conn->mux_error == 0 && conn->mux && !conn->mux_close_sent)
        conn->mux_error = __atomic_load_n(&conn->mux_pod->ring_error[conn->mux_ring], __ATOMIC_ACQUIRE);
    return conn->mux_error;
}

/* One check: the header and the rings' DPA lines in one copy. Registry lock held. */
static void pod_check(struct dmesh_pod_thread *p, uint64_t now)
{
    struct dpa_mux_status snap;
    const struct dpa_mux_status *h = &snap;
    uint64_t cur[MUX_COUNTERS], d[MUX_COUNTERS], stale = 0, bad = 0, errors = 0;
    double secs;
    char rings[DMESH_MUX_RINGS * 4 + 1] = "";
    size_t len = 0;
    bool stalled, stopped;

    (void)dmesh_doca_dpa_failed(p->thread.dpa); /* logs a dead context itself */
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    memcpy(&snap, p->status, sizeof(snap));

    /* A ring the DPA stopped: its worker closes the flow with an error. */
    for (unsigned r = 0; r < DMESH_MUX_RINGS; r++) {
        struct dmesh_conn *conn = p->ring_conn[r];

        if (conn == NULL)
            continue;
        if (p->ring_error[r] == 0 && h->st[r].state == DMESH_MUX_ERROR) {
            uint32_t e = h->st[r].error != 0 ? (uint32_t)h->st[r].error : DMESH_MUX_ERR_COPY;

            __atomic_store_n(&p->ring_error[r], e, __ATOMIC_RELEASE);
            DOCA_LOG_ERR("DPA mux thread %d ring %u (worker %d slot %ld): forward ring stopped, error %u",
                         p->idx, r, conn->objs->worker_idx, conn - conn->objs->conns, e);
        }
        if (len + 4 < sizeof(rings))
            len += (size_t)snprintf(rings + len, sizeof(rings) - len, "%s%u", len ? "," : "", r);
    }

    cur[0] = h->heartbeat;
    cur[1] = h->tail_reads;
    cur[2] = h->visits;
    cur[3] = h->copies;
    cur[4] = h->credit_waits;
    cur[5] = h->fifo_full;
    for (int i = 0; i < MUX_COUNTERS; i++)
        d[i] = cur[i] - p->last[i];
    secs = p->last_ns != 0 && now > p->last_ns ? (double)(now - p->last_ns) / 1e9 : 0;
    stopped = p->last_ns != 0 && d[0] == 0;
    stalled = h->oldest_age_us > registry.stall_us;
    if (registry.stats && secs > 0) {
        for (unsigned w = 0; w < registry.workers; w++) {
            stale += p->cons[w].stale_msgs;
            bad += p->cons[w].bad_msgs;
            errors += p->cons[w].recv_errors;
        }
        /* reads/pass: host lines read for tails per pass (one per sixteen
         * rings); visits/pass: rings that had new descriptors. */
        DOCA_LOG_INFO("DPA mux thread %d: session %llx/%llu rings=%u [%s] passes/s=%.0f tail_reads/s=%.0f "
                      "reads/pass=%.3f visits/s=%.0f visits/pass=%.4f copies/s=%.0f credit_waits/s=%.0f "
                      "fifo_full/s=%.0f in_flight=%llu oldest_us=%llu err_cqes=%llu bad_cqes=%llu "
                      "stale_msgs=%llu bad_msgs=%llu recv_errors=%llu",
                      p->idx, (unsigned long long)p->key, (unsigned long long)p->epoch, p->rings_used, rings,
                      (double)d[0] / secs, (double)d[1] / secs, d[0] ? (double)d[1] / (double)d[0] : 0,
                      (double)d[2] / secs, d[0] ? (double)d[2] / (double)d[0] : 0, (double)d[3] / secs,
                      (double)d[4] / secs, (double)d[5] / secs,
                      (unsigned long long)(h->fifo_tail - h->fifo_head), (unsigned long long)h->oldest_age_us,
                      (unsigned long long)h->err_cqes, (unsigned long long)h->bad_cqes,
                      (unsigned long long)stale, (unsigned long long)bad, (unsigned long long)errors);
    }
    memcpy(p->last, cur, sizeof(cur));
    p->last_ns = now;
    if ((stalled || stopped) && !p->stalled) {
        DOCA_LOG_ERR("DPA mux thread %d %s: oldest copy in flight %llu ms; rings of session %llx/%llu: %s. "
                     "A copy from a host process that died blocks the thread for good (dpa_mux.h)",
                     p->idx, stopped ? "is not running" : "is stalled",
                     (unsigned long long)(h->oldest_age_us / 1000), (unsigned long long)p->key,
                     (unsigned long long)p->epoch, len ? rings : "none");
        p->stalled = true;
    } else if (!stalled && !stopped && p->stalled) {
        DOCA_LOG_WARN("DPA mux thread %d is moving again", p->idx);
        p->stalled = false;
    }
}

/* Called by every worker's advance: one check per call when one is due, by
 * whichever worker comes first; each thread is checked about once per
 * MUX_WATCH_NS. A check reads the thread's published status from ARM
 * memory. Threads whose release failed are retried here. */
void dmesh_dpa_mux_watch(struct objects *objs)
{
    uint64_t now, due = __atomic_load_n(&registry.next_watch_ns, __ATOMIC_RELAXED);
    int live = 0, pick = -1;

    if (objs->mux == NULL || due == 0)
        return;
    now = now_ns();
    if (now < due || pthread_mutex_trylock(&registry.lock) != 0)
        return;
    if (now < registry.next_watch_ns) {
        pthread_mutex_unlock(&registry.lock);
        return;
    }
    for (int i = 0; i < MUX_MAX_THREADS; i++) {
        int k = (registry.watch_next + i) % MUX_MAX_THREADS;
        struct dmesh_pod_thread *p = &registry.t[k];

        if (!p->used)
            continue;
        if (p->retiring) {
            (void)pod_retire(p);
            continue;
        }
        live++;
        if (pick < 0)
            pick = k;
    }
    __atomic_store_n(&registry.next_watch_ns, now + MUX_WATCH_NS / (uint64_t)(live > 0 ? live : 1),
                     __ATOMIC_RELAXED);
    if (pick >= 0) {
        registry.watch_next = (pick + 1) % MUX_MAX_THREADS;
        pod_check(&registry.t[pick], now);
    }
    pthread_mutex_unlock(&registry.lock);
}

#else /* !DOCA_ARCH_DPU */

int dmesh_dpa_mux_threads(void) { return 0; }
doca_error_t dmesh_dpa_mux_init(struct objects *objs) { (void)objs; return DOCA_SUCCESS; }
doca_error_t dmesh_dpa_mux_add(struct dmesh_conn *conn) { (void)conn; return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_dpa_mux_del_step(struct dmesh_conn *conn) { (void)conn; return DOCA_SUCCESS; }
void dmesh_dpa_mux_release(struct dmesh_conn *conn) { (void)conn; }
doca_error_t dmesh_dpa_mux_rx_consumed(struct dmesh_conn *conn, uint32_t pos) { (void)conn; (void)pos; return DOCA_SUCCESS; }
uint32_t dmesh_dpa_mux_error(struct dmesh_conn *conn) { (void)conn; return 0; }
void dmesh_dpa_mux_watch(struct objects *objs) { (void)objs; }

#endif
