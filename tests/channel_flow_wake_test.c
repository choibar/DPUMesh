/* Flow wake setup and teardown against a fake Comch connection whose internal
 * message tasks are few, as on the BlueField: a consumer's start and stop each
 * hold one until the control PE progresses, and the consumer changes state on
 * its own PE only after that. */
#include <assert.h>
#include <stdio.h>
#include "src/transport/host/channel.c"

#define INTERNAL_TASKS 16
#define MAX_CONSUMERS 64

struct fake_consumer {
    struct doca_pe *pe;
    enum doca_ctx_states state;
    int in_flight;                      /* holds an internal task */
    int msg_done;                       /* its message completed on the control PE */
    int destroyed;
    uint32_t id;
    union doca_data user_data;
    doca_ctx_state_changed_callback_t state_cb;
    doca_comch_consumer_task_post_recv_completion_cb_t recv_failed;
    struct doca_comch_consumer_task_post_recv *posted[FLOW_WAKE_RECV_N];
    unsigned nposted;
};
struct fake_task { struct fake_consumer *c; struct doca_buf *buf; };

static struct dmesh_comch_client control;
static struct fake_consumer consumers[MAX_CONSUMERS];
static unsigned nconsumers, live_consumers, live_pes, live_mmaps, live_invs;
static int internal_tasks = INTERNAL_TASKS, peak_in_flight, faults;
static unsigned fail_start_at;          /* 1-based consumer whose start fails, 0: none */
static int buf_refs;
static char fake_buf;

static int in_flight(void)
{
    int n = 0;
    for (unsigned i = 0; i < nconsumers; ++i) n += consumers[i].in_flight;
    return n;
}
static void take_task(struct fake_consumer *c)
{
    --internal_tasks;
    c->in_flight = 1;
    if (in_flight() > peak_in_flight) peak_in_flight = in_flight();
}
static void set_state(struct fake_consumer *c, enum doca_ctx_states next)
{
    enum doca_ctx_states prev = c->state;
    c->state = next;
    if (c->state_cb) c->state_cb(c->user_data, (struct doca_ctx *)c, prev, next);
}

/* The control PE completes every message in flight; a consumer's own PE then
 * moves it on, and a stopped consumer flushes its receives. */
uint8_t doca_pe_progress(struct doca_pe *pe)
{
    uint8_t work = 0;
    if (pe == control.pe) {
        for (unsigned i = 0; i < nconsumers; ++i) {
            struct fake_consumer *c = &consumers[i];
            if (!c->in_flight) continue;
            if (c->destroyed) ++faults;     /* a completion for a freed consumer */
            c->in_flight = 0;
            c->msg_done = 1;
            ++internal_tasks;
            work = 1;
        }
        return work;
    }
    for (unsigned i = 0; i < nconsumers; ++i) {
        struct fake_consumer *c = &consumers[i];
        if (c->pe != pe || c->destroyed || !c->msg_done) continue;
        c->msg_done = 0;
        work = 1;
        if (c->state == DOCA_CTX_STATE_STARTING) {
            set_state(c, DOCA_CTX_STATE_RUNNING);
        } else if (c->state == DOCA_CTX_STATE_STOPPING) {
            while (c->nposted)
                c->recv_failed(c->posted[--c->nposted], (union doca_data){0}, c->user_data);
            set_state(c, DOCA_CTX_STATE_IDLE);
        }
    }
    return work;
}

doca_error_t doca_pe_create(struct doca_pe **pe) { *pe = calloc(1, 1); ++live_pes; return DOCA_SUCCESS; }
doca_error_t doca_pe_set_event_mode(struct doca_pe *pe, enum doca_pe_event_mode mode) { (void)pe; (void)mode; return DOCA_SUCCESS; }
doca_error_t doca_pe_destroy(struct doca_pe *pe)
{
    for (unsigned i = 0; i < nconsumers; ++i)
        if (consumers[i].pe == pe && !consumers[i].destroyed) ++faults;
    free(pe); --live_pes; return DOCA_SUCCESS;
}
doca_error_t doca_mmap_create(struct doca_mmap **m) { *m = calloc(1, 1); ++live_mmaps; return DOCA_SUCCESS; }
doca_error_t doca_mmap_add_dev(struct doca_mmap *m, struct doca_dev *d) { (void)m; (void)d; return DOCA_SUCCESS; }
doca_error_t doca_mmap_set_permissions(struct doca_mmap *m, uint32_t a) { (void)m; (void)a; return DOCA_SUCCESS; }
doca_error_t doca_mmap_set_memrange(struct doca_mmap *m, void *a, size_t n) { (void)m; (void)a; (void)n; return DOCA_SUCCESS; }
doca_error_t doca_mmap_start(struct doca_mmap *m) { (void)m; return DOCA_SUCCESS; }
doca_error_t doca_mmap_destroy(struct doca_mmap *m) { free(m); --live_mmaps; return DOCA_SUCCESS; }
doca_error_t doca_buf_inventory_create(size_t n, struct doca_buf_inventory **inv) { (void)n; *inv = calloc(1, 1); ++live_invs; return DOCA_SUCCESS; }
doca_error_t doca_buf_inventory_start(struct doca_buf_inventory *inv) { (void)inv; return DOCA_SUCCESS; }
doca_error_t doca_buf_inventory_destroy(struct doca_buf_inventory *inv) { free(inv); --live_invs; return DOCA_SUCCESS; }
doca_error_t doca_buf_inventory_buf_get_by_args(struct doca_buf_inventory *inv, struct doca_mmap *m, void *a,
                                                size_t n, void *d, size_t dn, struct doca_buf **buf)
{ (void)inv; (void)m; (void)a; (void)n; (void)d; (void)dn; ++buf_refs; *buf = (struct doca_buf *)&fake_buf; return DOCA_SUCCESS; }
doca_error_t doca_buf_dec_refcount(struct doca_buf *buf, uint16_t *refcount) { (void)buf; (void)refcount; --buf_refs; return DOCA_SUCCESS; }

doca_error_t doca_comch_consumer_create(struct doca_comch_connection *conn, struct doca_mmap *m,
                                        struct doca_comch_consumer **out)
{
    (void)m;
    assert(conn == control.connection && nconsumers < MAX_CONSUMERS);
    struct fake_consumer *c = &consumers[nconsumers++];
    memset(c, 0, sizeof(*c));
    c->id = nconsumers + 1;              /* 1 is the session's */
    ++live_consumers;
    *out = (struct doca_comch_consumer *)c;
    return DOCA_SUCCESS;
}
struct doca_ctx *doca_comch_consumer_as_ctx(struct doca_comch_consumer *c) { return (struct doca_ctx *)c; }
doca_error_t doca_pe_connect_ctx(struct doca_pe *pe, struct doca_ctx *ctx) { ((struct fake_consumer *)ctx)->pe = pe; return DOCA_SUCCESS; }
doca_error_t doca_comch_consumer_set_imm_data_len(struct doca_comch_consumer *c, uint32_t n) { (void)c; (void)n; return DOCA_SUCCESS; }
doca_error_t doca_comch_consumer_task_post_recv_set_conf(struct doca_comch_consumer *c,
    doca_comch_consumer_task_post_recv_completion_cb_t done, doca_comch_consumer_task_post_recv_completion_cb_t failed,
    uint32_t n)
{ (void)done; (void)n; ((struct fake_consumer *)c)->recv_failed = failed; return DOCA_SUCCESS; }
doca_error_t doca_ctx_set_user_data(struct doca_ctx *ctx, union doca_data d) { ((struct fake_consumer *)ctx)->user_data = d; return DOCA_SUCCESS; }
doca_error_t doca_ctx_set_state_changed_cb(struct doca_ctx *ctx, doca_ctx_state_changed_callback_t cb)
{ ((struct fake_consumer *)ctx)->state_cb = cb; return DOCA_SUCCESS; }

/* Start and stop each send the DPU a message: no internal task, no start. */
doca_error_t doca_ctx_start(struct doca_ctx *ctx)
{
    struct fake_consumer *c = (struct fake_consumer *)ctx;
    if (fail_start_at && (unsigned)(c - consumers) + 1 == fail_start_at) return DOCA_ERROR_NO_MEMORY;
    if (internal_tasks == 0) return DOCA_ERROR_NO_MEMORY;
    take_task(c);
    set_state(c, DOCA_CTX_STATE_STARTING);
    return DOCA_ERROR_IN_PROGRESS;
}
doca_error_t doca_ctx_stop(struct doca_ctx *ctx)
{
    struct fake_consumer *c = (struct fake_consumer *)ctx;
    if (c->state != DOCA_CTX_STATE_RUNNING && c->state != DOCA_CTX_STATE_STARTING) return DOCA_ERROR_BAD_STATE;
    if (internal_tasks == 0) return DOCA_ERROR_NO_MEMORY;
    take_task(c);
    set_state(c, DOCA_CTX_STATE_STOPPING);
    return DOCA_ERROR_IN_PROGRESS;
}
doca_error_t doca_ctx_get_state(const struct doca_ctx *ctx, enum doca_ctx_states *state)
{ *state = ((const struct fake_consumer *)ctx)->state; return DOCA_SUCCESS; }
doca_error_t doca_comch_consumer_destroy(struct doca_comch_consumer *cc)
{
    struct fake_consumer *c = (struct fake_consumer *)cc;
    if (c->state != DOCA_CTX_STATE_IDLE) return DOCA_ERROR_BAD_STATE;
    if (c->in_flight) ++faults;
    c->destroyed = 1;
    --live_consumers;
    return DOCA_SUCCESS;
}
doca_error_t doca_comch_consumer_get_id(const struct doca_comch_consumer *c, uint32_t *id)
{ *id = ((const struct fake_consumer *)c)->id; return DOCA_SUCCESS; }

doca_error_t doca_comch_consumer_task_post_recv_alloc_init(struct doca_comch_consumer *c, struct doca_buf *buf,
    struct doca_comch_consumer_task_post_recv **task)
{
    struct fake_task *t = malloc(sizeof(*t));
    t->c = (struct fake_consumer *)c; t->buf = buf;
    *task = (struct doca_comch_consumer_task_post_recv *)t;
    return DOCA_SUCCESS;
}
struct doca_task *doca_comch_consumer_task_post_recv_as_task(struct doca_comch_consumer_task_post_recv *task)
{ return (struct doca_task *)task; }
struct doca_buf *doca_comch_consumer_task_post_recv_get_buf(const struct doca_comch_consumer_task_post_recv *task)
{ return ((const struct fake_task *)task)->buf; }
doca_error_t doca_task_submit(struct doca_task *task)
{
    struct fake_consumer *c = ((struct fake_task *)task)->c;
    assert(c->state == DOCA_CTX_STATE_RUNNING && c->nposted < FLOW_WAKE_RECV_N);
    c->posted[c->nposted++] = (struct doca_comch_consumer_task_post_recv *)task;
    return DOCA_SUCCESS;
}
void doca_task_free(struct doca_task *task) { free(task); }

/* channel.c's other dependencies; none runs here. */
void thread_init_rpc(void) { assert(!"unexpected DPA invocation"); }
doca_error_t dmesh_comch_client_open(const char *s, struct dmesh_comch_client *o) { (void)s; (void)o; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_comch_client_send(struct dmesh_comch_client *o, const char *d, size_t n) { (void)o; (void)d; (void)n; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t alloc_dma_ring(struct dma_ring **r, struct doca_dev *d, size_t n) { (void)r; (void)d; (void)n; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t destroy_mmap_and_free_buffer(struct doca_mmap *m, void *b) { (void)m; (void)b; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_build_dma_metadata(struct doca_dev *d, const struct dma_ring *r, const struct dmesh_buffer *s,
    const struct dmesh_buffer *rb, const struct dmesh_flow_id *f, struct dmesh_export_metadata_msg *m)
{ (void)d; (void)r; (void)s; (void)rb; (void)f; (void)m; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_validate_reverse_metadata(const struct dmesh_export_rcv_ring_msg *m) { (void)m; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_dpa_quiesce_checked(struct dmesh_doca_dpa_thread *t, struct dmesh_doca_dpa_comch *c, struct doca_pe *p)
{ (void)t; (void)c; (void)p; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_dpa_comch_destroy_checked(struct dmesh_doca_dpa_thread *t, struct dmesh_doca_dpa_comch **c, struct doca_pe *p)
{ (void)t; (void)c; (void)p; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_doca_dpa_thread_destroy_checked(struct dmesh_doca_dpa_thread *t) { (void)t; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_doca_dpa_thread_create(struct dmesh_doca_dpa_thread *t) { (void)t; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_dpa_endpoint_init_comch(struct dmesh_dpa_endpoint *c, struct doca_dev *d) { (void)c; (void)d; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }
doca_error_t dmesh_doca_dpa_msgq_send(struct dmesh_doca_dpa_msgq *q, void *m, uint32_t n) { (void)q; (void)m; (void)n; assert(0); return DOCA_ERROR_NOT_SUPPORTED; }

static void session(struct channel_dev *dev)
{
    memset(dev, 0, sizeof(*dev));
    pthread_mutex_init(&dev->session_lock, NULL);
    control.pe = (struct doca_pe *)&control;
    control.connection = (struct doca_comch_connection *)&control;
    dev->control = &control;
    dev->fastpath = 1;
    nconsumers = 0; peak_in_flight = 0; faults = 0;
}

static int open_wakes(struct channel_dev *dev)
{
    pthread_mutex_lock(&dev->session_lock);
    int rc = flow_wakes_open_locked(dev);
    pthread_mutex_unlock(&dev->session_lock);
    return rc;
}

static void close_wakes(struct channel_dev *dev)
{
    pthread_mutex_lock(&dev->session_lock);
    flow_wakes_close_locked(dev);
    pthread_mutex_unlock(&dev->session_lock);
}

/* Nothing outlives teardown, and no message completes for a freed consumer. */
static void assert_released(struct channel_dev *dev)
{
    for (uint32_t id = 1; id <= DMESH_SESSION_MAX_FLOWS; ++id) assert(dev->flow_wake[id] == NULL);
    assert(!dev->flow_wake_on && faults == 0);
    assert(live_consumers == 0 && live_pes == 0 && live_mmaps == 0 && live_invs == 0 && buf_refs == 0);
    assert(internal_tasks == INTERNAL_TASKS);
}

int main(void)
{
    struct channel_dev dev;

    /* Every slot gets a running consumer with its receives and its own id,
     * although the connection holds fewer internal tasks than slots. */
    session(&dev);
    assert(open_wakes(&dev) == 0);
    assert(dev.flow_wake_on && peak_in_flight == 1);
    for (uint32_t id = 1; id <= DMESH_SESSION_MAX_FLOWS; ++id) {
        struct fake_consumer *c = (struct fake_consumer *)dev.flow_wake[id]->consumer;
        assert(c->state == DOCA_CTX_STATE_RUNNING && c->nposted == FLOW_WAKE_RECV_N);
        assert(dev.flow_wake[id]->id == id + 1 && channel_dev_flow_wake(&dev));
    }
    /* Session close stops each one: the stop completes on the control PE. */
    close_wakes(&dev);
    assert(nconsumers == DMESH_SESSION_MAX_FLOWS);
    assert_released(&dev);

    /* A failed start fails the session, with every started consumer
     * stopped before it is freed. */
    session(&dev);
    fail_start_at = 5;
    assert(open_wakes(&dev) == -1);
    fail_start_at = 0;
    assert(nconsumers == 5 && !channel_dev_flow_wake(&dev));
    assert_released(&dev);

    /* No flow wake without the fast path. */
    session(&dev);
    dev.fastpath = 0;
    assert(open_wakes(&dev) == 0);
    assert(nconsumers == 0 && !channel_dev_flow_wake(&dev));
    puts("channel flow wake: consumers start one at a time, stop through the control PE, all or the session fails: PASS");
    return 0;
}
