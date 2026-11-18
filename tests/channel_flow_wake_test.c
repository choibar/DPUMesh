/* Flow wake against a fake SDK: every flow slot's PE and sync event over the
 * slot's word of the tail table page, started with the session, all or none;
 * a wait task armed on the word's current value and completed once the word
 * changes; and the stop, which reads the PE's events and destroys the sync
 * event before its PE. */
#include <assert.h>
#include <stdio.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include "src/transport/host/channel.c"

#define SLOTS_N DMESH_SESSION_MAX_FLOWS

struct fake_se;
struct fake_pe { int fd, requests; struct fake_se *se; };
struct fake_wait { struct fake_se *se; uint64_t value; union doca_data data; int submitted; };
struct fake_se {
    struct fake_pe *pe;
    enum doca_ctx_states state;
    uint64_t *addr;
    int destroyed, stuck, remote_pub, cpu_sub;
    doca_sync_event_task_wait_neq_completion_cb_t done, failed;
    struct fake_wait *pending;
};

static struct fake_se events[2 * SLOTS_N];
static unsigned nevents, live_pes, live_events, waits_alloc, waits_freed;
static unsigned fail_start_at;          /* 1-based sync event whose start fails, 0: none */
static int faults;

doca_error_t doca_pe_create(struct doca_pe **pe)
{
    struct fake_pe *p = calloc(1, sizeof(*p));
    p->fd = eventfd(0, EFD_NONBLOCK);
    ++live_pes;
    *pe = (struct doca_pe *)p;
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_set_event_mode(struct doca_pe *pe, enum doca_pe_event_mode mode)
{
    (void)pe;
    assert(mode == DOCA_PE_EVENT_MODE_PROGRESS_ALL);
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_destroy(struct doca_pe *pe)
{
    struct fake_pe *p = (struct fake_pe *)pe;
    if (p->se != NULL && !p->se->destroyed) ++faults;   /* its sync event goes first */
    close(p->fd);
    free(p);
    --live_pes;
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_get_notification_handle(const struct doca_pe *pe, doca_notification_handle_t *handle)
{
    *handle = ((const struct fake_pe *)pe)->fd;
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_request_notification(struct doca_pe *pe)
{
    ++((struct fake_pe *)pe)->requests;
    return DOCA_SUCCESS;
}
doca_error_t doca_pe_clear_notification(struct doca_pe *pe, doca_notification_handle_t handle)
{
    (void)pe; (void)handle;
    return DOCA_SUCCESS;
}

/* The sync event starts, completes a wait whose word changed, and on a stop
 * flushes its wait and goes idle (unless stuck), all on its own PE. */
uint8_t doca_pe_progress(struct doca_pe *pe)
{
    struct fake_se *se = ((struct fake_pe *)pe)->se;
    if (se == NULL || se->destroyed)
        return 0;
    if (se->state == DOCA_CTX_STATE_STARTING) {
        se->state = DOCA_CTX_STATE_RUNNING;
        return 1;
    }
    struct fake_wait *w = se->pending;
    if (se->state == DOCA_CTX_STATE_STOPPING) {
        if (w != NULL) {
            se->pending = NULL;
            se->failed((struct doca_sync_event_task_wait_neq *)w, w->data, (union doca_data){0});
            return 1;
        }
        if (!se->stuck)
            se->state = DOCA_CTX_STATE_IDLE;
        return 0;
    }
    if (se->state == DOCA_CTX_STATE_RUNNING && w != NULL && *se->addr != w->value) {
        se->pending = NULL;
        se->done((struct doca_sync_event_task_wait_neq *)w, w->data, (union doca_data){0});
        return 1;
    }
    return 0;
}

doca_error_t doca_sync_event_create(struct doca_sync_event **event)
{
    assert(nevents < 2 * SLOTS_N);
    struct fake_se *se = &events[nevents++];
    memset(se, 0, sizeof(*se));
    se->state = DOCA_CTX_STATE_IDLE;
    ++live_events;
    *event = (struct doca_sync_event *)se;
    return DOCA_SUCCESS;
}
doca_error_t doca_sync_event_add_publisher_location_remote_pci(struct doca_sync_event *event)
{
    ((struct fake_se *)event)->remote_pub = 1;
    return DOCA_SUCCESS;
}
doca_error_t doca_sync_event_add_subscriber_location_cpu(struct doca_sync_event *event, struct doca_dev *dev)
{
    (void)dev;
    ((struct fake_se *)event)->cpu_sub = 1;
    return DOCA_SUCCESS;
}
doca_error_t doca_sync_event_set_addr(struct doca_sync_event *event, uint64_t *addr)
{
    ((struct fake_se *)event)->addr = addr;
    return DOCA_SUCCESS;
}
struct doca_ctx *doca_sync_event_as_ctx(struct doca_sync_event *event) { return (struct doca_ctx *)event; }
doca_error_t doca_pe_connect_ctx(struct doca_pe *pe, struct doca_ctx *ctx)
{
    struct fake_se *se = (struct fake_se *)ctx;
    se->pe = (struct fake_pe *)pe;
    se->pe->se = se;
    return DOCA_SUCCESS;
}
doca_error_t doca_sync_event_task_wait_neq_set_conf(struct doca_sync_event *event,
                                                    doca_sync_event_task_wait_neq_completion_cb_t completion_cb,
                                                    doca_sync_event_task_wait_neq_completion_cb_t error_cb,
                                                    uint32_t num_tasks)
{
    struct fake_se *se = (struct fake_se *)event;
    assert(num_tasks >= 1);
    se->done = completion_cb;
    se->failed = error_cb;
    return DOCA_SUCCESS;
}
doca_error_t doca_ctx_start(struct doca_ctx *ctx)
{
    struct fake_se *se = (struct fake_se *)ctx;
    if (fail_start_at && (unsigned)(se - events) + 1 == fail_start_at)
        return DOCA_ERROR_NO_MEMORY;
    assert(se->state == DOCA_CTX_STATE_IDLE && se->pe && se->addr && se->remote_pub && se->cpu_sub);
    se->state = DOCA_CTX_STATE_STARTING;
    return DOCA_ERROR_IN_PROGRESS;
}
doca_error_t doca_ctx_stop(struct doca_ctx *ctx)
{
    struct fake_se *se = (struct fake_se *)ctx;
    if (se->state != DOCA_CTX_STATE_IDLE)
        se->state = DOCA_CTX_STATE_STOPPING;
    return DOCA_ERROR_IN_PROGRESS;
}
doca_error_t doca_ctx_get_state(const struct doca_ctx *ctx, enum doca_ctx_states *state)
{
    *state = ((const struct fake_se *)ctx)->state;
    return DOCA_SUCCESS;
}
doca_error_t doca_sync_event_destroy(struct doca_sync_event *event)
{
    struct fake_se *se = (struct fake_se *)event;
    if (se->state != DOCA_CTX_STATE_IDLE || se->pending != NULL) ++faults;
    se->destroyed = 1;
    --live_events;
    return DOCA_SUCCESS;
}
doca_error_t doca_sync_event_task_wait_neq_alloc_init(struct doca_sync_event *event, uint64_t wait_val, uint64_t mask,
                                                      union doca_data user_data,
                                                      struct doca_sync_event_task_wait_neq **task)
{
    struct fake_wait *w = calloc(1, sizeof(*w));
    assert(mask == ~0ull && ((struct fake_se *)event)->state == DOCA_CTX_STATE_RUNNING);
    w->se = (struct fake_se *)event;
    w->value = wait_val;
    w->data = user_data;
    ++waits_alloc;
    *task = (struct doca_sync_event_task_wait_neq *)w;
    return DOCA_SUCCESS;
}
struct doca_task *doca_sync_event_task_wait_neq_as_doca_task(struct doca_sync_event_task_wait_neq *task)
{
    return (struct doca_task *)task;
}
doca_error_t doca_task_submit(struct doca_task *task)
{
    struct fake_wait *w = (struct fake_wait *)task;
    assert(!w->submitted && w->se->pending == NULL);
    w->submitted = 1;
    w->se->pending = w;
    return DOCA_SUCCESS;
}
void doca_task_free(struct doca_task *task)
{
    free(task);
    ++waits_freed;
}

static uint64_t tails_page[DMESH_TAILS_ALLOC / 8];

static void open_dev(struct channel_dev *dev)
{
    memset(dev, 0, sizeof(*dev));
    dev->tails = (volatile uint32_t *)tails_page;
    memset(tails_page, 0xff, sizeof(tails_page));
}

int main(void)
{
    struct channel_dev dev;

    /* Every slot's wake runs over its own word of the tail table page. */
    open_dev(&dev);
    assert(flow_wakes_open_locked(&dev) == 0);
    assert(live_pes == SLOTS_N && live_events == SLOTS_N);
    for (uint32_t id = 1; id <= SLOTS_N; ++id) {
        struct channel_flow_wake *fw = dev.flow_wake[id];
        struct fake_se *se = (struct fake_se *)fw->se;
        assert(fw->slot == id && se->state == DOCA_CTX_STATE_RUNNING);
        assert((uint8_t *)se->addr == (uint8_t *)tails_page + DMESH_EC_SE_OFF(id) && *se->addr == 0);
        assert(channel_flow_fd(&dev, id) == ((struct fake_pe *)fw->pe)->fd);
    }
    assert(channel_flow_fd(&dev, 0) == -1 && channel_flow_fd(&dev, SLOTS_N + 1) == -1);

    /* One wait task at a time, on the word's value when armed; every arm
     * requests the PE's notification. */
    struct channel_flow_wake *fw = dev.flow_wake[3];
    struct fake_se *se = (struct fake_se *)fw->se;
    struct fake_pe *pe = (struct fake_pe *)fw->pe;
    assert(channel_flow_arm(&dev, 3) == 0 && fw->se_pending && se->pending->value == 0);
    assert(channel_flow_arm(&dev, 3) == 0 && waits_alloc == 1 && pe->requests == 2);
    channel_flow_progress(&dev, 3);
    assert(fw->se_pending && fw->se_wakes == 0);       /* the word did not change */
    *se->addr = (UINT64_C(2) << 48) | 1;                /* the DPA published a batch */
    channel_flow_progress(&dev, 3);
    assert(!fw->se_pending && fw->se_wakes == 1 && waits_freed == 1);
    assert(channel_flow_arm(&dev, 3) == 0 && se->pending->value == ((UINT64_C(2) << 48) | 1));
    assert(fw->se_waits == 2 && pe->requests == 3);

    /* The stop flushes the pending wait and destroys each sync event before
     * its PE. */
    flow_wakes_close_locked(&dev);
    assert(live_pes == 0 && live_events == 0 && waits_freed == waits_alloc && faults == 0);
    for (uint32_t id = 1; id <= SLOTS_N; ++id)
        assert(dev.flow_wake[id] == NULL);

    /* All or none: a slot whose sync event cannot start fails the session
     * and leaves no wake behind. */
    nevents = 0;
    open_dev(&dev);
    fail_start_at = 5;
    errno = 0;
    assert(flow_wakes_open_locked(&dev) == -1 && errno == ENOMEM);
    assert(live_pes == 0 && live_events == 0 && faults == 0);
    for (uint32_t id = 1; id <= SLOTS_N; ++id)
        assert(dev.flow_wake[id] == NULL);
    fail_start_at = 0;

    /* A sync event that does not stop is retained with its PE. */
    nevents = 0;
    open_dev(&dev);
    assert(flow_wakes_open_locked(&dev) == 0);
    ((struct fake_se *)dev.flow_wake[7]->se)->stuck = 1;
    flow_wakes_close_locked(&dev);
    assert(dev.flow_wake[7] != NULL && live_pes == 1 && live_events == 1 && faults == 0);
    ((struct fake_se *)dev.flow_wake[7]->se)->stuck = 0;
    flow_wakes_close_locked(&dev);
    assert(dev.flow_wake[7] == NULL && live_pes == 0 && live_events == 0 && faults == 0);

    puts("channel flow wake: every slot's sync event over its word, one wait at a time, all or the session fails, retained when it does not stop: PASS");
    return 0;
}
