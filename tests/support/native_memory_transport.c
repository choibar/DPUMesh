/* Deterministic carrier for production-core tests. Never linked into libdpumesh.
 * Models custody separately from RX lease return and can hold ACK publication. */
#include "src/core/native_transport.h"
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/eventfd.h>
#include <unistd.h>

#define CHANNELS 8
#define EVENTS 16384
struct event_node { struct dmesh_native_event event; struct event_node *next; };
struct dmesh_native_transport {
    int pod, service, stripes, closed;
    size_t bytes, slots;
    unsigned char *tx, *rx, *leased;
    struct event_node *first[16], *last[16];
    uint16_t upstream[CHANNELS][256];
    uint16_t next_upstream;
};
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static struct dmesh_native_transport *channels[CHANNELS];
static int hold_acks;
static int fail_close;
void test_native_fail_close(int value) { fail_close = value; }
static struct event_node *held[CHANNELS];
static void push(struct dmesh_native_transport *t, struct dmesh_native_event e) {
    struct event_node *n = calloc(1, sizeof(*n)); assert(n);
    n->event = e;
    int stripe = (e.kind == DMESH_NATIVE_RX ? e.desc.dst_port : e.port) % t->stripes;
    if (t->last[stripe]) t->last[stripe]->next = n;
    else t->first[stripe] = n;
    t->last[stripe] = n;
}
void test_native_hold_acks(int value) {
    pthread_mutex_lock(&lock);
    hold_acks = value;
    if (!value) for (int i = 0; i < CHANNELS; ++i) {
        while (held[i]) {
            struct event_node *n = held[i]; held[i] = n->next;
            push(channels[i], n->event); free(n);
        }
    }
    pthread_mutex_unlock(&lock);
}
int dmesh_native_open(struct dmesh_native_transport **out, struct dmesh_native_config *c) {
    struct dmesh_native_transport *t = calloc(1, sizeof(*t));
    if (!t) return -1;
    t->bytes = c->bytes; t->slots = c->bytes / DPUMESH_SLOT_SIZE;
    t->tx = calloc(1, c->bytes); t->rx = calloc(1, c->bytes); t->leased = calloc(t->slots, 1);
    if (!t->tx || !t->rx || !t->leased) {
        free(t->tx); free(t->rx); free(t->leased); free(t); errno = ENOMEM; return -1;
    }
    t->stripes = c->rx_stripes ? c->rx_stripes : c->rings;
    t->service = c->service_name && *c->service_name ? 1 : -1;
    t->next_upstream = DMESH_UPORT_BASE;
    pthread_mutex_lock(&lock);
    int pod = 0; while (pod < CHANNELS && channels[pod]) ++pod;
    if (pod == CHANNELS) { pthread_mutex_unlock(&lock); free(t->tx); free(t->rx); free(t->leased); free(t); errno = ENOSPC; return -1; }
    t->pod = pod; channels[pod] = t;
    pthread_mutex_unlock(&lock);
    c->tx = t->tx; c->rx = t->rx; c->stripes = t->stripes;
    c->pod_id = pod; c->service_id = t->service; c->rx_bytes = c->bytes; *out = t;
    return 0;
}
int dmesh_native_close(struct dmesh_native_transport *t) {
    if (fail_close) { errno = EIO; return -1; }
    pthread_mutex_lock(&lock);
    for (size_t i = 0; i < t->slots; ++i) if (t->leased[i] == 1) {
        pthread_mutex_unlock(&lock); errno = EBUSY; return -1;
    }
    channels[t->pod] = NULL;
    for (int s = 0; s < t->stripes; ++s) while (t->first[s]) {
        struct event_node *n = t->first[s]; t->first[s] = n->next; free(n);
    }
    pthread_mutex_unlock(&lock);
    free(t->tx); free(t->rx); free(t->leased); free(t); return 0;
}
int dmesh_native_resolve(struct dmesh_native_transport *t, const char *name, uint32_t ip, uint16_t port) {
    (void)t; (void)ip; (void)port;
    if (name && strcmp(name, "echo:80")) { errno = ENOENT; return -1; }
    return 1;
}
int dmesh_native_submit(struct dmesh_native_transport *t, const sw_descriptor_t *d) {
    pthread_mutex_lock(&lock);
    struct dmesh_native_transport *dst = NULL;
    if (d->dst_pod >= 0 && d->dst_pod < CHANNELS) dst = channels[d->dst_pod];
    else for (int i = 0; i < CHANNELS; ++i)
        if (channels[i] && channels[i]->service == d->dst_service) { dst = channels[i]; break; }
    if (!dst) { pthread_mutex_unlock(&lock); errno = ENOENT; return -1; }
    size_t slot = 0; while (slot < dst->slots && dst->leased[slot]) ++slot;
    if (slot == dst->slots) { pthread_mutex_unlock(&lock); errno = EAGAIN; return -1; }
    uint16_t port = d->dst_port;
    if (!port) {
        assert(d->src_port < 256);
        port = dst->upstream[t->pod][d->src_port];
        if (!port) port = dst->upstream[t->pod][d->src_port] = dst->next_upstream++;
    }
    dst->leased[slot] = 2; /* landed, not yet handed to the native core */
    struct dmesh_native_event rx = {.kind = DMESH_NATIVE_RX, .desc = *d};
    rx.desc.body_buf_slot = slot * DPUMESH_SLOT_SIZE;
    rx.desc.dst_port = port; rx.desc.dst_pod = dst->pod;
    memcpy(dst->rx + rx.desc.body_buf_slot, t->tx + d->body_buf_slot, d->body_len);
    push(dst, rx);
    struct dmesh_native_event ack = {.kind = DMESH_NATIVE_ACK, .port = d->src_port, .seq = d->seq, .seq_count = 1};
    if (hold_acks) {
        struct event_node *n = calloc(1, sizeof(*n)); assert(n);
        n->event = ack; n->next = held[t->pod]; held[t->pod] = n;
    } else push(t, ack);
    pthread_mutex_unlock(&lock); return 0;
}
void dmesh_native_progress(struct dmesh_native_transport *t) { (void)t; }
int dmesh_native_poll(struct dmesh_native_transport *t, int stripe, struct dmesh_native_event *e) {
    pthread_mutex_lock(&lock);
    struct event_node *n = t->first[stripe];
    if (!n) { pthread_mutex_unlock(&lock); return 0; }
    t->first[stripe] = n->next; if (!n->next) t->last[stripe] = NULL;
    *e = n->event;
    if (e->kind == DMESH_NATIVE_RX) t->leased[e->desc.body_buf_slot / DPUMESH_SLOT_SIZE] = 1;
    free(n); pthread_mutex_unlock(&lock); return 1;
}
void dmesh_native_release(struct dmesh_native_transport *t, int pos) {
    pthread_mutex_lock(&lock);
    assert(pos >= 0 && (size_t)pos < t->bytes && pos % DPUMESH_SLOT_SIZE == 0);
    assert(t->leased[pos / DPUMESH_SLOT_SIZE]);
    t->leased[pos / DPUMESH_SLOT_SIZE] = 0;
    pthread_mutex_unlock(&lock);
}
/* No doorbells: every stripe keeps its consumer polling, unless a test lets
 * the channel sleep (test_native_sleepable), where idle_arm succeeds and the
 * wake fd is an eventfd the test raises. */
static int sleepable, wake_efd = -1;
static unsigned idle_arms, wake_clears;
void test_native_sleepable(int value) { sleepable = value; }
unsigned test_native_idle_arms(void) { return idle_arms; }
unsigned test_native_wake_clears(void) { return wake_clears; }
void test_native_raise_wake(void)
{
    uint64_t one = 1;
    assert(wake_efd >= 0 && write(wake_efd, &one, sizeof(one)) == sizeof(one));
}
int dmesh_native_stripe_fd(struct dmesh_native_transport *t, int stripe) { (void)t; (void)stripe; return -1; }
int dmesh_native_stripe_of(struct dmesh_native_transport *t, uint16_t port) { (void)t; (void)port; return -1; }
int dmesh_native_stripe_arm(struct dmesh_native_transport *t, int stripe) { (void)t; (void)stripe; return !sleepable; }
void dmesh_native_stripe_clear(struct dmesh_native_transport *t, int stripe) { (void)t; (void)stripe; }
int dmesh_native_wake_fd(struct dmesh_native_transport *t)
{
    (void)t;
    if (wake_efd < 0) wake_efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    return wake_efd;
}
int dmesh_native_idle_arm(struct dmesh_native_transport *t)
{
    (void)t;
    if (!sleepable) { errno = EAGAIN; return -1; }
    ++idle_arms;
    return 0;
}
void dmesh_native_wake_clear(struct dmesh_native_transport *t)
{
    (void)t;
    uint64_t v;
    ++wake_clears;
    while (wake_efd >= 0 && read(wake_efd, &v, sizeof(v)) > 0) {}
}
void dmesh_native_wake_counters(struct dmesh_native_transport *t, uint64_t *arms, uint64_t *doorbells)
{
    (void)t;
    *arms = idle_arms;
    *doorbells = wake_clears;
}
int dmesh_native_connect(struct dmesh_native_transport *t, uint16_t port, int service_id) {
    (void)t; (void)port; (void)service_id; return 0;
}
int dmesh_native_disconnect(struct dmesh_native_transport *t, uint16_t port) { (void)t; (void)port; return 0; }
