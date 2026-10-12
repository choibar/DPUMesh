/* Idle wake through the real dispatcher and worker mailboxes, no DOCA device:
 * ARM reaches the flow's owner, the owner rings once, and the dispatcher
 * forwards one DOORBELL per ARM. */
#include <assert.h>
#include "src/transport/dpu/dispatcher.c"

doca_error_t dmesh_session_send(struct objects *o, struct dmesh_session *s, uint16_t type,
    uint32_t id, uint32_t generation, const void *payload, size_t length, int32_t status)
{
    (void)o; (void)s; (void)type; (void)id; (void)generation; (void)payload; (void)length; (void)status;
    return DOCA_SUCCESS;
}

static void deliver_arm(struct dmesh_dispatcher *d, struct dmesh_session *s, uint64_t epoch,
                        const struct dmesh_session_arm_flow *flows, uint32_t count)
{
    uint8_t payload[DMESH_SESSION_ARM_HEADER_SIZE +
                    DMESH_SESSION_MAX_FLOWS * DMESH_SESSION_ARM_FLOW_SIZE];
    size_t len = dmesh_session_arm_encode(payload, sizeof(payload), epoch, flows, count);
    assert(len);
    struct dmesh_session_header h = {.type = DMESH_SESSION_ARM, .payload_len = (uint32_t)len};
    dmesh_dispatcher_request(d->controls[0], s, &h, payload);
}

static unsigned queued(struct queue *q) { return atomic_load(&q->count); }

int main(void)
{
    struct dmesh_dispatcher d = {.count = 2, .policy = dmesh_placement_least_flows};
    d.fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    d.controls = calloc(d.count, sizeof(*d.controls));
    d.workers = calloc(d.count, sizeof(*d.workers));
    d.loads = calloc(d.count, sizeof(*d.loads));
    d.flows = calloc(d.count * DMESH_MAX_CONNECTIONS, sizeof(*d.flows));
    struct objects *workers = calloc(d.count, sizeof(*workers));
    assert(d.controls && d.workers && d.loads && d.flows && workers);
    for (unsigned w = 0; w < d.count; ++w) {
        d.controls[w] = calloc(1, sizeof(struct objects));
        d.controls[w]->dispatcher = &d;
        d.workers[w].dispatcher = &d;
        d.workers[w].fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        atomic_store(&d.workers[w].ready, true);
        pthread_mutex_init(&d.workers[w].requests.lock, NULL);
        pthread_mutex_init(&d.workers[w].replies.lock, NULL);
        d.loads[w].capacity = DMESH_MAX_CONNECTIONS;
        workers[w].mailbox = &d.workers[w];
        workers[w].worker_idx = (int)w;
    }
    struct dmesh_session *s = &d.controls[0]->sessions[0];
    s->occupied = s->negotiated = true; s->epoch = 2;

    /* Two client flows; least-flows places them on different workers. */
    struct dmesh_export_metadata_msg metadata = {0};
    metadata.flow.mode = DMESH_FLOW_MODE_INGRESS_PUSH;
    struct dmesh_session_header open = {.type = DMESH_SESSION_OPEN, .flow_id = 1, .generation = 3,
                                        .payload_len = sizeof(metadata)};
    dmesh_dispatcher_request(d.controls[0], s, &open, &metadata);
    open.flow_id = 2; open.generation = 5;
    dmesh_dispatcher_request(d.controls[0], s, &open, &metadata);
    struct assignment *a1 = find(&d, s, 1), *a2 = find(&d, s, 2);
    assert(a1 && a2 && a1->location.worker != a2->location.worker);
    a1->ready = a2->ready = true;            /* READY was forwarded to the host */
    struct objects *w1 = &workers[a1->location.worker], *w2 = &workers[a2->location.worker];
    dmesh_dispatch_worker_drain(w1);
    dmesh_dispatch_worker_drain(w2);
    struct dmesh_conn *c1 = &w1->conns[a1->location.slot], *c2 = &w2->conns[a2->location.slot];
    assert(c1->state == DMESH_CONN_NEW && c2->state == DMESH_CONN_NEW);
    c1->state = c2->state = DMESH_CONN_RUNNING;
    c1->flow.mode = c2->flow.mode = DMESH_FLOW_MODE_INGRESS_PUSH;
    c1->push_seq = 4;                        /* descriptors 1..4 are in host memory */
    c2->push_seq = 9;

    /* The host saw everything: both owners stay armed and nothing rings. */
    struct dmesh_session_arm_flow seen[] = {{1, 3, 5}, {2, 5, 10}};
    deliver_arm(&d, s, 11, seen, 2);
    assert(s->armed && s->armed_epoch == 11 && !s->closing);
    assert(queued(&w1->mailbox->requests) == 1 && queued(&w2->mailbox->requests) == 1);
    dmesh_dispatch_worker_drain(w1);
    dmesh_dispatch_worker_drain(w2);
    assert(c1->session->armed && c1->session->armed_epoch == 11);
    assert(c2->session->armed && c1->state == DMESH_CONN_RUNNING); /* ARM is not a close */
    dmesh_dispatch_worker_flush(w1);
    dmesh_dispatch_worker_flush(w2);
    assert(queued(&w1->mailbox->replies) == 0 && queued(&w2->mailbox->replies) == 0);

    /* Both owners publish; the dispatcher forwards a single DOORBELL. */
    dmesh_session_push_published(c1->session);
    dmesh_session_push_published(c2->session);
    dmesh_dispatch_worker_flush(w1);
    dmesh_dispatch_worker_flush(w2);
    dmesh_dispatch_worker_flush(w1);         /* no second reply for the same epoch */
    assert(queued(&w1->mailbox->replies) == 1 && queued(&w2->mailbox->replies) == 1);
    flush_replies(&d);
    assert(!s->armed && s->doorbell_pending_epoch == 11);
    s->doorbell_sent_epoch = 11;             /* dmesh_sessions_advance sent it */

    /* A descriptor the ARM had not accounted for rings at once. */
    struct dmesh_session_arm_flow behind[] = {{1, 3, 4}};
    deliver_arm(&d, s, 12, behind, 1);
    dmesh_dispatch_worker_drain(w1);
    assert(!c1->session->armed && c1->session->doorbell_pending_epoch == 12);
    dmesh_dispatch_worker_flush(w1);
    flush_replies(&d);
    assert(s->doorbell_pending_epoch == 12);
    s->doorbell_sent_epoch = 12;

    /* A late ring for an older ARM is not forwarded. */
    struct dmesh_session_arm_flow ahead[] = {{2, 5, 11}};
    deliver_arm(&d, s, 13, ahead, 1);
    c2->session->doorbell_pending_epoch = 7; /* stale epoch from that worker */
    dmesh_dispatch_worker_flush(w2);
    flush_replies(&d);
    assert(s->armed && s->doorbell_pending_epoch == 12);
    dmesh_dispatch_worker_drain(w2);
    assert(c2->session->armed && c2->session->armed_epoch == 13);

    /* Stale generations and unknown flows reach no worker. */
    struct dmesh_session_arm_flow stale[] = {{1, 2, 1}, {7, 1, 1}};
    unsigned before1 = queued(&w1->mailbox->requests), before2 = queued(&w2->mailbox->requests);
    deliver_arm(&d, s, 14, stale, 2);
    assert(s->armed && s->armed_epoch == 14);
    assert(queued(&w1->mailbox->requests) == before1 && queued(&w2->mailbox->requests) == before2);

    /* An owner whose queue is full cannot hold the ARM: ring at once. */
    struct message filler = {.type = CLOSE};
    while (push(&w1->mailbox->requests, &filler)) {}
    deliver_arm(&d, s, 15, seen, 1);
    assert(!s->armed && s->doorbell_pending_epoch == 15);
    struct message drop;
    while (peek(&w1->mailbox->requests, &drop, true)) {}

    /* A malformed ARM fails the session. */
    uint8_t bad[DMESH_SESSION_ARM_HEADER_SIZE + DMESH_SESSION_ARM_FLOW_SIZE];
    assert(dmesh_session_arm_encode(bad, sizeof(bad), 16, seen, 1) == sizeof(bad));
    dmesh_session_put_u32(bad + 16, 0);      /* flow id 0 */
    struct dmesh_session_header h = {.type = DMESH_SESSION_ARM, .payload_len = sizeof(bad)};
    dmesh_dispatcher_request(d.controls[0], s, &h, bad);
    assert(s->closing && a1->lost_pending && a2->lost_pending);

    for (unsigned w = 0; w < d.count; ++w) {
        for (unsigned i = 0; i < DMESH_MAX_CONNECTIONS; ++i) free(workers[w].conns[i].pending_metadata);
        close(d.workers[w].fd);
        pthread_mutex_destroy(&d.workers[w].requests.lock);
        pthread_mutex_destroy(&d.workers[w].replies.lock);
        free(d.controls[w]);
    }
    close(d.fd); free(d.controls); free(d.workers); free(d.loads); free(d.flows); free(workers);
    puts("dispatch wake: ARM to owners, one DOORBELL per ARM, immediate ring, stale fencing, backpressure: PASS");
}
