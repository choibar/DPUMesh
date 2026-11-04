/* The DPU's requests to a host ride the client flow they concern, through the
 * real dispatcher with no DOCA device: PREPARE goes on the flow a migration
 * moves; SCALE on a client flow to the destination it scales (the one it was
 * asked on, else another of the session's, else it waits for one); SCALE_ACK
 * comes back on the flow. */
#include <assert.h>
#include "src/transport/dpu/dispatcher.c"

doca_error_t dmesh_wake_attach(struct dmesh_conn *c, struct doca_comch_connection *connection, uint32_t consumer)
{
    (void)c; (void)connection; (void)consumer;
    return DOCA_SUCCESS;
}

/* The PREPARE and SCALE messages the dispatcher sent. */
static struct { uint16_t type; uint32_t flow, generation, count, token; size_t len; } sent[8];
static unsigned n_sent;
doca_error_t dmesh_session_send(struct objects *o, struct dmesh_session *s, uint16_t type,
    uint32_t id, uint32_t generation, const void *payload, size_t length, int32_t status)
{
    (void)o; (void)s; (void)status;
    if (type != DMESH_SESSION_PREPARE && type != DMESH_SESSION_SCALE) return DOCA_SUCCESS;
    assert(n_sent < 8);
    sent[n_sent].type = type; sent[n_sent].flow = id; sent[n_sent].generation = generation;
    sent[n_sent].len = length;
    if (type == DMESH_SESSION_SCALE) {
        assert(length == 8);
        sent[n_sent].count = dmesh_session_get_u32(payload);
        sent[n_sent].token = dmesh_session_get_u32((const uint8_t *)payload + 4);
    }
    ++n_sent;
    return DOCA_SUCCESS;
}

/* A ready client flow of session s to ip:port. */
static struct assignment *open_client(struct dmesh_dispatcher *d, struct dmesh_session *s, uint32_t flow,
                                      uint32_t generation, uint32_t ip, uint16_t port)
{
    struct dmesh_export_metadata_msg metadata = {0};
    metadata.flow.mode = DMESH_FLOW_MODE_INGRESS_PUSH;
    metadata.flow.dst_ip = ip;
    metadata.flow.dst_port = port;
    struct dmesh_session_header open = {.type = DMESH_SESSION_OPEN, .flow_id = flow,
                                        .generation = generation, .payload_len = sizeof(metadata)};
    dmesh_dispatcher_request(d->controls[0], s, &open, &metadata);
    struct assignment *a = find(d, s, flow);
    assert(a);
    a->ready = true;                         /* READY was forwarded to the host */
    a->ready_ms = membership_now_ms();
    return a;
}

int main(void)
{
    struct dmesh_dispatcher d = {.count = 2, .policy = dmesh_placement_least_flows};
    d.fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    d.controls = calloc(d.count, sizeof(*d.controls));
    d.workers = calloc(d.count, sizeof(*d.workers));
    d.loads = calloc(d.count, sizeof(*d.loads));
    d.flows = calloc(d.count * DMESH_MAX_CONNECTIONS, sizeof(*d.flows));
    assert(d.controls && d.workers && d.loads && d.flows);
    for (unsigned w = 0; w < d.count; ++w) {
        d.controls[w] = calloc(1, sizeof(struct objects));
        d.controls[w]->dispatcher = &d;
        d.workers[w].dispatcher = &d;
        d.workers[w].fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        atomic_store(&d.workers[w].ready, true);
        pthread_mutex_init(&d.workers[w].requests.lock, NULL);
        pthread_mutex_init(&d.workers[w].replies.lock, NULL);
        d.loads[w].capacity = DMESH_MAX_CONNECTIONS;
    }
    struct dmesh_session *s = &d.controls[0]->sessions[0];
    s->occupied = s->negotiated = true; s->epoch = 2;
    const uint32_t ip = 0x0109630a, other = 0x0209630a;
    struct assignment *a1 = open_client(&d, s, 1, 3, ip, 9000), *a2 = open_client(&d, s, 2, 5, ip, 9000);
    struct assignment *a3 = open_client(&d, s, 3, 7, other, 9000);

    /* SCALE on the flow it was asked on, its count clamped to the workers. */
    assert(dmesh_scale(&d, d.controls[0], s, ip, 9000, 4, &a1->key) == 2);
    flush_scales(&d);
    assert(n_sent == 1 && sent[0].type == DMESH_SESSION_SCALE && sent[0].flow == 1 &&
           sent[0].generation == 3 && sent[0].count == 2 && sent[0].token != 0);

    /* The host's SCALE_ACK, on that flow, names the token. */
    uint8_t ack[8];
    dmesh_session_put_u32(ack, sent[0].token);
    dmesh_session_put_u32(ack + 4, 1);
    struct dmesh_session_header h = {.type = DMESH_SESSION_SCALE_ACK, .flow_id = 1, .generation = 3,
                                     .payload_len = sizeof(ack)};
    dmesh_dispatcher_request(d.controls[0], s, &h, ack);
    struct scale *e = find_scale(&d, s, ip, 9000);
    assert(e && e->acked && e->applied == 1);

    /* The flow it rode is closing: another client flow to the destination
     * carries the next SCALE. */
    a1->close_pending = true;
    assert(dmesh_scale(&d, d.controls[0], s, ip, 9000, 1, NULL) == 1);
    flush_scales(&d);
    assert(n_sent == 2 && sent[1].flow == 2 && sent[1].generation == 5 && sent[1].count == 1);

    /* With none left a SCALE waits, and goes out once one is ready again. */
    a2->close_pending = true;
    assert(dmesh_scale(&d, d.controls[0], s, ip, 9000, 2, NULL) == 2);
    flush_scales(&d);
    assert(n_sent == 2 && e->pending);
    a2->close_pending = false;
    flush_scales(&d);
    assert(n_sent == 3 && sent[2].flow == 2 && !e->pending);

    /* Unacknowledged (its flow may have closed under it), it is sent again. */
    flush_scales(&d);
    assert(n_sent == 3);
    e->sent_ms -= SCALE_RESEND_MS;
    flush_scales(&d);
    assert(n_sent == 4 && sent[3].flow == 2 && sent[3].token == sent[2].token);

    /* A spare migration: PREPARE goes on the flow it moves, with no payload. */
    d.migrate_period_ms = 1; d.migrate_spare = true; d.steer_ms = 1000;
    a3->ready_ms = 0;                        /* due; a1 is closing and a2 just became ready */
    a2->ready_ms = membership_now_ms();
    migrate_due(&d);
    assert(n_sent == 5 && sent[4].type == DMESH_SESSION_PREPARE && sent[4].flow == 3 &&
           sent[4].generation == 7 && sent[4].len == 0 && a3->awaiting_spare);

    printf("dispatch requests: SCALE on its flow, then another to the destination, waits for one, "
           "sent again until acknowledged; SCALE_ACK by token; PREPARE on the moved flow: PASS\n");
    return 0;
}
