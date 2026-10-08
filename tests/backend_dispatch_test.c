/* Real dispatcher policy/control queues with no DOCA device. */
#include <assert.h>
#include "src/transport/dpu/dispatcher.c"
static unsigned requests, acknowledgements;
static uint32_t last_worker, last_token;
static doca_error_t send_result = DOCA_SUCCESS;
doca_error_t dmesh_session_send(struct objects *o, struct dmesh_session *s, uint16_t type,
    uint32_t id, uint32_t generation, const void *payload, size_t length, int32_t status)
{
    (void)o; (void)s; (void)id; (void)length; (void)status;
    if (send_result != DOCA_SUCCESS) return send_result;
    if (type == DMESH_SESSION_BACKEND_REQUEST) {
        ++requests; last_worker = dmesh_session_get_u32(payload); last_token = generation;
    }
    if (type == DMESH_SESSION_LISTEN_ACK) ++acknowledgements;
    return DOCA_SUCCESS;
}
int main(void)
{
    struct dmesh_dispatcher d = {.count = 2, .policy = dmesh_placement_least_flows};
    d.fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    d.controls = calloc(d.count, sizeof(*d.controls));
    d.workers = calloc(d.count, sizeof(*d.workers));
    d.loads = calloc(d.count, sizeof(*d.loads));
    d.flows = calloc(d.count * DMESH_MAX_CONNECTIONS, sizeof(*d.flows));
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
    struct objects requester = {.mailbox = &d.workers[1]};
    struct dmesh_session *s = &d.controls[0]->sessions[0];
    s->occupied = s->negotiated = true; s->epoch = 4;
    uint8_t addr[8]; dmesh_session_put_u32(addr, 0x0100510a); dmesh_session_put_u32(addr+4, 8080);
    struct dmesh_session_header h = {.type = DMESH_SESSION_LISTEN, .generation = 1, .payload_len = 8};
    dmesh_dispatcher_request(d.controls[0], s, &h, addr);
    assert(s->listen_port == 8080 && !s->backend[1].token);
    assert(dmesh_dispatch_listener_known(s->listen_ip, 8080));
    flush_listeners(&d);
    assert(acknowledgements == 1 && requests == 0); /* listener has no data flow */
    assert(dmesh_dispatch_backend_request(&requester, s->listen_ip, 8080) == 0);
    flush_replies(&d);
    send_result = DOCA_ERROR_AGAIN;
    flush_listeners(&d);
    assert(!s->backend[1].sent && requests == 0); /* retained under backpressure */
    send_result = DOCA_SUCCESS;
    flush_listeners(&d);
    assert(requests == 1 && last_worker == 1 && last_token);
    uint32_t token = last_token;
    for (int i = 0; i < 3; ++i) {
        assert(dmesh_dispatch_backend_request(&requester, s->listen_ip, 8080) == 0);
        flush_replies(&d); flush_listeners(&d);
    }
    assert(requests == 1); /* concurrent clients coalesce */
    struct dmesh_export_metadata_msg metadata = {0};
    metadata.flow.mode = DMESH_FLOW_MODE_BACKEND;
    metadata.flow.dst_ip = s->listen_ip; metadata.flow.dst_port = s->listen_port;
    uint8_t open[8 + sizeof(metadata)];
    dmesh_session_put_u32(open, 1); dmesh_session_put_u32(open+4, token);
    memcpy(open+8, &metadata, sizeof(metadata));
    h = (struct dmesh_session_header){.type = DMESH_SESSION_BACKEND_OPEN,
        .flow_id = 3, .generation = 1, .payload_len = sizeof(open)};
    d.loads[1].assigned = 1; /* ordinary placement would choose worker 0 */
    dmesh_dispatcher_request(d.controls[0], s, &h, open);
    struct assignment *a = &d.flows[DMESH_MAX_CONNECTIONS];
    assert(a->used && a->location.worker == 1 && a->backend_token == token);
    assert(d.loads[1].assigned == 2 && d.loads[0].assigned == 0);
    dmesh_dispatcher_request(d.controls[0], s, &h, open);
    h.flow_id = 4; dmesh_dispatcher_request(d.controls[0], s, &h, open);
    assert(d.loads[1].assigned == 2); /* duplicate replies cannot create a second flow */
    struct message retired = {.type = RETIRED, .key = a->key, .location = a->location};
    assert(push(&d.workers[1].replies, &retired)); flush_replies(&d);
    assert(!s->backend[1].token && !a->used);
    assert(dmesh_dispatch_backend_request(&requester, s->listen_ip, 8080) == 0);
    flush_replies(&d); flush_listeners(&d);
    assert(requests == 2 && last_token != token);
    /* Late response to an old request cannot bind new resources. */
    h.flow_id = 4; dmesh_dispatcher_request(d.controls[0], s, &h, open);
    assert(d.loads[1].assigned == 1);
    /* The same replica can independently establish a flow on worker 0. */
    requester.mailbox = &d.workers[0];
    assert(dmesh_dispatch_backend_request(&requester, s->listen_ip, 8080) == 0);
    flush_replies(&d); flush_listeners(&d);
    assert(requests == 3 && last_worker == 0 && s->backend[1].token);
    /* A pinned owner at capacity must reject, never spill to another worker. */
    dmesh_session_put_u32(open, 0); dmesh_session_put_u32(open + 4, last_token);
    h.flow_id = 5;
    d.loads[0].assigned = DMESH_MAX_CONNECTIONS;
    dmesh_dispatcher_request(d.controls[0], s, &h, open);
    assert(!find(&d, s, 5) && d.loads[1].assigned == 1);
    /* Listener loss removes admission while preserving DMA membership. */
    s->closing = true;
    assert(listener(&d, s->listen_ip, s->listen_port) == NULL);
    assert(dmesh_dispatch_listener_known(s->listen_ip, s->listen_port));
    for (unsigned w = 0; w < d.count; ++w) {
        close(d.workers[w].fd);
        pthread_mutex_destroy(&d.workers[w].requests.lock);
        pthread_mutex_destroy(&d.workers[w].replies.lock);
        free(d.controls[w]);
    }
    forget_listeners(&d);
    assert(!dmesh_dispatch_listener_known(0x0100510a, 8080));
    close(d.fd); free(d.controls); free(d.workers); free(d.loads); free(d.flows);
    puts("backend dispatcher: lazy allocation, pinned owner, dedup, backpressure, stale fencing: PASS");
}
