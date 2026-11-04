/* least-instr placement through the real dispatcher, no DOCA device: a flow
 * placed but not yet measured counts as the mean measured flow, so opens
 * between two statistics samples spread instead of all taking the worker that
 * looked least loaded at the last sample. */
#include <assert.h>
#include "src/transport/dpu/dispatcher.c"

doca_error_t dmesh_session_send(struct objects *o, struct dmesh_session *s, uint16_t type,
    uint32_t id, uint32_t generation, const void *payload, size_t length, int32_t status)
{
    (void)o; (void)s; (void)type; (void)id; (void)generation; (void)payload; (void)length; (void)status;
    return DOCA_SUCCESS;
}

int main(void)
{
    struct dmesh_dispatcher d = {.count = 3, .policy = dmesh_placement_least_instructions};
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
        d.workers[w].instr_fd = -1;
        atomic_store(&d.workers[w].ready, true);
        pthread_mutex_init(&d.workers[w].requests.lock, NULL);
        pthread_mutex_init(&d.workers[w].replies.lock, NULL);
        d.loads[w].capacity = DMESH_MAX_CONNECTIONS;
    }
    struct dmesh_session *s = &d.controls[0]->sessions[0];
    s->occupied = s->negotiated = true; s->epoch = 2;

    /* The last sample: w0 retires 100M instructions/s, w1 and w2 300M each;
     * a measured flow averages 150M. */
    d.loads[0].flow_instr_ps = 100000000;
    d.loads[1].flow_instr_ps = d.loads[2].flow_instr_ps = 300000000;
    d.flow_instr_mean = 150000000;

    /* Three opens before the next sample, each a different pair so the pair
     * rule leaves every worker eligible. The first two fit on w0 (100M, then
     * 100M + 150M assumed); the third would make w0 400M, so it goes to w1. */
    struct dmesh_export_metadata_msg metadata = {0};
    metadata.flow.mode = DMESH_FLOW_MODE_INGRESS_PUSH;
    struct dmesh_session_header open = {.type = DMESH_SESSION_OPEN, .generation = 1,
                                        .payload_len = sizeof(metadata)};
    int owners[3];
    for (uint32_t i = 0; i < 3; ++i) {
        open.flow_id = i + 1;
        metadata.flow.dst_port = (uint16_t)(8000 + i);
        dmesh_dispatcher_request(d.controls[0], s, &open, &metadata);
        struct assignment *a = find(&d, s, i + 1);
        assert(a && a->opening);
        owners[i] = (int)a->location.worker;
    }
    assert(owners[0] == 0 && owners[1] == 0 && owners[2] == 1);
    assert(d.loads[0].pending_instr_ps == 2 * d.flow_instr_mean);

    /* Once measured, a flow counts by its measurement only. */
    find(&d, s, 1)->stat_valid = find(&d, s, 2)->stat_valid = true;
    pending_instr(&d);
    assert(d.loads[0].pending_instr_ps == 0 && d.loads[1].pending_instr_ps == d.flow_instr_mean);

    /* Migration takes the policy's choice too, the not-yet-reconnected
     * migration counting on its target: two measured flows (250M each) leave
     * w0 in one pass; with w1 at 300M and w2 at 100M, the first goes to w2,
     * which then stands at 350M, so the second goes to w1. (The old rule sent
     * both to the next worker, w1.) */
    find(&d, s, 3)->stat_valid = true;
    d.loads[0].flow_instr_ps = 500000000;
    d.loads[1].flow_instr_ps = 300000000;
    d.loads[2].flow_instr_ps = 100000000;
    d.migrate_period_ms = 1; d.steer_ms = 5000;
    for (uint32_t i = 0; i < 2; ++i) {
        struct assignment *a = &d.flows[10 + i];
        *a = (struct assignment){.used = true, .ready = true, .control = d.controls[0], .session = s,
            .key = {1, s->epoch, 10 + i, 1}, .location = {.worker = 0, .slot = 10 + i, .ownership_epoch = 1},
            .mode = DMESH_FLOW_MODE_INGRESS_PUSH, .pair = 0x10 + i,
            .stat_valid = true, .rps = 1000, .instr_per_req = 250000};
    }
    migrate_due(&d);
    int to[2] = {-1, -1};
    for (unsigned j = 0; j < DMESH_MAX_CONNECTIONS; ++j)
        if (d.steers[j].session && d.steers[j].replaced_key.flow_id >= 10)
            to[d.steers[j].replaced_key.flow_id - 10] = (int)d.steers[j].worker;
    assert(to[0] == 2 && to[1] == 1);
    assert(d.flows[10].draining && d.flows[11].draining);

    printf("least-instr placement: unmeasured flows assumed at the mean, opens spread, "
           "migrations go to the least loaded other worker: PASS\n");
    return 0;
}
