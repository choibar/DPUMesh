/* Production peer context scheduler: bounded fairness, detach and yield. */
#include <assert.h>
#include <stdio.h>
#define __dpa_global__
#define __dpa_rpc__
#include "src/transport/peer/device/peer_runtime.c"

static unsigned calls, contexts[64], budgets[64], notified, rescheduled;
static int detach, idle;
unsigned peer_write_imm_dpa_drain(struct peer_dpa_thread_arg *arg, unsigned context,
                                  uint64_t state, unsigned budget)
{
    assert(state && budget && calls < 64);
    contexts[calls] = context; budgets[calls++] = budget;
    if (detach) {
        if (context == DPA_RDMA_CONTEXTS) arg->rdma_state = 0;
        else arg->rdma_states[context] = 0;
        return 1;
    }
    return idle ? 0 : budget;
}
void doca_dpa_dev_thread_notify(uint64_t id) { assert(id == 99); notified++; }
void doca_dpa_dev_thread_reschedule(void) { rescheduled++; }
int doca_dpa_dev_get_completion(uint64_t cq, doca_dpa_dev_completion_element_t *element)
{ (void)cq; (void)element; assert(0); return 0; }
void doca_dpa_dev_completion_ack(uint64_t cq, unsigned count)
{ (void)cq; (void)count; assert(0); }

int main(void)
{
    struct peer_dpa_thread_arg arg = {.rdma_count = 3, .rdma_states = {1, 2, 3},
        .yield_notification = 99};
    assert(peer_dpa_drain(&arg, 0) == 0 && !calls);
    assert(peer_dpa_drain(&arg, 100) == 100 && calls == 2);
    assert(contexts[0] == 0 && budgets[0] == 64 && contexts[1] == 1 && budgets[1] == 36);
    calls = 0;
    assert(peer_dpa_drain(&arg, 10) == 10 && contexts[0] == 2 && arg.rdma_cursor == 0);
    arg.rdma_states[0] = 0; calls = 0;
    assert(peer_dpa_drain(&arg, 64) == 64 && calls == 1 && contexts[0] == 1);
    calls = 0; detach = 1;
    assert(peer_dpa_attached(&arg));
    dpumesh_peer_run((uintptr_t)&arg);
    assert(!peer_dpa_attached(&arg) && rescheduled == 1 && !notified);
    calls = 0; detach = 0; idle = 1;
    arg.rdma_states[0] = 1;
    dpumesh_peer_run((uintptr_t)&arg);
    assert(calls == 64 && notified == 1 && rescheduled == 2); /* Idle work yields. */
    calls = 0; idle = 0; arg.rdma_count = DPA_RDMA_CONTEXTS + 1;
    assert(peer_dpa_drain(&arg, 256) == 0 && !calls);
    arg.rdma_count = 0; arg.rdma_state = 1;
    assert(peer_dpa_drain(&arg, 32) == 32 && contexts[0] == DPA_RDMA_CONTEXTS);
    puts("peer_dpa_runtime_test: budgets, round-robin fairness, STOP detach, idle yield: PASS");
    return 0;
}
