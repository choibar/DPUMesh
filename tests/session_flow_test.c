#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "src/transport/common/object.h"
#include "src/transport/common/dpa.h"
#include "src/transport/common/dpa_bench.h"

/* Kernel host stubs and fake SDK creation; no device is opened or run. */
void run_dma_manager(void)
{
    abort();
}
void run_dpa_benchmark(void)
{
    abort();
}

static struct {
    bool enabled;
    size_t allocated;
    doca_dpa_func_t *func;
    doca_dpa_dev_uintptr_t local_storage;
    doca_error_t tls_error, destroy_error;
    unsigned starts, frees;
    bool destroyed;
    struct dpa_bench_state storage;
} creation;

doca_error_t doca_dpa_mem_alloc(struct doca_dpa *dpa, size_t size, doca_dpa_dev_uintptr_t *ptr)
{
    assert(creation.enabled && dpa == (void *)&creation);
    assert(size <= sizeof(creation.storage));
    creation.allocated = size;
    creation.local_storage = 0;
    creation.func = NULL;
    creation.starts = creation.frees = 0;
    creation.destroyed = false;
    *ptr = (uintptr_t)&creation.storage;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_thread_create(struct doca_dpa *dpa, struct doca_dpa_thread **thread)
{
    assert(creation.enabled && dpa == (void *)&creation);
    *thread = (void *)&creation;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_thread_set_func_arg(struct doca_dpa_thread *thread,
                                       doca_dpa_func_t *func, uint64_t arg)
{
    assert(creation.enabled && thread == (void *)&creation);
    assert(arg == 0); /* Kernel state must never travel as an input argument. */
    creation.func = func;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_thread_set_local_storage(struct doca_dpa_thread *thread,
                                             doca_dpa_dev_uintptr_t ptr)
{
    assert(creation.enabled && thread == (void *)&creation && creation.func);
    assert(!creation.starts && ptr == (uintptr_t)&creation.storage);
    if (creation.tls_error) return creation.tls_error;
    creation.local_storage = ptr;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_thread_start(struct doca_dpa_thread *thread)
{
    assert(creation.enabled && thread == (void *)&creation && creation.func);
    assert(creation.local_storage == (uintptr_t)&creation.storage);
    ++creation.starts;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_thread_destroy(struct doca_dpa_thread *thread)
{
    assert(creation.enabled && thread == (void *)&creation);
    if (creation.destroy_error) return creation.destroy_error;
    creation.destroyed = true;
    return DOCA_SUCCESS;
}
doca_error_t doca_dpa_mem_free(struct doca_dpa *dpa, doca_dpa_dev_uintptr_t ptr)
{
    assert(creation.enabled && dpa == (void *)&creation);
    assert(creation.destroyed && ptr == (uintptr_t)&creation.storage);
    ++creation.frees;
    return DOCA_SUCCESS;
}

static void test_native_and_benchmark_arguments(void)
{
    creation.enabled = true;
    /* A host reverse-DMA thread must stay native even in a benchmark process. */
    assert(setenv("DMESH_DPA_BENCH_MODE", "5", 1) == 0);
    struct dmesh_doca_dpa_thread native = {.dpa = (void *)&creation};
    assert(dmesh_doca_dpa_thread_create(&native) == DOCA_SUCCESS);
    assert(native.started && !native.bench_mode);
    assert(creation.allocated == sizeof(struct dpa_thread_ctx));
    assert(creation.func == run_dma_manager);
    assert(dmesh_doca_dpa_bench_thread_create(&native, 1) == DOCA_ERROR_BAD_STATE);
    assert(creation.allocated == sizeof(struct dpa_thread_ctx));
    for (uint32_t mode = 1; mode <= 5; ++mode) {
        struct dmesh_doca_dpa_thread bench = {.dpa = (void *)&creation};
        assert(dmesh_doca_dpa_bench_thread_create(&bench, mode) == DOCA_SUCCESS);
        assert(bench.started && bench.bench_mode == mode);
        assert(creation.allocated == sizeof(struct dpa_bench_state));
        assert(creation.func == run_dpa_benchmark);
    }
    struct dmesh_doca_dpa_thread invalid = {.dpa = (void *)&creation};
    assert(dmesh_doca_dpa_bench_thread_create(&invalid, 0) == DOCA_ERROR_INVALID_VALUE);
    assert(dmesh_doca_dpa_bench_thread_create(&invalid, 6) == DOCA_ERROR_INVALID_VALUE);
    assert(!invalid.local_storage && !invalid.thread);

    /* TLS setup failure must not start a thread or lose its allocation.
     * Even then, a failed thread destruction must retain that allocation. */
    struct dmesh_doca_dpa_thread failed = {.dpa = (void *)&creation};
    creation.tls_error = DOCA_ERROR_DRIVER;
    assert(dmesh_doca_dpa_thread_create(&failed) == DOCA_ERROR_DRIVER);
    assert(failed.local_storage && failed.thread && !failed.started);
    assert(!creation.starts && !creation.frees);
    creation.destroy_error = DOCA_ERROR_DRIVER;
    assert(dmesh_doca_dpa_thread_destroy_checked(&failed) == DOCA_ERROR_DRIVER);
    assert(failed.local_storage && failed.thread && !creation.frees);
    creation.destroy_error = DOCA_SUCCESS;
    assert(dmesh_doca_dpa_thread_destroy_checked(&failed) == DOCA_SUCCESS);
    assert(!failed.local_storage && !failed.thread && creation.frees == 1);
    creation.tls_error = DOCA_SUCCESS;
    assert(unsetenv("DMESH_DPA_BENCH_MODE") == 0);
    creation.enabled = false;
}

static void test_flow_identity(void)
{
    struct objects *objs = calloc(1, sizeof(*objs));
    unsigned char peer_storage[2];
    struct doca_comch_connection *session_a = (void *)&peer_storage[0];
    struct doca_comch_connection *session_b = (void *)&peer_storage[1];
    assert(objs);

    assert(dmesh_flow_open(NULL, session_a, 1, 1) == NULL);
    assert(dmesh_flow_open(objs, NULL, 1, 1) == NULL);
    assert(dmesh_flow_open(objs, session_a, 0, 1) == NULL);
    assert(dmesh_flow_open(objs, session_a, DMESH_MAX_CONNECTIONS + 1, 1) == NULL);
    assert(dmesh_flow_open(objs, session_a, 1, 0) == NULL);
    assert(dmesh_flow_get(NULL, session_a, 1, 1) == NULL);
    assert(dmesh_flow_get(objs, NULL, 1, 1) == NULL);

    struct dmesh_conn *a = dmesh_flow_open(objs, session_a, 1, 7);
    struct dmesh_conn *b = dmesh_flow_open(objs, session_a, 2, 3);
    struct dmesh_conn *c = dmesh_flow_open(objs, session_b, 1, 7);
    assert(a && b && c && a != b && a != c && b != c);
    assert(a->objs == objs && b->objs == objs && c->objs == objs);
    assert(a->connection == b->connection && c->connection != a->connection);
    assert(a->multiplexed && b->multiplexed && c->multiplexed);
    assert(a->state == DMESH_CONN_NEW && b->state == DMESH_CONN_NEW);
    assert(dmesh_flow_get(objs, session_a, 1, 7) == a);
    assert(dmesh_flow_get(objs, session_a, 2, 3) == b);
    assert(dmesh_flow_get(objs, session_b, 1, 7) == c);
    assert(dmesh_flow_get(objs, session_a, 1, 6) == NULL);
    assert(dmesh_flow_get(objs, session_a, 1, 8) == NULL);
    assert(dmesh_flow_get(objs, session_b, 2, 3) == NULL);

    /* A repeated OPEN must not zero active metadata or allocate another slot.
     * Generation admission is the session dispatcher's job; lookup must never
     * route a stale generation onto the current live flow. */
    a->state = DMESH_CONN_RUNNING;
    a->remote_consumer_id = 42;
    assert(dmesh_flow_open(objs, session_a, 1, 7) == a);
    assert(a->state == DMESH_CONN_RUNNING && a->remote_consumer_id == 42);
    assert(b->state == DMESH_CONN_NEW && c->state == DMESH_CONN_NEW);
    free(objs);
}

static void test_flow_table_capacity(void)
{
    struct objects *objs = calloc(1, sizeof(*objs));
    unsigned char peer_storage[2];
    struct doca_comch_connection *session_a = (void *)&peer_storage[0];
    struct doca_comch_connection *session_b = (void *)&peer_storage[1];
    struct dmesh_conn *flows[DMESH_MAX_CONNECTIONS];
    assert(objs);

    for (uint32_t id = 1; id <= DMESH_MAX_CONNECTIONS; ++id) {
        flows[id - 1] = dmesh_flow_open(objs, session_a, id, id + 100);
        assert(flows[id - 1]);
        for (uint32_t old = 1; old < id; ++old)
            assert(flows[old - 1] != flows[id - 1]);
    }
    assert(dmesh_flow_open(objs, session_b, 1, 1) == NULL);
    for (uint32_t id = 1; id <= DMESH_MAX_CONNECTIONS; ++id) {
        assert(dmesh_flow_open(objs, session_a, id, id + 100) == flows[id - 1]);
        assert(dmesh_flow_get(objs, session_a, id, id + 100) == flows[id - 1]);
        assert(dmesh_flow_get(objs, session_a, id, id + 99) == NULL);
    }
    free(objs);
}

static void test_session_disconnect_fanout(void)
{
    struct objects *objs = calloc(1, sizeof(*objs));
    unsigned char peer_storage[3];
    struct doca_comch_connection *session_a = (void *)&peer_storage[0];
    struct doca_comch_connection *session_b = (void *)&peer_storage[1];
    struct doca_comch_connection *unknown = (void *)&peer_storage[2];
    assert(objs);
    struct dmesh_conn *a = dmesh_flow_open(objs, session_a, 1, 1);
    struct dmesh_conn *b = dmesh_flow_open(objs, session_a, 2, 1);
    struct dmesh_conn *c = dmesh_flow_open(objs, session_a, 3, 1);
    struct dmesh_conn *other = dmesh_flow_open(objs, session_b, 1, 1);
    assert(a && b && c && other);
    a->state = DMESH_CONN_RUNNING;
    b->state = DMESH_CONN_AWAIT_METADATA;
    c->state = DMESH_CONN_ERROR;
    other->state = DMESH_CONN_RUNNING;

    dmesh_flow_close_session(NULL, session_a);
    dmesh_flow_close_session(objs, NULL);
    dmesh_flow_close_session(objs, unknown);
    assert(a->state == DMESH_CONN_RUNNING && other->state == DMESH_CONN_RUNNING);
    dmesh_flow_close_session(objs, session_a);
    assert(a->state == DMESH_CONN_CLOSING);
    assert(b->state == DMESH_CONN_CLOSING);
    assert(c->state == DMESH_CONN_CLOSING);
    assert(other->state == DMESH_CONN_RUNNING);
    /* Disconnect requests deferred teardown; it must not recycle live flow
     * identities or mutate an unrelated peer while a callback is running. */
    assert(dmesh_flow_get(objs, session_a, 1, 1) == a);
    assert(dmesh_flow_get(objs, session_b, 1, 1) == other);
    for (int i = 4; i < DMESH_MAX_CONNECTIONS; ++i)
        assert(objs->conns[i].state == DMESH_CONN_FREE);
    dmesh_flow_close_session(objs, session_a);
    assert(a->state == DMESH_CONN_CLOSING && other->state == DMESH_CONN_RUNNING);
    dmesh_flow_close_session(objs, session_b);
    assert(other->state == DMESH_CONN_CLOSING);
    free(objs);
}

static void test_pool_ownership_is_per_flow(void)
{
    struct objects *objs = calloc(1, sizeof(*objs));
    struct dmesh_dpa_thread_pool pool = {0};
    unsigned char peer_storage;
    unsigned char thread_storage[2];
    struct doca_comch_connection *session = (void *)&peer_storage;
    assert(objs);
    struct dmesh_conn *a = dmesh_flow_open(objs, session, 1, 1);
    struct dmesh_conn *b = dmesh_flow_open(objs, session, 2, 1);
    struct dmesh_conn *c = dmesh_flow_open(objs, session, 3, 1);
    assert(a && b && c);

    assert(dmesh_dpa_thread_pool_alloc(objs, a) == NULL);
    dmesh_dpa_thread_pool_release(objs, a);
    objs->dpa_pool = &pool;
    assert(dmesh_dpa_thread_pool_alloc(objs, a) == NULL);
    pool.size = 2;
    pool.threads[0].thread = (void *)&thread_storage[0];
    pool.threads[1].thread = (void *)&thread_storage[1];
    pool.threads[0].started = pool.threads[1].started = true;
    assert(dmesh_dpa_thread_pool_alloc(objs, NULL) == NULL);

    struct dmesh_doca_dpa_thread *ta = dmesh_dpa_thread_pool_alloc(objs, a);
    struct dmesh_doca_dpa_thread *tb = dmesh_dpa_thread_pool_alloc(objs, b);
    assert(ta && tb && ta != tb);
    assert(pool.owner[0] == a && pool.owner[1] == b);
    assert(dmesh_dpa_thread_pool_alloc(objs, a) == ta);
    assert(dmesh_dpa_thread_pool_alloc(objs, b) == tb);
    assert(dmesh_dpa_thread_pool_alloc(objs, c) == NULL);

    /* Closing one logical flow on the shared Comch session must not return
     * its sibling's DPA thread. A later flow can take only the released slot. */
    objs->dpa_thread = ta;
    dmesh_dpa_thread_pool_release(objs, a);
    assert(pool.owner[0] == NULL && pool.owner[1] == b);
    assert(objs->dpa_thread == NULL);
    assert(dmesh_dpa_thread_pool_alloc(objs, b) == tb);
    assert(dmesh_dpa_thread_pool_alloc(objs, c) == ta);
    assert(pool.owner[0] == c && pool.owner[1] == b);
    dmesh_dpa_thread_pool_release(objs, a); /* old owner cannot free c's slot */
    dmesh_dpa_thread_pool_release(objs, NULL);
    assert(pool.owner[0] == c && pool.owner[1] == b);
    dmesh_dpa_thread_pool_release(objs, b);
    assert(pool.owner[0] == c && pool.owner[1] == NULL);
    dmesh_dpa_thread_pool_release(objs, c);
    assert(pool.owner[0] == NULL && pool.owner[1] == NULL);
    free(objs);
}

int main(void)
{
    test_flow_identity();
    test_flow_table_capacity();
    test_session_disconnect_fanout();
    test_pool_ownership_is_per_flow();
    test_native_and_benchmark_arguments();
    puts("session_flow_test: PASS");
    return 0;
}
