#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "src/transport/common/object.h"
#include "src/transport/common/dpa.h"

/* The pool tests use already-created thread handles and never enter device
 * creation. This otherwise-unused kernel symbol lets the production dpa.c
 * allocator link without compiling a DPA program or requiring a device. */
void run_dma_manager(void)
{
    abort();
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
    puts("session_flow_test: PASS");
    return 0;
}
