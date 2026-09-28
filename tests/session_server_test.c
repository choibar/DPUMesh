#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "src/transport/common/object.h"
#include "src/transport/common/session_protocol.h"

/* Exercise real session callbacks and checked close sequencing with injected
 * SDK failures; no device or DPA kernel is needed. Hardware completion behavior
 * remains covered by the checked-helper contract, not simulated here. */
static struct objects *callback_objects;
static unsigned char server_storage, context_storage;
static struct dmesh_session_header attempted_response;
static struct doca_comch_connection *response_peer;
static unsigned response_attempts;
static unsigned disconnect_attempts;
static doca_error_t disconnect_result = DOCA_SUCCESS;

struct doca_comch_server *
doca_comch_server_get_server_ctx(const struct doca_comch_connection *connection)
{
    assert(connection);
    return (void *)&server_storage;
}

struct doca_ctx *doca_comch_server_as_ctx(struct doca_comch_server *server)
{
    assert(server == (void *)&server_storage);
    return (void *)&context_storage;
}

doca_error_t doca_ctx_get_user_data(const struct doca_ctx *ctx, union doca_data *data)
{
    assert(ctx == (void *)&context_storage && callback_objects);
    data->ptr = callback_objects;
    return DOCA_SUCCESS;
}

doca_error_t doca_comch_server_task_send_alloc_init(
    struct doca_comch_server *server, struct doca_comch_connection *peer,
    const void *msg, uint32_t len, struct doca_comch_task_send **task)
{
    const uint8_t *payload;
    assert(server == (void *)&server_storage);
    assert(dmesh_session_decode(msg, len, &attempted_response, &payload) == 0);
    response_peer = peer;
    ++response_attempts;
    *task = NULL;
    /* Capture the protocol error, then model a full SDK task queue. This keeps
     * all callback state transitions real without fabricating a DOCA task. */
    return DOCA_ERROR_NO_MEMORY;
}

doca_error_t doca_comch_server_disconnect(struct doca_comch_server *server,
                                          struct doca_comch_connection *connection)
{
    assert(server == (void *)&server_storage && connection);
    ++disconnect_attempts;
    return disconnect_result;
}

#include "../src/transport/dpu/comch_server.c"


static char cleanup_order[64];
static unsigned cleanup_count, release_count, mmap_calls;
static char failed_phase;
static unsigned failed_mmap, quiesce_pending;

static doca_error_t cleanup_phase(char phase)
{
    assert(cleanup_count + 1 < sizeof(cleanup_order));
    cleanup_order[cleanup_count++] = phase;
    cleanup_order[cleanup_count] = 0;
    return failed_phase == phase ? DOCA_ERROR_IO_FAILED : DOCA_SUCCESS;
}

doca_error_t dmesh_doca_dpa_quiesce_step(struct dmesh_conn *conn)
{
    assert(conn->dma_closing);
    doca_error_t result = cleanup_phase('Q');
    if (result == DOCA_SUCCESS && quiesce_pending != 0) {
        --quiesce_pending;
        return DOCA_ERROR_AGAIN;
    }
    return result;
}

bool dmesh_doca_dpa_failed(struct doca_dpa *dpa)
{
    (void)dpa;
    return false;
}

doca_error_t cleanup_dma_tasks(struct dmesh_conn *conn)
{
    doca_error_t result = cleanup_phase('D');
    if (result == DOCA_SUCCESS)
        conn->dma_ctx = NULL;
    return result;
}

doca_error_t dmesh_doca_dpa_comch_destroy_checked(struct dmesh_conn *conn)
{
    doca_error_t result = cleanup_phase('M');
    if (result == DOCA_SUCCESS)
        conn->dpa_comch = NULL;
    return result;
}

doca_error_t dmesh_doca_dpa_thread_destroy_checked(struct dmesh_doca_dpa_thread *thread)
{
    (void)thread;
    return cleanup_phase('T');
}

doca_error_t doca_buf_arr_destroy(struct doca_buf_arr *array)
{
    assert(array);
    return cleanup_phase('B');
}

doca_error_t doca_mmap_destroy(struct doca_mmap *mmap)
{
    assert(mmap);
    ++mmap_calls;
    doca_error_t result = cleanup_phase('X');
    return failed_mmap == mmap_calls ? DOCA_ERROR_IO_FAILED : result;
}

void dmesh_dpa_thread_pool_release(struct objects *objs, struct dmesh_conn *conn)
{
    assert(objs == conn->objs);
    ++release_count;
    (void)cleanup_phase('P');
}

static void reset_cleanup(void)
{
    cleanup_count = release_count = mmap_calls = 0;
    cleanup_order[0] = 0;
    failed_phase = 0;
    failed_mmap = quiesce_pending = 0;
}

static void deliver(struct doca_comch_connection *peer, uint16_t type,
                    uint32_t flow, uint32_t generation,
                    const void *payload, size_t payload_len)
{
    uint8_t storage[DMESH_SESSION_MAX_FRAME + 1];
    uint8_t *frame = storage + 1; /* SDK receive buffers need not be aligned. */
    size_t len = dmesh_session_encode(frame, sizeof(storage) - 1, type,
                                      flow, generation, 0, payload, payload_len);
    assert(len);
    server_message_recv_callback(NULL, frame, (uint32_t)len, peer);
    memset(storage, 0xa5, sizeof(storage));
}

static unsigned live_flows(const struct objects *objs)
{
    unsigned count = 0;
    for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
        count += objs->conns[i].state != DMESH_CONN_FREE;
    return count;
}

static void expect_error(unsigned before, struct doca_comch_connection *peer,
                         uint32_t flow, uint32_t generation, int status)
{
    assert(response_attempts == before + 1 && response_peer == peer);
    assert(attempted_response.type == DMESH_SESSION_ERROR);
    assert(attempted_response.flow_id == flow);
    assert(attempted_response.generation == generation);
    assert(attempted_response.status == status && attempted_response.payload_len == 0);
}

static void receive_test(void)
{
    unsigned char peer_storage[2];
    struct doca_comch_connection *peer_a = (void *)&peer_storage[0];
    struct doca_comch_connection *peer_b = (void *)&peer_storage[1];
    struct objects *objs = calloc(1, sizeof(*objs));
    assert(objs);
    callback_objects = objs;
    objs->cc_server = (void *)&server_storage;

    server_connection_event_callback(NULL, peer_a, 1);
    assert(live_flows(objs) == 0);
    struct dmesh_session *sa = session_get(objs, peer_a, false);
    assert(sa && !sa->negotiated);
    deliver(peer_a, DMESH_SESSION_HELLO, 0, 0, NULL, 0);
    assert(sa->negotiated && sa->hello_pending && !sa->closing);
    assert(live_flows(objs) == 0 && response_attempts == 0);

    struct dmesh_export_metadata_msg first = {0}, second = {0};
    first.type = second.type = DMESH_MSG_EXPORT_METADATA;
    first.flow.src_port = 101;
    first.flow.dst_port = 8080;
    first.flow.mode = DMESH_FLOW_MODE_INGRESS_PUSH;
    first.ring_desc_len = 2;
    first.ring_desc[0] = 0x11;
    first.ring_desc[1] = 0xaa;
    second.flow.src_port = 202;
    second.flow.dst_port = 9090;
    second.flow.mode = DMESH_FLOW_MODE_BACKEND_PULL;
    second.ring_desc_len = 1;
    second.ring_desc[0] = 0x22;

    deliver(peer_a, DMESH_SESSION_OPEN, 1, 7, &first, sizeof(first));
    deliver(peer_a, DMESH_SESSION_OPEN, 2, 4, &second, sizeof(second));
    struct dmesh_conn *a = dmesh_flow_get(objs, peer_a, 1, 7);
    struct dmesh_conn *b = dmesh_flow_get(objs, peer_a, 2, 4);
    assert(a && b && a != b && a->connection == b->connection);
    assert(a->session == sa && b->session == sa);
    assert(a->state == DMESH_CONN_NEW && b->state == DMESH_CONN_NEW);
    assert(a->pending_metadata && b->pending_metadata);
    assert(a->pending_metadata != b->pending_metadata);
    assert(memcmp(a->pending_metadata, &first, sizeof(first)) == 0);
    assert(memcmp(b->pending_metadata, &second, sizeof(second)) == 0);
    assert(sa->generation[0] == 7 && sa->generation[1] == 4);

    /* Retransmitting an OPEN while setup is pending keeps the original flow
     * and its owned metadata, even if the duplicate payload differs. */
    struct dmesh_export_metadata_msg *owned = a->pending_metadata;
    deliver(peer_a, DMESH_SESSION_OPEN, 1, 7, &second, sizeof(second));
    assert(dmesh_flow_get(objs, peer_a, 1, 7) == a);
    assert(a->pending_metadata == owned && live_flows(objs) == 2);
    assert(memcmp(owned, &first, sizeof(first)) == 0);
    assert(response_attempts == 0);

    unsigned before = response_attempts;
    deliver(peer_a, DMESH_SESSION_OPEN, 1, 6, &first, sizeof(first));
    expect_error(before, peer_a, 1, 6, ESTALE);
    before = response_attempts;
    deliver(peer_a, DMESH_SESSION_OPEN, 1, 8, &first, sizeof(first));
    expect_error(before, peer_a, 1, 8, EBUSY);
    assert(a->pending_metadata == owned && sa->generation[0] == 7);

    /* An old incarnation's delayed CLOSE must not close the live generation. */
    before = response_attempts;
    deliver(peer_a, DMESH_SESSION_CLOSE, 1, 6, NULL, 0);
    assert(response_attempts == before + 1 && response_peer == peer_a);
    assert(attempted_response.type == DMESH_SESSION_CLOSED && attempted_response.status == 0);
    assert(attempted_response.generation == 6);
    assert(a->state == DMESH_CONN_NEW && b->state == DMESH_CONN_NEW);
    deliver(peer_a, DMESH_SESSION_CLOSE, 1, 7, NULL, 0);
    assert(a->state == DMESH_CONN_CLOSING && b->state == DMESH_CONN_NEW);
    assert(!sa->closing && live_flows(objs) == 2);

    server_connection_event_callback(NULL, peer_b, 1);
    deliver(peer_b, DMESH_SESSION_HELLO, 0, 0, NULL, 0);
    deliver(peer_b, DMESH_SESSION_OPEN, 1, 7, &first, sizeof(first));
    struct dmesh_conn *other = dmesh_flow_get(objs, peer_b, 1, 7);
    struct dmesh_session *sb = session_get(objs, peer_b, false);
    assert(other && other != a && other->session == sb && sb != sa);
    assert(other->state == DMESH_CONN_NEW && live_flows(objs) == 3);

    server_disconnection_event_callback(NULL, peer_a, 1);
    assert(sa->closing && !sb->closing);
    assert(a->state == DMESH_CONN_CLOSING && b->state == DMESH_CONN_CLOSING);
    assert(other->state == DMESH_CONN_NEW);
    deliver(peer_a, DMESH_SESSION_OPEN, 3, 1, &first, sizeof(first));
    assert(live_flows(objs) == 3); /* A closing session admits no new flow. */
    server_disconnection_event_callback(NULL, peer_a, 1);
    assert(other->state == DMESH_CONN_NEW);
    assert(sa->sends_pending == 0 && sb->sends_pending == 0);

    for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
        free(objs->conns[i].pending_metadata);
    free(objs);
    callback_objects = NULL;
}


static struct dmesh_conn *new_test_flow(struct objects *objs,
                                        struct doca_comch_connection *peer,
                                        unsigned id, unsigned generation)
{
    struct dmesh_session *s = session_get(objs, peer, true);
    assert(s);
    s->negotiated = true;
    struct dmesh_conn *conn = dmesh_flow_open(objs, peer, id, generation);
    assert(conn);
    conn->session = s;
    s->generation[id - 1] = generation;
    s->closed[id - 1] = false;
    s->close_pending[id - 1] = false;
    s->close_status[id - 1] = 0;
    conn->state = DMESH_CONN_CLOSING;
    conn->close_requested = true;
    return conn;
}

static void reader_fence_test(void)
{
    unsigned char peer_storage;
    struct doca_comch_connection *peer = (void *)&peer_storage;
    struct objects *objs = calloc(1, sizeof(*objs));
    assert(objs);
    objs->cc_server = (void *)&server_storage;
    objs->external_readers = true;
    assert(!dmesh_objects_have_live_flows(objs));
    assert(!dmesh_objects_have_live_flows(NULL));
    assert(dmesh_flow_readers_detached(NULL, 0) == DOCA_ERROR_INVALID_VALUE);
    assert(dmesh_flow_readers_detached(objs, -1) == DOCA_ERROR_INVALID_VALUE);
    assert(dmesh_flow_readers_detached(objs, DMESH_MAX_CONNECTIONS) == DOCA_ERROR_INVALID_VALUE);
    assert(dmesh_flow_readers_detached(objs, 0) == DOCA_ERROR_BAD_STATE);
    struct dmesh_conn *conn = new_test_flow(objs, peer, 1, 1);
    struct dmesh_session *s = conn->session;
    assert(dmesh_objects_have_live_flows(objs));
    reset_cleanup();
    unsigned replies = response_attempts;
    unsigned disconnects = disconnect_attempts;
    dmesh_flow_close_advance(conn);
    assert(conn->state == DMESH_CONN_CLOSING && !conn->readers_detached);
    assert(cleanup_count == 0 && response_attempts == replies && !s->close_pending[0]);
    assert(dmesh_flow_readers_detached(objs, (int)(conn - objs->conns)) == DOCA_SUCCESS);
    dmesh_flow_close_advance(conn);
    assert(conn->state == DMESH_CONN_FREE && release_count == 1);
    assert(s->closed[0] && s->close_pending[0] && s->close_status[0] == 0);
    assert(!dmesh_objects_have_live_flows(objs));
    assert(disconnect_attempts == disconnects);
    assert(dmesh_flow_readers_detached(objs, (int)(conn - objs->conns)) == DOCA_ERROR_BAD_STATE);

    /* Slot reuse clears the local reader fence, including session-loss paths. */
    conn = new_test_flow(objs, peer, 1, 2);
    assert(!conn->readers_detached);
    conn->close_requested = false;
    conn->reverse_exported = true;
    conn->flow.mode = DMESH_FLOW_MODE_BACKEND_PULL;
    session_fail(objs, s);
    reset_cleanup();
    dmesh_flow_close_advance(conn);
    sessions_advance(objs);
    assert(conn->state == DMESH_CONN_CLOSING && !conn->quarantined);
    assert(cleanup_count == 0 && disconnect_attempts == disconnects);
    assert(dmesh_flow_readers_detached(objs, (int)(conn - objs->conns)) == DOCA_SUCCESS);
    dmesh_flow_close_advance(conn);
    assert(conn->quarantined && strcmp(cleanup_order, "Q") == 0);
    assert(dmesh_objects_have_live_flows(objs) && release_count == 0);
    free(objs);
}

static void physical_disconnect_test(void)
{
    unsigned char peers[2];
    struct doca_comch_connection *peer = (void *)&peers[0];
    struct doca_comch_connection *other_peer = (void *)&peers[1];
    struct objects *objs = calloc(1, sizeof(*objs));
    assert(objs);
    callback_objects = objs;
    objs->cc_server = (void *)&server_storage;
    objs->connection = peer;
    struct dmesh_session *s = session_get(objs, peer, true);
    s->negotiated = true;
    s->sends_pending = 1;
    unsigned disconnects = disconnect_attempts;
    server_disconnection_event_callback(NULL, peer, 1);
    assert(s->closing && !s->connection && !objs->connection);
    sessions_advance(objs);
    assert(s->occupied && disconnect_attempts == disconnects);
    s->sends_pending = 0;
    sessions_advance(objs);
    assert(!s->occupied && disconnect_attempts == disconnects);
    /* A delayed receive cannot resurrect a retired physical registration. */
    deliver(peer, DMESH_SESSION_HELLO, 0, 0, NULL, 0);
    assert(session_get(objs, peer, false) == NULL);
    sessions_advance(objs);
    assert(disconnect_attempts == disconnects);

    /* Race fallback: local close finds the peer already absent, without a
     * disconnect event. Retire once; never retry in the busy progress loop. */
    s = session_get(objs, peer, true);
    s->closing = true;
    s->negotiated = true;
    disconnect_result = DOCA_ERROR_NOT_CONNECTED;
    sessions_advance(objs);
    assert(!s->occupied && disconnect_attempts == disconnects + 1);
    for (int i = 0; i < 4; ++i) sessions_advance(objs);
    assert(disconnect_attempts == disconnects + 1);

    /* Terminal peer retirement does not release quarantined DMA resources,
     * and another session can safely receive a reused SDK pointer. */
    struct dmesh_conn *conn = new_test_flow(objs, peer, 1, 9);
    s = conn->session;
    conn->close_requested = false;
    conn->reverse_exported = true;
    conn->flow.mode = DMESH_FLOW_MODE_BACKEND_PULL;
    conn->ring_mmap = (void *)(uintptr_t)7;
    struct dmesh_session *other = session_get(objs, other_peer, true);
    other->negotiated = true;
    server_disconnection_event_callback(NULL, peer, 1);
    assert(conn->state == DMESH_CONN_CLOSING && !conn->connection);
    reset_cleanup();
    dmesh_flow_close_advance(conn);
    sessions_advance(objs);
    assert(conn->quarantined && conn->ring_mmap && s->occupied && !s->connection);
    assert(other->connection == other_peer && !other->closing);
    assert(disconnect_attempts == disconnects + 1 && release_count == 0);
    struct dmesh_session *replacement = session_get(objs, peer, true);
    assert(replacement && replacement != s && replacement->connection == peer);
    disconnect_result = DOCA_SUCCESS;
    free(objs);
    callback_objects = NULL;
}

static void checked_close_test(void)
{
    unsigned char peers[2];
    struct doca_comch_connection *peer = (void *)&peers[0];
    struct objects *objs = calloc(1, sizeof(*objs));
    assert(objs);
    callback_objects = objs;
    objs->cc_server = (void *)&server_storage;
    struct dmesh_conn *conn = new_test_flow(objs, peer, 1, 7);
    struct dmesh_conn *sibling = new_test_flow(objs, peer, 2, 1);
    sibling->state = DMESH_CONN_RUNNING;
    struct dmesh_session *s = conn->session;
    conn->dpa_thread = (void *)(uintptr_t)1;
    conn->dpa_comch = (void *)(uintptr_t)2;
    conn->dma_ctx = (void *)(uintptr_t)3;
    conn->buf_arr = (void *)(uintptr_t)4;
    conn->ring_mmap = (void *)(uintptr_t)5;
    conn->sndbuf.mmap = (void *)(uintptr_t)6;
    conn->rcvbuf.mmap = (void *)(uintptr_t)7;
    conn->local_mmap = (void *)(uintptr_t)8;
    conn->dma_buffer = malloc(16);
    conn->pending_metadata = calloc(1, sizeof(*conn->pending_metadata));
    assert(conn->dma_buffer && conn->pending_metadata);

    const char phases[] = "QDMTB";
    for (unsigned i = 0; i < sizeof(phases) - 1; ++i) {
        reset_cleanup();
        failed_phase = phases[i];
        conn->state = DMESH_CONN_CLOSING;
        dmesh_flow_close_advance(conn);
        assert(conn->state == DMESH_CONN_ERROR && s->close_pending[0]);
        assert(s->close_status[0] == EIO && !s->closed[0]);
        assert(conn->ring_mmap && conn->dma_buffer && conn->dpa_thread);
        assert(release_count == 0 && mmap_calls == 0);
        assert(cleanup_count == i + 1);
        assert(memcmp(cleanup_order, phases, i + 1) == 0);
        assert(sibling->state == DMESH_CONN_RUNNING && !s->closing);
    }
    /* A DPA kernel that has not stopped yet keeps the flow closing; each
     * pass rechecks it and nothing past the fence runs. */
    reset_cleanup();
    quiesce_pending = 2;
    conn->state = DMESH_CONN_CLOSING;
    dmesh_flow_close_advance(conn);
    dmesh_flow_close_advance(conn);
    assert(conn->state == DMESH_CONN_CLOSING && strcmp(cleanup_order, "QQ") == 0);
    assert(conn->ring_mmap && conn->dpa_thread && release_count == 0);
    /* An mmap failure preserves its remaining mappings and pool reservation;
     * a retry skips already destroyed mappings and frees once. */
    reset_cleanup();
    failed_mmap = 2;
    conn->state = DMESH_CONN_CLOSING;
    dmesh_flow_close_advance(conn);
    assert(conn->state == DMESH_CONN_ERROR && !conn->ring_mmap);
    assert(conn->sndbuf.mmap && conn->rcvbuf.mmap && conn->local_mmap);
    assert(conn->dma_buffer && release_count == 0);
    reset_cleanup();
    conn->state = DMESH_CONN_CLOSING;
    dmesh_flow_close_advance(conn);
    assert(conn->state == DMESH_CONN_FREE && !conn->dpa_thread && !conn->pending_metadata);
    assert(!conn->sndbuf.mmap && !conn->rcvbuf.mmap && !conn->dma_buffer);
    assert(s->closed[0] && s->close_pending[0] && s->close_status[0] == 0);
    assert(release_count == 1 && sibling->state == DMESH_CONN_RUNNING);
    unsigned disconnects = disconnect_attempts;
    sessions_advance(objs);
    assert(attempted_response.type == DMESH_SESSION_CLOSED && attempted_response.status == 0);
    assert(s->close_pending[0]); /* Full SDK send queue: ACK must retry. */
    assert(disconnect_attempts == disconnects);

    /* No-teardown remains an explicit resource-retaining close error. */
    conn = new_test_flow(objs, peer, 1, 8);
    conn->dpa_thread = (void *)(uintptr_t)1;
    reset_cleanup();
    assert(setenv("DMESH_NO_TEARDOWN", "1", 1) == 0);
    dmesh_flow_close_advance(conn);
    assert(conn->state == DMESH_CONN_ERROR && conn->dpa_thread);
    assert(s->close_status[0] == EOPNOTSUPP && !s->closed[0]);
    assert(cleanup_count == 0 && release_count == 0);
    unsetenv("DMESH_NO_TEARDOWN");

    /* A lost session cannot prove that the host's reverse DPA has stopped.
     * Keep its mappings/slot, but detach its old Comch address after close. */
    conn->close_requested = false;
    conn->reverse_exported = true;
    conn->flow.mode = DMESH_FLOW_MODE_BACKEND_PULL;
    conn->ring_mmap = (void *)(uintptr_t)9;
    session_fail(objs, s);
    reset_cleanup();
    dmesh_flow_close_advance(conn);
    assert(conn->quarantined && conn->ring_mmap && conn->dpa_thread);
    assert(conn->state == DMESH_CONN_ERROR && strcmp(cleanup_order, "Q") == 0);
    assert(release_count == 0 && mmap_calls == 0);
    dmesh_flow_close_advance(sibling); /* Explicitly closed sibling may drain. */
    assert(sibling->state == DMESH_CONN_FREE);
    disconnect_result = DOCA_ERROR_AGAIN;
    sessions_advance(objs);
    assert(s->connection == peer && conn->connection == peer);
    disconnect_result = DOCA_SUCCESS;
    sessions_advance(objs);
    assert(s->occupied && !s->connection && !conn->connection);
    assert(conn->quarantined && conn->ring_mmap);
    struct dmesh_session *replacement = session_get(objs, peer, true);
    assert(replacement && replacement != s); /* no stale pointer alias */

    /* A legacy or silent peer times out without consuming any flow slot. */
    struct doca_comch_connection *peer2 = (void *)&peers[1];
    struct dmesh_session *unnegotiated = session_get(objs, peer2, true);
    unnegotiated->hello_deadline_ms = 0;
    disconnects = disconnect_attempts;
    sessions_advance(objs);
    assert(disconnect_attempts == disconnects + 1 && !unnegotiated->occupied);
    free(objs);
    callback_objects = NULL;
}

int main(void)
{
    receive_test();
    checked_close_test();
    reader_fence_test();
    physical_disconnect_test();
    puts("session_server_test: PASS");
    return 0;
}
