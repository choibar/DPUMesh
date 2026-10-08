#include "object.h"

#include <string.h>

#include <doca_log.h>
#include <doca_dev.h>
#include <doca_pe.h>
#include <doca_comch.h>

#include "dma.h"

DOCA_LOG_REGISTER(OBJECT);

struct dmesh_conn *
dmesh_flow_get(struct objects *objs, struct doca_comch_connection *connection,
               uint32_t flow_id, uint32_t generation)
{
    if (objs == NULL || connection == NULL || flow_id == 0 ||
        flow_id > DMESH_MAX_CONNECTIONS || generation == 0)
        return NULL;
    for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        struct dmesh_conn *conn = &objs->conns[i];
        if (conn->state != DMESH_CONN_FREE && conn->connection == connection &&
            conn->flow_id == flow_id && conn->generation == generation)
            return conn;
    }
    return NULL;
}

struct dmesh_conn *
dmesh_flow_open(struct objects *objs, struct doca_comch_connection *connection,
                uint32_t flow_id, uint32_t generation)
{
    struct dmesh_conn *conn = dmesh_flow_get(objs, connection, flow_id, generation);
    if (conn != NULL)
        return conn;
    if (objs == NULL || connection == NULL || flow_id == 0 ||
        flow_id > DMESH_MAX_CONNECTIONS || generation == 0)
        return NULL;
    for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i) {
        conn = &objs->conns[i];
        if (conn->state == DMESH_CONN_FREE) {
            memset(conn, 0, sizeof(*conn));
            conn->objs = objs;
            conn->connection = connection;
            conn->flow_id = flow_id;
            conn->generation = generation;
            conn->multiplexed = true;
            conn->state = DMESH_CONN_NEW;
            return conn;
        }
    }
    return NULL;
}

void
dmesh_flow_close_session(struct objects *objs, struct doca_comch_connection *connection)
{
    if (objs == NULL || connection == NULL)
        return;
    for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
        if (objs->conns[i].state != DMESH_CONN_FREE &&
            objs->conns[i].connection == connection)
            objs->conns[i].state = DMESH_CONN_CLOSING;
}

doca_error_t
dmesh_flow_readers_detached(struct objects *objs, int slot)
{
    if (objs == NULL || slot < 0 || slot >= DMESH_MAX_CONNECTIONS)
        return DOCA_ERROR_INVALID_VALUE;
    struct dmesh_conn *conn = &objs->conns[slot];
    if (conn->state != DMESH_CONN_CLOSING)
        return DOCA_ERROR_BAD_STATE;
    conn->readers_detached = true;
    return DOCA_SUCCESS;
}

bool
dmesh_objects_have_live_flows(const struct objects *objs)
{
    if (objs != NULL)
        for (int i = 0; i < DMESH_MAX_CONNECTIONS; ++i)
            if (objs->conns[i].state != DMESH_CONN_FREE)
                return true;
    return false;
}

struct dmesh_conn *
dmesh_conn_get(struct objects *objs, struct doca_comch_connection *connection)
{
    int i;

    if (connection == NULL)
        return NULL;

    for (i = 0; i < DMESH_MAX_CONNECTIONS; i++) {
        if (objs->conns[i].state != DMESH_CONN_FREE &&
            objs->conns[i].connection == connection)
            return &objs->conns[i];
    }
    return NULL;
}

struct dmesh_conn *
dmesh_conn_open(struct objects *objs, struct doca_comch_connection *connection)
{
    struct dmesh_conn *conn;
    int i;

    conn = dmesh_conn_get(objs, connection);
    if (conn != NULL)
        return conn;

    for (i = 0; i < DMESH_MAX_CONNECTIONS; i++) {
        conn = &objs->conns[i];
        if (conn->state == DMESH_CONN_FREE) {
            memset(conn, 0, sizeof(*conn));
            conn->objs = objs;
            conn->connection = connection;
            conn->state = DMESH_CONN_NEW;
            DOCA_LOG_INFO("Bound connection %p to slot %d", (void *)connection, i);
            return conn;
        }
    }

    DOCA_LOG_WARN("No free connection slot (max %d)", DMESH_MAX_CONNECTIONS);
    return NULL;
}

void
dmesh_conn_close(struct objects *objs, struct doca_comch_connection *connection)
{
    struct dmesh_conn *conn = dmesh_conn_get(objs, connection);

    if (conn == NULL)
        return;

    /* NOTE: consumer/msgq/mmaps/buf_arr of this slot are not torn down yet
     * (no full per-connection teardown exists); the slot is only unbound. */
    conn->state = DMESH_CONN_FREE;
    conn->connection = NULL;
    DOCA_LOG_INFO("Unbound connection %p from slot %ld", (void *)connection, conn - objs->conns);
}

/*
 * Stop a ctx and progress until it reports IDLE. doca_ctx_stop() is
 * asynchronous - it returns DOCA_ERROR_IN_PROGRESS while the ctx settles, and
 * the ctx only leaves STOPPING once its pending tasks have been flushed by a
 * progress call. Both engines are progressed because the comch endpoint shares
 * a device with the per-connection consumers on consumer_pe; a consumer that
 * has not idled keeps the endpoint from settling. Bounded so shutdown can
 * never hang.
 */
static void
stop_comch_ctx(struct doca_ctx *ctx, struct objects *objs)
{
    enum doca_ctx_states state;
    doca_error_t result;
    int spins = 0;

    if (ctx == NULL)
        return;
    if (doca_ctx_get_state(ctx, &state) != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
        return;

    result = doca_ctx_stop(ctx);
    if (result != DOCA_SUCCESS && result != DOCA_ERROR_IN_PROGRESS) {
        DOCA_LOG_ERR("Failed to stop comch context: %s", doca_error_get_descr(result));
        return;
    }

    while (spins++ < 100000) {
        if (doca_ctx_get_state(ctx, &state) != DOCA_SUCCESS || state == DOCA_CTX_STATE_IDLE)
            return;
        if (objs->pe != NULL)
            (void)doca_pe_progress(objs->pe);
        if (objs->consumer_pe != NULL)
            (void)doca_pe_progress(objs->consumer_pe);
    }

    DOCA_LOG_ERR("Comch context did not reach IDLE (state=%d) within bound; "
                 "device objects will leak until the driver reclaims them", state);
}

doca_error_t
cleanup_objects(struct objects *objs)
{
    doca_error_t result;
    int i;

    if (objs == NULL) return DOCA_SUCCESS;
    if (objs->release_dpa != NULL) {
        result = objs->release_dpa(objs);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Retaining worker resources: DPA cleanup failed: %s", doca_error_get_name(result));
            return result;
        }
    }
    /* tear down each connection's private DMA engine */
    for (i = 0; i < DMESH_MAX_CONNECTIONS; i++)
        cleanup_dma_tasks(&objs->conns[i]);

    /* The comch endpoint must be STOPPED before it can be destroyed. Skipping
     * this made destroy fail with BAD_STATE ("associated context is still
     * running"), which then cascaded: the ctx stayed attached to the PE so
     * doca_pe_destroy() failed, the PD stayed busy (devx_obj_destroy EBUSY)
     * and doca_dev_close() failed with IN_USE. The channel service object then
     * outlived the process and the next run could not create one
     * (devx syndrome 0x64b4 -> DOCA_ERROR_CONNECTION_ABORTED). */
    if (objs->cc_server != NULL) {
        if (objs->is_server) {
            stop_comch_ctx(doca_comch_server_as_ctx(objs->cc_server), objs);
            result = doca_comch_server_destroy(objs->cc_server);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to destroy cc server properly with error = %s",
                             doca_error_get_name(result));
                return result;
            }
        } else {
            /* cc_server/cc_client share a union - destroying a client through
             * the server API leaks the endpoint. */
            stop_comch_ctx(doca_comch_client_as_ctx(objs->cc_client), objs);
            result = doca_comch_client_destroy(objs->cc_client);
            if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to destroy cc client properly with error = %s",
                             doca_error_get_name(result));
                return result;
            }
        }
        objs->cc_server = NULL;
        objs->connection = NULL;
    }

    if (objs->is_server && objs->consumer_pe != NULL) {
        result = doca_pe_destroy(objs->consumer_pe);
        if (result != DOCA_SUCCESS) return result;
        objs->consumer_pe = NULL;
    }

    if (objs->pe) {
        result = doca_pe_destroy(objs->pe);
        if(result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to destroy pe properly with error = %s", doca_error_get_name(result));
            return result;
        }
        objs->pe = NULL;
    }

    if (objs->rep_dev) {
        result = doca_dev_rep_close(objs->rep_dev);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to close rep device properly with error = %s", doca_error_get_name(result));
            return result;
        }
        objs->rep_dev = NULL;
    }

    if (objs->dev) {
        result = doca_dev_close(objs->dev);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to close device properly with error = %s", doca_error_get_name(result));
            return result;
        }
        objs->dev = NULL;
    }
    return DOCA_SUCCESS;
}
