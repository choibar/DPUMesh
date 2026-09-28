#include "comch_common.h"
#include "session_protocol.h"

#include <doca_log.h>
#include <doca_error.h>
#include <doca_mmap.h>
#include <doca_dpa.h>

#include "object.h"
#include "comch_client.h"
#include "comch_server.h"
#include "dpa_common.h"
#include "dpa.h"
#include "ring.h"
#include "buffer.h"
#include "dma.h"          /* BUFFER_SIZE */
DOCA_LOG_REGISTER(COMCH_COMMON);

_Static_assert(sizeof(struct dmesh_export_metadata_msg) <= DMESH_SESSION_MAX_PAYLOAD,
               "OPEN metadata must fit a session frame");
_Static_assert(sizeof(struct dmesh_export_rcv_ring_msg) <= DMESH_SESSION_MAX_PAYLOAD,
               "reverse metadata must fit a session frame");

/*
 * Export all host-side DMA metadata (ring + send buffer + receive buffer) to
 * the DPU in a single control-path message. Must be called after
 * setup_dma_ring() and both init_dmesh_buffer() calls.
 */
static doca_error_t
copy_export(struct doca_mmap *mmap, struct doca_dev *dev,
            uint8_t *destination, size_t capacity, size_t *length)
{
    const void *data;
    size_t len;
    doca_error_t result = doca_mmap_export_pci(mmap, dev, &data, &len);
    if (result != DOCA_SUCCESS)
        return result;
    if (len == 0 || len > capacity)
        return DOCA_ERROR_INVALID_VALUE;
    memcpy(destination, data, len);
    *length = len;
    return DOCA_SUCCESS;
}

doca_error_t
dmesh_build_dma_metadata(struct doca_dev *dev, const struct dma_ring *ring,
                         const struct dmesh_buffer *sndbuf, const struct dmesh_buffer *rcvbuf,
                         const struct dmesh_flow_id *flow, struct dmesh_export_metadata_msg *msg)
{
    if (!dev || !msg || !ring || !sndbuf || !rcvbuf || !flow)
        return DOCA_ERROR_INVALID_VALUE;
    memset(msg, 0, sizeof(*msg));
    msg->type = DMESH_MSG_EXPORT_METADATA;
    msg->flow = *flow;
    msg->ring_buf = ring->buffer;
    msg->ring_buf_len = sizeof(struct dma_ring_ctrl) + ring->size * sizeof(struct dma_desc);
    msg->sndbuf = sndbuf->buf;
    msg->sndbuf_len = sndbuf->size;
    msg->rcvbuf = rcvbuf->buf;
    msg->rcvbuf_len = rcvbuf->size;
    doca_error_t result = copy_export(ring->mmap, dev,
                                     msg->ring_desc, sizeof(msg->ring_desc), &msg->ring_desc_len);
    if (result == DOCA_SUCCESS)
        result = copy_export(sndbuf->mmap, dev,
                             msg->snd_desc, sizeof(msg->snd_desc), &msg->snd_desc_len);
    if (result == DOCA_SUCCESS)
        result = copy_export(rcvbuf->mmap, dev,
                             msg->rcv_desc, sizeof(msg->rcv_desc), &msg->rcv_desc_len);
    return result;
}

doca_error_t
build_dma_metadata(struct objects *objs, struct dmesh_export_metadata_msg *msg)
{
    if (!objs) return DOCA_ERROR_INVALID_VALUE;
    return dmesh_build_dma_metadata(objs->dev, objs->dma_ring, &objs->sndbuf, &objs->rcvbuf, &objs->flow, msg);
}

doca_error_t
export_dma_metadata(struct objects *objs)
{
    struct dmesh_export_metadata_msg msg;
    doca_error_t result = build_dma_metadata(objs, &msg);
    if (result != DOCA_SUCCESS)
        return result;
    return client_send_msg(objs, (const char *)&msg, sizeof(msg));
}

/*
 * DPU-side handler for the combined metadata message: creates the ring mmap
 * and both remote buffer mmaps in one shot.
 */
doca_error_t
process_export_metadata_msg(struct dmesh_conn *conn, struct dmesh_export_metadata_msg *metadata_msg)
{
    struct objects *objs = conn->objs;
    doca_error_t result;

    if (!metadata_msg || metadata_msg->type != DMESH_MSG_EXPORT_METADATA ||
        metadata_msg->ring_desc_len == 0 || metadata_msg->ring_desc_len > sizeof(metadata_msg->ring_desc) ||
        metadata_msg->snd_desc_len == 0 || metadata_msg->snd_desc_len > sizeof(metadata_msg->snd_desc) ||
        metadata_msg->rcv_desc_len == 0 || metadata_msg->rcv_desc_len > sizeof(metadata_msg->rcv_desc) ||
        !metadata_msg->ring_buf || !metadata_msg->sndbuf || !metadata_msg->rcvbuf ||
        metadata_msg->ring_buf_len != sizeof(struct dma_ring_ctrl) + DMA_RING_SIZE * sizeof(struct dma_desc) ||
        metadata_msg->sndbuf_len == 0 || metadata_msg->rcvbuf_len != BUFFER_SIZE ||
        metadata_msg->flow.mode > DMESH_FLOW_MODE_BACKEND_PULL)
        return DOCA_ERROR_INVALID_VALUE;
    if (conn->ring_mmap || conn->sndbuf.mmap || conn->rcvbuf.mmap)
        return DOCA_ERROR_BAD_STATE;

    conn->flow = metadata_msg->flow;
    conn->flow.src_workload[sizeof(conn->flow.src_workload) - 1] = '\0';

    DOCA_LOG_INFO("Received export metadata message: flow %u.%u.%u.%u:%u -> %u.%u.%u.%u:%u (%s), "
                  "ring_buf=%p, ring_buf_len=%zu, sndbuf_len=%zu, rcvbuf_len=%zu",
                  conn->flow.src_ip & 0xff, (conn->flow.src_ip >> 8) & 0xff,
                  (conn->flow.src_ip >> 16) & 0xff, (conn->flow.src_ip >> 24) & 0xff,
                  conn->flow.src_port,
                  conn->flow.dst_ip & 0xff, (conn->flow.dst_ip >> 8) & 0xff,
                  (conn->flow.dst_ip >> 16) & 0xff, (conn->flow.dst_ip >> 24) & 0xff,
                  conn->flow.dst_port,
                  conn->flow.src_workload,
                  metadata_msg->ring_buf, metadata_msg->ring_buf_len,
                  metadata_msg->sndbuf_len, metadata_msg->rcvbuf_len);

    /* DMA ring */
    result = doca_mmap_create_from_export(NULL, metadata_msg->ring_desc,
                                            metadata_msg->ring_desc_len,
                                            objs->dev, &conn->ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for DMA ring from export desc with error = %s",
                     doca_error_get_name(result));
        return result;
    }

    /* send buffer */
    result = doca_mmap_create_from_export(NULL, metadata_msg->snd_desc,
                                            metadata_msg->snd_desc_len,
                                            objs->dev, &conn->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for send buffer from export desc with error = %s",
                     doca_error_get_name(result));
        goto destroy_ring_mmap;
    }
    conn->sndbuf.buf = metadata_msg->sndbuf;
    conn->sndbuf.size = metadata_msg->sndbuf_len;

    /* receive buffer */
    result = doca_mmap_create_from_export(NULL, metadata_msg->rcv_desc,
                                            metadata_msg->rcv_desc_len,
                                            objs->dev, &conn->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create remote mmap for receive buffer from export desc with error = %s",
                     doca_error_get_name(result));
        goto destroy_sndbuf_mmap;
    }
    conn->rcvbuf.buf = metadata_msg->rcvbuf;
    conn->rcvbuf.size = metadata_msg->rcvbuf_len;

    result = doca_mmap_start(conn->sndbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start send buffer mmap with error = %s", doca_error_get_name(result));
        goto destroy_rcvbuf_mmap;
    }

    result = doca_mmap_start(conn->rcvbuf.mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to start receive buffer mmap with error = %s", doca_error_get_name(result));
        goto destroy_rcvbuf_mmap;
    }

    DOCA_LOG_INFO("Successfully created remote mmaps for DMA ring, send and receive buffers");

    return DOCA_SUCCESS;

destroy_rcvbuf_mmap:
    doca_mmap_destroy(conn->rcvbuf.mmap);
    conn->rcvbuf.mmap = NULL;
destroy_sndbuf_mmap:
    doca_mmap_destroy(conn->sndbuf.mmap);
    conn->sndbuf.mmap = NULL;
destroy_ring_mmap:
    doca_mmap_destroy(conn->ring_mmap);
    conn->ring_mmap = NULL;
    return result;
}

doca_error_t
process_dpa_comp_msg(struct objects *objs, struct dmesh_dpa_comp_msg *dpa_comp_msg)
{
    objs->remote_dpa_producer = dpa_comp_msg->dpa_producer;
    objs->remote_dpa_producer_comp = dpa_comp_msg->dpa_producer_comp;

    return DOCA_SUCCESS;
}

/* DPU side: allocate this connection's reverse rcv_ring + tx_staging (once) and
 * ship their PCI export descriptors to the host. The mirror of
 * export_dma_metadata, but DPU -> host (server -> client). */
doca_error_t
export_rcv_ring_metadata(struct dmesh_conn *conn)
{
    struct objects *objs = conn->objs;
    struct dmesh_export_rcv_ring_msg *msg;
    doca_error_t result;
    const void *export_desc;
    size_t export_desc_len;

    if (conn->reverse_exported)
        return DOCA_SUCCESS;

    if (conn->rcv_ring == NULL) {
        result = alloc_dma_ring(&conn->rcv_ring, objs->dev, DMA_RING_SIZE);
        if (result != DOCA_SUCCESS)
            return result;
    }
    if (conn->tx_staging == NULL) {
        result = alloc_buffer_and_set_mmap(&conn->tx_staging_mmap, objs->dev,
                                           &conn->tx_staging, BUFFER_SIZE,
                                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
        if (result != DOCA_SUCCESS)
            return result;
        conn->tx_staging_len = BUFFER_SIZE;
        conn->tx_pos = 0;
    }

    msg = (struct dmesh_export_rcv_ring_msg *)malloc(sizeof(*msg));
    if (msg == NULL)
        return DOCA_ERROR_NO_MEMORY;
    memset(msg, 0, sizeof(*msg));
    msg->type = DMESH_MSG_EXPORT_RCV_RING;

    /* descriptor ring */
    result = doca_mmap_export_pci(conn->rcv_ring->mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export reverse rcv_ring mmap: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }
    msg->ring_buf = conn->rcv_ring->buffer;
    msg->ring_buf_len = sizeof(struct dma_ring_ctrl) + conn->rcv_ring->size * sizeof(struct dma_desc);
    if (export_desc_len == 0 || export_desc_len > sizeof(msg->ring_desc)) {
        free(msg);
        return DOCA_ERROR_INVALID_VALUE;
    }
    msg->ring_desc_len = export_desc_len;
    memcpy(msg->ring_desc, export_desc, export_desc_len);

    /* tx staging buffer */
    result = doca_mmap_export_pci(conn->tx_staging_mmap, objs->dev, &export_desc, &export_desc_len);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export reverse tx_staging mmap: %s", doca_error_get_descr(result));
        free(msg);
        return result;
    }
    msg->tx_staging = conn->tx_staging;
    msg->tx_staging_len = conn->tx_staging_len;
    if (export_desc_len == 0 || export_desc_len > sizeof(msg->tx_desc)) {
        free(msg);
        return DOCA_ERROR_INVALID_VALUE;
    }
    msg->tx_desc_len = export_desc_len;
    memcpy(msg->tx_desc, export_desc, export_desc_len);

    if (conn->multiplexed)
        result = server_send_flow_msg(conn, DMESH_SESSION_REVERSE_EXPORT, msg, sizeof(*msg), 0);
    else
        result = server_send_msg_conn(objs, conn->connection, (const char *)msg, sizeof(*msg));
    if (result != DOCA_SUCCESS) {
        free(msg);
        return result;
    }

    conn->reverse_exported = true;
    DOCA_LOG_INFO("Exported reverse rcv_ring (buf=%p len=%zu) + tx_staging (buf=%p len=%zu) to host",
                  msg->ring_buf, msg->ring_buf_len, msg->tx_staging, msg->tx_staging_len);
    free(msg);
    return DOCA_SUCCESS;
}

/* Host side: stash the DPU's export descriptors. The actual mmap import + the
 * reverse DPA thread are set up by the host worker (setup_reverse_dpa) once it
 * has opened the reverse PCI function (94:00.0) - a flexio process is
 * one-per-function and the DPU proxy already holds this host's 94:00.1, so the
 * reverse DPA must live on a different function and import on that device. */
doca_error_t
process_export_rcv_ring_msg(struct objects *objs, struct dmesh_export_rcv_ring_msg *msg)
{
    doca_error_t result = dmesh_validate_reverse_metadata(msg);
    if (result != DOCA_SUCCESS) return result;
    objs->rev_msg = *msg;
    objs->reverse_ready = true;
    DOCA_LOG_INFO("Stashed reverse rcv_ring (%p, %zu) + tx_staging (%p, %zu) export from DPU",
                  msg->ring_buf, msg->ring_buf_len, msg->tx_staging, msg->tx_staging_len);
    return DOCA_SUCCESS;
}

doca_error_t dmesh_validate_reverse_metadata(const struct dmesh_export_rcv_ring_msg *msg)
{
    if (!msg || msg->type != DMESH_MSG_EXPORT_RCV_RING ||
        msg->ring_desc_len == 0 || msg->ring_desc_len > sizeof(msg->ring_desc) ||
        msg->tx_desc_len == 0 || msg->tx_desc_len > sizeof(msg->tx_desc) ||
        !msg->ring_buf || !msg->tx_staging ||
        msg->ring_buf_len != sizeof(struct dma_ring_ctrl) + DMA_RING_SIZE * sizeof(struct dma_desc) ||
        msg->tx_staging_len != BUFFER_SIZE)
        return DOCA_ERROR_INVALID_VALUE;
    return DOCA_SUCCESS;
}
