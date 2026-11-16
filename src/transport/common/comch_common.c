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

/* One region's PCI export descriptor, copied into the metadata message. */
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
        !DMESH_FLOW_MODE_VALID(metadata_msg->flow.mode))
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
