#include "ring.h"
#include <stdlib.h>
#include <string.h>
#include <doca_log.h>
#include "dpa_common.h"
#include "object.h"
#include "buffer.h"
#include "comch_common.h"

DOCA_LOG_REGISTER(RING);

#define DMA_RING_LOG_INTERVAL 1024

/* Allocate a standalone descriptor ring on `dev` (mmap'd for PCI export) and
 * return it: a host flow's forward ring, which the DPU's DPA polls. */
doca_error_t
alloc_dma_ring(struct dma_ring **ring_out, struct doca_dev *dev, size_t size)
{
    doca_error_t result;
    struct dma_ring *ring;

    ring = (struct dma_ring *)malloc(sizeof(struct dma_ring));
    if (ring == NULL)
        return DOCA_ERROR_NO_MEMORY;

    ring->size = size;
    ring->head = 0;
    ring->tail = 0;
    ring->buffer = NULL;
    ring->ctrl = NULL;
    ring->descs = NULL;

    result = alloc_buffer_and_set_mmap(&ring->mmap, dev, &ring->buffer,
                           sizeof(struct dma_ring_ctrl) + ring->size * sizeof(struct dma_desc),
                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to allocate reverse DMA ring: %s", doca_error_get_descr(result));
        free(ring);
        return result;
    }

    memset(ring->buffer, 0, sizeof(struct dma_ring_ctrl) + ring->size * sizeof(struct dma_desc));
    ring->ctrl = (struct dma_ring_ctrl *)ring->buffer;
    ring->descs = (struct dma_desc *)((uint8_t *)ring->buffer + sizeof(struct dma_ring_ctrl));

    *ring_out = ring;
    return DOCA_SUCCESS;
}

/* Free a ring allocated by alloc_dma_ring (destroys its mmap + buffer). */
void
free_dma_ring(struct dma_ring *ring)
{
    if (ring == NULL)
        return;
    if (ring->mmap != NULL)
        destroy_mmap_and_free_buffer(ring->mmap, ring->buffer);
    free(ring);
}

struct dma_desc *get_next_dma_desc(struct dma_ring *ring)
{
    uint64_t ring_mask = ring->size - 1;

    while (ring->head - ring->ctrl->consumer_head >= ring->size) {
        // DOCA_LOG_INFO("Host ring full: producer_tail=%lu, local_head=%lu, consumer_head=%lu",
        //               ring->ctrl->producer_tail, ring->head, ring->ctrl->consumer_head);
    }

    struct dma_desc *desc = ring->descs + (ring->head & ring_mask);
    ring->head++;
    // DOCA_LOG_INFO("Get next DMA desc - head: %u, tail: %u, desc: %p", ring->head, ring->tail, desc);
    return desc;
}

void commit_dma_desc(struct dma_ring *ring)
{
    __sync_synchronize();
    ring->ctrl->producer_tail = ring->head;

    if ((ring->head & (DMA_RING_LOG_INTERVAL - 1)) == 0) {
        DOCA_LOG_INFO("Host ring publish: producer_tail=%lu, consumer_head=%lu",
                      ring->ctrl->producer_tail, ring->ctrl->consumer_head);
    }
}
