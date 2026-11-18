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
