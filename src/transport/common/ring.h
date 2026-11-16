#ifndef RING_H
#define RING_H

#include <stdint.h>
#include <stddef.h>
#include <doca_error.h>

#define DMA_RING_SIZE 1024

struct dma_desc;
struct dma_ring_ctrl;
struct doca_mmap;
struct objects;

struct dma_ring {
    struct doca_mmap *mmap;
    uint64_t head;
    uint64_t tail;
    uint32_t size;
    void *buffer;
    struct dma_ring_ctrl *ctrl;
    struct dma_desc *descs;
};

struct doca_dev;

/* Standalone ring helpers: allocate/free a ring on an explicit device (a host
 * flow's forward ring). */
doca_error_t alloc_dma_ring(struct dma_ring **ring_out, struct doca_dev *dev, size_t size);
void free_dma_ring(struct dma_ring *ring);

struct dma_desc *get_next_dma_desc(struct dma_ring *ring);
void commit_dma_desc(struct dma_ring *ring);
#endif /* RING_H */
