#ifndef DMESH_DPA_BENCH_H
#define DMESH_DPA_BENCH_H

#include "dpa_common.h"

#define DMESH_DPA_BENCH_SCRATCH_SIZE (64u * 1024u)

/* Benchmark-only TLS state. The native state is a prefix so the common
 * stop/completion fence can inspect it using the same offsets. Native
 * threads allocate only dpa_thread_ctx, never this larger state. */
struct dpa_bench_state {
    struct dpa_thread_ctx dma;
    uint64_t host_addr;
    uint64_t scratch;
    uint32_t host_size;
    uint32_t mode; /* 1: DMA throughput, 2: latency, 3: HPACK walk,
                   * 4: HPACK decode, 5: decode + re-encode */
    uint32_t msg_size;
    uint32_t num_ops;
};

#endif
