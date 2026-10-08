/* Standalone DMA and HPACK measurements; not part of the native DMA loop. */
#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "dpaintrin.h"
#include "dpa_common.h"
#include "dpa_bench.h"
#include "../hpack_walk.h"
#include "dsb_blocks.h"
#include "hpack_term.h"

_Static_assert(sizeof(struct dpa_bench_state) == 128, "benchmark DPA argument layout");

#define DMA_FLUSH_BATCH 32

/*
 * producer_dma_copy microbenchmark: copy bench_msg_size bytes from the host
 * sndbuf to the DPU staging buffer bench_num_ops times, straight from this DPA
 * thread. Throughput mode pipelines copies (credit-gated, doorbell every
 * DMA_FLUSH_BATCH); latency mode serializes: FLUSH each copy and poll the
 * producer completion before issuing the next. Results are reported in
 * __dpa_thread_time() ticks; the DPU-side recv counters give the wall-clock
 * cross-check.
 */
static void run_dma_copy_bench(struct dpa_bench_state *a)
{
    doca_dpa_dev_comch_producer_t producer = a->dma.dpa_producer;
    doca_dpa_dev_completion_element_t comp;
    struct comch_dma_comp_msg msg = {0};
    uint32_t size = a->msg_size;
    uint32_t n = a->num_ops;
    uint64_t dst_pos = 0, src_pos = 0;
    uint64_t t0, t1;
    uint32_t i;

    msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    msg.length = size;
    msg.count = 1;

    DOCA_DPA_DEV_LOG_INFO("BENCH start: mode=%u size=%u ops=%u host=0x%lx/%u dpu=0x%lx/%u\n",
                          a->mode, size, n,
                          a->host_addr, a->host_size,
                          a->dma.src_addr, a->dma.buf_size);

    if (a->mode == 1) {
        /* throughput: keep the pipe full, doorbell in batches; the final copy
         * carries a completion report so t1 covers full drain */
        t0 = __dpa_thread_time();
        for (i = 0; i < n; i++) {
            uint64_t flags;

            if (i == n - 1)
                flags = DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH; /* reported */
            else if ((i & (DMA_FLUSH_BATCH - 1)) == DMA_FLUSH_BATCH - 1)
                flags = DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS |
                        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH;
            else
                flags = DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS;

            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
            }

            msg.pos = (uint32_t)dst_pos;
            doca_dpa_dev_comch_producer_dma_copy(producer,
                                                 /*consumer_id=*/1,
                                                 a->dma.dpu_mmap,
                                                 a->dma.src_addr + dst_pos,
                                                 a->dma.host_mmap,
                                                 a->host_addr + src_pos,
                                                 size,
                                                 (uint8_t *)&msg,
                                                 sizeof(struct comch_dma_comp_msg),
                                                 flags);
            a->dma.dma_submitted++;

            dst_pos += size;
            if (dst_pos + size > a->dma.buf_size)
                dst_pos = 0;
            src_pos += size;
            if (src_pos + size > a->host_size)
                src_pos = 0;
        }
        while (doca_dpa_dev_get_completion(a->dma.dpa_producer_comp, &comp) == 0) {
        }
        t1 = __dpa_thread_time();
        doca_dpa_dev_completion_ack(a->dma.dpa_producer_comp, 1);
        DOCA_DPA_DEV_LOG_INFO("BENCH_TPUT size=%u ops=%u ticks=%lu\n", size, n, t1 - t0);
    } else {
        /* latency: one copy at a time, completion-polled. Per-op deltas use
         * the cycle counter; the 1 MHz thread timer over the whole run
         * calibrates cycles -> ns. */
        uint64_t total = 0, tmin = (uint64_t)-1, tmax = 0, d;
        uint64_t run_t0 = __dpa_thread_time();

        for (i = 0; i < n; i++) {
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
            }

            t0 = __dpa_thread_cycles();
            msg.pos = (uint32_t)dst_pos;
            doca_dpa_dev_comch_producer_dma_copy(producer,
                                                 /*consumer_id=*/1,
                                                 a->dma.dpu_mmap,
                                                 a->dma.src_addr + dst_pos,
                                                 a->dma.host_mmap,
                                                 a->host_addr + src_pos,
                                                 size,
                                                 (uint8_t *)&msg,
                                                 sizeof(struct comch_dma_comp_msg),
                                                 DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            a->dma.dma_submitted++;
            while (doca_dpa_dev_get_completion(a->dma.dpa_producer_comp, &comp) == 0) {
            }
            t1 = __dpa_thread_cycles();
            doca_dpa_dev_completion_ack(a->dma.dpa_producer_comp, 1);

            d = t1 - t0;
            total += d;
            if (d < tmin)
                tmin = d;
            if (d > tmax)
                tmax = d;

            dst_pos += size;
            if (dst_pos + size > a->dma.buf_size)
                dst_pos = 0;
            src_pos += size;
            if (src_pos + size > a->host_size)
                src_pos = 0;
        }
        DOCA_DPA_DEV_LOG_INFO("BENCH_LAT size=%u ops=%u total_cycles=%lu min=%lu max=%lu run_us=%lu\n",
                              size, n, total, tmin, tmax,
                              __dpa_thread_time() - run_t0);
    }
}

/* Selective-HPACK-walk microbenchmark (bench_mode 3): measures the pure
 * compute cost of the connection-state HPACK walker on a DPA EU, using real
 * header blocks captured from the h2load workload (11B steady-state block =
 * 99.998% of traffic; 39B first block exercises dynamic-table inserts +
 * huffman). No DMA involved — this isolates walk ns/block so it can be
 * compared with the ARM figure from the same walker source. */
static const unsigned char hpack_block_first[39] = {
    0x04,0x85,0x60,0xf1,0xf4,0x0f,0x5f,0x86,0x41,0x8b,0x08,0x9d,0x5c,0x0b,
    0x81,0x70,0xdc,0x0b,0x60,0x00,0x7f,0x82,0x7a,0x8f,0x9c,0x54,0x1c,0x72,
    0x29,0x54,0xd3,0xa5,0x35,0x89,0x80,0xae,0xd3,0x2b,0x83};
static const unsigned char hpack_block_steady[11] = {
    0x04,0x85,0x60,0xf1,0xf4,0x0f,0x5f,0x86,0xbf,0x82,0xbe};

#define DSB_BLK(i) (dsb_steady_data + dsb_steady_off[i]), \
                   (int)(dsb_steady_off[(i) + 1] - dsb_steady_off[(i)])
#define DSB_NI(i)  (dsb_ni_steady_data + dsb_ni_steady_off[i]), \
                   (int)(dsb_ni_steady_off[(i) + 1] - dsb_ni_steady_off[(i)])

/* Program globals are shared by every worker using the same DPA context.
 * Keep benchmark state in each thread's heap allocation, off its small stack. */
union hpack_bench_scratch {
    struct { struct hw_state st; struct hw_out out; } walk;
    struct {
        struct ht_table dec, enc;
        struct ht_field fields[HT_MAX_FIELDS];
        unsigned char blk[512];
    } term;
};
_Static_assert(sizeof(union hpack_bench_scratch) <= DMESH_DPA_BENCH_SCRATCH_SIZE,
               "increase per-thread HPACK scratch allocation");

/* bench_mode 3: selective walk over the DSB gRPC blocks (256-block cycle,
 * churning dynamic table). Old h2load blocks kept for reference. */
static void run_hpack_walk_bench(struct dpa_bench_state *a)
{
    union hpack_bench_scratch *scratch = (void *)a->scratch;
    if (!scratch) return;
    struct hw_state *st = &scratch->walk.st;
    struct hw_out *out = &scratch->walk.out;
    uint32_t n = a->num_ops;
    uint64_t t0, t1, c0, c1;
    uint32_t i;
    int rc;
    volatile unsigned int sink = 0;

    hw_init(st);
    rc = hw_walk(dsb_first, sizeof(dsb_first), st, out, 0);
    DOCA_DPA_DEV_LOG_INFO("HPACK_BENCH dsb first rc=%d have=%x plen=%u\n",
                          rc, out->have, out->plen);

    t0 = __dpa_thread_time();
    c0 = __dpa_thread_cycles();
    for (i = 0; i < n; i++) {
        if (i % DSB_STEADY_N == 0) {   /* connection replay boundary */
            hw_init(st);
            hw_walk(dsb_first, sizeof(dsb_first), st, out, 0);
        }
        rc = hw_walk(DSB_BLK(i % DSB_STEADY_N), st, out, 0);
        if (rc) { DOCA_DPA_DEV_LOG_INFO("HPACK_BENCH walk FAIL i=%u\n", i); return; }
        sink += out->plen;
    }
    c1 = __dpa_thread_cycles();
    t1 = __dpa_thread_time();
    DOCA_DPA_DEV_LOG_INFO("HPACK_BENCH dsb SELECTIVE ops=%u cycles=%lu us=%lu have=%x sink=%u\n",
                          n, c1 - c0, t1 - t0, out->have, sink);

    /* NI variant: trace-id without indexing */
    t0 = __dpa_thread_time();
    c0 = __dpa_thread_cycles();
    for (i = 0; i < n; i++) {
        if (i % DSB_STEADY_N == 0) {
            hw_init(st);
            hw_walk(dsb_ni_first, sizeof(dsb_ni_first), st, out, 0);
        }
        rc = hw_walk(DSB_NI(i % DSB_STEADY_N), st, out, 0);
        if (rc) { DOCA_DPA_DEV_LOG_INFO("HPACK_BENCH NI walk FAIL i=%u\n", i); return; }
        sink += out->plen;
    }
    c1 = __dpa_thread_cycles();
    t1 = __dpa_thread_time();
    DOCA_DPA_DEV_LOG_INFO("HPACK_BENCH dsb NI-SELECTIVE ops=%u cycles=%lu us=%lu sink=%u\n",
                          n, c1 - c0, t1 - t0, sink);
}

/* bench_mode 4: FULL decode (materialize all 8 fields).
 * bench_mode 5: decode + re-encode (termination baseline). */
static void run_hpack_term_bench(struct dpa_bench_state *a, int reencode)
{
    union hpack_bench_scratch *scratch = (void *)a->scratch;
    if (!scratch) return;
    struct ht_table *dec = &scratch->term.dec, *enc = &scratch->term.enc;
    struct ht_field *fields = scratch->term.fields;
    unsigned char *blk = scratch->term.blk;
    uint32_t n = a->num_ops;
    uint64_t t0, t1, c0, c1;
    uint32_t i;
    int rc, nf = 0, el = 0;
    volatile int sink = 0;

    ht_init(dec); ht_init(enc);
    rc = ht_decode(dsb_first, sizeof(dsb_first), dec, fields, &nf);
    if (reencode)
        el = ht_encode(fields, nf, enc, blk, sizeof(scratch->term.blk));
    DOCA_DPA_DEV_LOG_INFO("HPACK_TERM first rc=%d nf=%d el=%d mode=%d\n",
                          rc, nf, el, reencode);

    t0 = __dpa_thread_time();
    c0 = __dpa_thread_cycles();
    for (i = 0; i < n; i++) {
        if (i % DSB_STEADY_N == 0) {   /* connection replay boundary */
            ht_init(dec); ht_init(enc);
            ht_decode(dsb_first, sizeof(dsb_first), dec, fields, &nf);
            if (reencode)
                ht_encode(fields, nf, enc, blk, sizeof(scratch->term.blk));
        }
        rc = ht_decode(DSB_BLK(i % DSB_STEADY_N), dec, fields, &nf);
        if (rc) { DOCA_DPA_DEV_LOG_INFO("HPACK_TERM decode FAIL i=%u\n", i); return; }
        if (reencode) {
            el = ht_encode(fields, nf, enc, blk, sizeof(scratch->term.blk));
            if (el <= 0) { DOCA_DPA_DEV_LOG_INFO("HPACK_TERM encode FAIL i=%u\n", i); return; }
            sink += el;
        } else {
            sink += nf;
        }
    }
    c1 = __dpa_thread_cycles();
    t1 = __dpa_thread_time();
    DOCA_DPA_DEV_LOG_INFO("HPACK_TERM %s ops=%u cycles=%lu us=%lu sink=%d\n",
                          reencode ? "DEC+REENC" : "DECODE",
                          n, c1 - c0, t1 - t0, sink);

    /* NI variant */
    t0 = __dpa_thread_time();
    c0 = __dpa_thread_cycles();
    for (i = 0; i < n; i++) {
        if (i % DSB_STEADY_N == 0) {
            ht_init(dec); ht_init(enc);
            ht_decode(dsb_ni_first, sizeof(dsb_ni_first), dec, fields, &nf);
            if (reencode)
                ht_encode(fields, nf, enc, blk, sizeof(scratch->term.blk));
        }
        rc = ht_decode(DSB_NI(i % DSB_STEADY_N), dec, fields, &nf);
        if (rc) { DOCA_DPA_DEV_LOG_INFO("HPACK_TERM NI decode FAIL i=%u\n", i); return; }
        if (reencode) {
            el = ht_encode(fields, nf, enc, blk, sizeof(scratch->term.blk));
            if (el <= 0) { DOCA_DPA_DEV_LOG_INFO("HPACK_TERM NI encode FAIL i=%u\n", i); return; }
            sink += el;
        } else sink += nf;
    }
    c1 = __dpa_thread_cycles();
    t1 = __dpa_thread_time();
    DOCA_DPA_DEV_LOG_INFO("HPACK_TERM NI-%s ops=%u cycles=%lu us=%lu sink=%d\n",
                          reencode ? "DEC+REENC" : "DECODE",
                          n, c1 - c0, t1 - t0, sink);
}

/* A separate entry keeps benchmark selection out of the native kernel. */
__dpa_global__ void run_dpa_benchmark(uint64_t thread_arg)
{
    struct dpa_bench_state *arg = (void *)thread_arg;

    if (arg->dma.dpa_dev != 0)
        doca_dpa_dev_device_set((doca_dpa_dev_t)arg->dma.dpa_dev);
    if (arg->mode != 0) {
        if (arg->mode == 3)
            run_hpack_walk_bench(arg);
        else if (arg->mode == 4)
            run_hpack_term_bench(arg, 0);
        else if (arg->mode == 5)
            run_hpack_term_bench(arg, 1);
        else
            run_dma_copy_bench(arg);
        arg->mode = 0;
    }
    /* Measurements run once and never enter the native ring loop. Publish
     * the common exit fence so CPU teardown can drain matching receives. */
    __dpa_thread_window_writeback();
    arg->dma.stopped = 1;
    __dpa_thread_window_writeback();
    doca_dpa_dev_thread_finish();
}
