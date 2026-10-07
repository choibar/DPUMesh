#include "doca_dpa_dev.h"
#include "doca_dpa_dev_comch_msgq.h"
#include "doca_dpa_dev_buf.h"
#include "dpaintrin.h"
#include "dpa_common.h"
#include "hpack_walk.h"
#include "dsb_blocks.h"
#include "hpack_term.h"
/*
 * RPC for initializing DPA IO thread called before running the thread
 *
 * @consumer [in]: The DPA Comch consumer
 * @return: returns RPC_RETURN_STATUS_SUCCESS on success and RPC_RETURN_STATUS_ERROR otherwise
 */

static inline uint32_t bswap32(uint32_t x) {
    return ((x & 0x000000FF) << 24) |
           ((x & 0x0000FF00) << 8)  |
           ((x & 0x00FF0000) >> 8)  |
           
           ((x & 0xFF000000) >> 24);
}

/* dpa_dev: extended DPA context handle whose device the consumer belongs to
 * (0 = base context); an RPC runs outside the thread, so it must switch too. */
__dpa_rpc__ uint64_t thread_init_rpc(doca_dpa_dev_comch_consumer_t consumer, uint32_t num_msg, uint64_t dpa_dev)
{
    DOCA_DPA_DEV_LOG_INFO("recv thread init RPC, num_msg: %u\n", num_msg);
    if (dpa_dev != 0)
        doca_dpa_dev_device_set((doca_dpa_dev_t)dpa_dev);
	doca_dpa_dev_comch_consumer_ack(consumer, num_msg);

	return 0;
}

static void send_msgs(struct dpa_thread_arg *thread_arg, int num_msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    doca_dpa_dev_completion_element_t comp;
    struct comch_msg msg;
    uint64_t start, end, tick;
    doca_dpa_dev_completion_type_t t;
    tick = __dpa_thread_time();

    DOCA_DPA_DEV_LOG_INFO("tick frequency: %lu\n", tick);
    for (int i = 0; i < num_msg; i++) {
        start =  __dpa_thread_time();
        doca_dpa_dev_comch_producer_post_send_imm_only(producer,
                                                   /*consumer_id=*/1,
                                                   (uint8_t *)&msg,
                                                   sizeof(struct comch_msg),
                                                   DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            								    //    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS);
        
        while (doca_dpa_dev_get_completion(thread_arg->dpa_producer_comp, &comp) == 0) {
        }

        end =  __dpa_thread_time();
        t = doca_dpa_dev_get_completion_type(comp);
                                                
        DOCA_DPA_DEV_LOG_INFO("type: %d, cycles: %lu\n", t, end - start);
    }
}
static void handle_dpu_msg(struct dpa_thread_arg *thread_arg, const struct comch_msg *msg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    // doca_dpa_dev_comch_producer_t producer;
    doca_dpa_dev_completion_element_t comp;
    uint64_t start, end;

    switch(msg->type) {
        case COMCH_MSG_TYPE_DMA_REQ:
            struct comch_dma_req_msg *dma_msg = (struct comch_dma_req_msg *)msg;
            if (dma_msg->dpa_producer)
                producer = dma_msg->dpa_producer;
            // producer = dma_msg->dpa_producer;
            DOCA_DPA_DEV_LOG_INFO("Received DMA REQ msg from host: producer=0x%lx, src_mmap=%u, dst_mmap=%u, src_addr=0x%lx, dst_addr=0x%lx, length=%u\n",
                                producer,                 
                                dma_msg->src_mmap,
                                 dma_msg->dst_mmap,
                                 dma_msg->src_addr,
                                 dma_msg->dst_addr,
                                 dma_msg->length);
                
            if (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1)) {
                DOCA_DPA_DEV_LOG_INFO("Host consumer is empty, cannot send DMA completion\n");
                break;
            }

            doca_dpa_dev_comch_producer_dma_copy(producer,
                                    /*consumer_id=*/1,
                                    dma_msg->dst_mmap,
                                    dma_msg->dst_addr,
                                    dma_msg->src_mmap,
                                    dma_msg->src_addr,
                                    dma_msg->length,
                                    "test_dma_imm",
                                    sizeof("test_dma_imm"),
                                    DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS | DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            break;
        default:
            DOCA_DPA_DEV_LOG_INFO("Unknown msg type received from host: %d\n", msg->type);
            break;
    }
}

static void handle_msgs(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_consumer_completion_element_t completion;
    struct comch_msg *msg;
    uint32_t msg_size;
    doca_dpa_dev_comch_consumer_t consumer = thread_arg->dpa_consumer;
	doca_dpa_dev_comch_consumer_completion_t consumer_comp = thread_arg->dpa_consumer_comp;
    uint32_t num_msgs = 0;

    while (doca_dpa_dev_comch_consumer_get_completion(consumer_comp, &completion) != 0) {
        msg = (struct comch_msg *)doca_dpa_dev_comch_consumer_get_completion_imm(completion, &msg_size);
        handle_dpu_msg(thread_arg, msg);
        num_msgs++;
    }

    // send_msgs(thread_arg, num_msgs);
    if (num_msgs != 0) {
        doca_dpa_dev_comch_consumer_completion_ack(consumer_comp, num_msgs);
		doca_dpa_dev_comch_consumer_completion_request_notification(consumer_comp);
		doca_dpa_dev_comch_consumer_ack(consumer, num_msgs);
    }
    // DOCA_DPA_DEV_LOG_INFO("Handled %u msgs from host\n", num_msgs);
}
#define CONSUMER_HEAD_PUBLISH_BATCH 1
#define DMA_RING_LOG_INTERVAL 1024
/* Max descriptors / bytes coalesced into a single DMA copy + completion
 * message. HARD CONSTRAINT: single DPA DMA transfers above 8192B do not work
 * on this platform, so a coalesced copy must never exceed 8192B (coalescing
 * therefore only helps messages smaller than 8KB). */
#define DMA_BATCH_MAX_DESCS 8
#define DMA_BATCH_MAX_BYTES 8192
/* The standalone throughput microbenchmark batches doorbells; native ring
 * copies flush every submission to make bounded shutdown possible. */
#define DMA_FLUSH_BATCH 32
/* Bound every activation, including idle and credit-starved polling. The SDK
 * watchdog forbids indefinitely scheduled kernels. Retrigger retains this
 * flow's thread/EU ownership model; it does not multiplex other rings. */
#define DMA_POLL_ACTIVATION_BUDGET 65536u

/* Every native copy is flushed when submitted, so stopping never needs an
 * extra receive credit merely to flush previously queued DMA operations. */
static void stop_desc_ring(struct dpa_thread_arg *arg, uint64_t submitted)
{
    arg->dma_submitted = submitted;
    __dpa_thread_window_writeback();
    arg->stopped = 1;
    __dpa_thread_window_writeback();
    doca_dpa_dev_thread_finish();
}

static void poll_desc_ring(struct dpa_thread_arg *thread_arg)
{
    doca_dpa_dev_comch_producer_t producer = thread_arg->dpa_producer;
    struct comch_dma_comp_msg msg = {0};
    doca_dpa_dev_uintptr_t dev_ptr;
    doca_dpa_dev_buf_t buf;
    struct dma_ring_ctrl *ctrl;
    struct dma_desc *desc;
    uint64_t producer_tail;
    uint64_t consumer_head;
    uint64_t last_published_head;

    uint32_t ring_size = thread_arg->buf_arr_size;
    uint32_t ring_mask = ring_size - 1;
    uint32_t buf_size = thread_arg->buf_size;
    uint64_t submitted = thread_arg->dma_submitted;
    uint32_t budget = DMA_POLL_ACTIVATION_BUDGET;
    
    buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, 0);
    dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
    ctrl = (struct dma_ring_ctrl *)dev_ptr;

    consumer_head = ctrl->consumer_head;
    last_published_head = consumer_head;

    /* polling descriptor ring in host memory */
    while (1) {

        __dpa_thread_window_read_inv();

        if (thread_arg->stop) {
            stop_desc_ring(thread_arg, submitted);
            return;
        }
        if (--budget == 0)
            goto checkpoint;

        buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, 0);
        dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
        ctrl = (struct dma_ring_ctrl *)dev_ptr;
        consumer_head = ctrl->consumer_head;
        producer_tail = ctrl->producer_tail;

        while (consumer_head < producer_tail) {
            /* Staging backpressure (rd_fc): never copy into the DPU staging
             * ring past what the reader released. Unread = (pos - rd_pos)
             * circularly; leave room for this batch, a wrap's wasted tail and
             * a margin (3*8064). If short, stop consuming descriptors -
             * consumer_head stalls, the host's forward gate holds its writes,
             * and the pressure reaches the h2 client. Re-checked next spin. */
            if (thread_arg->rd_fc) {
                uint32_t rdp = thread_arg->rd_pos;
                uint32_t unread = ((uint32_t)thread_arg->pos + buf_size - rdp) % buf_size;

                if (buf_size - unread < 3u * 8064u)
                    break;
            }
            uint64_t batch_src;
            uint32_t batch_len, batch_cnt;
            uint32_t dst_room = buf_size - (uint32_t)thread_arg->pos;

            buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr, (consumer_head & ring_mask) + 1);
            dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
            desc = (struct dma_desc *)dev_ptr;

            batch_src = desc->addr;
            batch_len = (uint32_t)desc->size;
            batch_cnt = 1;

            /* Coalesce contiguous descriptors into one DMA copy: one WQE, one
             * doorbell and one completion message then cover the whole batch,
             * amortizing the fixed per-operation cost. The host writes
             * messages back-to-back in sndbuf, so descriptors are contiguous
             * except at the buffer wrap; the batch also may not cross the
             * staging-buffer wrap on the destination side. */
            // while (batch_cnt < DMA_BATCH_MAX_DESCS &&
            //        consumer_head + batch_cnt < producer_tail) {
            //     buf = doca_dpa_dev_buf_array_get_buf(thread_arg->dpa_buf_arr,
            //                                          ((consumer_head + batch_cnt) & ring_mask) + 1);
            //     dev_ptr = doca_dpa_dev_buf_get_external_ptr(buf);
            //     desc = (struct dma_desc *)dev_ptr;

            //     if (desc->addr != batch_src + batch_len)   /* source discontinuity (wrap) */
            //         break;
            //     if (batch_len + desc->size > dst_room)     /* staging wrap on destination */
            //         break;
            //     if (batch_len + desc->size > DMA_BATCH_MAX_BYTES)
            //         break;
            //     batch_len += (uint32_t)desc->size;
            //     batch_cnt++;
            // }

            /* Wrap the destination cursor BEFORE a copy that would cross the
             * buffer end, so every copy stays within [0, buf_size) and no
             * segment straddles the wrap. A straddling segment has pos+len >
             * buf_size, which the reader (Rust staging read / host bridge)
             * cannot represent as one [pos,len) and skips - losing the response
             * and stalling the stream exactly once the buffer first fills.
             * batch_len <= 8064 << buf_size, so wrapping always leaves room. */
            if ((uint32_t)thread_arg->pos + batch_len > buf_size)
                thread_arg->pos = 0;

            /* if consumer is empty, wait */
            while (doca_dpa_dev_comch_producer_is_consumer_empty(producer, /*consumer_id=*/1) == 1) {
                __dpa_thread_window_read_inv();
                if (thread_arg->stop) {
                    stop_desc_ring(thread_arg, submitted);
                    return;
                }
                if (--budget == 0)
                    goto checkpoint;
            }

            msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
            msg.pos = thread_arg->pos;
            msg.length = batch_len;
            msg.count = batch_cnt;

            /* Do not retain unflushed WQEs: when receive credits run out,
             * a final flush marker would itself need an unavailable credit.
             * Report coalescing is independent and remains enabled. */
            {
                uint64_t submit_flags = DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS |
                                        DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH;

                doca_dpa_dev_comch_producer_dma_copy(producer,
                                            /*consumer_id=*/1,
                                            thread_arg->dpu_mmap,
                                            thread_arg->src_addr + thread_arg->pos,
                                            thread_arg->host_mmap,
                                            batch_src,
                                            batch_len,
                                            (uint8_t *)&msg,
                                            sizeof(struct comch_dma_comp_msg),
                                            submit_flags);
                submitted++;
            }

            thread_arg->pos += batch_len;
            if (thread_arg->pos >= buf_size) {
                thread_arg->pos = 0;
            }

            consumer_head += batch_cnt;
            if (--budget == 0)
                goto checkpoint;
        }

        if (consumer_head - last_published_head >= CONSUMER_HEAD_PUBLISH_BATCH) {
            ctrl->consumer_head = consumer_head;
            __dpa_thread_window_writeback();
            last_published_head = consumer_head;
        }
    }

checkpoint:
    /* Never replay descriptors after retrigger, and keep the cumulative copy
     * count used by the CPU teardown fence across all activations. This also
     * covers yielding while waiting for credit: that descriptor was not sent. */
    ctrl->consumer_head = consumer_head;
    thread_arg->dma_submitted = submitted;
    __dpa_thread_window_writeback();

}

/*
 * producer_dma_copy microbenchmark: copy bench_msg_size bytes from the host
 * sndbuf to the DPU staging buffer bench_num_ops times, straight from this DPA
 * thread. Throughput mode pipelines copies (credit-gated, doorbell every
 * DMA_FLUSH_BATCH); latency mode serializes: FLUSH each copy and poll the
 * producer completion before issuing the next. Results are reported in
 * __dpa_thread_time() ticks; the DPU-side recv counters give the wall-clock
 * cross-check.
 */
static void run_dma_copy_bench(struct dpa_thread_arg *a)
{
    doca_dpa_dev_comch_producer_t producer = a->dpa_producer;
    doca_dpa_dev_completion_element_t comp;
    struct comch_dma_comp_msg msg = {0};
    uint32_t size = a->bench_msg_size;
    uint32_t n = a->bench_num_ops;
    uint64_t dst_pos = 0, src_pos = 0;
    uint64_t t0, t1;
    uint32_t i;

    msg.type = COMCH_MSG_TYPE_DMA_COMPLETED;
    msg.length = size;
    msg.count = 1;

    DOCA_DPA_DEV_LOG_INFO("BENCH start: mode=%u size=%u ops=%u host=0x%lx/%u dpu=0x%lx/%u\n",
                          a->bench_mode, size, n,
                          a->bench_host_addr, a->bench_host_size,
                          a->src_addr, a->buf_size);

    if (a->bench_mode == 1) {
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
                                                 a->dpu_mmap,
                                                 a->src_addr + dst_pos,
                                                 a->host_mmap,
                                                 a->bench_host_addr + src_pos,
                                                 size,
                                                 (uint8_t *)&msg,
                                                 sizeof(struct comch_dma_comp_msg),
                                                 flags);
            a->dma_submitted++;

            dst_pos += size;
            if (dst_pos + size > a->buf_size)
                dst_pos = 0;
            src_pos += size;
            if (src_pos + size > a->bench_host_size)
                src_pos = 0;
        }
        while (doca_dpa_dev_get_completion(a->dpa_producer_comp, &comp) == 0) {
        }
        t1 = __dpa_thread_time();
        doca_dpa_dev_completion_ack(a->dpa_producer_comp, 1);
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
                                                 a->dpu_mmap,
                                                 a->src_addr + dst_pos,
                                                 a->host_mmap,
                                                 a->bench_host_addr + src_pos,
                                                 size,
                                                 (uint8_t *)&msg,
                                                 sizeof(struct comch_dma_comp_msg),
                                                 DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);
            a->dma_submitted++;
            while (doca_dpa_dev_get_completion(a->dpa_producer_comp, &comp) == 0) {
            }
            t1 = __dpa_thread_cycles();
            doca_dpa_dev_completion_ack(a->dpa_producer_comp, 1);

            d = t1 - t0;
            total += d;
            if (d < tmin)
                tmin = d;
            if (d > tmax)
                tmax = d;

            dst_pos += size;
            if (dst_pos + size > a->buf_size)
                dst_pos = 0;
            src_pos += size;
            if (src_pos + size > a->bench_host_size)
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
static void run_hpack_walk_bench(struct dpa_thread_arg *a)
{
    union hpack_bench_scratch *scratch = (void *)a->bench_scratch;
    if (!scratch) return;
    struct hw_state *st = &scratch->walk.st;
    struct hw_out *out = &scratch->walk.out;
    uint32_t n = a->bench_num_ops;
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
static void run_hpack_term_bench(struct dpa_thread_arg *a, int reencode)
{
    union hpack_bench_scratch *scratch = (void *)a->bench_scratch;
    if (!scratch) return;
    struct ht_table *dec = &scratch->term.dec, *enc = &scratch->term.enc;
    struct ht_field *fields = scratch->term.fields;
    unsigned char *blk = scratch->term.blk;
    uint32_t n = a->bench_num_ops;
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

__dpa_global__ void run_dma_manager(uint64_t arg)
{
    /* extended DPA context: make its device current before any object access */
    if (((struct dpa_thread_arg *)arg)->dpa_dev != 0)
        doca_dpa_dev_device_set((doca_dpa_dev_t)((struct dpa_thread_arg *)arg)->dpa_dev);

    struct dpa_thread_arg *thread_arg = (struct dpa_thread_arg *)arg;


    if (thread_arg->bench_mode != 0) {
        if (thread_arg->bench_mode == 3)
            run_hpack_walk_bench(thread_arg);
        else if (thread_arg->bench_mode == 4)
            run_hpack_term_bench(thread_arg, 0);
        else if (thread_arg->bench_mode == 5)
            run_hpack_term_bench(thread_arg, 1);
        else
            run_dma_copy_bench(thread_arg);
        /* run once per thread activation only */
        thread_arg->bench_mode = 0;
        doca_dpa_dev_thread_reschedule();
    }

    /* bench-only launch (host-driven): no descriptor ring to poll */
    if (thread_arg->dpa_buf_arr == 0)
        doca_dpa_dev_thread_reschedule();

    poll_desc_ring(thread_arg);

    /* A descriptor-ring writer does not signal a completion, so ordinary
     * reschedule could sleep forever after an idle activation. Retrigger
     * requests immediate execution and resets the bounded activation. */
    if (thread_arg->stop)
        stop_desc_ring(thread_arg, thread_arg->dma_submitted);
    else
        doca_dpa_dev_thread_retrigger();
}
