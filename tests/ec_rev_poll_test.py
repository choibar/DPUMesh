#!/usr/bin/env python3
"""Run the reverse DPA thread's production kernel with delayed copies.

A copy writes the host window only when the test completes it, and a copy
the test never completes stands for one into a host process that died. The
kernel must keep publishing every other flow, and start a flow's copies only
when the host's cursor leaves room for them. Hardware behavior is verified
separately on devices.
"""
from pathlib import Path
import os
import shlex
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
source = (root / 'src/transport/device/ec_rev_kernel.c').read_text()
region = source[source.index('#define EC_ACTIVATION_US'):]
prefix = r'''
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "dpa_common.h"
typedef uint64_t doca_dpa_dev_async_ops_t;
typedef uint64_t doca_dpa_dev_completion_t;
typedef unsigned doca_dpa_dev_completion_element_t;
#define __dpa_global__
#define DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH 1
#define DOCA_DPA_DEV_COMP_SEND 0
#define DOCA_DPA_DEV_COMP_SEND_ERR 13
static struct dmesh_ec_tls tls;
static struct dmesh_ec_ctl ctl;
static unsigned retriggers;
static uint64_t clock_ns, clock_step = 1000;
static uint64_t doca_dpa_dev_thread_get_local_storage(void) { return (uintptr_t)&tls; }
static void doca_dpa_dev_thread_retrigger(void) { ++retriggers; }
/* Each read advances clock_step us; an activation ends after EC_ACTIVATION_US. */
static uint64_t __dpa_thread_time(void) { return clock_ns += clock_step; }
static void __dpa_thread_window_writeback(void) {}
static void __dpa_thread_window_read_inv(void) {}
/* Handles are 1 + an index into these; addresses are the test's pointers. */
static void *doca_dpa_dev_mmap_get_external_ptr(doca_dpa_dev_mmap_t m, uint64_t addr)
{
    assert(m != 0);
    return (void *)(uintptr_t)addr;
}
/* Copies wait here, per queue, until the test completes them. */
struct copy { uint64_t ops, dst, src; uint32_t len, mmap_dst, mmap_src; };
static struct copy posted[256];
static unsigned nposted;
static void doca_dpa_dev_post_memcpy(doca_dpa_dev_async_ops_t ops, doca_dpa_dev_mmap_t dm, uint64_t dst,
                                     doca_dpa_dev_mmap_t sm, uint64_t src, uint32_t len, int flags)
{
    assert(flags == DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH && nposted < 256);
    posted[nposted++] = (struct copy){ops, dst, src, len, (uint32_t)dm, (uint32_t)sm};
}
/* The shared completion queue: user data and type of each completion. */
static struct { uint32_t id, type; } cq[256];
static unsigned cq_head, cq_tail, acked;
static int doca_dpa_dev_get_completion(doca_dpa_dev_completion_t c, doca_dpa_dev_completion_element_t *e)
{
    assert(c == 7);
    if (cq_head == cq_tail) return 0;
    *e = cq_head % 256;
    return 1;
}
static uint32_t doca_dpa_dev_get_completion_user_data(doca_dpa_dev_completion_element_t e) { return cq[e].id; }
static int doca_dpa_dev_get_completion_type(doca_dpa_dev_completion_element_t e) { return (int)cq[e].type; }
static void doca_dpa_dev_completion_ack(doca_dpa_dev_completion_t c, unsigned n) { (void)c; acked += n; cq_head += n; }
'''
tests = r'''
/* One host window per flow: slot ring at 0, data ring at DMESH_EC_DATA_OFF. */
static unsigned char win[4][DMESH_EC_DATA_OFF + 65536] __attribute__((aligned(64)));
static unsigned char src[4][65536];
static uint64_t se_words[4][8];

/* The host's consumption cursor in the flow's window. */
static uint64_t *cursor(uint32_t slot) { return (uint64_t *)&win[slot][DMESH_EC_CURSOR_OFF]; }
static void consumed(uint32_t slot, uint64_t seq, uint64_t bytes)
{
    cursor(slot)[1] = seq;
    cursor(slot)[2] = bytes;
}
/* A 64 KiB data ring and batches of at most 16 KiB. */
static void flow(uint32_t slot, uint32_t gen, uint32_t id)
{
    ctl.flow[slot] = (struct dmesh_ec_flow){.ops = 100 + id, .gen = gen, .id = id,
        .src_mmap = 1, .win_mmap = 2, .se_mmap = 3, .ring = 65536,
        .win_addr = (uintptr_t)win[slot], .se_addr = (uintptr_t)se_words[slot], .slack = 16384};
    cursor(slot)[0] = DMESH_EC_FC_MAGIC;
    consumed(slot, 0, 0);
}
static void post(uint32_t slot, uint32_t gen, uint64_t seq, uint32_t pos, uint32_t len, uint64_t cum)
{
    ctl.ring[ctl.tail % DMESH_EC_RING] = (struct dmesh_ec_ent){slot, gen, seq, (uint64_t)pos << 32 | len,
                                                              (uintptr_t)src[slot] + pos, cum};
    ctl.tail++;
}
/* The queue's oldest copy completes: it writes the window, then its CQE. */
static void complete(uint64_t ops, uint32_t type)
{
    for (unsigned i = 0; i < nposted; i++) {
        if (posted[i].ops != ops) continue;
        if (type == DOCA_DPA_DEV_COMP_SEND)
            memcpy((void *)(uintptr_t)posted[i].dst, (void *)(uintptr_t)posted[i].src, posted[i].len);
        memmove(&posted[i], &posted[i + 1], (nposted - i - 1) * sizeof(posted[0]));
        nposted--;
        cq[cq_tail % 256].id = (uint32_t)(ops - 100);
        cq[cq_tail++ % 256].type = type;
        return;
    }
    assert(!"no copy on that queue");
}
static uint64_t done_seq(uint32_t slot, uint32_t gen)
{
    uint64_t w = ctl.done[slot].seq;
    return (w >> DMESH_EC_GEN_SHIFT) == gen ? (w & ((1ull << DMESH_EC_GEN_SHIFT) - 1)) : 0;
}
struct desc { uint64_t seq; uint32_t pos, len; };
static const struct desc *desc(uint32_t slot, uint64_t seq)
{
    return (const void *)&win[slot][(seq % DMESH_EC_DESC_N) * DMESH_EC_DESC_SZ];
}

int main(void)
{
    tls.acomp = 7;
    tls.wid = 3;
    tls.ctl_mmap = 9;
    tls.ctl_addr = (uintptr_t)&ctl;
    for (int s = 0; s < 4; s++)
        for (int i = 0; i < 65536; i++)
            src[s][i] = (unsigned char)(s * 31 + i);

    /* A (slot 0) and B (slot 1): each flow's copies go to its own queue. */
    flow(0, 1, 64);
    flow(1, 1, 65);
    post(0, 1, 1, 0, 100, 100);
    post(1, 1, 1, 0, 200, 200);
    post(0, 1, 2, 100, 50, 150);
    post(1, 1, 2, 200, 300, 500);
    post(1, 1, 3, 500, 0, 500);          /* B's end of stream */
    run_ec_rev();
    assert(retriggers == 1 && ctl.head == 5 && nposted == 4 && tls.copies == 4);
    assert(posted[0].ops == 164 && posted[0].dst == (uintptr_t)win[0] + DMESH_EC_DATA_OFF && posted[0].len == 100);
    assert(posted[1].ops == 165 && posted[2].ops == 164 && posted[2].dst == (uintptr_t)win[0] + DMESH_EC_DATA_OFF + 100);
    assert(!done_seq(0, 1) && !done_seq(1, 1) && !se_words[1][0]);

    /* A's copy never completes (its host died). B completes and publishes,
     * in order, its end of stream last. */
    complete(165, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(done_seq(1, 1) == 1 && ctl.done[1].bytes == 200 && desc(1, 1)->seq == 1 && desc(1, 1)->len == 200);
    assert(se_words[1][0] == (3ull << 48 | 1));
    assert(!memcmp(win[1] + DMESH_EC_DATA_OFF, src[1], 200));
    complete(165, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(done_seq(1, 1) == 3 && ctl.done[1].bytes == 500 && desc(1, 3)->seq == 3 && desc(1, 3)->len == 0);
    assert(desc(1, 2)->pos == 200 && desc(1, 2)->len == 300 && se_words[1][0] == (3ull << 48 | 3));
    assert(!done_seq(0, 1) && !desc(0, 1)->seq && nposted == 2);

    /* The worker retires A's queue (seq 0) before destroying it. Whatever the
     * old queue still reports, a late copy or a flush, reaches no flow. */
    post(0, 0, 0, 0, 0, 0);
    run_ec_rev();
    complete(164, DOCA_DPA_DEV_COMP_SEND);
    complete(164, DOCA_DPA_DEV_COMP_SEND_ERR);
    run_ec_rev();
    assert(tls.stale_cqes == 2 && tls.err_cqes == 0 && ctl.done[0].failed == 0);
    assert(!desc(0, 1)->seq && !done_seq(0, 1) && !se_words[0][0]);

    /* A's slot gets a new flow on a new queue. */
    flow(0, 2, 128);
    post(0, 2, 1, 0, 64, 64);
    run_ec_rev();
    assert(nposted == 1 && posted[0].ops == 228);
    complete(228, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(tls.stale_cqes == 2 && done_seq(0, 2) == 1 && ctl.done[0].bytes == 64 && desc(0, 1)->len == 64);

    /* B's slot reuses its queue for its next flow (the last one finished).
     * A failed copy fails the flow; nothing after it is published. */
    flow(1, 2, 65);
    post(1, 2, 1, 0, 10, 10);
    post(1, 2, 2, 10, 10, 20);
    post(1, 2, 3, 20, 0, 20);
    run_ec_rev();
    complete(165, DOCA_DPA_DEV_COMP_SEND_ERR);
    complete(165, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(ctl.done[1].failed == 2 && tls.err_cqes == 1 && tls.stale_cqes == 3 && !done_seq(1, 2));
    post(1, 2, 4, 20, 10, 30);
    run_ec_rev();
    assert(nposted == 0 && !done_seq(1, 2));

    /* A batch out of sequence, or a line of another flow, fails the flow. */
    flow(2, 1, 66);
    post(2, 1, 2, 0, 10, 10);
    run_ec_rev();
    assert(ctl.done[2].failed == 1 && nposted == 0);
    flow(3, 5, 67);
    post(3, 4, 1, 0, 10, 10);
    run_ec_rev();
    assert(ctl.done[3].failed == 4 && nposted == 0);
    flow(1, 3, 65);
    ctl.flow[1].slack = ctl.flow[1].ring;    /* no batch could ever fit */
    post(1, 3, 1, 0, 10, 10);
    run_ec_rev();
    assert(ctl.done[1].failed == 3 && nposted == 0);

    /* A flow's full window in flight: DMESH_EC_FLOW_Q - 2 batches, then each
     * completion publishes one, in order. */
    flow(2, 2, 130);
    for (uint64_t i = 1; i <= DMESH_EC_FLOW_Q - 2; i++)
        post(2, 2, i, (uint32_t)(i - 1) * 4, 4, i * 4);
    while (ctl.head != ctl.tail) run_ec_rev();
    assert(nposted == DMESH_EC_FLOW_Q - 2);
    for (uint64_t i = 1; i <= DMESH_EC_FLOW_Q - 2; i++) {
        complete(230, DOCA_DPA_DEV_COMP_SEND);
        run_ec_rev();
        assert(done_seq(2, 2) == i && ctl.done[2].bytes == i * 4);
    }
    assert(!memcmp(win[2] + DMESH_EC_DATA_OFF, src[2], (DMESH_EC_FLOW_Q - 2) * 4));

    /* The host's descriptor slots: with nothing consumed, a flow's batch
     * DMESH_EC_DESC_N - 1 waits; the end of stream may take one slot more. A
     * cursor without its magic is no cursor. */
    flow(3, 6, 131);
    for (uint64_t i = 1; i <= DMESH_EC_DESC_N - 2; i++)
        post(3, 6, i, 0, 4, i * 4);
    while (ctl.head != ctl.tail) run_ec_rev();
    assert(nposted == DMESH_EC_DESC_N - 2);
    for (uint64_t i = 1; i <= DMESH_EC_DESC_N - 2; i++)
        complete(231, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(done_seq(3, 6) == DMESH_EC_DESC_N - 2);
    post(3, 6, DMESH_EC_DESC_N - 1, 0, 4, (DMESH_EC_DESC_N - 1) * 4);
    post(3, 6, DMESH_EC_DESC_N, 0, 0, (DMESH_EC_DESC_N - 1) * 4);
    run_ec_rev();
    assert(nposted == 0 && (tls.waiting & 1ull << 3) && done_seq(3, 6) == DMESH_EC_DESC_N - 2);
    uint64_t reads = tls.cursor_reads;
    cursor(3)[0] = 0;
    consumed(3, 2, 8);
    run_ec_rev();
    assert(nposted == 0 && tls.cursor_reads > reads && tls.f[3].cseq == 0);
    cursor(3)[0] = DMESH_EC_FC_MAGIC;
    run_ec_rev();
    assert(nposted == 1 && !(tls.waiting & 1ull << 3) && tls.f[3].cseq == 2);
    complete(231, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(done_seq(3, 6) == DMESH_EC_DESC_N && desc(3, DMESH_EC_DESC_N)->len == 0);

    /* The host's data ring: a batch fits with one largest batch to spare. A
     * cursor past what was published counts as what was published. */
    flow(0, 3, 192);
    for (uint32_t i = 0; i < 4; i++)
        post(0, 3, i + 1, i * 16384, 16384, (i + 1) * 16384ull);
    run_ec_rev();
    assert(nposted == 3 && (tls.waiting & 1));
    consumed(0, 9, 1ull << 30);
    complete(292, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(tls.f[0].cseq == 1 && tls.f[0].cbytes == 16384 && !(tls.waiting & 1));
    assert(nposted == 3 && posted[2].dst == (uintptr_t)win[0] + DMESH_EC_DATA_OFF + 49152);
    for (int i = 0; i < 3; i++)
        complete(292, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(done_seq(0, 3) == 4 && !memcmp(win[0] + DMESH_EC_DATA_OFF, src[0], 65536));

    /* A flow that keeps waiting reads the cursor at doubling intervals: about
     * 35 reads in 2 ms (4, 8, 16, 32 us, then every 64 us), and one more read
     * after the host makes room. */
    consumed(0, 1, 16384);
    clock_step = 1;
    reads = tls.cursor_reads;
    post(0, 3, 5, 0, 16384, 5 * 16384);
    run_ec_rev();
    assert(nposted == 0 && tls.cursor_reads - reads >= 25 && tls.cursor_reads - reads <= 40);
    consumed(0, 2, 32768);
    reads = tls.cursor_reads;
    run_ec_rev();
    assert(nposted == 1 && tls.cursor_reads - reads == 1 && !(tls.waiting & 1));

    /* A waiting flow whose queue is retired starts nothing more. */
    post(0, 3, 6, 16384, 16384, 6 * 16384);
    run_ec_rev();
    assert(nposted == 1 && (tls.waiting & 1));
    post(0, 0, 0, 0, 0, 0);
    consumed(0, 5, 5 * 16384);
    run_ec_rev();
    complete(292, DOCA_DPA_DEV_COMP_SEND);
    run_ec_rev();
    assert(nposted == 0 && !(tls.waiting & 1) && tls.stale_cqes == 4);
    clock_step = 1000;

    assert(ctl.publishes == tls.publishes && ctl.activations == tls.activations);
    assert(ctl.cursor_reads == tls.cursor_reads);
    puts("reverse DPA thread: per-flow queues, a stuck flow holds back no other, order, end of stream, "
         "retired queue, failed copy, bad entries, full window, host room (slots, bytes, cursor clamp, "
         "read backoff) PASS");
}
'''
flags = shlex.split(subprocess.check_output(['pkg-config', '--cflags', 'doca-common'], text=True))
with tempfile.TemporaryDirectory() as directory:
    c = Path(directory) / 'ec_rev.c'
    exe = Path(directory) / 'ec_rev'
    c.write_text(prefix + region + tests)
    subprocess.run(shlex.split(os.environ.get('CC', 'cc')) + [
        '-std=c11', '-O2', '-Wall', '-Wno-unused-function', '-DDOCA_ALLOW_EXPERIMENTAL_API',
        '-I' + str(root / 'src/transport/common'), *flags, str(c), '-o', str(exe)], check=True)
    subprocess.run([str(exe)], check=True, timeout=5)
