#!/usr/bin/env python3
"""Execute the actual device ring loop with SDK substitutes, without a device.

The selected source region is compiled unchanged. SDK calls model buffer lookup,
credit starvation, copy submission and finish; they do not emulate hardware DMA.
"""
from pathlib import Path
import os
import re
import shlex
import subprocess
import tempfile

root=Path(__file__).resolve().parents[1]
source=(root/'src/transport/device/dpa_kernel.c').read_text()
region=source[source.index('static void stop_desc_ring('):source.index('static void run_dma_copy_bench(')]
prefix=r'''
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "dpa_common.h"
typedef unsigned doca_dpa_dev_buf_t;
#define CONSUMER_HEAD_PUBLISH_BATCH 1
#define DMA_POLL_ACTIVATION_BUDGET 65536u
#define DOCA_DPA_DEV_SUBMIT_FLAG_OPTIMIZE_REPORTS 2
#define DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH 1
static struct dma_ring_ctrl ring;
static struct dma_desc descriptors[4];
static unsigned copies, credit_limit, finished, invalidate_calls;
static uint64_t sources[32];
static void __dpa_thread_window_writeback(void) {}
static void __dpa_thread_window_read_inv(void) { ++invalidate_calls; }
static void doca_dpa_dev_thread_finish(void) { ++finished; }
static unsigned doca_dpa_dev_buf_array_get_buf(uint64_t a, unsigned i) { (void)a;return i; }
static uint64_t doca_dpa_dev_buf_get_external_ptr(unsigned i) {
 return (uintptr_t)(i ? (void *)&descriptors[i-1] : (void *)&ring);
}
static int doca_dpa_dev_comch_producer_is_consumer_empty(uint64_t p,int c) {
 (void)p;(void)c;return copies>=credit_limit;
}
static void doca_dpa_dev_comch_producer_dma_copy(uint64_t p,int c,uint32_t dm,uint64_t da,
 uint32_t sm,uint64_t sa,uint32_t len,uint8_t *m,uint32_t n,uint64_t flags) {
 (void)p;(void)c;(void)dm;(void)da;(void)sm;(void)m;(void)n;
 assert(flags & DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);assert(len==16);assert(copies<32);
 sources[copies++]=sa;
}
'''
prefix=prefix.replace('#define DMA_POLL_ACTIVATION_BUDGET 65536u',
    re.search(r'^#define DMA_POLL_ACTIVATION_BUDGET .+$',source,re.M)[0])
tests=r'''
int main(void) {
 struct dpa_thread_arg arg={.dpa_buf_arr=1,.buf_arr_size=4,.buf_size=1024};
 credit_limit=32;
 poll_desc_ring(&arg); /* Idle must return, without faking a stopped thread. */
 assert(invalidate_calls>0 && copies==0 && arg.stopped==0 && finished==0);
 for(int i=0;i<4;i++)descriptors[i]=(struct dma_desc){.addr=100+i,.size=16};
 ring.producer_tail=4;credit_limit=2;
 poll_desc_ring(&arg); /* Checkpoint even when receive credits stop arriving. */
 assert(copies==2 && ring.consumer_head==2 && arg.dma_submitted==2 && arg.pos==32);
 credit_limit=32;poll_desc_ring(&arg);
 assert(copies==4 && ring.consumer_head==4 && arg.dma_submitted==4 && arg.pos==64);
 for(int i=0;i<2;i++)descriptors[i]=(struct dma_desc){.addr=104+i,.size=16};
 ring.producer_tail=6;poll_desc_ring(&arg);
 assert(copies==6 && ring.consumer_head==6 && arg.dma_submitted==6);
 for(int i=0;i<6;i++)assert(sources[i]==100u+i); /* No replay across activations/wrap. */
 arg.buf_size=1024*1024;arg.rd_fc=1;arg.pos=arg.buf_size-8;arg.rd_pos=0;
 descriptors[2]=(struct dma_desc){.addr=106,.size=16};ring.producer_tail=7;
 poll_desc_ring(&arg); /* A full staging ring must also yield without consuming. */
 assert(copies==6 && ring.consumer_head==6 && arg.dma_submitted==6);
 arg.rd_pos=arg.pos;poll_desc_ring(&arg);
 assert(copies==7 && ring.consumer_head==7 && arg.pos==16 && sources[6]==106);
 arg.stop=1;poll_desc_ring(&arg);
 assert(finished==1 && arg.stopped==1 && arg.dma_submitted==7);
 puts("DPA polling: idle/credit checkpoint, resume/wrap and cumulative stop fence PASS");
}
'''
flags=shlex.split(subprocess.check_output(['pkg-config','--cflags','doca-common'],text=True))
with tempfile.TemporaryDirectory() as d:
    c=Path(d)/'poll.c';exe=Path(d)/'poll';c.write_text(prefix+region+tests)
    subprocess.run(shlex.split(os.environ.get('CC','cc'))+['-std=c11','-O2','-Wno-deprecated-declarations','-DDOCA_ALLOW_EXPERIMENTAL_API',
        '-I'+str(root/'src/transport/common'),*flags,str(c),'-o',str(exe)],check=True)
    subprocess.run([str(exe)],check=True,timeout=5)
