#!/usr/bin/env python3
"""Run the production kernel loop with delayed DMA/CQE SDK substitutes.

Copies read their source only when explicitly completed. This catches early
source reclamation; real hardware ordering is verified separately on devices.
"""
from pathlib import Path
import os
import re
import shlex
import subprocess
import tempfile

root = Path(__file__).resolve().parents[1]
source = (root / 'src/transport/device/dpa_kernel.c').read_text()
region = source[source.index('static void stop_desc_ring('):]
prefix = r'''
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "dpa_common.h"
typedef unsigned doca_dpa_dev_buf_t;
typedef unsigned doca_dpa_dev_completion_element_t;
typedef uint64_t doca_dpa_dev_t;
#define __dpa_global__
#define DOCA_DPA_DEV_LOG_ERR(...) ((void)0)
#define DMA_POLL_ACTIVATION_BUDGET 65536u
#define DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH 1
#define DOCA_DPA_DEV_COMP_SEND 0
static struct dma_ring_ctrl ring;
static struct dma_desc descriptors[256];
static unsigned copies, credit_limit, finished, ready, read_cq, acked, arms;
static struct dpa_thread_ctx *tls;
static unsigned tls_reads, retriggers, reschedules;
static uint64_t selected_device;
static uint64_t doca_dpa_dev_thread_get_local_storage(void) { ++tls_reads;return (uintptr_t)tls; }
static void doca_dpa_dev_device_set(uint64_t dev) { selected_device=dev; }
static void doca_dpa_dev_thread_retrigger(void) { ++retriggers; }
static void doca_dpa_dev_thread_reschedule(void) { ++reschedules; }
static unsigned char src[256][8064], dst[1024*1024];
static struct { uint64_t src, dst; uint32_t len, type; } pending[1024];
static void __dpa_thread_window_writeback(void) {}
static void __dpa_thread_window_read_inv(void) {}
static void doca_dpa_dev_thread_finish(void) { ++finished; }
static unsigned doca_dpa_dev_buf_array_get_buf(uint64_t a, unsigned i) { (void)a;return i; }
static uint64_t doca_dpa_dev_buf_get_external_ptr(unsigned i) {
 assert(i<=256);return (uintptr_t)(i ? (void *)&descriptors[i-1] : (void *)&ring);
}
static void doca_dpa_dev_completion_request_notification(uint64_t c) { (void)c;++arms; }
static int doca_dpa_dev_get_completion(uint64_t c,unsigned *comp) {
 (void)c;assert(arms);if(read_cq==ready)return 0;*comp=read_cq++;return 1;
}
static unsigned doca_dpa_dev_get_completion_type(unsigned c) { return pending[c].type; }
static void doca_dpa_dev_completion_ack(uint64_t c,unsigned n) { (void)c;acked+=n;assert(acked<=read_cq); }
static int doca_dpa_dev_comch_producer_is_consumer_empty(uint64_t p,int c) {
 (void)p;(void)c;return copies>=credit_limit;
}
static void doca_dpa_dev_comch_producer_dma_copy(uint64_t p,int c,uint32_t dm,uint64_t da,
 uint32_t sm,uint64_t sa,uint32_t len,uint8_t *m,uint32_t n,uint64_t flags) {
 (void)p;(void)c;(void)dm;(void)sm;(void)m;(void)n;
 assert(flags==DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);assert(copies<1024);
 assert(copies-acked<DMESH_DPA_MAX_INFLIGHT);
 pending[copies].src=sa;pending[copies].dst=da;pending[copies].len=len;++copies;
}
static void complete_to(unsigned n) {
 assert(n<=copies);
 while(ready<n) {
  memcpy((void *)(uintptr_t)pending[ready].dst,(void *)(uintptr_t)pending[ready].src,pending[ready].len);
  ++ready;
 }
}
static struct dpa_thread_ctx reset(unsigned size) {
 memset(&ring,0,sizeof(ring));memset(descriptors,0,sizeof(descriptors));
 memset(pending,0,sizeof(pending));memset(dst,0,sizeof(dst));
 copies=finished=ready=read_cq=acked=arms=0;credit_limit=1024;
 return (struct dpa_thread_ctx){.dpa_buf_arr=1,.buf_arr_size=size,
 .buf_size=sizeof(dst),.src_addr=(uintptr_t)dst};
}
static void descriptor(unsigned seq,unsigned ring_size,unsigned len) {
 unsigned slot=seq%ring_size;
 memset(src[slot],(seq%250)+1,len);
 descriptors[slot]=(struct dma_desc){.addr=(uintptr_t)src[slot],.size=len};
}
'''
prefix = prefix.replace('#define DMA_POLL_ACTIVATION_BUDGET 65536u',
    re.search(r'^#define DMA_POLL_ACTIVATION_BUDGET .+$', source, re.M)[0])
tests = r'''
int main(void) {
 assert(sizeof(struct dma_ring_ctrl)==64);
 struct dpa_thread_ctx a=reset(4);
 poll_desc_ring(&a);assert(!finished && !copies);
 for(unsigned i=0;i<4;i++)descriptor(i,4,16);
 ring.producer_tail=4;credit_limit=2;
 poll_desc_ring(&a);
 assert(copies==2 && a.submit_head==2 && a.dma_submitted==2);
 assert(ring.consumer_head==0 && ring.completed_bytes==0 && dst[0]==0);
 credit_limit=4;poll_desc_ring(&a);
 assert(copies==4 && ring.consumer_head==0 && a.submit_head==4);
 poll_desc_ring(&a);assert(copies==4); /* retrigger cannot replay */
 complete_to(2);poll_desc_ring(&a); /* CQ drains even with no receive credit */
 assert(ring.consumer_head==2 && ring.completed_bytes==32 && acked==2);
 assert(dst[0]==1 && dst[16]==2);
 for(unsigned i=4;i<6;i++)descriptor(i,4,16); /* only completed slots reused */
 ring.producer_tail=6;credit_limit=6;poll_desc_ring(&a);
 assert(copies==6 && ring.consumer_head==2);
 complete_to(6);poll_desc_ring(&a);
 assert(ring.consumer_head==6 && ring.completed_bytes==96 && a.dma_submitted==6);
 for(unsigned i=0;i<6;i++)assert(dst[i*16]==i+1);
 a.rd_fc=1;a.pos=a.buf_size-8;a.rd_pos=0;
 descriptor(6,4,16);ring.producer_tail=7;credit_limit=7;
 poll_desc_ring(&a);assert(copies==6); /* byte backpressure */
 a.rd_pos=a.pos;poll_desc_ring(&a);
 assert(copies==7 && a.pos==16 && ring.consumer_head==6);
 a.stop=1;poll_desc_ring(&a);
 assert(!finished && !a.stopped); /* kernel cannot exit with pending DMA */
 complete_to(7);poll_desc_ring(&a);
 assert(finished==1 && a.stopped && a.dma_submitted==7);
 assert(ring.consumer_head==7 && ring.completed_bytes==112 && dst[0]==7);

 a=reset(256);
 for(unsigned i=0;i<256;i++)descriptor(i,256,16);
 ring.producer_tail=256;poll_desc_ring(&a);
 assert(copies==DMESH_DPA_MAX_INFLIGHT && ring.consumer_head==0);
 complete_to(copies);poll_desc_ring(&a);
 assert(copies==256 && ring.consumer_head==128);
 a.stop=1;complete_to(256);poll_desc_ring(&a);
 assert(a.stopped && ring.completed_bytes==4096);

 /* Recycle slots through more operations than the hardware CQ capacity. */
 a=reset(4);
 for(unsigned base=0;base<600;base+=4) {
  for(unsigned i=base;i<base+4;i++)descriptor(i,4,16);
  ring.producer_tail=base+4;poll_desc_ring(&a);
  assert(copies==base+4 && ring.consumer_head==base);
  complete_to(base+4);poll_desc_ring(&a);
  assert(ring.consumer_head==base+4 && acked==base+4);
  for(unsigned i=base;i<base+4;i++)assert(dst[i*16]==(i%250)+1);
 }
 assert(ring.completed_bytes==9600);

 a=reset(4);descriptor(0,4,16);descriptor(1,4,16);ring.producer_tail=2;
 poll_desc_ring(&a);complete_to(1);poll_desc_ring(&a);
 pending[1].type=13;ready=2;poll_desc_ring(&a);
 assert(a.stopped && a.dma_error && ring.error);
 assert(ring.consumer_head==1 && ring.completed_bytes==16); /* no ACK over failed DMA */

 a=reset(4);descriptor(0,4,16);descriptors[0].size=8193;ring.producer_tail=1;
 poll_desc_ring(&a);assert(!copies && ring.error && a.stopped && !ring.consumer_head);

 /* Exercise the actual no-argument entry through TLS, including a retrigger
  * with a DMA still pending and switching to another thread's local state. */
 a=reset(4);a.dpa_dev=11;tls=&a;
 descriptor(0,4,16);ring.producer_tail=1;
 run_dma_manager();run_dma_manager();
 assert(tls_reads==2 && retriggers==2 && copies==1 && a.submit_head==1);
 assert(selected_device==11 && ring.consumer_head==0);
 complete_to(1);a.stop=1;run_dma_manager();
 assert(a.stopped && finished==1 && retriggers==2 && ring.consumer_head==1);
 struct dpa_thread_ctx other=reset(4);other.dpa_dev=22;tls=&other;
 run_dma_manager();
 assert(selected_device==22 && !other.stopped && other.submit_head==0);
 assert(a.stopped && a.submit_head==1 && retriggers==3);
 other.dpa_buf_arr=0;run_dma_manager();assert(reschedules==1 && retriggers==3);
 tls=NULL;run_dma_manager();assert(finished==1 && retriggers==3);
 puts("DPA: delayed completion, source lifetime, pipeline bound, credit starvation, wrap, stop, errors and TLS entry PASS");
}
'''
flags = shlex.split(subprocess.check_output(['pkg-config', '--cflags', 'doca-common'], text=True))
with tempfile.TemporaryDirectory() as directory:
    c = Path(directory) / 'poll.c'
    exe = Path(directory) / 'poll'
    c.write_text(prefix + region + tests)
    subprocess.run(shlex.split(os.environ.get('CC', 'cc')) + [
        '-std=c11', '-O2', '-Wno-deprecated-declarations', '-DDOCA_ALLOW_EXPERIMENTAL_API',
        '-I' + str(root / 'src/transport/common'), *flags, str(c), '-o', str(exe)], check=True)
    subprocess.run([str(exe)], check=True, timeout=5)
