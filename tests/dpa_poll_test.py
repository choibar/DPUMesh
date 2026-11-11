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
typedef unsigned doca_dpa_dev_comch_consumer_completion_element_t;
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
/* run_dma_mux bounds an activation by time: each read advances 1 us. */
static uint64_t mux_clock;
static uint64_t __dpa_thread_time(void) { return mux_clock += 1000; }
static unsigned char src[256][8064], dst[1024*1024];
static struct { uint64_t src, dst; uint32_t len, type, consumer; struct dmesh_mux_msg msg; } pending[1024];
/* Pod mux: three rings on both tail lines (0, 1, 17), each with its own
 * host ring, and the session's tail table. */
static struct { struct dma_ring_ctrl ctrl; struct dma_desc desc[16]; } mring[3];
static uint32_t mtails[DMESH_MUX_RINGS];
static unsigned consumer_blocked[8];
static void __dpa_thread_window_writeback(void) {}
static void __dpa_thread_window_read_inv(void) {}
static void doca_dpa_dev_thread_finish(void) { ++finished; }
/* A pooled thread's consumer completions: none pending here. */
static int doca_dpa_dev_comch_consumer_get_completion(uint64_t c,unsigned *e) { (void)c;(void)e;return 0; }
static void doca_dpa_dev_comch_consumer_completion_ack(uint64_t c,unsigned n) { (void)c;(void)n; }
static void doca_dpa_dev_comch_consumer_ack(uint64_t c,unsigned n) { (void)c;(void)n; }
static void doca_dpa_dev_comch_consumer_completion_request_notification(uint64_t c) { (void)c; }
/* Array 1: the native thread's ring; 2-4: the mux rings; 100: the tail table. */
static unsigned doca_dpa_dev_buf_array_get_buf(uint64_t a, unsigned i) { return (unsigned)(a << 16 | i); }
static uint64_t doca_dpa_dev_buf_get_external_ptr(unsigned b) {
 unsigned a=b>>16,i=b&0xffff;
 if(a==100){assert(i<DMESH_TAILS_LINES);return (uintptr_t)&mtails[i*DMESH_TAILS_PER_LINE];}
 if(a>=2&&a<5){assert(i<=16);return (uintptr_t)(i ? (void *)&mring[a-2].desc[i-1] : (void *)&mring[a-2].ctrl);}
 assert(a==1&&i<=256);return (uintptr_t)(i ? (void *)&descriptors[i-1] : (void *)&ring);
}
/* The mux thread's status window: the handle is unused, the address is the ARM's copy. */
static uint64_t doca_dpa_dev_mmap_get_external_ptr(uint64_t h,uint64_t addr) { assert(h==1);return addr; }
static void doca_dpa_dev_completion_request_notification(uint64_t c) { (void)c;++arms; }
static int doca_dpa_dev_get_completion(uint64_t c,unsigned *comp) {
 (void)c;assert(arms);if(read_cq==ready)return 0;*comp=read_cq++;return 1;
}
static unsigned doca_dpa_dev_get_completion_type(unsigned c) { return pending[c].type; }
static void doca_dpa_dev_completion_ack(uint64_t c,unsigned n) { (void)c;acked+=n;assert(acked<=read_cq); }
static int doca_dpa_dev_comch_producer_is_consumer_empty(uint64_t p,int c) {
 (void)p;assert(c>=0&&c<8);return copies>=credit_limit||consumer_blocked[c];
}
static void doca_dpa_dev_comch_producer_dma_copy(uint64_t p,int c,uint32_t dm,uint64_t da,
 uint32_t sm,uint64_t sa,uint32_t len,uint8_t *m,uint32_t n,uint64_t flags) {
 (void)p;(void)dm;(void)sm;
 assert(flags==DOCA_DPA_DEV_SUBMIT_FLAG_FLUSH);assert(copies<1024);
 assert(copies-acked<DMESH_DPA_MAX_INFLIGHT);
 pending[copies].src=sa;pending[copies].dst=da;pending[copies].len=len;pending[copies].consumer=c;
 if(n==sizeof(struct dmesh_mux_msg))memcpy(&pending[copies].msg,m,n);
 ++copies;
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
static struct dpa_mux_ctx mctx;
static struct dpa_mux_status mstatus; /* what the ARM reads */
static const unsigned mux_ring[3] = {0, 1, 17};
static void mux_open(unsigned k) {
 unsigned r=mux_ring[k];
 memset(&mring[k],0,sizeof(mring[k]));
 mctx.cfg[r]=(struct dpa_mux_ring_cfg){.buf_arr=2+k,.staging_addr=(uintptr_t)(dst+k*65536),
  .buf_size=65536,.ring_size=16,.host_base=(uintptr_t)src,.host_len=sizeof(src),
  .slot=10+k,.generation=100+k,.consumer=1+k,.cmd=DMESH_MUX_CMD_OPEN};
 memset(&mctx.s.st[r],0,sizeof(mctx.s.st[r]));memset(&mstatus.st[r],0,sizeof(mstatus.st[r]));
 mtails[r]=0;mctx.ctl_seq++;
}
/* The host's post: descriptors, the ring's producer_tail, then its table slot. */
static void mux_post(unsigned k,unsigned n,unsigned len) {
 for(unsigned i=0;i<n;i++) {
  unsigned seq=(unsigned)mring[k].ctrl.producer_tail+i,slot=seq%16,s=k*16+slot;
  memset(src[s],(seq%250)+1,len);
  mring[k].desc[slot]=(struct dma_desc){.addr=(uintptr_t)src[s],.size=len};
 }
 mring[k].ctrl.producer_tail+=n;
 mtails[mux_ring[k]]=(uint32_t)mring[k].ctrl.producer_tail;
}
static void mux_tests(void) {
 memset(pending,0,sizeof(pending));memset(dst,0,sizeof(dst));
 copies=finished=ready=read_cq=acked=arms=0;credit_limit=1024;
 mctx.tails=100;mctx.status=1;mctx.status_addr=(uintptr_t)&mstatus;tls=(void *)&mctx;
 for(unsigned k=0;k<3;k++)mux_open(k);
 unsigned rt=retriggers,f=finished;
 /* Idle: one read per tail line with an open ring, no ring visited. */
 run_dma_mux();
 assert(retriggers==rt+1 && !copies && mctx.s.heartbeat>1);
 assert(mctx.s.open_mask==(1ull|2ull|1ull<<17) && mctx.s.ctl_seen==mctx.ctl_seq);
 assert(mctx.s.tail_reads==2*mctx.s.heartbeat && mctx.s.visits==0);
 for(unsigned k=0;k<3;k++)assert(mctx.s.st[mux_ring[k]].state==DMESH_MUX_ACTIVE);
 /* Every activation publishes the counters and the thread's state; ring
  * lines are published only when a ring stops or closes. */
 assert(memcmp(&mstatus,&mctx.s,offsetof(struct dpa_mux_status,st))==0 && mstatus.heartbeat==mctx.s.heartbeat);
 assert(mstatus.st[0].state==DMESH_MUX_IDLE && mstatus.st[17].state==DMESH_MUX_IDLE);
 /* Only the ring whose tail moved is visited; its copies and messages go to
  * its worker's consumer, and its custody ACK waits for their completion. */
 mux_post(1,3,64);run_dma_mux();
 assert(copies==3 && mctx.s.visits==1 && mctx.s.copies==3 && mctx.s.st[1].submit==3 && mctx.s.st[1].inflight==3);
 for(unsigned i=0;i<3;i++) {
  assert(pending[i].consumer==2 && pending[i].msg.type==DMESH_MUX_MSG_DMA_DONE && pending[i].msg.ring==1);
  assert(pending[i].msg.slot==11 && pending[i].msg.generation==101 && pending[i].msg.pos==i*64);
  assert(pending[i].msg.length==64 && pending[i].dst==(uintptr_t)(dst+65536+i*64));
 }
 assert(mring[1].ctrl.consumer_head==0 && !mctx.s.st[0].submit && !mctx.s.st[17].submit);
 complete_to(3);run_dma_mux();
 assert(mring[1].ctrl.consumer_head==3 && mring[1].ctrl.completed_bytes==192 && !mctx.s.st[1].inflight);
 assert(dst[65536]==1 && dst[65536+128]==3 && mctx.s.visits==1);
 /* A worker without receive credit holds back only its own rings. */
 consumer_blocked[3]=1;
 mux_post(0,2,32);mux_post(2,2,32);
 unsigned c0=copies,waits=mctx.s.credit_waits;
 run_dma_mux();
 assert(copies==c0+2 && mctx.s.credit_waits>waits && mctx.s.st[0].submit==2 && !mctx.s.st[17].submit);
 assert(pending[c0].consumer==1 && pending[c0].msg.ring==0 && pending[c0+1].msg.ring==0);
 consumer_blocked[3]=0;run_dma_mux();
 assert(copies==c0+4 && mctx.s.st[17].submit==2 && pending[c0+2].consumer==3 && pending[c0+2].msg.ring==17);
 assert(pending[c0+2].msg.slot==12 && pending[c0+3].dst==(uintptr_t)(dst+2*65536+32));
 complete_to(copies);run_dma_mux();
 assert(mring[0].ctrl.consumer_head==2 && mring[2].ctrl.consumer_head==2 && mctx.s.fifo_tail==mctx.s.fifo_head);
 /* A tail past the ring stops that ring alone, with the error in its ctrl. */
 mtails[1]=(uint32_t)mctx.s.st[1].submit+17;run_dma_mux();
 assert(mctx.s.st[1].state==DMESH_MUX_ERROR && mctx.s.st[1].error==DMESH_MUX_ERR_TAIL);
 assert(mring[1].ctrl.error==DMESH_MUX_ERR_TAIL && !(mctx.s.open_mask&2) && (mctx.s.open_mask&1));
 assert(mstatus.st[1].state==DMESH_MUX_ERROR && mstatus.st[1].error==DMESH_MUX_ERR_TAIL);
 /* Close: CLOSED once the ring's last copy completed; the ERROR ring too. */
 mux_post(0,1,32);run_dma_mux();assert(mctx.s.st[0].inflight==1);
 mctx.cfg[0].cmd=DMESH_MUX_CMD_CLOSE;mctx.cfg[1].cmd=DMESH_MUX_CMD_CLOSE;mctx.ctl_seq++;
 run_dma_mux();
 assert(mctx.s.st[1].state==DMESH_MUX_CLOSED && mctx.s.st[0].state==DMESH_MUX_ACTIVE && (mctx.s.closing_mask&1));
 mux_post(0,1,32);unsigned c1=copies;run_dma_mux();assert(copies==c1); /* closing: no new copies */
 complete_to(copies);run_dma_mux();
 assert(mctx.s.st[0].state==DMESH_MUX_CLOSED && !mctx.s.closing_mask && mring[0].ctrl.consumer_head==3);
 assert(mstatus.st[0].state==DMESH_MUX_CLOSED && mstatus.st[0].submit==3 && !mstatus.st[0].inflight);
 assert(mstatus.st[1].state==DMESH_MUX_CLOSED && mstatus.closing_mask==0);
 assert(mctx.s.open_mask==1ull<<17);
 /* The ARM fills ring 0 again for the session's next flow on that id. */
 mux_open(0);mctx.cfg[0].generation=200;mux_post(0,1,16);run_dma_mux();
 assert(copies==c1+1 && pending[c1].msg.ring==0 && pending[c1].msg.generation==200 && pending[c1].msg.pos==0);
 /* Stop waits for the copy in flight, then the thread finishes. */
 mctx.stop=1;run_dma_mux();assert(!mctx.s.stopped && finished==f);
 assert(!mstatus.stopped);
 complete_to(copies);run_dma_mux();assert(mctx.s.stopped && finished==f+1 && mstatus.stopped);
 puts("DPA pod mux: tail lines read once per pass, only moved rings visited, per-worker consumers and credit, ring errors, close, refill, stop, status published PASS");
}

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
 a.pos=a.buf_size-8;a.rx_consumed_pos=0;
 descriptor(6,4,16);ring.producer_tail=7;credit_limit=7;
 poll_desc_ring(&a);assert(copies==6); /* byte backpressure */
 a.rx_consumed_pos=a.pos;poll_desc_ring(&a);
 assert(copies==7 && a.pos==16 && ring.consumer_head==6);
 a.stop=1;poll_desc_ring(&a);
 assert(!finished && !a.stopped); /* kernel cannot exit with pending DMA */
 complete_to(7);poll_desc_ring(&a);
 assert(finished==1 && a.stopped && a.dma_submitted==7);
 assert(ring.consumer_head==7 && ring.completed_bytes==112 && dst[0]==7);

 /* A slow reader must stop submissions even with CQ and receive credits.
  * Zero-initialized TLS needs no opt-in. CQEs still drain while RX is full. */
 a=reset(16);a.buf_size=64*1024;
 for(unsigned i=0;i<10;i++)descriptor(i,16,8064);
 ring.producer_tail=10;poll_desc_ring(&a);
 assert(copies==8 && a.pos==8*8064 && ring.consumer_head==0);
 complete_to(8);poll_desc_ring(&a);
 assert(copies==8 && ring.consumer_head==8 && ring.completed_bytes==8*8064);
 poll_desc_ring(&a);assert(copies==8 && dst[0]==1); /* completion is not consumption */
 a.rx_consumed_pos=8064;poll_desc_ring(&a);
 assert(copies==8 && a.pos==8*8064); /* wrap + copy would consume the last byte */
 a.rx_consumed_pos=2*8064;poll_desc_ring(&a);
 assert(copies==9 && a.pos==8064 && pending[8].dst==(uintptr_t)dst);
 complete_to(9);poll_desc_ring(&a);
 assert(copies==9 && ring.consumer_head==9 && dst[0]==9 && dst[2*8064]==3);
 a.stop=1;poll_desc_ring(&a);assert(a.stopped && finished==1);

 /* Contiguous copy: exactly enough space is full, one extra byte permits it.
  * Small descriptors work even with far less than the old fixed headroom. */
 a=reset(4);a.buf_size=1024;a.pos=128;a.rx_consumed_pos=256;
 descriptor(0,4,128);ring.producer_tail=1;
 poll_desc_ring(&a);assert(!copies && a.pos==128);
 a.rx_consumed_pos=257;poll_desc_ring(&a);
 assert(copies==1 && a.pos==256 && pending[0].dst==(uintptr_t)(dst+128));
 complete_to(1);poll_desc_ring(&a);assert(ring.consumer_head==1);

 /* Total free bytes may fit the payload but not the skipped tail + payload.
  * Neither a failed space check nor missing receive credit may move pos. */
 a=reset(4);a.buf_size=1024;a.pos=896;a.rx_consumed_pos=128;
 descriptor(0,4,256);ring.producer_tail=1;dst[256]=0x7e;
 poll_desc_ring(&a);assert(!copies && a.pos==896); /* free=256, required=384 */
 a.rx_consumed_pos=256;poll_desc_ring(&a);
 assert(!copies && a.pos==896); /* free=384: would look empty after submission */
 a.rx_consumed_pos=257;credit_limit=0;poll_desc_ring(&a);
 assert(!copies && a.pos==896);
 credit_limit=1;poll_desc_ring(&a);
 assert(copies==1 && a.pos==256 && pending[0].dst==(uintptr_t)dst && pending[0].len==256);
 complete_to(1);poll_desc_ring(&a);
 assert(ring.consumer_head==1 && ring.completed_bytes==256 && dst[256]==0x7e);

 /* A copy ending exactly at the buffer end needs no wrap padding. The CPU
  * may publish buf_size itself as the end offset of that consumed segment. */
 a=reset(4);a.buf_size=1024;a.pos=896;a.rx_consumed_pos=0;
 descriptor(0,4,128);ring.producer_tail=1;
 poll_desc_ring(&a);assert(!copies && a.pos==896);
 a.rx_consumed_pos=1;poll_desc_ring(&a);
 assert(copies==1 && a.pos==0 && pending[0].dst==(uintptr_t)(dst+896));
 complete_to(1);poll_desc_ring(&a);
 a.rx_consumed_pos=a.buf_size;descriptor(1,4,128);ring.producer_tail=2;
 poll_desc_ring(&a);assert(copies==2 && a.pos==128 && pending[1].dst==(uintptr_t)dst);
 complete_to(2);poll_desc_ring(&a);assert(ring.consumer_head==2);

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
 mux_tests();
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
