#include "host_worker.h"
#include "object.h"
#include "comch_client.h"
#include "comch_producer.h"
#include "comch_consumer.h"
#include "comch_common.h"
#include "config.h"
#include "buffer.h"
#include "comch_msgq.h"
#include "dma.h"
#include "dpa.h"
#include "dpa_common.h"
#include "dpa_bench.h"
#include "ring.h"

#include "common.h"

#include <doca_log.h>
#include <doca_buf_array.h>
#include <doca_dpa.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_mmap.h>
#include <time.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BUFFER_SIZE (1024 * 1024)

/* DPA kernel entry points (dpacc host stubs) */
extern doca_dpa_func_t thread_init_rpc;

/* defined in main.c */
extern double diff_sec(const struct timespec *start, const struct timespec *end);

DOCA_LOG_REGISTER(HOST_WORKER);

/*
 * Reverse (response) path, host side. Once the DPU has exported this
 * connection's rcv_ring + tx_staging (objs->reverse_ready), the host launches a
 * DPA thread that mirrors the DPU forward datapath: it polls the DPU-resident
 * rcv_ring for descriptors and DMAs the referenced tx_staging bytes into the
 * host's local rcvbuf, delivering a fused completion to a host-side consumer.
 * This reuses the exact machinery of run_host_dpa_bench, but with a real
 * descriptor ring (dpa_buf_arr over the imported rcv_ring) and DMA
 * source=tx_staging(DPU) / dest=rcvbuf(host).
 */
static doca_error_t
setup_reverse_dpa(struct objects *objs)
{
    doca_error_t result;
    struct dmesh_conn *conn;
    struct dpa_thread_arg arg = {0};
    doca_dpa_dev_completion_t dpa_producer_comp;
    doca_dpa_dev_comch_producer_t dpa_producer;
    doca_dpa_dev_comch_consumer_t dpa_consumer;
    doca_dpa_dev_mmap_t src_mmap, dst_mmap;
    doca_dpa_dev_buf_arr_t dpa_buf_arr;
    struct comch_msg kick = {0};
    uint64_t rpc_ret;

    /* The reverse DPA must run on a DIFFERENT host PCI function than
     * comch/forward (94:00.1): flexio is one-process-per-function and the DPU
     * proxy already holds this host's 94:00.1. Open 94:00.0 (env override) into
     * its own struct objects and run everything reverse there. */
    const char *rev_pci = getenv("DMESH_REV_PCI");
    struct objects *rev;

    if (rev_pci == NULL)
        rev_pci = "94:00.0";

    rev = calloc(1, sizeof(*rev));
    if (rev == NULL)
        return DOCA_ERROR_NO_MEMORY;
    objs->rev_objs = rev;

    result = open_doca_device_with_pci(rev_pci, NULL, &rev->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to open reverse device %s: %s", rev_pci, doca_error_get_descr(result));
        return result;
    }
    result = init_dpa_objects(rev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to init DPA on %s: %s", rev_pci, doca_error_get_descr(result));
        return result;
    }
    result = doca_pe_create(&rev->consumer_pe);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to create consumer PE: %s", doca_error_get_descr(result));
        return result;
    }

    /* reverse DMA destination: a local rcvbuf on the reverse device */
    result = init_dmesh_buffer(rev->dev, &objs->rev_rcvbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to alloc reverse rcvbuf: %s", doca_error_get_descr(result));
        return result;
    }

    /* import the DPU's tx_staging (DMA source) + rcv_ring on the reverse device */
    result = doca_mmap_create_from_export(NULL, objs->rev_msg.tx_desc, objs->rev_msg.tx_desc_len,
                                          rev->dev, &rev->rev_tx_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to import tx_staging: %s", doca_error_get_name(result));
        return result;
    }
    result = doca_mmap_start(rev->rev_tx_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to start tx_staging mmap: %s", doca_error_get_name(result));
        return result;
    }
    result = doca_mmap_create_from_export(NULL, objs->rev_msg.ring_desc, objs->rev_msg.ring_desc_len,
                                          rev->dev, &rev->rev_ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to import rcv_ring: %s", doca_error_get_name(result));
        return result;
    }

    conn = calloc(1, sizeof(*conn));
    if (conn == NULL)
        return DOCA_ERROR_NO_MEMORY;
    conn->objs = rev;
    conn->dpa_thread = calloc(1, sizeof(*conn->dpa_thread));
    if (conn->dpa_thread == NULL)
        return DOCA_ERROR_NO_MEMORY;
    conn->dpa_thread->dpa = rev->dpa_pool->dpa;

    result = dmesh_doca_dpa_thread_create(conn->dpa_thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to create DPA thread: %s", doca_error_get_descr(result));
        return result;
    }

    result = init_comch_dpa_msgq(conn, rev->consumer_pe);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to init DPA msgqs: %s", doca_error_get_descr(result));
        return result;
    }

    /* descriptor ring: buf_array over the imported DPU rcv_ring */
    result = setup_dpa_buf_array(conn, DMA_RING_SIZE, rev->rev_ring_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to set up buf array over rcv_ring: %s", doca_error_get_descr(result));
        return result;
    }

    /* completed-segment ring the recv callback fills; drained by the host loop */
    conn->recv_segs = calloc(DMESH_RECV_SEG_MAX, sizeof(struct dmesh_recv_seg));
    if (conn->recv_segs == NULL)
        return DOCA_ERROR_NO_MEMORY;

    /* DPA handles (all on the reverse device) */
    result = doca_dpa_completion_get_dpa_handle(conn->dpa_comch->producer_comp, &dpa_producer_comp);
    if (result == DOCA_SUCCESS)
        result = doca_comch_consumer_get_dpa_handle(conn->dpa_comch->send.consumer, &dpa_consumer);
    if (result == DOCA_SUCCESS)
        result = doca_comch_producer_get_dpa_handle(conn->dpa_comch->recv.producer, &dpa_producer);
    if (result == DOCA_SUCCESS)
        result = doca_buf_arr_get_dpa_handle(conn->buf_arr, &dpa_buf_arr);
    if (result == DOCA_SUCCESS)
        result = doca_mmap_dev_get_dpa_handle(rev->rev_tx_mmap, rev->dev, &src_mmap);      /* DMA source = DPU tx_staging */
    if (result == DOCA_SUCCESS)
        result = doca_mmap_dev_get_dpa_handle(objs->rev_rcvbuf.mmap, rev->dev, &dst_mmap); /* DMA dest = reverse rcvbuf */
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to get DPA handles: %s", doca_error_get_descr(result));
        return result;
    }

    arg = (struct dpa_thread_arg) {
        .dpa_producer_comp = dpa_producer_comp,
        .dpa_producer = dpa_producer,
        .dpa_buf_arr = dpa_buf_arr,
        .buf_arr_size = DMA_RING_SIZE,
        .host_mmap = src_mmap,               /* DMA source: DPU tx_staging */
        .dpu_mmap = dst_mmap,                /* DMA destination: reverse rcvbuf */
        .src_addr = (uint64_t)objs->rev_rcvbuf.buf,   /* destination base */
        .buf_size = (uint32_t)objs->rev_rcvbuf.size,
    };

    result = doca_dpa_rpc(conn->dpa_thread->dpa, thread_init_rpc, &rpc_ret,
                          dpa_consumer, (uint32_t)CC_DPA_MAX_MSG_NUM, (uint64_t)0);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: init RPC failed: %s", doca_error_get_descr(result));
        return result;
    }
    result = doca_dpa_h2d_memcpy(conn->dpa_thread->dpa, conn->dpa_thread->arg, &arg, sizeof(arg));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to copy thread arg: %s", doca_error_get_descr(result));
        return result;
    }
    result = doca_dpa_thread_run(conn->dpa_thread->thread);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to run DPA thread: %s", doca_error_get_descr(result));
        return result;
    }
    /* kick the thread so it enters its poll loop (a thread only wakes on a
     * completion; the kick plays send_dma_request_to_dpa's role) */
    result = dmesh_doca_dpa_msgq_send(&conn->dpa_comch->send, &kick, sizeof(kick));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("reverse: failed to send kick message: %s", doca_error_get_descr(result));
        return result;
    }

    objs->rev_conn = conn;
    DOCA_LOG_INFO("reverse: host DPA thread running (rcv_ring polling, tx_staging -> rcvbuf DMA)");
    return DOCA_SUCCESS;
}

/*
 * Host-side drain loop for the reverse path: progress the reverse consumer PE
 * (DMA completions) and the control PE, and hand any completed response segment
 * to stdout. Response bytes live in the host's local rcvbuf at [pos, pos+len).
 * Never returns (the connection is long-lived).
 */
static void
host_reverse_drain(struct objects *objs)
{
    struct dmesh_conn *conn = objs->rev_conn;
    struct objects *rev = objs->rev_objs;

    while (true) {
        if (rev != NULL)
            doca_pe_progress(rev->consumer_pe); /* reverse DMA completions (rev dev) */
        doca_pe_progress(objs->pe);             /* control path (forward dev) */

        if (conn == NULL)
            continue;

        while (conn->recv_seg_cnt > 0) {
            struct dmesh_recv_seg *s = &conn->recv_segs[conn->recv_seg_head];
            uint32_t pos = s->pos, len = s->len;

            conn->recv_seg_head = (conn->recv_seg_head + 1) % DMESH_RECV_SEG_MAX;
            conn->recv_seg_cnt--;

            if (len > 0 && (size_t)pos + len <= objs->rev_rcvbuf.size) {
                printf("DMESH_RESPONSE %u bytes @ %u:\n", len, pos);
                fwrite((char *)objs->rev_rcvbuf.buf + pos, 1, len, stdout);
                printf("\n---\n");
                fflush(stdout);
            }
        }
    }
}

/*
 * TCP <-> DMA bridge for protocol-level benchmarking (e.g. HTTP/2 via h2load).
 * libnghttp2 dev headers are unavailable here, so rather than implement HTTP/2
 * in C we make the host a transparent byte pipe and let the battle-tested
 * h2load binary drive the protocol:
 *
 *   h2load (h2c) -> TCP -> [this bridge] -> forward DMA -> DPU proxy (detects
 *   HTTP/2, routes/LB/mTLS) -> backend; response <- reverse DMA (94:00.0) <-
 *
 * One TCP connection maps to this process's single dmesh connection, so run
 * h2load with -c 1 (HTTP/2 multiplexes concurrency over -m streams on the one
 * connection). Forward bytes are streamed into sndbuf as DMA descriptors;
 * reverse response segments are written back to the socket. Busy-polls for
 * lowest latency. Never returns until the peer closes.
 */
static void
run_host_h2_bridge(struct objects *objs, int port)
{
    doca_dpa_dev_mmap_t local_mmap;
    struct objects *rev = objs->rev_objs;
    struct dmesh_conn *rconn = objs->rev_conn;
    struct sockaddr_in addr;
    int lfd, cfd, one = 1;
    size_t spos = 0;
    char tmp[16384];

    if (doca_mmap_dev_get_dpa_handle(objs->sndbuf.mmap, objs->dev, &local_mmap) != DOCA_SUCCESS) {
        DOCA_LOG_ERR("bridge: failed to get sndbuf DPA handle");
        return;
    }

    lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { DOCA_LOG_ERR("bridge: socket() failed"); return; }
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        DOCA_LOG_ERR("bridge: bind(:%d) failed: %s", port, strerror(errno));
        close(lfd);
        return;
    }
    listen(lfd, 1);
    DOCA_LOG_INFO("bridge: listening on :%d (run h2load -c1 http://<host>:%d/)", port, port);

    cfd = accept(lfd, NULL, NULL);
    if (cfd < 0) { DOCA_LOG_ERR("bridge: accept() failed"); close(lfd); return; }
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    fcntl(cfd, F_SETFL, O_NONBLOCK);
    DOCA_LOG_INFO("bridge: client connected; bridging TCP <-> DMA");

    unsigned long dbg_fwd_bytes = 0, dbg_fwd_descs = 0, dbg_rev_bytes = 0, dbg_spin = 0;
    int dbg_first = 1;
    /* Round-trip probe: t_fwd stamped when a forward request burst is committed;
     * measured when the first reverse (response) segment arrives. RTT here =
     * forward DMA + DPU (linkerd+backend) + reverse DMA, from the host's view. */
    struct timespec t_fwd = {0};
    int have_fwd = 0;
    unsigned long rtt_sum_us = 0, rtt_cnt = 0;

    /* Non-blocking reverse: response segments are copied out of rev_rcvbuf into
     * this pending ring immediately (so the DPA can keep filling rev_rcvbuf) and
     * flushed to the TCP socket without blocking. This breaks the bidirectional
     * deadlock where a full TCP send buffer would trap the single bridge thread
     * in a send() spin, stalling both directions under high concurrency. */
#define BRIDGE_PENDING_SIZE (8 * 1024 * 1024)
    char *pend = malloc(BRIDGE_PENDING_SIZE);
    size_t pend_head = 0, pend_len = 0;
    int peer_gone = 0;
    if (pend == NULL) { DOCA_LOG_ERR("bridge: pending buffer alloc failed"); close(cfd); close(lfd); return; }

    while (true) {
        /* Forward: TCP -> sndbuf -> DMA descriptors (chunked into the ring) */
        ssize_t n = recv(cfd, tmp, sizeof(tmp), 0);
        if (n == 0) {
            DOCA_LOG_INFO("bridge: peer closed (fwd=%lu bytes/%lu descs, rev=%lu bytes)",
                          dbg_fwd_bytes, dbg_fwd_descs, dbg_rev_bytes);
            if (rtt_cnt)
                DOCA_LOG_INFO("bridge: host-view RTT (fwd-commit -> rev-arrive) mean %lu us over %lu reqs "
                              "[= forward DMA + DPU(linkerd+backend) + reverse DMA]",
                              rtt_sum_us / rtt_cnt, rtt_cnt);
            break;                      /* peer closed */
        }
        if (n > 0) {
            size_t off = 0;
            (void)dbg_first;
            dbg_fwd_bytes += (unsigned long)n;
            while (off < (size_t)n) {
                size_t remaining = (size_t)n - off;
                size_t chunk;
                struct dma_desc *d;

                /* producer_dma_copy (the fused copy+notify the DPA runs) fires a
                 * completion only when the copy is a multiple of 128B, or a
                 * single sub-block <=128B. Emit the largest 128-aligned copy
                 * (<=8064 = 63*128, under the 8KB single-DMA limit); the final
                 * <=128B tail is a valid single sub-block. This 128-aligns every
                 * DMA while keeping copies large. */
                if (remaining <= 128)
                    chunk = remaining;
                else if (remaining >= 8064)
                    chunk = 8064;
                else
                    chunk = remaining & ~(size_t)127;
                if (spos + chunk > objs->sndbuf.size)
                    spos = 0;           /* wrap sndbuf */
                memcpy((char *)objs->sndbuf.buf + spos, tmp + off, chunk);
                d = get_next_dma_desc(objs->dma_ring);
                d->mmap = local_mmap;
                d->addr = (uint64_t)objs->sndbuf.buf + spos;
                d->size = chunk;
                commit_dma_desc(objs->dma_ring);
                spos += chunk;
                off += chunk;
                dbg_fwd_descs++;
            }
            clock_gettime(CLOCK_MONOTONIC, &t_fwd);
            have_fwd = 1;
        }

        if ((++dbg_spin & 0xFFFFF) == 0 && rconn != NULL)
            DOCA_LOG_INFO("bridge: recv_seg_cnt=%d dropped=%ld pend_len=%zu fwd_descs=%lu rev=%lu",
                          rconn->recv_seg_cnt, rconn->recv_seg_dropped, pend_len,
                          dbg_fwd_descs, dbg_rev_bytes);
        /* Reverse step 1: progress the PEs, then copy completed response
         * segments out of rev_rcvbuf into the pending ring (non-blocking, no
         * TCP write yet) so the DPA/rev_rcvbuf never wait on the socket. */
        if (rev != NULL)
            doca_pe_progress(rev->consumer_pe);
        doca_pe_progress(objs->pe);
        while (rconn != NULL && rconn->recv_seg_cnt > 0) {
            struct dmesh_recv_seg *s = &rconn->recv_segs[rconn->recv_seg_head];
            uint32_t pos = s->pos, len = s->len;
            size_t tail, first;

            if (pend_len + len > BRIDGE_PENDING_SIZE)
                break;                  /* pending full: backpressure, leave in ring */

            rconn->recv_seg_head = (rconn->recv_seg_head + 1) % DMESH_RECV_SEG_MAX;
            rconn->recv_seg_cnt--;

            if (have_fwd) {
                struct timespec now;
                clock_gettime(CLOCK_MONOTONIC, &now);
                rtt_sum_us += (unsigned long)((now.tv_sec - t_fwd.tv_sec) * 1000000L +
                                              (now.tv_nsec - t_fwd.tv_nsec) / 1000);
                rtt_cnt++;
                have_fwd = 0;
            }

            if ((size_t)pos + len > objs->rev_rcvbuf.size)
                continue;               /* defensive: skip out-of-range segment */

            /* enqueue [rev_rcvbuf+pos, len) into the pending ring */
            tail = (pend_head + pend_len) % BRIDGE_PENDING_SIZE;
            first = len < BRIDGE_PENDING_SIZE - tail ? len : BRIDGE_PENDING_SIZE - tail;
            memcpy(pend + tail, (char *)objs->rev_rcvbuf.buf + pos, first);
            if (first < len)
                memcpy(pend, (char *)objs->rev_rcvbuf.buf + pos + first, len - first);
            pend_len += len;
            dbg_rev_bytes += len;
        }

        /* Reverse step 2: flush the pending ring to TCP without blocking. On
         * EAGAIN stop and retry next iteration (keep reading forward + draining
         * reverse meanwhile) - this is what breaks the deadlock. */
        while (pend_len > 0) {
            size_t run = pend_len < BRIDGE_PENDING_SIZE - pend_head
                             ? pend_len : BRIDGE_PENDING_SIZE - pend_head;
            ssize_t k = send(cfd, pend + pend_head, run, MSG_NOSIGNAL);
            if (k > 0) {
                pend_head = (pend_head + (size_t)k) % BRIDGE_PENDING_SIZE;
                pend_len -= (size_t)k;
            } else if (k < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break;                  /* socket full: retry next loop iteration */
            } else {
                peer_gone = 1;          /* peer closed / error */
                break;
            }
        }
        if (peer_gone) {
            DOCA_LOG_INFO("bridge: peer gone (fwd=%lu bytes/%lu descs, rev=%lu bytes)",
                          dbg_fwd_bytes, dbg_fwd_descs, dbg_rev_bytes);
            break;
        }
    }

    free(pend);
    close(cfd);
    close(lfd);
    DOCA_LOG_INFO("bridge: connection closed");
}

/*
 * Shared TCP<->push-DMA splice used by both push bridges (backend = connect
 * side, ingress = listen side). Socket-side bytes go to the forward path
 * (sndbuf staging + 128-aligned descriptors); DPU-side bytes arrive as push
 * batches in rcvbuf's slot ring and are flushed to the socket non-blocking.
 */
static void
run_host_push_splice(struct objects *objs, int cfd, const char *tag)
{
    doca_dpa_dev_mmap_t local_mmap;
    volatile struct dmesh_push_desc *descs =
        (volatile struct dmesh_push_desc *)objs->rcvbuf.buf;
    volatile struct dmesh_push_cursor *cursor =
        (volatile struct dmesh_push_cursor *)((char *)objs->rcvbuf.buf +
                                              DMESH_PUSH_CURSOR_OFF);
    char *data_area = (char *)objs->rcvbuf.buf + DMESH_PUSH_DATA_OFF;
    size_t data_size = objs->rcvbuf.size - DMESH_PUSH_DATA_OFF;
    uint64_t expected = 1;

    /* Advertise push flow control: the DPU read-DMAs this cursor and stops
     * publishing past unconsumed slots/bytes (see struct dmesh_push_cursor). */
    cursor->consumed_seq = 0;
    cursor->consumed_bytes = 0;
    cursor->magic = DMESH_PUSH_FC_MAGIC;
    size_t spos = 0;
    char tmp[16384];

    if (doca_mmap_dev_get_dpa_handle(objs->sndbuf.mmap, objs->dev, &local_mmap) != DOCA_SUCCESS) {
        DOCA_LOG_ERR("%s: failed to get sndbuf DPA handle", tag);
        return;
    }

#define BACKEND_PENDING_SIZE (8 * 1024 * 1024)
    char *pend = malloc(BACKEND_PENDING_SIZE);
    size_t pend_head = 0, pend_len = 0;
    int peer_gone = 0;
    unsigned long req_bytes = 0, resp_bytes = 0;
    /* Idle backoff: N bridges busy-polling starve the host's h2load clients
     * (32 hot processes on 18 cores at M=16). After 64 workless iterations
     * sleep 5us, after 4096 sleep 50us; any work resets. DMESH_SPLICE_SPIN=1
     * restores the pure spin for A/B. */
    unsigned idle_iters = 0;
    const int spin = getenv("DMESH_SPLICE_SPIN") != NULL;
    if (pend == NULL) return;

    while (!peer_gone) {
        int did_work = 0;
        /* DPU->host: consume published push batches in order -> pending ring */
        while (descs[expected % DMESH_PUSH_DESC_N].seq == expected) {
            uint32_t pos = descs[expected % DMESH_PUSH_DESC_N].pos;
            uint32_t len = descs[expected % DMESH_PUSH_DESC_N].len;
            size_t tail, first;

            if (len == 0 || (size_t)pos + len > data_size ||
                pend_len + len > BACKEND_PENDING_SIZE)
                break;                  /* bogus desc or backpressure */

            tail = (pend_head + pend_len) % BACKEND_PENDING_SIZE;
            first = len < BACKEND_PENDING_SIZE - tail ? len : BACKEND_PENDING_SIZE - tail;
            memcpy(pend + tail, data_area + pos, first);
            if (first < len)
                memcpy(pend, data_area + pos + first, len - first);
            pend_len += len;
            req_bytes += len;
            expected++;
            did_work = 1;
            /* Batch copied out of the data ring: publish consumption. */
            cursor->consumed_seq = expected - 1;
            cursor->consumed_bytes = req_bytes;
        }

        /* Flush pending DPU bytes to the TCP peer (non-blocking) */
        while (pend_len > 0) {
            size_t run = pend_len < BACKEND_PENDING_SIZE - pend_head
                             ? pend_len : BACKEND_PENDING_SIZE - pend_head;
            ssize_t k = send(cfd, pend + pend_head, run, MSG_NOSIGNAL);
            if (k > 0) {
                did_work = 1;
                pend_head = (pend_head + (size_t)k) % BACKEND_PENDING_SIZE;
                pend_len -= (size_t)k;
            } else if (k < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break;
            } else {
                peer_gone = 1;
                break;
            }
        }

        /* host->DPU: TCP -> sndbuf -> forward DMA descriptors */
        ssize_t n = recv(cfd, tmp, sizeof(tmp), 0);
        if (n == 0) {
            peer_gone = 1;
        } else if (n > 0) {
            size_t off = 0;
            did_work = 1;
            resp_bytes += (unsigned long)n;
            while (off < (size_t)n) {
                size_t remaining = (size_t)n - off;
                size_t chunk;
                struct dma_desc *d;
                volatile struct dma_ring_ctrl *rc =
                    (volatile struct dma_ring_ctrl *)objs->dma_ring->ctrl;

                /* Forward backpressure (see host_lib.c): cap outstanding
                 * descriptors so sndbuf never wraps over unconsumed bytes.
                 * Bytes already read from the socket but not yet staged are
                 * dropped in this legacy bridge path only under extreme
                 * pressure; the library path returns a partial write. */
                if (rc->producer_tail - rc->consumer_head >= 120) {
                    doca_pe_progress(objs->pe);
                    continue;
                }

                /* producer_dma_copy completion rule: multiple of 128B, or a
                 * single <=128B block (see the client bridge) */
                if (remaining <= 128)
                    chunk = remaining;
                else if (remaining >= 8064)
                    chunk = 8064;
                else
                    chunk = remaining & ~(size_t)127;
                if (spos + chunk > objs->sndbuf.size)
                    spos = 0;
                memcpy((char *)objs->sndbuf.buf + spos, tmp + off, chunk);
                d = get_next_dma_desc(objs->dma_ring);
                d->mmap = local_mmap;
                d->addr = (uint64_t)objs->sndbuf.buf + spos;
                d->size = chunk;
                commit_dma_desc(objs->dma_ring);
                spos += chunk;
                off += chunk;
            }
        }
        /* n < 0 with EAGAIN: nothing to read this iteration */

        if (doca_pe_progress(objs->pe)) /* control path (comch) */
            did_work = 1;

        if (did_work)
            idle_iters = 0;
        else if (++idle_iters > 64 && !spin)
            usleep(idle_iters > 4096 ? 50 : 5);
    }

    DOCA_LOG_INFO("%s: closed (from-DPU=%lu bytes, to-DPU=%lu bytes)",
                  tag, req_bytes, resp_bytes);
    free(pend);
}

/*
 * Push-transport INGRESS bridge (listen side): h2load connects here; the DPU
 * serves the flow as a CLIENT (h2 server) while the reverse path is the
 * DPA-free push channel, so any number of these processes can run.
 * DMESH_PUSH_BRIDGE_PORT=<port>. One connection then exit, like the peers.
 */
/* Accept the ingress TCP client BEFORE the DMA channel is exported: the proxy
 * starts protocol detection (10s budget) the moment the flow attaches, so the
 * first request bytes must effectively be in hand at attach time or the flow
 * silently degrades to an opaque L4 forward. Polls the comch PE while waiting
 * so the control path stays serviced. */
static int
host_push_ingress_accept(struct objects *objs, int port)
{
    struct sockaddr_in addr;
    int lfd, cfd = -1, one = 1;

    lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { DOCA_LOG_ERR("push-ingress: socket() failed"); return -1; }
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        DOCA_LOG_ERR("push-ingress: bind(:%d) failed: %s", port, strerror(errno));
        close(lfd);
        return -1;
    }
    listen(lfd, 1);
    fcntl(lfd, F_SETFL, O_NONBLOCK);
    DOCA_LOG_INFO("push-ingress: listening on :%d (h2load -c1 here)", port);
    while (cfd < 0) {
        cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                DOCA_LOG_ERR("push-ingress: accept() failed: %s", strerror(errno));
                close(lfd);
                return -1;
            }
            doca_pe_progress(objs->pe);
            usleep(1000);
        }
    }
    close(lfd);
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    fcntl(cfd, F_SETFL, O_NONBLOCK);
    DOCA_LOG_INFO("push-ingress: client accepted, attaching DMA channel");
    return cfd;
}

static void
run_host_push_ingress_bridge(struct objects *objs, int cfd)
{
    run_host_push_splice(objs, cfd, "push-ingress");
    close(cfd);
}

/*
 * Backend bridge (plan "안 2"): this host process provides a local TCP backend
 * (e.g. nginx on the host) to the DPU proxy over DMA, with NO host DPA.
 *
 *   requests  (proxy -> backend): the DPU pushes batches into our rcvbuf's
 *     data ring with its doca_dma engine and publishes each one via a 16B
 *     descriptor slot (struct dmesh_push_desc); we busy-poll the slots in
 *     order - plain reads of our own memory - and write the bytes to the TCP
 *     connection we opened to the real backend.
 *   responses (backend -> proxy): identical to the client bridge's forward
 *     path - TCP recv into sndbuf, 128-aligned descriptors into the DMA ring;
 *     the DPU-side DPA thread (the proxy's, on the DPU function) pulls them.
 *
 * connect_addr is "ip:port" of the real backend on this host.
 */
static void
run_host_backend_bridge(struct objects *objs, const char *connect_addr)
{
    struct sockaddr_in addr;
    char ip[64];
    int port, cfd, one = 1;
    const char *colon = strrchr(connect_addr, ':');

    if (colon == NULL || (size_t)(colon - connect_addr) >= sizeof(ip)) {
        DOCA_LOG_ERR("backend bridge: bad DMESH_BACKEND_CONNECT '%s' (want ip:port)", connect_addr);
        return;
    }
    memcpy(ip, connect_addr, colon - connect_addr);
    ip[colon - connect_addr] = '\0';
    port = atoi(colon + 1);

    cfd = socket(AF_INET, SOCK_STREAM, 0);
    if (cfd < 0) { DOCA_LOG_ERR("backend bridge: socket() failed"); return; }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons((uint16_t)port);
    if (connect(cfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        DOCA_LOG_ERR("backend bridge: connect(%s:%d) failed: %s", ip, port, strerror(errno));
        close(cfd);
        return;
    }
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    fcntl(cfd, F_SETFL, O_NONBLOCK);
    DOCA_LOG_INFO("backend bridge: connected to %s:%d; serving over DMA (push mode)", ip, port);

    run_host_push_splice(objs, cfd, "backend bridge");
    close(cfd);
}

void
run_host_worker(struct objects *objs, const char *server_name)
{
    doca_error_t result;

    DOCA_LOG_INFO("Starting Host worker, connecting to server '%s'", server_name);

    /* initialize DOCA Comch client objects */
    result = init_comch_ctrl_path_client(server_name, objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init comch control path client: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* initialize DOCA Comch producer objects */
    result = init_comch_datapath_producer(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to send message over comch data path: %s", doca_error_get_descr(result));
        cleanup_objects(objs);
        return;
    }

    /* setup DMA ring */
    result = setup_dma_ring(objs, DMA_RING_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to setup DMA ring: %s", doca_error_get_descr(result));
        return;
    }

    /* allocate local buffer and set mmap for PCI export */
    result = init_dmesh_buffer(objs->dev, &objs->sndbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init dmesh buffer: %s", doca_error_get_descr(result));
        return;
    }

    result = init_dmesh_buffer(objs->dev, &objs->rcvbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to init dmesh buffer: %s", doca_error_get_descr(result));
        return;
    }

    /* Backend provider mode (안 2): declare it in the flow metadata BEFORE the
     * export; the DPU then pushes into our rcvbuf instead of exporting a
     * rcv_ring, and we run no host DPA at all. */
    const char *backend_connect = getenv("DMESH_BACKEND_CONNECT");
    if (backend_connect != NULL) {
        objs->flow.mode = DMESH_FLOW_MODE_BACKEND;
        /* descriptor slots must start empty (seq 0) */
        memset(objs->rcvbuf.buf, 0, DMESH_PUSH_DATA_OFF);
    }

    /* Push-transport ingress (client-semantics flow, DPA-free reverse):
     * declared in the metadata like BACKEND so the DPU uses the push engine,
     * but served as a client flow by the proxy. */
    const char *push_port = getenv("DMESH_PUSH_BRIDGE_PORT");
    int ingress_cfd = -1;
    if (push_port != NULL && atoi(push_port) > 0) {
        objs->flow.mode = DMESH_FLOW_MODE_INGRESS_PUSH;
        memset(objs->rcvbuf.buf, 0, DMESH_PUSH_DATA_OFF);
        /* block for the TCP client BEFORE exporting: see host_push_ingress_accept */
        ingress_cfd = host_push_ingress_accept(objs, atoi(push_port));
        if (ingress_cfd < 0)
            return;
    }

    /* export DMA ring + send/receive buffer metadata to DPU in one message */
    result = export_dma_metadata(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to export DMA metadata: %s", doca_error_get_descr(result));
        return;
    }

    if (backend_connect != NULL) {
        run_host_backend_bridge(objs, backend_connect);
        return;
    }

    if (ingress_cfd >= 0) {
        run_host_push_ingress_bridge(objs, ingress_cfd);
        return;
    }

    /* Reverse path: wait for the DPU to export its rcv_ring + tx_staging (sent
     * once the connection reaches RUNNING), then launch the host DPA thread that
     * DMAs responses back into the local rcvbuf. */
    DOCA_LOG_INFO("Waiting for reverse-path metadata from DPU...");
    while (!objs->reverse_ready)
        doca_pe_progress(objs->pe);
    /* Forward-only throughput bench: DMESH_SKIP_REVERSE=1 skips the reverse
     * host DPA (flexio is one-process-per-function, so multiple host threads
     * can't each open 94:00.0) and goes straight to the forward send loop.
     * Bridge/response modes are unavailable in this mode. */
    const char *skip_reverse = getenv("DMESH_SKIP_REVERSE");
    if (skip_reverse == NULL || atoi(skip_reverse) == 0) {
        result = setup_reverse_dpa(objs);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to set up reverse path: %s", doca_error_get_descr(result));
            return;
        }

    /* Benchmark bridge mode: expose the DMA path as a local TCP endpoint so
     * h2load (or any TCP client) can drive it. DMESH_BRIDGE_PORT=<port>. */
    {
        const char *bridge_port = getenv("DMESH_BRIDGE_PORT");
        if (bridge_port != NULL && atoi(bridge_port) > 0) {
            run_host_h2_bridge(objs, atoi(bridge_port));
            return;
        }
    }
    }   /* end !DMESH_SKIP_REVERSE */

    // result = init_dpa_objects(objs);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to init DPA objects: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

    // result = dmesh_doca_dpa_thread_create(objs->dpa_thread);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to create DPA thread: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

    // result = init_comch_dpa_msgq(objs, objs->producer_pe);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to init comch DPA datapath: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

    // result = setup_dpa_buf_array(objs, DMA_RING_SIZE, objs->local_mmap);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to setup DPA buffer array: %s", doca_error_get_descr(result));
    //     goto argp_cleanup;
    // }

    // result = dmesh_doca_run_dpa_thread(objs, objs->dpa_thread, objs->dpa_comch);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to run DPA thread: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp_cleanup;
    // }

    // result = send_dma_request_to_dpa(objs);
    // if (result != DOCA_SUCCESS) {
    //     DOCA_LOG_ERR("Failed to send DMA request to DPA: %s", doca_error_get_descr(result));
    //     cleanup_objects(objs);
    //     goto argp _cleanup;
    // }

    int idx = 9999;
    size_t pos = 0;
    struct dma_desc *desc;

    doca_dpa_dev_mmap_t local_mmap;
    doca_mmap_dev_get_dpa_handle(objs->sndbuf.mmap, objs->dev, &local_mmap);
    
    /* HTTP request mode: send exactly one request (<=8KB) as a single DMA
     * message, then idle to keep the connection open for the response path.
     * This exercises the outbound stack (detect/discovery/LB/mTLS) end to end.
     * DMESH_HTTP=1 sends a canned GET; DMESH_HTTP_REQ overrides the payload. */
    /* DMESH_HTTP_FILE=<path> sends the file's raw bytes (binary-safe: use this
     * for HTTP/2 prior-knowledge, gRPC, TLS ClientHello, etc.). Falls back to
     * DMESH_HTTP_REQ (text) or the canned GET under DMESH_HTTP=1. */
    const char *http_file = getenv("DMESH_HTTP_FILE");
    const char *http_req = getenv("DMESH_HTTP_REQ");
    if (http_req == NULL && http_file == NULL && getenv("DMESH_HTTP") != NULL)
        http_req = "GET / HTTP/1.1\r\nHost: dmesh\r\nConnection: close\r\n\r\n";
    if (http_file != NULL) {
        FILE *f = fopen(http_file, "rb");
        size_t len = 0;
        if (f == NULL) {
            DOCA_LOG_ERR("Failed to open DMESH_HTTP_FILE '%s'", http_file);
            return;
        }
        len = fread(objs->sndbuf.buf, 1, objs->sndbuf.size, f);
        fclose(f);
        desc = get_next_dma_desc(objs->dma_ring);
        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->sndbuf.buf;
        desc->size = len;
        commit_dma_desc(objs->dma_ring);
        DOCA_LOG_INFO("Host worker sent %zu bytes from %s; draining response path", len, http_file);
        host_reverse_drain(objs);
        return;
    }
    if (http_req != NULL) {
        size_t len = strlen(http_req);
        if (len > objs->sndbuf.size)
            len = objs->sndbuf.size;
        memcpy(objs->sndbuf.buf, http_req, len);
        desc = get_next_dma_desc(objs->dma_ring);
        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->sndbuf.buf;
        desc->size = len;
        commit_dma_desc(objs->dma_ring);
        DOCA_LOG_INFO("Host worker sent HTTP request (%zu bytes); draining response path", len);
        host_reverse_drain(objs);
        return;
    }

    int msg_size = 8192;
    const char *msg_size_env = getenv("DMESH_MSG_SIZE");
    if (msg_size_env != NULL && atoi(msg_size_env) > 0)
        msg_size = atoi(msg_size_env);
    DOCA_LOG_INFO("Host worker msg_size = %d bytes", msg_size);
    while (true) {
        // if (doca_pe_progress(objs->pe) == 0)
            // nanosleep(&ts, &ts);

        desc = get_next_dma_desc(objs->dma_ring);
        desc->mmap = local_mmap;
        desc->addr = (uint64_t)objs->sndbuf.buf + pos;
        *(uint32_t *)desc->addr = idx--;
        desc->size = (size_t)msg_size;
        commit_dma_desc(objs->dma_ring);
        pos += msg_size;
        if (pos >= objs->sndbuf.size) {
            pos = 0;
        }
        // if (cur_ts.tv_nsec - prev_ts.tv_nsec >= 100) {
        //     prev_ts = cur_ts;
        // }
        // break;
    }

    DOCA_LOG_INFO("Finished Host worker");
}

/* One drain worker per host CPU core: busy-polls its own PE. Completion
 * callbacks run inside doca_pe_progress, so each drain group's counters
 * (its private struct objects) are only ever written by its own thread. */
struct host_bench_drain_ctx {
    struct doca_pe *pe;
    volatile int *stop;
};

static void *
host_bench_drain(void *a)
{
    struct host_bench_drain_ctx *ctx = a;

    while (!*ctx->stop)
        doca_pe_progress(ctx->pe);
    return NULL;
}

/*
 * Standalone host-launched DPA producer_dma_copy benchmark: the host CPU
 * creates the DPA instance, thread and comch msgqs itself (no DPU app, no
 * comch server involved) and launches the bench kernel. The DMA copies
 * host sndbuf -> host staging buffer; completion messages land on the host
 * CPU consumer, which gives the wall-clock rate. Configured via
 * DMESH_DPA_BENCH_MODE/SIZE/OPS/THREADS/CORES, like the DPU-launched variant.
 * DPA threads are distributed round-robin over CORES PEs, each drained by a
 * dedicated host pthread; the main thread only monitors the counters.
 */
static void
run_host_dpa_bench(const struct global_config *gcfg)
{
#define HOST_BENCH_MAX_THREADS 32
#define HOST_BENCH_MAX_CORES 8
    struct objects *objs;
    struct objects *gobjs[HOST_BENCH_MAX_CORES] = {0};
    struct doca_pe *pes[HOST_BENCH_MAX_CORES] = {0};
    pthread_t drain_threads[HOST_BENCH_MAX_CORES];
    struct host_bench_drain_ctx drain_ctxs[HOST_BENCH_MAX_CORES];
    volatile int drain_stop = 0;
    uint32_t bench_cores = 1;
    long total_recv;
    struct dmesh_conn *conns[HOST_BENCH_MAX_THREADS] = {0};
    struct dmesh_doca_dpa_thread *dpa_threads[HOST_BENCH_MAX_THREADS] = {0};
    struct dpa_bench_state arg = {0};
    doca_dpa_dev_completion_t dpa_producer_comp;
    doca_dpa_dev_comch_producer_t dpa_producer;
    doca_dpa_dev_comch_consumer_t dpa_consumer;
    doca_dpa_dev_mmap_t src_mmap, dst_mmap;
    struct timespec t_start, t_now, t_first = {0}, t_rep = {0};
    long first_seen = 0, rep_cnt = 0, total_ops;
    uint64_t rpc_ret;
    doca_error_t result;
    const char *env;
    uint32_t bench_mode = 1, bench_size = 4096, bench_ops = 1000000, bench_threads = 1;
    uint32_t i;

    if ((env = getenv("DMESH_DPA_BENCH_MODE")) != NULL && atoi(env) > 0)
        bench_mode = (uint32_t)atoi(env);
    if ((env = getenv("DMESH_DPA_BENCH_SIZE")) != NULL && atoi(env) > 0)
        bench_size = (uint32_t)atoi(env);
    if ((env = getenv("DMESH_DPA_BENCH_OPS")) != NULL && atoi(env) > 0)
        bench_ops = (uint32_t)atoi(env);
    if ((env = getenv("DMESH_DPA_BENCH_THREADS")) != NULL && atoi(env) > 0)
        bench_threads = (uint32_t)atoi(env);
    if (bench_threads > HOST_BENCH_MAX_THREADS)
        bench_threads = HOST_BENCH_MAX_THREADS;
    if ((env = getenv("DMESH_DPA_BENCH_CORES")) != NULL && atoi(env) > 0)
        bench_cores = (uint32_t)atoi(env);
    if (bench_cores > HOST_BENCH_MAX_CORES)
        bench_cores = HOST_BENCH_MAX_CORES;
    if (bench_cores > bench_threads)
        bench_cores = bench_threads;
    total_ops = (long)bench_ops * bench_threads;

    objs = calloc(1, sizeof(*objs));
    if (objs == NULL)
        return;

    result = open_doca_device_with_pci(gcfg->dev_pci_addr, NULL, &objs->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to open DOCA device: %s", doca_error_get_descr(result));
        return;
    }

    result = init_dpa_objects(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to init DPA (host-side DPA may be unsupported in this mode): %s",
                     doca_error_get_descr(result));
        return;
    }

    /* one PE per host drain core; each drain group gets a private counter
     * object (sharing the same doca_dev) so counters are single-writer */
    for (i = 0; i < bench_cores; i++) {
        result = doca_pe_create(&pes[i]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to create PE %u: %s", i, doca_error_get_descr(result));
            return;
        }
        gobjs[i] = calloc(1, sizeof(*gobjs[i]));
        if (gobjs[i] == NULL)
            return;
        gobjs[i]->dev = objs->dev;
    }

    /* shared DMA read source in host memory */
    result = init_dmesh_buffer(objs->dev, &objs->sndbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to init src buffer: %s", doca_error_get_descr(result));
        return;
    }
    result = doca_mmap_dev_get_dpa_handle(objs->sndbuf.mmap, objs->dev, &src_mmap);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("host bench: failed to get src mmap handle: %s", doca_error_get_descr(result));
        return;
    }

    /* set up all DPA threads first so the later launch loop is tight and the
     * threads run concurrently with minimal start stagger */
    for (i = 0; i < bench_threads; i++) {
        struct dmesh_doca_dpa_thread *dpa_thread;
        struct dmesh_conn *conn;

        dpa_thread = calloc(1, sizeof(*dpa_thread));
        conn = calloc(1, sizeof(*conn));
        if (dpa_thread == NULL || conn == NULL)
            return;
        dpa_threads[i] = dpa_thread;
        conns[i] = conn;

        dpa_thread->dpa = objs->dpa_pool->dpa;
        result = dmesh_doca_dpa_bench_thread_create(dpa_thread, bench_mode);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to create DPA thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        conn->objs = gobjs[i % bench_cores];
        conn->dpa_thread = dpa_thread;

        result = init_comch_dpa_msgq(conn, pes[i % bench_cores]);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to init DPA msgqs for thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        /* per-thread staging destination */
        result = alloc_buffer_and_set_mmap(&conn->local_mmap, objs->dev,
                                           &conn->dma_buffer, BUFFER_SIZE,
                                           DOCA_ACCESS_FLAG_PCI_READ_WRITE);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to alloc staging buffer %u: %s", i, doca_error_get_descr(result));
            return;
        }

        result = doca_dpa_completion_get_dpa_handle(conn->dpa_comch->producer_comp, &dpa_producer_comp);
        if (result == DOCA_SUCCESS)
            result = doca_comch_consumer_get_dpa_handle(conn->dpa_comch->send.consumer, &dpa_consumer);
        if (result == DOCA_SUCCESS)
            result = doca_comch_producer_get_dpa_handle(conn->dpa_comch->recv.producer, &dpa_producer);
        if (result == DOCA_SUCCESS)
            result = doca_mmap_dev_get_dpa_handle(conn->local_mmap, objs->dev, &dst_mmap);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to get DPA handles for thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        arg = (struct dpa_bench_state) {
            .dma = {
                .dpa_producer_comp = dpa_producer_comp,
                .dpa_producer = dpa_producer,
                .host_mmap = src_mmap, /* shared host sndbuf */
                .dpu_mmap = dst_mmap,  /* per-thread staging destination */
                .src_addr = (uint64_t)conn->dma_buffer,
                .buf_size = BUFFER_SIZE,
            },
            .host_addr = (uint64_t)objs->sndbuf.buf,
            .host_size = (uint32_t)objs->sndbuf.size,
            .mode = bench_mode,
            .msg_size = bench_size,
            .num_ops = bench_ops,
        };

        result = doca_dpa_rpc(dpa_thread->dpa, thread_init_rpc, &rpc_ret,
                              dpa_consumer, (uint32_t)CC_DPA_MAX_MSG_NUM, (uint64_t)0);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: init RPC failed for thread %u: %s", i, doca_error_get_descr(result));
            return;
        }

        result = doca_dpa_h2d_memcpy(dpa_thread->dpa, dpa_thread->arg, &arg, sizeof(arg));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to copy thread arg %u: %s", i, doca_error_get_descr(result));
            return;
        }
    }

    DOCA_LOG_INFO("host bench: launching %u DPA kernel(s) on %u drain core(s) (mode=%u size=%u ops=%u/thread)",
                  bench_threads, bench_cores, bench_mode, bench_size, bench_ops);
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* launch + kick all threads back to back; a thread only wakes on a
     * completion event, so the kick message plays the role that
     * send_dma_request_to_dpa has in the DPU flow */
    for (i = 0; i < bench_threads; i++) {
        struct comch_msg kick = {0};

        result = doca_dpa_thread_run(dpa_threads[i]->thread);
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to run DPA thread %u: %s", i, doca_error_get_descr(result));
            return;
        }
        result = dmesh_doca_dpa_msgq_send(&conns[i]->dpa_comch->send, &kick, sizeof(kick));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("host bench: failed to send kick message %u: %s", i, doca_error_get_descr(result));
            return;
        }
    }

    /* one drain pthread per host core; the main thread only monitors */
    for (i = 0; i < bench_cores; i++) {
        drain_ctxs[i].pe = pes[i];
        drain_ctxs[i].stop = &drain_stop;
        if (pthread_create(&drain_threads[i], NULL, host_bench_drain, &drain_ctxs[i]) != 0) {
            DOCA_LOG_ERR("host bench: failed to create drain thread %u", i);
            return;
        }
    }

    while (true) {
        total_recv = 0;
        for (i = 0; i < bench_cores; i++)
            total_recv += gobjs[i]->recv_msg_cnt;

        if (!first_seen && total_recv > 0) {
            clock_gettime(CLOCK_MONOTONIC, &t_first);
            t_rep = t_first;
            first_seen = 1;
        }
        if (total_recv >= total_ops) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double sec = diff_sec(&t_first, &t_now);
            printf("HOST_BENCH_DONE threads=%u cores=%u size=%u total_ops=%ld wall_sec=%.6f ops_per_sec=%.0f gbps=%.2f\n",
                   bench_threads, bench_cores, bench_size, total_ops, sec,
                   total_ops / sec,
                   (double)total_ops * bench_size * 8 / sec / 1e9);
            fflush(stdout);
            break;
        }

        clock_gettime(CLOCK_MONOTONIC, &t_now);
        if (diff_sec(&t_start, &t_now) >= 60.0) {
            printf("HOST_BENCH_TIMEOUT recv=%ld/%ld\n", total_recv, total_ops);
            fflush(stdout);
            break;
        }
        if (first_seen && diff_sec(&t_rep, &t_now) >= 1.0) {
            printf("HOST_BENCH_RATE %.0f ops/s (recv %ld/%ld)\n",
                   (total_recv - rep_cnt) / diff_sec(&t_rep, &t_now),
                   total_recv, total_ops);
            fflush(stdout);
            rep_cnt = total_recv;
            t_rep = t_now;
        }
        usleep(100);
    }

    drain_stop = 1;
    for (i = 0; i < bench_cores; i++)
        pthread_join(drain_threads[i], NULL);

    /* give the device log a moment to flush BENCH_* lines, then exit */
    sleep(1);
    exit(0);
}

struct host_worker_thread_ctx {
    const struct global_config *gcfg;
    int idx;
};

/*
 * One host worker thread = one connection to the DPU. Each thread opens its
 * own DOCA device handle and owns a private struct objects, so threads share
 * no DOCA state and need no locking.
 */
static void *
host_worker_thread(void *arg)
{
    struct host_worker_thread_ctx *ctx = arg;
    struct objects *objs;
    char server_name[32];
    doca_error_t result;

    objs = calloc(1, sizeof(*objs));
    if (objs == NULL) {
        DOCA_LOG_ERR("worker %d: failed to allocate objects", ctx->idx);
        return NULL;
    }

    result = open_doca_device_with_pci(ctx->gcfg->dev_pci_addr, NULL, &objs->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("worker %d: failed to open DOCA device: %s",
                     ctx->idx, doca_error_get_descr(result));
        free(objs);
        return NULL;
    }

    /* Flow identity for this worker's connection. A real host shim fills
     * this from the intercepted pod flow; the benchmark synthesizes it
     * (destination overridable via DMESH_DST_IP / DMESH_DST_PORT). */
    const char *dst_ip_env = getenv("DMESH_DST_IP");
    const char *dst_port_env = getenv("DMESH_DST_PORT");
    /* Source IP conveyed in the flow identity (used by inbound authz network
     * matching). Overridable via DMESH_SRC_IP; defaults to loopback. Note the
     * proxy's inbound policy auto-permits 127.0.0.1/32 (localhost), so testing
     * identity-based deny requires a non-loopback src. */
    const char *src_ip_env = getenv("DMESH_SRC_IP");
    objs->flow.src_ip = inet_addr(src_ip_env != NULL ? src_ip_env : "127.0.0.1");
    objs->flow.src_port = (uint16_t)(40000 + ctx->idx);
    objs->flow.dst_ip = inet_addr(dst_ip_env != NULL ? dst_ip_env : "127.0.0.1");
    objs->flow.dst_port = (uint16_t)(dst_port_env != NULL ? atoi(dst_port_env) : 8080);
    snprintf(objs->flow.src_workload, sizeof(objs->flow.src_workload),
             "host-worker-%d", ctx->idx);

    /* spread connections across the DPU worker servers round-robin;
     * DMESH_SERVER_IDX pins a standalone bridge process to one worker */
    int num_dpu_workers = ctx->gcfg->num_dpu_workers > 0 ? ctx->gcfg->num_dpu_workers : 1;
    const char *sidx = getenv("DMESH_SERVER_IDX");
    int idx_eff = sidx != NULL ? atoi(sidx) : (ctx->idx % num_dpu_workers);
    snprintf(server_name, sizeof(server_name), "DPUMesh%d", idx_eff);

    DOCA_LOG_INFO("worker %d: connecting to DPU server '%s'", ctx->idx, server_name);
    run_host_worker(objs, server_name);

    /* run_host_worker only returns on error (the data loop never exits) */
    DOCA_LOG_ERR("worker %d: exited", ctx->idx);
    free(objs);
    return NULL;
}

void
run_host_workers(const struct global_config *gcfg)
{
    pthread_t *threads;
    struct host_worker_thread_ctx *ctxs;
    int num_threads = gcfg->num_threads > 0 ? gcfg->num_threads : 1;
    int i;

    /* host-launched DPA bench replaces the normal workers */
    if (getenv("DMESH_DPA_BENCH_MODE") != NULL) {
        run_host_dpa_bench(gcfg);
        return;
    }

    DOCA_LOG_INFO("Starting %d host worker thread(s), one connection each", num_threads);

    threads = calloc(num_threads, sizeof(*threads));
    ctxs = calloc(num_threads, sizeof(*ctxs));
    if (threads == NULL || ctxs == NULL) {
        DOCA_LOG_ERR("Failed to allocate worker thread state");
        free(threads);
        free(ctxs);
        return;
    }

    for (i = 0; i < num_threads; i++) {
        ctxs[i].gcfg = gcfg;
        ctxs[i].idx = i;
        if (pthread_create(&threads[i], NULL, host_worker_thread, &ctxs[i]) != 0) {
            DOCA_LOG_ERR("Failed to create host worker thread %d", i);
            break;
        }
    }

    /* workers run forever; join blocks unless a worker errors out */
    for (i = i - 1; i >= 0; i--)
        pthread_join(threads[i], NULL);

    free(threads);
    free(ctxs);
}
