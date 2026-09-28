/*
 * libdmesh_host — host-side DMA channel as a byte-stream API.
 *
 * This is run_host_worker's connect sequence plus run_host_push_splice's two
 * datapath halves, with the TCP socket replaced by caller-driven read/write.
 * See host_lib.h for the contract.
 */

#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <doca_dev.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_pe.h>
#include <doca_dpa.h>
#include <doca_comch.h>
#include <doca_comch_producer.h>

#include "object.h"
#include "common.h"
#include "buffer.h"
#include "ring.h"
#include "dma.h"
#include "comch_common.h"
#include "comch_client.h"
#include "comch_producer.h"
#include "dpa_common.h"

DOCA_LOG_REGISTER(HOST_LIB);

#define DMESH_LIB_PENDING_SIZE (8 * 1024 * 1024)

struct dmesh_chan {
    struct objects *objs;
    pthread_mutex_t lock;           /* serializes reader/writer over the PE */

    /* DPU -> host: push slot ring + data ring inside rcvbuf */
    volatile struct dmesh_push_desc *descs;
    volatile struct dmesh_push_cursor *cursor;
    char *data_area;
    size_t data_size;
    uint64_t expected;              /* next push desc seq */
    uint64_t rx_bytes;              /* total bytes drained from the data ring */
    char *pend;                     /* staging for consumed-but-unread bytes */
    size_t pend_head, pend_len;

    /* host -> DPU: sndbuf staging + forward descriptor ring */
    doca_dpa_dev_mmap_t local_mmap;
    size_t spos;

    int claimed;                    /* any bytes seen from the DPU yet */
    int dead;
};

static int lib_log_inited;

/* Consume published push batches into the pending ring. Caller holds lock. */
static void
chan_pump_rx(struct dmesh_chan *c)
{
    while (c->descs[c->expected % DMESH_PUSH_DESC_N].seq == c->expected) {
        uint32_t pos = c->descs[c->expected % DMESH_PUSH_DESC_N].pos;
        uint32_t len = c->descs[c->expected % DMESH_PUSH_DESC_N].len;
        size_t tail, first;

        if (len == 0 || (size_t)pos + len > c->data_size ||
            c->pend_len + len > DMESH_LIB_PENDING_SIZE)
            break;                  /* bogus desc or backpressure */

        tail = (c->pend_head + c->pend_len) % DMESH_LIB_PENDING_SIZE;
        first = len < DMESH_LIB_PENDING_SIZE - tail ? len : DMESH_LIB_PENDING_SIZE - tail;
        memcpy(c->pend + tail, c->data_area + pos, first);
        if (first < len)
            memcpy(c->pend, c->data_area + pos + first, len - first);
        c->pend_len += len;
        c->rx_bytes += len;
        c->expected++;
        c->claimed = 1;
        /* Batch copied out of the data ring: publish consumption. */
        c->cursor->consumed_seq = c->expected - 1;
        c->cursor->consumed_bytes = c->rx_bytes;
    }
}

struct dmesh_chan *
dmesh_chan_connect(const char *pci_addr, const char *server_name,
		   const char *src_ip, uint16_t src_port,
		   const char *dst_ip, uint16_t dst_port,
		   const char *workload, uint32_t mode)
{
    struct dmesh_chan *c;
    struct objects *objs;
    doca_error_t result;

    if (!lib_log_inited) {
        struct doca_log_backend *sdk_log;

        (void)doca_log_backend_create_standard();
        if (doca_log_backend_create_with_file_sdk(stderr, &sdk_log) == DOCA_SUCCESS) {
            const char *lvl = getenv("DMESH_SDK_LOG");

            (void)doca_log_backend_set_sdk_level(sdk_log,
                lvl != NULL && strcmp(lvl, "debug") == 0 ? DOCA_LOG_LEVEL_DEBUG
                                                         : DOCA_LOG_LEVEL_WARNING);
        }
        lib_log_inited = 1;
    }

    c = calloc(1, sizeof(*c));
    objs = calloc(1, sizeof(*objs));
    if (c == NULL || objs == NULL)
        goto err_alloc;
    c->objs = objs;
    pthread_mutex_init(&c->lock, NULL);

    result = open_doca_device_with_pci(pci_addr, NULL, &objs->dev);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("chan: failed to open device %s: %s", pci_addr,
                     doca_error_get_descr(result));
        goto err_alloc;
    }

    objs->flow.src_ip = inet_addr(src_ip != NULL ? src_ip : "127.0.0.1");
    objs->flow.src_port = src_port;
    objs->flow.dst_ip = inet_addr(dst_ip != NULL ? dst_ip : "127.0.0.1");
    objs->flow.dst_port = dst_port;
    objs->flow.mode = mode;
    snprintf(objs->flow.src_workload, sizeof(objs->flow.src_workload), "%s",
             workload != NULL ? workload : "");

    result = init_comch_ctrl_path_client(server_name, objs, true);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("chan: comch client init failed: %s", doca_error_get_descr(result));
        goto err_dev;
    }
    result = init_comch_datapath_producer(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("chan: producer init failed: %s", doca_error_get_descr(result));
        goto err_cleanup;
    }
    result = setup_dma_ring(objs, DMA_RING_SIZE);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("chan: dma ring setup failed: %s", doca_error_get_descr(result));
        goto err_cleanup;
    }
    result = init_dmesh_buffer(objs->dev, &objs->sndbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS)
        goto err_cleanup;
    result = init_dmesh_buffer(objs->dev, &objs->rcvbuf, BUFFER_SIZE);
    if (result != DOCA_SUCCESS)
        goto err_cleanup;

    /* Push transport (both modes): descriptor slots must start empty. */
    memset(objs->rcvbuf.buf, 0, DMESH_PUSH_DATA_OFF);

    result = export_dma_metadata(objs);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("chan: export failed: %s", doca_error_get_descr(result));
        goto err_cleanup;
    }

    if (doca_mmap_dev_get_dpa_handle(objs->sndbuf.mmap, objs->dev, &c->local_mmap)
        != DOCA_SUCCESS) {
        DOCA_LOG_ERR("chan: failed to get sndbuf DPA handle");
        goto err_cleanup;
    }

    c->descs = (volatile struct dmesh_push_desc *)objs->rcvbuf.buf;
    c->cursor = (volatile struct dmesh_push_cursor *)((char *)objs->rcvbuf.buf +
                                                      DMESH_PUSH_CURSOR_OFF);
    c->cursor->consumed_seq = 0;
    c->cursor->consumed_bytes = 0;
    c->cursor->magic = DMESH_PUSH_FC_MAGIC;
    c->data_area = (char *)objs->rcvbuf.buf + DMESH_PUSH_DATA_OFF;
    c->data_size = objs->rcvbuf.size - DMESH_PUSH_DATA_OFF;
    c->expected = 1;
    c->pend = malloc(DMESH_LIB_PENDING_SIZE);
    if (c->pend == NULL)
        goto err_cleanup;

    DOCA_LOG_INFO("chan: connected to %s (mode %u, dst %s:%u)", server_name, mode,
                  dst_ip != NULL ? dst_ip : "?", dst_port);
    return c;

err_cleanup:
    cleanup_objects(objs);          /* tears down DOCA state, not the struct */
    free(objs);
    free(c->pend);
    free(c);
    return NULL;

err_dev:
err_alloc:
    if (objs != NULL && objs->dev != NULL)
        (void)doca_dev_close(objs->dev);
    free(objs);
    if (c != NULL)
        free(c->pend);
    free(c);
    return NULL;
}

ssize_t
dmesh_chan_read(struct dmesh_chan *c, void *buf, size_t len)
{
    size_t n, first;

    if (c == NULL)
        return -1;
    pthread_mutex_lock(&c->lock);
    if (c->dead) {
        pthread_mutex_unlock(&c->lock);
        return -1;
    }
    (void)doca_pe_progress(c->objs->pe);        /* control path (comch) */
    if (c->objs->peer_gone) {                   /* DPU tore the slot down: EOF */
        pthread_mutex_unlock(&c->lock);
        return -1;
    }
    chan_pump_rx(c);
    n = c->pend_len < len ? c->pend_len : len;
    if (n > 0) {
        first = n < DMESH_LIB_PENDING_SIZE - c->pend_head
                    ? n : DMESH_LIB_PENDING_SIZE - c->pend_head;
        memcpy(buf, c->pend + c->pend_head, first);
        if (first < n)
            memcpy((char *)buf + first, c->pend, n - first);
        c->pend_head = (c->pend_head + n) % DMESH_LIB_PENDING_SIZE;
        c->pend_len -= n;
    }
    pthread_mutex_unlock(&c->lock);
    return (ssize_t)n;
}

ssize_t
dmesh_chan_write(struct dmesh_chan *c, const void *buf, size_t len)
{
    size_t off = 0;

    if (c == NULL)
        return -1;
    pthread_mutex_lock(&c->lock);
    if (c->dead) {
        pthread_mutex_unlock(&c->lock);
        return -1;
    }
    (void)doca_pe_progress(c->objs->pe);
    if (c->objs->peer_gone) {                   /* peer gone: fail the write */
        pthread_mutex_unlock(&c->lock);
        return -1;
    }
    while (off < len) {
        size_t remaining = len - off;
        size_t chunk;
        struct dma_desc *d;

        /* Forward backpressure: the DPA advances consumer_head as it drains
         * descriptors; never run more than ~120 outstanding (each <=8064B, so
         * sndbuf's 1MB can never wrap over unconsumed request bytes). The
         * partial write returns and the Go side retries. */
        {
            volatile struct dma_ring_ctrl *rc =
                (volatile struct dma_ring_ctrl *)c->objs->dma_ring->ctrl;

            if (rc->producer_tail - rc->consumer_head >= 120)
                break;
        }

        /* producer_dma_copy completion rule: multiple of 128B, or a single
         * <=128B block (mirrors the push splice / client bridge). */
        if (remaining <= 128)
            chunk = remaining;
        else if (remaining >= 8064)
            chunk = 8064;
        else
            chunk = remaining & ~(size_t)127;
        if (c->spos + chunk > c->objs->sndbuf.size)
            c->spos = 0;
        memcpy((char *)c->objs->sndbuf.buf + c->spos, (const char *)buf + off, chunk);
        d = get_next_dma_desc(c->objs->dma_ring);
        d->mmap = c->local_mmap;
        d->addr = (uint64_t)c->objs->sndbuf.buf + c->spos;
        d->size = chunk;
        commit_dma_desc(c->objs->dma_ring);
        c->spos += chunk;
        off += chunk;
    }
    pthread_mutex_unlock(&c->lock);
    return (ssize_t)off;
}

int
dmesh_chan_claimed(struct dmesh_chan *c)
{
    int v;

    if (c == NULL)
        return 0;
    pthread_mutex_lock(&c->lock);
    if (c->dead) {
        pthread_mutex_unlock(&c->lock);
        return 0;
    }
    (void)doca_pe_progress(c->objs->pe);
    /* A dead channel counts as claimed so Listener.Accept stops waiting on
     * it; the next read on it returns -1 and the caller closes it. */
    if (c->objs->peer_gone) {
        pthread_mutex_unlock(&c->lock);
        return 1;
    }
    chan_pump_rx(c);
    v = c->claimed;
    pthread_mutex_unlock(&c->lock);
    return v;
}

void
dmesh_chan_close(struct dmesh_chan *c)
{
    struct objects *objs;
    enum doca_ctx_states st;
    int spins;

    if (c == NULL)
        return;
    pthread_mutex_lock(&c->lock);
    if (c->dead) {
        pthread_mutex_unlock(&c->lock);
        return;
    }
    c->dead = 1;
    objs = c->objs;

    /* Graceful comch disconnect. Without completing this handshake the
     * firmware-side channel object of this client leaks on the service, and
     * later client registrations fail at devx creation (syndrome 0xe5300 ->
     * CONNECTION_ABORTED) - the historical "reconnect needs a proxy restart"
     * failure. Stop the producer first, then the client ctx, progressing
     * their PEs until IDLE (bounded so close can never hang). */
    if (objs->producer != NULL) {
        (void)doca_ctx_stop(doca_comch_producer_as_ctx(objs->producer));
        spins = 0;
        while (spins++ < 100000 &&
               doca_ctx_get_state(doca_comch_producer_as_ctx(objs->producer), &st) == DOCA_SUCCESS &&
               st != DOCA_CTX_STATE_IDLE)
            (void)doca_pe_progress(objs->producer_pe != NULL ? objs->producer_pe : objs->pe);
        (void)doca_comch_producer_destroy(objs->producer);
        objs->producer = NULL;
    }
    if (objs->cc_client != NULL) {
        (void)doca_ctx_stop(doca_comch_client_as_ctx(objs->cc_client));
        spins = 0;
        while (spins++ < 100000 &&
               doca_ctx_get_state(doca_comch_client_as_ctx(objs->cc_client), &st) == DOCA_SUCCESS &&
               st != DOCA_CTX_STATE_IDLE)
            (void)doca_pe_progress(objs->pe);
        (void)doca_comch_client_destroy(objs->cc_client);
        objs->cc_client = NULL;
    }

    /* Buffers and mmaps (the reader/writer are fenced out by c->dead). */
    if (objs->sndbuf.mmap != NULL) {
        destroy_mmap_and_free_buffer(objs->sndbuf.mmap, objs->sndbuf.buf);
        objs->sndbuf.mmap = NULL; objs->sndbuf.buf = NULL;
    }
    if (objs->rcvbuf.mmap != NULL) {
        destroy_mmap_and_free_buffer(objs->rcvbuf.mmap, objs->rcvbuf.buf);
        objs->rcvbuf.mmap = NULL; objs->rcvbuf.buf = NULL;
    }
    free(c->pend);
    c->pend = NULL;
    /* The chan/objs structs stay allocated (tiny) so a racing caller can only
     * ever observe dead=1, never freed memory. */
    pthread_mutex_unlock(&c->lock);
}
