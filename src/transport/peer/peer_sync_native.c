#define _GNU_SOURCE
#include "peer_sync_native.h"
#include "peer_write_sync_dpa_shared.h"
#include <doca_ctx.h>
#include <doca_rdma.h>
#include <doca_sync_event.h>
#include <doca_version.h>
#include <string.h>
#if (DOCA_VERSION_MAJOR != 3 || DOCA_VERSION_MINOR != 5 || DOCA_VERSION_PATCH != 98)
#error "Native WRITE/Sync QPN bridge requires DOCA 3.5.0098 headers"
#endif
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <pthread.h>

struct peer_sync_native {
    struct doca_dev *dev;
    struct doca_dpa *dpa;
    struct doca_rdma *rdma;
    struct doca_rdma_connection *connection;
    struct doca_sync_event *local[2];
    struct doca_sync_event_remote_net *remote[2];
    unsigned started, connected, event_started[2];
    int immediate;
};

static int checked(doca_error_t e, const char *stage)
{
    if (e == DOCA_SUCCESS)
        return 0;

    fprintf(stderr, "WRITE_SYNC_NATIVE stage=%s error=%s\n", stage, doca_error_get_descr(e));
    return -1;
}

#define TRY(call)                                                                                  \
    do {                                                                                           \
        if (checked((call), #call))                                                                \
            goto fail;                                                                             \
    } while (0)
/* DOCA 3.5.0098 compatibility boundary, explicitly opted into for the QPN
 * IPsec integration. QPN discovery uses no object offsets or export parsing. All
 * private SDK declarations live here; never use them on another runtime.
 * The separately scoped RNR hook below also pins one local-attribute ABI. */
struct native_connection_details;
extern doca_error_t
priv_doca_rdma_connection_details_create_from_connection(struct doca_rdma_connection *,
                                                         struct native_connection_details **);
extern doca_error_t priv_doca_rdma_connection_get_qpn(struct doca_rdma_connection *, uint32_t *);
extern void priv_doca_rdma_connection_details_destroy(struct native_connection_details *);
extern uint32_t
priv_doca_rdma_connection_details_get_qpn_or_dct_num(const struct native_connection_details *);
extern uint32_t
priv_doca_rdma_connection_details_get_sq_psn(const struct native_connection_details *);

/* DOCA RDMA 3.5.0098 hardcodes min_rnr_timer=12 in its RC connect
 * builder; the public RDMA API only exposes the retry COUNT. Scope this
 * compatibility hook to our synchronous IMM connect on the current thread.
 * It never patches an SDK file or dereferences a private QP/connection object.
 *
 * Verified on the installed aarch64 and x86_64 SDKs: local_attr occupies 40
 * bytes, and byte32 supplies QPC.min_rnr_nak[4:0] in INIT->RTR. Preserve every
 * other byte. See design/ARM_WRITE_SYNC.md for the ABI evidence and limits. */
struct native_qp_local_attr_350098 {
    uint8_t prefix[32];
    uint8_t min_rnr_timer;
    uint8_t suffix[7];
} __attribute__((aligned(8)));

_Static_assert(sizeof(struct native_qp_local_attr_350098) == 40, "DOCA 3.5 local attr ABI");
_Static_assert(offsetof(struct native_qp_local_attr_350098, min_rnr_timer) == 32,
               "DOCA 3.5 RNR timer ABI");

typedef doca_error_t (*native_qp_connect_fn)(void *, const void *, const void *);
static native_qp_connect_fn native_qp_connect_real;
static pthread_once_t native_qp_connect_once = PTHREAD_ONCE_INIT;
static _Thread_local unsigned native_imm_connect_scope, native_imm_connect_calls;

static void native_qp_connect_resolve(void)
{
    /* Retain this handle for the lifetime of the cached function pointer. */
    void *library = dlopen("libdoca_common.so.2", RTLD_NOW | RTLD_LOCAL);
    if (library)
        native_qp_connect_real = (native_qp_connect_fn)dlvsym(
            library, "priv_doca_qp_connect", "INTERNAL");
}

/* Exported intentionally: libdoca_rdma calls this symbol through its PLT.
 * Other SDK users and the Sync carrier are forwarded without alteration. */
__attribute__((visibility("default")))
doca_error_t priv_doca_qp_connect(void *qp, const void *local, const void *remote)
{
    pthread_once(&native_qp_connect_once, native_qp_connect_resolve);
    if (!native_qp_connect_real || native_qp_connect_real == priv_doca_qp_connect)
        return DOCA_ERROR_NOT_SUPPORTED;

    if (!native_imm_connect_scope)
        return native_qp_connect_real(qp, local, remote);

    if (!local || !remote || strcmp(doca_version_runtime(), "3.5.0098") ||
        strcmp(doca_version(), "3.5.0098"))
        return DOCA_ERROR_NOT_SUPPORTED;

    struct native_qp_local_attr_350098 attr;
    memcpy(&attr, local, sizeof(attr));
    if (attr.min_rnr_timer != 12 || native_imm_connect_calls)
        return DOCA_ERROR_BAD_STATE;

    attr.min_rnr_timer = PEER_RDMA_MIN_RNR_TIMER;
    native_imm_connect_calls++;
    return native_qp_connect_real(qp, &attr, remote);
}

int peer_sync_native_endpoint(struct peer_sync_native *n, uint32_t *qpn, uint32_t *psn)
{
    struct native_connection_details *d = NULL;
    if (!n || !n->connection || !qpn || !psn ||
        (strcmp(doca_version_runtime(), "3.5.0098") || strcmp(doca_version(), "3.5.0098"))) {
        fprintf(stderr, "WRITE_SYNC_NATIVE private QPN bridge requires DOCA 3.5.0098\n");
        return -1;
    }
    if (checked(priv_doca_rdma_connection_details_create_from_connection(n->connection, &d),
                "private-connection-details"))
        return -1;

    *qpn = priv_doca_rdma_connection_details_get_qpn_or_dct_num(d);
    *psn = priv_doca_rdma_connection_details_get_sq_psn(d);
    priv_doca_rdma_connection_details_destroy(d);

    uint32_t actual = 0;
    if (checked(priv_doca_rdma_connection_get_qpn(n->connection, &actual), "private-QPN") ||
        actual != *qpn)
        return -1;

    return !*qpn || *qpn > 0xffffff || *psn > 0xffffff ? -1 : 0;
}

static int native_new(struct doca_dev *dev,
                      struct doca_dpa *dpa,
                      struct doca_dpa_completion *cq,
                      uint32_t gid,
                      uint16_t mtu,
                      struct peer_sync_native **out,
                      int immediate)
{
    if (!out || !dev || !dpa || !cq)
        return -1;

    *out = NULL;

    struct peer_sync_native *n = calloc(1, sizeof(*n));
    if (!n)
        return -1;

    n->immediate = immediate;
    n->dev = dev;
    n->dpa = dpa;

    /* SDK-owned event storage is intentional: external doca_buf + native DPA
     * subscriber is a different configuration (and fails on our SDK 3.5). */
    for (unsigned i = 0; !immediate && i < 2; i++) {
        TRY(doca_sync_event_create(&n->local[i]));
        TRY(doca_sync_event_add_publisher_location_remote_net(n->local[i]));
        TRY(doca_sync_event_add_subscriber_location_dpa(n->local[i], dpa));
        TRY(doca_sync_event_start(n->local[i]));
        n->event_started[i] = 1;
    }

    TRY(doca_rdma_create(dev, &n->rdma));
    TRY(doca_rdma_set_transport_type(n->rdma, DOCA_RDMA_TRANSPORT_TYPE_RC));
    TRY(doca_rdma_set_permissions(
        n->rdma,
        DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_RDMA_WRITE |
            (immediate ? 0 : (DOCA_ACCESS_FLAG_RDMA_READ | DOCA_ACCESS_FLAG_RDMA_ATOMIC))));
    TRY(doca_rdma_set_grh_enabled(n->rdma, true));
    TRY(doca_rdma_set_gid_index(n->rdma, gid));

    enum doca_mtu_size native_mtu;
    switch (mtu) {
    case 256:
        native_mtu = DOCA_MTU_SIZE_256_BYTES;
        break;

    case 512:
        native_mtu = DOCA_MTU_SIZE_512_BYTES;
        break;

    case 1024:
        native_mtu = DOCA_MTU_SIZE_1K_BYTES;
        break;

    case 2048:
        native_mtu = DOCA_MTU_SIZE_2K_BYTES;
        break;

    case 4096:
        native_mtu = DOCA_MTU_SIZE_4K_BYTES;
        break;

    default:
        goto fail;
    }

    TRY(doca_rdma_set_mtu(n->rdma, native_mtu));
    TRY(doca_rdma_set_max_num_connections(n->rdma, 1));
    TRY(doca_ctx_set_datapath_on_dpa(doca_rdma_as_ctx(n->rdma), dpa));

    TRY(doca_rdma_set_send_queue_size(n->rdma, WSD_SQ));
    TRY(doca_rdma_set_recv_queue_size(n->rdma, immediate ? WSD_IMM_RQ : 1));
    TRY(doca_rdma_dpa_completion_attach(n->rdma, cq));

    TRY(doca_ctx_start(doca_rdma_as_ctx(n->rdma)));
    n->started = 1;

    *out = n;
    return 0;

fail:
    if (peer_sync_native_free(n))
        *out = n;

    return -1;
}

int peer_sync_native_new(struct doca_dev *dev,
                         struct doca_dpa *dpa,
                         struct doca_dpa_completion *cq,
                         uint32_t gid,
                         uint16_t mtu,
                         struct peer_sync_native **out)
{
    return native_new(dev, dpa, cq, gid, mtu, out, 0);
}

int peer_sync_native_new_imm(struct doca_dev *dev,
                             struct doca_dpa *dpa,
                             struct doca_dpa_completion *cq,
                             uint32_t gid,
                             uint16_t mtu,
                             struct peer_sync_native **out)
{
    return native_new(dev, dpa, cq, gid, mtu, out, 1);
}

int peer_sync_native_export(struct peer_sync_native *n, const void **p, size_t *bytes)
{
    if (!n || !n->started || n->connection || !p || !bytes)
        return -1;

    return checked(doca_rdma_export(n->rdma, p, bytes, &n->connection), "rdma-export");
}

int peer_sync_native_connect(struct peer_sync_native *n, const void *p, size_t bytes)
{
    if (!n || !n->connection || n->connected || !p || !bytes)
        return -1;
    if (native_imm_connect_scope)
        return -1;

    native_imm_connect_calls = 0;
    native_imm_connect_scope = n->immediate;
    doca_error_t rc = doca_rdma_connect(n->rdma, p, bytes, n->connection);
    native_imm_connect_scope = 0;

    if (checked(rc, "rdma-connect"))
        return -1;
    if (n->immediate && native_imm_connect_calls != 1) {
        fprintf(stderr, "WRITE_IMM_NATIVE RNR timer hook was not applied\n");
        return -1;
    }

    if (n->immediate)
        fprintf(stderr, "WRITE_IMM_NATIVE_CONFIG rq=%u min_rnr_timer=%u scope=connect sdk=3.5.0098\n",
                WSD_IMM_RQ, PEER_RDMA_MIN_RNR_TIMER);
    n->connected = 1;

    return 0;
}

int peer_sync_native_event_export(struct peer_sync_native *n,
                                  unsigned i,
                                  const void **p,
                                  size_t *bytes)
{
    if (!n || i >= 2 || !p || !bytes)
        return -1;

    const uint8_t *data = NULL;
    int rc =
        checked(doca_sync_event_export_to_remote_net(n->local[i], &data, bytes), "event-export");
    *p = data;

    return rc;
}

int peer_sync_native_event_import(struct peer_sync_native *n,
                                  unsigned i,
                                  const void *p,
                                  size_t bytes)
{
    if (!n || i >= 2 || n->remote[i] || !p || !bytes)
        return -1;

    return checked(doca_sync_event_remote_net_create_from_export(n->dev, p, bytes, &n->remote[i]),
                   "event-import");
}

int peer_sync_native_handles(struct peer_sync_native *n,
                             doca_dpa_dev_rdma_t *rdma,
                             uint32_t *connection,
                             doca_dpa_dev_sync_event_t local[2],
                             doca_dpa_dev_sync_event_remote_net_t remote[2])
{
    if (!n || !n->connection || !rdma || !connection || !local || !remote)
        return -1;

    TRY(doca_rdma_get_dpa_handle(n->rdma, rdma));
    TRY(doca_rdma_connection_get_id(n->connection, connection));

    for (unsigned i = 0; i < 2; i++) {
        local[i] = 0;

        if (n->local[i])
            TRY(doca_sync_event_get_dpa_handle(n->local[i], n->dpa, &local[i]));
        if (!n->remote[i])
            remote[i] = 0;
        else
            TRY(doca_sync_event_remote_net_get_dpa_handle(n->remote[i], n->dpa, &remote[i]));
    }
    return 0;

fail:
    return -1;
}

int peer_sync_native_free(struct peer_sync_native *n)
{
    if (!n)
        return 0;

    if (n->rdma) {
        if (n->started) {
            if (checked(doca_ctx_stop(doca_rdma_as_ctx(n->rdma)), "rdma-stop"))
                return -1;

            n->started = 0;
        }
        if (checked(doca_rdma_destroy(n->rdma), "rdma-destroy"))
            return -1;

        n->rdma = NULL;
    }

    for (unsigned i = 0; i < 2; i++) {
        if (n->remote[i]) {
            if (checked(doca_sync_event_remote_net_destroy(n->remote[i]), "remote-event-destroy"))
                return -1;

            n->remote[i] = NULL;
        }
        if (n->local[i]) {
            if (n->event_started[i]) {
                if (checked(doca_sync_event_stop(n->local[i]), "event-stop"))
                    return -1;

                n->event_started[i] = 0;
            }
            if (checked(doca_sync_event_destroy(n->local[i]), "event-destroy"))
                return -1;

            n->local[i] = NULL;
        }
    }

    free(n);

    return 0;
}
