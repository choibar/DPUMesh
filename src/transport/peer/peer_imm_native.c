#define _GNU_SOURCE
#include "peer_imm_native.h"
#include "peer_write_imm_dpa.h"
#include <doca_ctx.h>
#include <doca_dev.h>
#include <doca_rdma_bridge.h>
#include <doca_rdma.h>
#include <doca_version.h>
#include <string.h>
#if (DOCA_VERSION_MAJOR != 3 || DOCA_VERSION_MINOR != 5 || DOCA_VERSION_PATCH != 98)
#error "Native WRITE_WITH_IMM QPN bridge requires DOCA 3.5.0098 headers"
#endif
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <pthread.h>

struct peer_imm_native {
    struct doca_rdma *rdma;
    struct doca_rdma_connection *connection;
    unsigned started, connected;
};

static int checked(doca_error_t e, const char *stage)
{
    if (e == DOCA_SUCCESS)
        return 0;

    fprintf(stderr, "WRITE_IMM_NATIVE stage=%s error=%s\n", stage, doca_error_get_descr(e));
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
 * Other SDK users are forwarded without alteration. */
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

int peer_imm_native_endpoint(struct peer_imm_native *n, uint32_t *qpn, uint32_t *psn)
{
    struct native_connection_details *d = NULL;
    if (!n || !n->connection || !qpn || !psn ||
        (strcmp(doca_version_runtime(), "3.5.0098") || strcmp(doca_version(), "3.5.0098"))) {
        fprintf(stderr, "WRITE_IMM_NATIVE private QPN bridge requires DOCA 3.5.0098\n");
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

int peer_imm_native_new(struct doca_dev *dev,
                      struct doca_dpa *dpa,
                      struct doca_dpa_completion *cq,
                      uint32_t gid,
                      uint16_t mtu,
                      struct peer_imm_native **out)
{
    if (!out || !dev || !dpa || !cq)
        return -1;

    *out = NULL;

    struct peer_imm_native *n = calloc(1, sizeof(*n));
    if (!n)
        return -1;

    TRY(doca_rdma_create(dev, &n->rdma));
    TRY(doca_rdma_set_transport_type(n->rdma, DOCA_RDMA_TRANSPORT_TYPE_RC));
    TRY(doca_rdma_set_permissions(
        n->rdma,
        DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_RDMA_WRITE));
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

    TRY(doca_rdma_set_send_queue_size(n->rdma, WIM_SQ));
    TRY(doca_rdma_set_recv_queue_size(n->rdma, WIM_IMM_RQ));
    TRY(doca_rdma_dpa_completion_attach(n->rdma, cq));

    TRY(doca_ctx_start(doca_rdma_as_ctx(n->rdma)));
    n->started = 1;

    *out = n;
    return 0;

fail:
    if (peer_imm_native_free(n))
        *out = n;

    return -1;
}

int peer_imm_native_export(struct peer_imm_native *n, const void **p, size_t *bytes)
{
    if (!n || !n->started || n->connection || !p || !bytes)
        return -1;

    return checked(doca_rdma_export(n->rdma, p, bytes, &n->connection), "rdma-export");
}

int peer_imm_native_connect(struct peer_imm_native *n, const void *p, size_t bytes)
{
    if (!n || !n->connection || n->connected || !p || !bytes)
        return -1;
    if (native_imm_connect_scope)
        return -1;

    native_imm_connect_calls = 0;
    native_imm_connect_scope = 1;
    doca_error_t rc = doca_rdma_connect(n->rdma, p, bytes, n->connection);
    native_imm_connect_scope = 0;

    if (checked(rc, "rdma-connect"))
        return -1;
    if (native_imm_connect_calls != 1) {
        fprintf(stderr, "WRITE_IMM_NATIVE RNR timer hook was not applied\n");
        return -1;
    }

    fprintf(stderr, "WRITE_IMM_NATIVE_CONFIG rq=%u min_rnr_timer=%u scope=connect sdk=3.5.0098\n",
                WIM_IMM_RQ, PEER_RDMA_MIN_RNR_TIMER);
    n->connected = 1;

    return 0;
}

int peer_imm_native_handles(struct peer_imm_native *n,
                             doca_dpa_dev_rdma_t *rdma, uint32_t *connection)
{
    if (!n || !n->connection || !rdma || !connection)
        return -1;
    TRY(doca_rdma_get_dpa_handle(n->rdma, rdma));
    TRY(doca_rdma_connection_get_id(n->connection, connection));
    return 0;
fail:
    return -1;
}

int peer_imm_native_free(struct peer_imm_native *n)
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

    free(n);

    return 0;
}


struct peer_rdma_device {
    struct doca_dev *dev;
    struct ibv_pd *pd; /* Borrowed from dev; registrations must retire first. */
};

int peer_rdma_device_new(const char *name, struct doca_dpa *dpa, struct peer_rdma_device **out)
{
    if (!name || !dpa || !out)
        return -1;
    *out = NULL;
    struct peer_rdma_device *d = calloc(1, sizeof(*d));
    struct doca_devinfo **list = NULL;
    uint32_t count = 0;
    if (!d)
        return -1;
    if (doca_devinfo_create_list(&list, &count) != DOCA_SUCCESS)
        goto fail;
    for (uint32_t i = 0; i < count; i++) {
        char candidate[DOCA_DEVINFO_IBDEV_NAME_SIZE] = {0};
        if (doca_devinfo_get_ibdev_name(list[i], candidate, sizeof(candidate)) == DOCA_SUCCESS &&
            !strcmp(name, candidate)) {
            if (doca_dev_open(list[i], &d->dev) != DOCA_SUCCESS)
                goto fail;
            break;
        }
    }
    doca_devinfo_destroy_list(list);
    list = NULL;
    if (!d->dev || doca_rdma_bridge_get_dev_pd(d->dev, &d->pd) != DOCA_SUCCESS)
        goto fail;
    *out = d;
    return 0;
fail:
    if (list)
        doca_devinfo_destroy_list(list);
    (void)peer_rdma_device_free(d);
    return -1;
}

struct doca_dev *peer_rdma_device_dev(struct peer_rdma_device *d) { return d->dev; }
struct ibv_pd *peer_rdma_device_pd(struct peer_rdma_device *d) { return d->pd; }

int peer_rdma_device_free(struct peer_rdma_device *d)
{
    if (!d)
        return 0;
    if (d->dev && doca_dev_close(d->dev) != DOCA_SUCCESS)
        return -1;
    free(d);
    return 0;
}
