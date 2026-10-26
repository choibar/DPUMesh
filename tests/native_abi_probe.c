/* Public API compatibility probe. Also compiled as C++ without linking a
 * transport, so this checks the installed header rather than a mock library. */
#include <stddef.h>
#include <stdio.h>
#include <dpumesh/dmesh.h>

#ifdef __cplusplus
#include <type_traits>
#define ALIGN(type) alignof(type)
#define SIGNATURE(name, ...) \
    static_assert(std::is_same<decltype(&name), __VA_ARGS__>::value, #name)
#else
#define ALIGN(type) _Alignof(type)
#define SIGNATURE(name, ...) \
    _Static_assert(__builtin_types_compatible_p(__typeof__(&name), __VA_ARGS__), #name)
#endif

SIGNATURE(dmesh_create_channel, dmesh_channel_t *(*)(void));
SIGNATURE(dmesh_destroy_channel, int (*)(dmesh_channel_t *));
SIGNATURE(dmesh_pod_id, int (*)(dmesh_channel_t *));
SIGNATURE(dmesh_msg_max, int (*)(dmesh_channel_t *));
SIGNATURE(dmesh_post_max, int (*)(dmesh_channel_t *));
SIGNATURE(dmesh_create_eq, dmesh_eq_t *(*)(dmesh_channel_t *));
SIGNATURE(dmesh_destroy_eq, int (*)(dmesh_eq_t *));
SIGNATURE(dmesh_eq_fd, int (*)(dmesh_eq_t *));
SIGNATURE(dmesh_eq_next_deadline_ns, int64_t (*)(dmesh_eq_t *));
SIGNATURE(dmesh_create_qp, dmesh_qp_t *(*)(dmesh_eq_t *, const char *));
SIGNATURE(dmesh_destroy_qp, int (*)(dmesh_qp_t *));
SIGNATURE(dmesh_abort_qp, int (*)(dmesh_qp_t *));
SIGNATURE(dmesh_channel_fd, int (*)(dmesh_channel_t *));
SIGNATURE(dmesh_accept, dmesh_qp_t *(*)(dmesh_channel_t *, dmesh_eq_t *));
SIGNATURE(dmesh_alloc, void *(*)(dmesh_qp_t *, uint32_t));
SIGNATURE(dmesh_post_send, int (*)(dmesh_qp_t *, const void *, uint32_t));
SIGNATURE(dmesh_flush, int (*)(dmesh_qp_t *));
SIGNATURE(dmesh_tx_inflight, int (*)(dmesh_qp_t *));
SIGNATURE(dmesh_get_tx_stats, void (*)(dmesh_channel_t *, dmesh_tx_stats_t *));
SIGNATURE(dmesh_poll_eq, int (*)(dmesh_eq_t *, dmesh_event_t *, int));
SIGNATURE(dmesh_release_rx_buffer, void (*)(dmesh_channel_t *, dmesh_event_t *));

#define SIZE(type) do { \
    printf("sizeof.%s=%zu\n", #type, sizeof(type)); \
    printf("alignof.%s=%zu\n", #type, (size_t)ALIGN(type)); \
} while (0)
#define FIELD(type, field) do { \
    printf("offset.%s.%s=%zu\n", #type, #field, offsetof(type, field)); \
    printf("size.%s.%s=%zu\n", #type, #field, sizeof(((type *)0)->field)); \
} while (0)
#define VALUE(name) printf("value.%s=%lld\n", #name, (long long)(name))

int main(void)
{
    SIZE(void *);
    SIZE(long);
    SIZE(dmesh_channel_t);
    FIELD(dmesh_channel_t, ctx);
    FIELD(dmesh_channel_t, pod_id);
    FIELD(dmesh_channel_t, slot_size);
    FIELD(dmesh_channel_t, block_size);
    SIZE(dmesh_qp_t);
    FIELD(dmesh_qp_t, ep);
    FIELD(dmesh_qp_t, eq);
    FIELD(dmesh_qp_t, user_data);
    FIELD(dmesh_qp_t, role);
    FIELD(dmesh_qp_t, local_port);
    FIELD(dmesh_qp_t, dst_service);
    FIELD(dmesh_qp_t, remote_pod);
    FIELD(dmesh_qp_t, remote_port);
    FIELD(dmesh_qp_t, peer_closed);
    FIELD(dmesh_qp_t, fin_sent);
    FIELD(dmesh_qp_t, seq);
    FIELD(dmesh_qp_t, rx_slot);
    FIELD(dmesh_qp_t, rx_buf);
    FIELD(dmesh_qp_t, rx_len);
    FIELD(dmesh_qp_t, rx_pos);
    SIZE(dmesh_event_type_t);
    SIZE(dmesh_event_t);
    FIELD(dmesh_event_t, qp);
    FIELD(dmesh_event_t, type);
    FIELD(dmesh_event_t, buf);
    FIELD(dmesh_event_t, len);
    FIELD(dmesh_event_t, _rx_token);
    SIZE(dmesh_tx_stats_t);
    FIELD(dmesh_tx_stats_t, pool_grabs);
    FIELD(dmesh_tx_stats_t, pool_returns);
    FIELD(dmesh_tx_stats_t, recycle_hits);
    FIELD(dmesh_tx_stats_t, grow_waits);
    FIELD(dmesh_tx_stats_t, block_pads);
    VALUE(DMESH_EVENT_RECV);
    VALUE(DMESH_EVENT_RECV_FIN);
    VALUE(DMESH_EVENT_CONN_REQ);
    VALUE(DMESH_EVENT_TX_READY);
    VALUE(DMESH_EVENT_TX_ERROR);
    VALUE(DMESH_POD_BLANK);
    VALUE(DMESH_POD_REMOTE);
    VALUE(DMESH_POD_ABORT);
    VALUE(DMESH_PORT_BLANK);
    VALUE(DMESH_SVC_NONE);
    VALUE(DMESH_UPORT_BASE);
    VALUE(DMESH_ROLE_FREE);
    VALUE(DMESH_ROLE_CLIENT);
    VALUE(DMESH_ROLE_SERVER);
    VALUE(DMESH_ROLE_SERVER_PENDING);
    return 0;
}
