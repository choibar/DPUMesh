/* Adapter for pre-channel standalone programs. Native channels never allocate objects. */
#include "object.h"
#include "comch_client.h"
#include "comch_common.h"

static void legacy_message(void *owner, const uint8_t *data, size_t len)
{
    struct objects *objs = owner;
    if (objs->control_message_cb) {
        objs->control_message_cb(objs, data, len);
        return;
    }
    if (len < sizeof(enum dmesh_msg_type)) return;
    struct dmesh_comch_msg *msg = (struct dmesh_comch_msg *)data;
    if (msg->type == DMESH_MSG_EXPORT_DPA_COMP && len == sizeof(struct dmesh_dpa_comp_msg))
        (void)process_dpa_comp_msg(objs, (struct dmesh_dpa_comp_msg *)data);
    else if (msg->type == DMESH_MSG_EXPORT_RCV_RING && len == sizeof(struct dmesh_export_rcv_ring_msg))
        (void)process_export_rcv_ring_msg(objs, (struct dmesh_export_rcv_ring_msg *)data);
}

static void legacy_disconnected(void *owner) { ((struct objects *)owner)->peer_gone = 1; }
static void legacy_consumer(void *owner, uint32_t id) { ((struct objects *)owner)->remote_consumer_id = id; }

doca_error_t init_comch_ctrl_path_client(const char *server, struct objects *objs, bool fast)
{
    objs->legacy_client = (struct dmesh_comch_client){
        .dev = objs->dev, .owner = objs, .message = legacy_message,
        .disconnected = legacy_disconnected, .consumer_arrived = fast ? legacy_consumer : NULL,
    };
    doca_error_t result = dmesh_comch_client_open(server, &objs->legacy_client);
    /* Retain partially created resources for the caller's checked cleanup. */
    objs->pe = objs->legacy_client.pe;
    objs->cc_client = objs->legacy_client.cc_client;
    objs->connection = objs->legacy_client.connection;
    objs->peer_gone = objs->legacy_client.peer_gone;
    objs->is_server = false;
    return result;
}

doca_error_t client_send_msg(struct objects *objs, const char *msg, size_t len)
{
    return dmesh_comch_client_send(&objs->legacy_client, msg, len);
}
