#ifndef DMESH_LOCAL_REGISTRATION_PROTOCOL_H
#define DMESH_LOCAL_REGISTRATION_PROTOCOL_H

#include <stdint.h>
#include "peer_config.h"

/* Registration wire ABI is shared with the trusted node agent. It is separate
 * from the native Host DMA descriptor and Comch envelope. */
#define DMESH_LOCAL_VERSION 6u
#define DMESH_MSG_REG_ASSERTION 16u

#define DMESH_WORKLOAD_MAX 384

/* Trusted workload registration is the only way a Pod enters the mesh. The DPU
 * creates a fresh challenge for every Comch connection. The broker relays it
 * to dpumeshd, which seals the connection's identity under a key derived from
 * the authenticated paired-host control session; the broker carries the sealed
 * record back on the same connection. Neither the workload nor the broker holds
 * that key. */
#define DMESH_IDENTITY_VERSION 1u
#define DMESH_IDENTITY_TYPE 1u
#define DMESH_FEED_MAC_SIZE 32u
#define DMESH_KEY_ID_MAX 32u
#define DMESH_POD_IP_MAX 16u
#define DMESH_CLUSTER_ID_MAX 64u
#define DMESH_CONTAINER_ID_MAX 65u
#define DMESH_CONTROL_SESSION_SIZE 16u  /* paired-host control session id */
#define DMESH_REG_MAC_SIZE 32u          /* HMAC-SHA256 seal of an assertion */

/* Fixed, bounded control frames for the paired host: liveness and
 * registration teardown. Identities do not travel here; they arrive on Comch
 * as assertions sealed under the authenticated paired-host session id. */
#define DMESH_LOCAL_MAGIC "DMESHLC2"
/* Operation 2 is retired and never reassigned. */
enum { DMESH_LOCAL_PING = 1, DMESH_LOCAL_UNREGISTER = 3, DMESH_LOCAL_STATUS = 4 };
enum { DMESH_LOCAL_OK = 0, DMESH_LOCAL_INVALID, DMESH_LOCAL_STALE,
       DMESH_LOCAL_PENDING };
struct dmesh_local_request {
    uint8_t operation, reserved[7], sequence[8],
            session[DMESH_CONTROL_SESSION_SIZE], connection_id[DMESH_REG_NONCE_SIZE];
};
struct dmesh_local_response {
    uint8_t magic[8], session[DMESH_CONTROL_SESSION_SIZE], sequence[8], status[4];
};
_Static_assert(sizeof(struct dmesh_local_request) == 64, "local request ABI");
_Static_assert(sizeof(struct dmesh_local_response) == 36, "local response ABI");

struct dmesh_registration_challenge_msg {
    uint8_t type;               /* = DMESH_MSG_REG_CHALLENGE */
    uint8_t version;            /* = DMESH_LOCAL_VERSION */
    uint8_t trusted_required;   /* always 1; a Host that reads 0 refuses to register */
    uint8_t reserved;           /* must be zero */
    uint8_t nonce[DMESH_REG_NONCE_SIZE];
};
_Static_assert(sizeof(struct dmesh_registration_challenge_msg) == 36,
               "dmesh_registration_challenge_msg ABI drift");

/* Canonical identity metadata, accepted only inside an assertion sealed under
 * the authenticated paired-host control session. Numeric fields are
 * little-endian and text is zero-padded. */
struct dmesh_workload_identity {
    uint8_t  type;                     /* = DMESH_IDENTITY_TYPE */
    uint8_t  version;                  /* = DMESH_IDENTITY_VERSION */
    uint8_t  flags;                    /* zero */
    uint8_t  reserved;                 /* zero */
    uint8_t  issued_at_le[8];
    uint8_t  expires_at_le[8];

    uint8_t  nonce[DMESH_REG_NONCE_SIZE];      /* the DPU's connection challenge */
    uint8_t  channel_slot_le[4];
    uint8_t  channel_generation_le[8];
    uint8_t  daemon_incarnation[DMESH_DAEMON_INCARNATION_SIZE];

    char     cluster_id[DMESH_CLUSTER_ID_MAX];
    char     node_name[DMESH_K8S_NAME_MAX];    /* checked against the verifier's node */
    char     pod_uid[DMESH_POD_UID_MAX];       /* RFC 4122 text, 36 used */
    char     namespace_name[DMESH_K8S_NAMESPACE_MAX]; /* also qualifies service_name */
    char     pod_name[DMESH_K8S_NAME_MAX];
    char     service_account[DMESH_K8S_NAME_MAX];
    char     container_name[DMESH_K8S_NAME_MAX];
    char     container_id[DMESH_CONTAINER_ID_MAX];
    char     service_name[DMESH_SVC_NAME_MAX]; /* label; empty = no Service */
    char     pod_ip[DMESH_POD_IP_MAX];         /* dotted IPv4, e.g. "10.244.1.17" */

};
_Static_assert(sizeof(struct dmesh_workload_identity) == 1433,
               "dmesh_workload_identity ABI drift");

/* Host→DPU: the identity of the connection it arrives on. dpumeshd builds and
 * seals it; the broker forwards the bytes unchanged. `mac` is HMAC-SHA256 over
 * every byte before it, keyed from the paired-host control session id
 * (dmesh_registration_seal), so it verifies only while that session is the
 * authenticated one, and only on the connection whose challenge it names. */
struct dmesh_registration_assertion_msg {
    uint8_t type;               /* = DMESH_MSG_REG_ASSERTION */
    uint8_t version;            /* = DMESH_LOCAL_VERSION */
    uint8_t reserved[2];        /* must be zero */
    struct dmesh_workload_identity identity;
    uint8_t mac[DMESH_REG_MAC_SIZE];
};
_Static_assert(sizeof(struct dmesh_registration_assertion_msg) == 1469,
               "dmesh_registration_assertion_msg ABI drift");


#endif
