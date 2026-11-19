#ifndef DMESH_PEER_IDENTITY_H
#define DMESH_PEER_IDENTITY_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include "peer_config.h"
#include "local_registration_protocol.h"

#define DMESH_KEY_SIZE 32u

/* ---- Identity representations ----------------------------------------- */
/* Private Rust/C bridge, never Host-controlled wire metadata. */
struct peer_flow_origin {
    uint64_t session_id, session_epoch, generation;
    uint32_t channel_slot;
    char pod_uid[DMESH_POD_UID_MAX];
    char namespace_name[DMESH_K8S_NAMESPACE_MAX];
    char service_account[DMESH_K8S_NAME_MAX];
    char service_name[DMESH_SVC_NAME_MAX];
};
_Static_assert(sizeof(struct peer_flow_origin) == 480, "peer origin Rust/C ABI");

/* Retained from the verified registration assertion, never filled from
 * application input.
 * This is a host representation, not a wire structure. dma_generation fences
 * local DMA callbacks; it does not replace this registration incarnation. */
struct dmesh_peer_registration {
    uint8_t daemon[DMESH_DAEMON_INCARNATION_SIZE];
    uint32_t slot;
    uint64_t generation;
    uint8_t nonce[DMESH_REG_NONCE_SIZE];
};

struct dmesh_peer_pair {
    char local_uid[DMESH_POD_UID_MAX];
    char remote_uid[DMESH_POD_UID_MAX];
    struct dmesh_peer_registration local;
    struct dmesh_peer_registration remote;
    /* Certified by the control/crypto owner after the lane activation barrier.
     * Zero on an outbound request, immutable after channel authentication. */
    uint8_t association[16];
    uint64_t lane_id, lane_generation;
};

/* Claims a successful verification hands to the registration. The Service pair
 * (namespace_name, service_name) is compared against the Service the
 * registration requests; the rest is retained on the Pod's state. */
struct dmesh_identity_claims {
    char workload[DMESH_WORKLOAD_MAX];
    char pod_uid[DMESH_POD_UID_MAX];
    char namespace_name[DMESH_K8S_NAMESPACE_MAX];
    char service_account[DMESH_K8S_NAME_MAX];
    char service_name[DMESH_SVC_NAME_MAX];
    char pod_ip[DMESH_POD_IP_MAX];
    uint8_t daemon_incarnation[DMESH_DAEMON_INCARNATION_SIZE];
    uint32_t channel_slot;
    uint64_t channel_generation;
};

/* ---- Registration comparison ------------------------------------------ */
static inline int dmesh_peer_registration_valid(const struct dmesh_peer_registration *r)
{
    uint8_t daemon = 0, nonce = 0;
    for (size_t i = 0; i < sizeof(r->daemon); i++)
        daemon |= r->daemon[i];

    for (size_t i = 0; i < sizeof(r->nonce); i++)
        nonce |= r->nonce[i];

    return r->generation && daemon && nonce;
}

static inline int dmesh_peer_registration_equal(const struct dmesh_peer_registration *a,
                                                const struct dmesh_peer_registration *b)
{
    /* Do not compare padding in a C structure. */
    return a->slot == b->slot && a->generation == b->generation &&
           !memcmp(a->daemon, b->daemon, sizeof(a->daemon)) &&
           !memcmp(a->nonce, b->nonce, sizeof(a->nonce));
}

/* ---- Metadata decoding and registration seals -------------------------- */
enum dmesh_identity_result {
    DMESH_IDENTITY_OK = 0,
    DMESH_IDENTITY_BAD_TYPE,
    DMESH_IDENTITY_BAD_VERSION,
    DMESH_IDENTITY_NONCANONICAL,
    DMESH_IDENTITY_WRONG_NODE,
    DMESH_IDENTITY_BAD_TIME,
    DMESH_IDENTITY_BAD_NONCE,
};

/* Metadata decoder. It authorizes nothing by itself: the caller verifies the
 * assertion's seal (dmesh_registration_verify) before decoding. */
enum dmesh_identity_result dmesh_identity_decode(
    const struct dmesh_workload_identity *, const char *, const char *,
    const uint8_t [DMESH_REG_NONCE_SIZE], uint64_t,
    struct dmesh_identity_claims *);

/* Registration seal. The key is HMAC-SHA256 keyed with the paired-host control
 * session id over the label "dpumesh-registration"; the seal is HMAC-SHA256
 * under that key over every byte of the assertion before `mac`. dpumeshd seals
 * with the session it holds and the DPU verifies with the session it
 * authenticated, so a seal outlives neither. Seal returns 0 on success; verify
 * returns 1 on a match and compares in constant time. */
int dmesh_registration_seal(const uint8_t session[DMESH_CONTROL_SESSION_SIZE],
                            struct dmesh_registration_assertion_msg *assertion);
int dmesh_registration_verify(
    const uint8_t session[DMESH_CONTROL_SESSION_SIZE],
    const struct dmesh_registration_assertion_msg *assertion);

/* ---- Key loading and signed document verification ---------------------- */
/* Authoritative feeds are signed by the feed keyring (DPUMESH_FEED_KEY_DIR),
 * which is disjoint from the topology signing key so each signature has one
 * protocol role. The envelope is a final line
 * `signature=<key-id>,<64 hex>`; the MAC covers every byte before it. */
enum dmesh_feed_result {
    DMESH_FEED_OK = 0,
    DMESH_FEED_UNSIGNED,
    DMESH_FEED_BAD_KEY_ID,
    DMESH_FEED_BAD_MAC,
    DMESH_FEED_INTERNAL,
};

/* Verify a feed document against the keyring in `key_dir`. On success
 * `signed_length` is the prefix the caller may parse; bytes after the envelope
 * are refused rather than ignored, so nothing unsigned is ever read. */
enum dmesh_feed_result
dmesh_feed_verify(const char *document, size_t length, const char *key_dir,
                  size_t *signed_length);

/* Verify a topology generation's envelope: the same signed-prefix rule as
 * dmesh_feed_verify, but the signature is a 64-byte Ed25519 (128 hex) verified
 * against `key_dir` public keys the DPU cannot sign with. BAD_MAC reads as a
 * bad signature. */
enum dmesh_feed_result
dmesh_gen_verify(const char *document, size_t length, const char *key_dir,
                 size_t *signed_length);


void dmesh_wire_put_u64_le(uint8_t out[8], uint64_t value);
uint64_t dmesh_wire_get_u64_le(const uint8_t in[8]);
uint32_t dmesh_wire_get_u32_le(const uint8_t in[4]);

/* Key files are either 32 raw bytes or 64 lowercase/uppercase hexadecimal
 * digits (an optional final newline is accepted). They must be regular,
 * owned by the effective uid and inaccessible to group/other users. */
int dmesh_load_key(const char *path, uint8_t key[DMESH_KEY_SIZE],
                         char *error, size_t error_len);

#endif
