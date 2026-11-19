#ifndef DMESH_LOCAL_CONTROL_SESSION_H
#define DMESH_LOCAL_CONTROL_SESSION_H

#include "peer_authority.h"

/* Dispatcher-owned local Host-to-DPU TLS control session, topology refresh and
 * Pod registration lifecycle. The Host peer is the trusted daemon; Pod
 * assertions arrive on Comch and are bound to this authenticated session.
 * DPU-to-DPU control lives in peer_control_session.h. */
struct local_control_session;
/* Enabled by DMESH_PEER_REGISTRATION=1. All settings are mandatory when
 * enabled; a partial configuration is an error, never an unauthenticated mode. */
int local_control_session_new(unsigned capacity, struct local_control_session **out);
void local_control_session_progress(struct local_control_session *);
int local_control_session_healthy(struct local_control_session *);
struct peer_authority *local_control_session_authority(struct local_control_session *);
int local_control_session_register(struct local_control_session *, uint64_t id, uint64_t epoch,
                                   const uint8_t nonce[32],
                                   const struct dmesh_registration_assertion_msg *);
int local_control_session_free(struct local_control_session *);

#endif
