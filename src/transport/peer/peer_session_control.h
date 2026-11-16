#ifndef DMESH_PEER_SESSION_CONTROL_H
#define DMESH_PEER_SESSION_CONTROL_H

#include "peer_authority.h"

struct peer_session_control;
/* Enabled by DMESH_PEER_REGISTRATION=1. All settings are mandatory when
 * enabled; a partial configuration is an error, never an unauthenticated mode. */
int peer_session_control_new(unsigned capacity, struct peer_session_control **out);
void peer_session_control_progress(struct peer_session_control *);
int peer_session_control_healthy(struct peer_session_control *);
struct peer_authority *peer_session_control_authority(struct peer_session_control *);
int peer_session_control_register(struct peer_session_control *, uint64_t id, uint64_t epoch,
                                  const uint8_t nonce[32],
                                  const struct dmesh_registration_assertion_msg *);
int peer_session_control_free(struct peer_session_control *);

#endif
