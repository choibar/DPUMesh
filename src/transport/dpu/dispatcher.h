#ifndef DMESH_DISPATCHER_H
#define DMESH_DISPATCHER_H

#include <doca_error.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "session_protocol.h"

struct objects;
struct dmesh_conn;
struct dmesh_session;
struct dmesh_dispatcher;
struct dmesh_worker_mailbox;

/* Stable identity and execution location are deliberately separate. Ownership
 * epoch is currently 1: changing owners/migrating live flows is NOT supported. */
struct dmesh_flow_key {
    uint64_t session_id, session_epoch;
    uint32_t flow_id, generation;
};
struct dmesh_flow_location {
    uint64_t ownership_epoch;
    uint32_t worker, slot;
};

/* Takes ownership of controls only on success. Workers retain their own
 * device handles and all data resources. No worker pointer is used by the
 * dispatcher thread after attachment. */
doca_error_t dmesh_dispatcher_start(struct objects **controls, struct objects **workers,
                                  size_t count, struct dmesh_dispatcher **out);
void dmesh_dispatcher_request(struct objects *, struct dmesh_session *,
                              const struct dmesh_session_header *, const void *);
void dmesh_dispatcher_session_lost(struct objects *, struct dmesh_session *);
bool dmesh_dispatcher_session_busy(struct objects *, struct dmesh_session *);
bool dmesh_dispatcher_accepting(struct objects *);
/* Worker control request; no SDK access, copied into the dispatcher's mailbox. */
int dmesh_dispatch_backend_request(struct objects *, uint32_t ip, uint16_t port);
/* Thread-safe membership only; no SDK or live session pointers escape. */
int dmesh_dispatch_listener_known(uint32_t ip, uint16_t port);

int dmesh_dispatch_worker_fd(struct objects *);
void dmesh_dispatch_worker_arm(struct objects *);
void dmesh_dispatch_worker_drain(struct objects *);
void dmesh_dispatch_worker_flush(struct objects *);
void dmesh_dispatch_worker_ready(struct objects *);
void dmesh_dispatch_worker_depart(struct objects *);
bool dmesh_dispatch_worker_stop_admission(struct objects *);
doca_error_t dmesh_dispatch_reply(struct dmesh_conn *, uint16_t, const void *, size_t, int32_t);

/* Dispatcher-only helpers implemented alongside the Comch callbacks. */
doca_error_t dmesh_session_send(struct objects *, struct dmesh_session *, uint16_t,
                              uint32_t, uint32_t, const void *, size_t, int32_t);
void dmesh_sessions_advance(struct objects *);

#endif
