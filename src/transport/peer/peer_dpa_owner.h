#ifndef DMESH_PEER_DPA_OWNER_H
#define DMESH_PEER_DPA_OWNER_H
#include "peer_pair_dpa.h"

struct peer_dpa_owner;
/* Initialization is serialized before worker tasks start. The returned owner
 * borrows dev/dpa, which must outlive it. One context and MsgQ per ARM worker. */
int peer_dpa_owner_new(struct doca_dev *, struct doca_dpa *, unsigned worker,
                       unsigned workers, unsigned physical_eu, struct peer_dpa_owner **);
const struct peer_dpa_runtime *peer_dpa_owner_runtime(struct peer_dpa_owner *);
int peer_dpa_owner_progress(struct peer_dpa_owner *);
int peer_dpa_owner_fd(struct peer_dpa_owner *);
int peer_dpa_owner_arm(struct peer_dpa_owner *);
int peer_dpa_owner_disarm(struct peer_dpa_owner *);
/* Destroy transport first: REMOVE/QP destruction/STOP must have detached all
 * device state. Failure retains the object and its mappings for retry. */
int peer_dpa_owner_free(struct peer_dpa_owner **);
#endif
