#ifndef DMESH_PEER_NODE_H
#define DMESH_PEER_NODE_H

#include "peer_stream.h"
#include "peer_session_control.h"
struct doca_dev;
struct peer_node;
struct peer_node_worker;

/* No transport when DPUMESH_PEER_TRANSPORT is unset. Otherwise registration,
 * node authentication and the crypto owner are mandatory. Creates all SDK
 * children before worker tasks start; opens independent device references. */
int peer_node_new(struct peer_session_control *, struct doca_dev **, unsigned workers,
                   int (*listener)(uint32_t, uint16_t), struct peer_node **);
struct peer_node_worker *peer_node_worker(struct peer_node *, unsigned worker);
struct peer_stream_worker *peer_node_streams(struct peer_node_worker *);
int peer_node_worker_progress(struct peer_node_worker *);
int peer_node_worker_fds(struct peer_node_worker *, int *carrier, int *msgq);
int peer_node_worker_arm(struct peer_node_worker *);
int peer_node_worker_disarm(struct peer_node_worker *);
int peer_node_worker_stop(struct peer_node_worker *);
/* Dispatcher-owned control maintenance. Never touches a worker's QP. */
void peer_node_control_progress(struct peer_node *);
int peer_node_free(struct peer_node **);
/* Routing membership only: 1 remote ready endpoint, -1 remote unavailable,
 * 0 not remote. The stream OPEN revalidates UID/registration independently. */
int peer_node_remote(uint32_t ip, uint16_t port);

#endif
