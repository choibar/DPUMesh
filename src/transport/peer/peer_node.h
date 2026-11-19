#ifndef DMESH_PEER_NODE_H
#define DMESH_PEER_NODE_H

#include "peer_connection_io.h"
#include "local_control_session.h"
struct doca_dev;
struct peer_node;
struct peer_node_worker;

/* Unset/empty DPUMESH_PEER_RDMA_DEVICE leaves peer transport disabled.
 * A configured device always starts WRITE_WITH_IMM+DPA; registration, node
 * authentication and the crypto owner are mandatory. Creates all SDK
 * children before worker tasks start; opens independent device references. */
int peer_node_new(struct local_control_session *, struct doca_dev **, unsigned workers,
                   int (*listener)(uint32_t, uint16_t), struct peer_node **);
struct peer_node_worker *peer_node_worker(struct peer_node *, unsigned worker);
struct peer_connection_worker *peer_node_connections(struct peer_node_worker *);
int peer_node_worker_progress(struct peer_node_worker *);
int peer_node_worker_fds(struct peer_node_worker *, int *carrier, int *msgq);
int peer_node_worker_arm(struct peer_node_worker *);
int peer_node_worker_disarm(struct peer_node_worker *);
int peer_node_worker_stop(struct peer_node_worker *);
/* Dispatcher-owned control maintenance. Never touches a worker's QP. */
void peer_node_control_progress(struct peer_node *);
int peer_node_free(struct peer_node **);
/* Routing membership only: 1 remote ready endpoint, -1 remote unavailable,
 * 0 not remote. The connection OPEN revalidates UID/registration independently. */
int peer_node_remote(uint32_t ip, uint16_t port);

#endif
