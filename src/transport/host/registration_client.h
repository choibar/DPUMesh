#ifndef DMESH_HOST_REGISTRATION_CLIENT_H
#define DMESH_HOST_REGISTRATION_CLIENT_H

#include <stdint.h>
#include "../peer/peer_registration_wire.h"

struct dmesh_registration_client {
    int fd;
    unsigned state;
    uint8_t nonce[32];
    struct dmesh_registration_assertion_msg assertion;
};

/* A root-owned node agent authenticates the caller from SO_PEERCRED and its
 * Kubernetes cgroup. The workload receives only a nonce-bound sealed record. */
int dmesh_registration_client_begin(struct dmesh_registration_client *, const uint8_t nonce[32]);
/* 1 complete, 0 pending, negative errno. Nonblocking; never called inside a PE callback. */
int dmesh_registration_client_progress(struct dmesh_registration_client *);
void dmesh_registration_client_close(struct dmesh_registration_client *);

#endif
