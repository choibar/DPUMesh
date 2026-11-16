#ifndef DMESH_PEER_DATAPATH_CONFIG_H
#define DMESH_PEER_DATAPATH_CONFIG_H

#include <stddef.h>

/* Optional explicit E2E profile. Unset preserves the existing transport knobs.
 * A selected profile must match the compiled RDMA offload mode on BOTH peers. */
int peer_datapath_configure(int dpa_offload, char *error, size_t error_size);

#endif
