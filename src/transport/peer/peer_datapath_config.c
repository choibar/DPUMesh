#include "peer_datapath_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
peer_datapath_configure(int dpa_offload, char *error, size_t error_size)
{
    const char *profile = getenv("DPUMESH_PEER_DATAPATH");
    if (!profile || !*profile)
        return 0;

    int imm = !strcmp(profile, "imm") || !strcmp(profile, "imm-dpa");
    int dpa = !strcmp(profile, "send-dpa") || !strcmp(profile, "imm-dpa");
    if (!imm && strcmp(profile, "send") && strcmp(profile, "send-dpa")) {
        snprintf(error, error_size,
                 "DPUMESH_PEER_DATAPATH must be send, send-dpa, imm or imm-dpa");
        return -1;
    }

    if (dpa != !!dpa_offload) {
        snprintf(error, error_size, "profile %s requires dpa_rdma_offload=%d; binary has %d",
                 profile, dpa, !!dpa_offload);
        return -1;
    }

    const char *transport = imm ? "rdma-write-imm" : "rdma";
    const char *security = "ipsec-pod-pair";
    const char *old_transport = getenv("DPUMESH_PEER_TRANSPORT");
    const char *old_security = getenv("DPUMESH_PEER_SECURITY");
    if ((old_transport && *old_transport && strcmp(old_transport, transport)) ||
        (old_security && *old_security && strcmp(old_security, security))) {
        snprintf(error, error_size, "profile %s conflicts with PEER_TRANSPORT/PEER_SECURITY; "
                 "expected %s/%s", profile, transport, security);
        return -1;
    }

    if (setenv("DPUMESH_PEER_TRANSPORT", transport, 1) ||
        setenv("DPUMESH_PEER_SECURITY", security, 1)) {
        snprintf(error, error_size, "cannot set peer profile environment");
        return -1;
    }
    return 0;
}
