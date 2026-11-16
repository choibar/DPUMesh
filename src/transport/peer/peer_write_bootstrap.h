#ifndef DMESH_PEER_WRITE_BOOTSTRAP_H
#define DMESH_PEER_WRITE_BOOTSTRAP_H

#define PEER_WRITE_SYNC_HELLO 4096u
#define PEER_WRITE_IMM_HELLO 64u
/* ARM Sync may reuse its 64B TX slot for two atomic results only after
 * the HELLO SEND completion. No steady-state operation reads the HELLO. */
#define PEER_WRITE_ARM_SYNC_RESULTS 0u

static inline unsigned peer_write_hello_size(int immediate, int dpa_offload)
{
    return dpa_offload && !immediate ? PEER_WRITE_SYNC_HELLO : PEER_WRITE_IMM_HELLO;
}

static inline unsigned peer_write_scratch_size(int immediate, int dpa_offload)
{
    return 2u * peer_write_hello_size(immediate, dpa_offload);
}

#endif
