#ifndef DMESH_PEER_DPA_ABI_H
#define DMESH_PEER_DPA_ABI_H

#include <stdint.h>
#include <stddef.h>
#include "peer_config.h"

/* Private ARM/peer-DPA thread and notification ABI. Host DMA uses common/ring.h. */
#define DPA_MSG_RDMA_READY 7u
#define DPA_MSG_RDMA_COUNTER 8u

struct peer_dpa_thread_arg {
    uint64_t dpa_producer_comp, dpa_producer;
    uint64_t yield_notification;
    uint32_t dpu_consumer_id, eu_index, producer_deferred, reserved;
    uint64_t rdma_state, rdma_states[DPA_RDMA_CONTEXTS];
    uint32_t rdma_count, rdma_cursor;
} __attribute__((aligned(8)));

#define DPA_RDMA_NOTIFY_VERSION 2u
struct dpa_rdma_notify_msg {
    uint8_t type, version, eu_index, worker;
};
_Static_assert(sizeof(struct dpa_rdma_notify_msg) == 4, "RDMA MsgQ notification ABI");
static inline int dpa_rdma_notify_valid(const struct dpa_rdma_notify_msg *m,
    unsigned bytes, unsigned worker, unsigned workers, unsigned threads)
{
    return bytes == sizeof(*m) && m->type == DPA_MSG_RDMA_READY &&
        m->version == DPA_RDMA_NOTIFY_VERSION && m->worker < workers &&
        workers && worker < workers && m->eu_index < threads &&
        m->eu_index % workers == worker;
}

/* Full counters fit the MsgQ 32-byte immediate limit. Cookie and generation
 * distinguish engines and reused lanes. */
struct dpa_rdma_counter_msg {
    uint8_t type, version, eu_index, worker;
    uint32_t cookie, generation;
    uint16_t lane, reserved;
    uint64_t tail, head;
};
_Static_assert(sizeof(struct dpa_rdma_counter_msg)==32, "native RDMA counter notification ABI");
static inline int dpa_rdma_counter_valid(const struct dpa_rdma_counter_msg *m,
    unsigned bytes, unsigned worker, unsigned workers, unsigned threads)
{
    struct dpa_rdma_notify_msg hint={DPA_MSG_RDMA_READY,m->version,m->eu_index,m->worker};
    return bytes==sizeof(*m) && m->type==DPA_MSG_RDMA_COUNTER && !m->reserved &&
        m->cookie && m->generation && m->lane<PEER_RDMA_LANES &&
        dpa_rdma_notify_valid(&hint,sizeof(hint),worker,workers,threads);
}


#endif
