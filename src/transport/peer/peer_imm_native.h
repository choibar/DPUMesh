#ifndef DMESH_PEER_IMM_NATIVE_H
#define DMESH_PEER_IMM_NATIVE_H
#include <stddef.h>
#include <stdint.h>
#include <doca_dpa.h>
#include <doca_rdma.h>
struct doca_dev;
struct peer_imm_native;
/* Setup only. Exported bytes contain capabilities: exchange only through an
 * authenticated, confidential control channel. Never log the export. */
int peer_imm_native_new(struct doca_dev *,
                             struct doca_dpa *,
                             struct doca_dpa_completion *,
                             uint32_t gid,
                             uint16_t mtu,
                             struct peer_imm_native **);
int peer_imm_native_export(struct peer_imm_native *, const void **, size_t *);
int peer_imm_native_connect(struct peer_imm_native *, const void *, size_t);
int peer_imm_native_endpoint(struct peer_imm_native *, uint32_t *qpn, uint32_t *psn);
int peer_imm_native_handles(struct peer_imm_native *,
                             doca_dpa_dev_rdma_t *,
                             uint32_t *);
/* Call only after stopping/removing the DPA engine's reference. Failure
 * retains the object and all still-live resources for retry/quarantine. */
int peer_imm_native_free(struct peer_imm_native *);
/* Native RDMA device and its borrowed PD; registrations retire before free. */
struct peer_rdma_device;
int peer_rdma_device_new(const char *, struct doca_dpa *, struct peer_rdma_device **);
struct doca_dev *peer_rdma_device_dev(struct peer_rdma_device *);
struct ibv_pd *peer_rdma_device_pd(struct peer_rdma_device *);
int peer_rdma_device_free(struct peer_rdma_device *);
#endif
