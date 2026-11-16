#ifndef DMESH_PEER_SYNC_NATIVE_H
#define DMESH_PEER_SYNC_NATIVE_H
#include <stddef.h>
#include <stdint.h>
#include <doca_dpa.h>
#include <doca_rdma.h>
#include <doca_sync_event.h>
struct doca_dev;
struct doca_mmap;
struct peer_sync_native;
/* Setup only. Exported bytes contain capabilities: exchange only through an
 * authenticated, confidential control channel. Never log the export. */
int peer_sync_native_new(struct doca_dev *,
                         struct doca_dpa *,
                         struct doca_dpa_completion *,
                         uint32_t gid,
                         uint16_t mtu,
                         struct peer_sync_native **);
/* Same version-pinned native QPN/IPsec boundary, without Sync Events. */
int peer_sync_native_new_imm(struct doca_dev *,
                             struct doca_dpa *,
                             struct doca_dpa_completion *,
                             uint32_t gid,
                             uint16_t mtu,
                             struct peer_sync_native **);
int peer_sync_native_export(struct peer_sync_native *, const void **, size_t *);
int peer_sync_native_connect(struct peer_sync_native *, const void *, size_t);
int peer_sync_native_endpoint(struct peer_sync_native *, uint32_t *qpn, uint32_t *psn);
int peer_sync_native_event_export(struct peer_sync_native *, unsigned, const void **, size_t *);
int peer_sync_native_event_import(struct peer_sync_native *, unsigned, const void *, size_t);
int peer_sync_native_handles(struct peer_sync_native *,
                             doca_dpa_dev_rdma_t *,
                             uint32_t *,
                             doca_dpa_dev_sync_event_t local[2],
                             doca_dpa_dev_sync_event_remote_net_t remote[2]);
/* Call only after stopping/removing the DPA engine's reference. Failure
 * retains the object and all still-live resources for retry/quarantine. */
int peer_sync_native_free(struct peer_sync_native *);
#endif
