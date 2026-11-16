#ifndef DMESH_PEER_SYNC_EVENT_H
#define DMESH_PEER_SYNC_EVENT_H
#include <stdint.h>
struct ibv_pd;
struct doca_dev;
struct doca_dpa;
struct peer_sync_engine;
struct peer_sync_events;
/* ARM setup owns the normal DOCA device/PD and real external-buffer events.
 * CPU mode uses external backing plus wait tasks/get. The native DPA carrier
 * uses this engine only for device/PD ownership; its events and connection
 * are managed by peer_sync_native, without CPU wait tasks/get. */
int peer_sync_engine_new(const char *ibdev, struct peer_sync_engine **);
int peer_sync_engine_new_dpa(const char *, struct doca_dpa *, struct peer_sync_engine **);
struct doca_dev *peer_sync_engine_dev(struct peer_sync_engine *);
/* DPA mode borrows one aligned128B range inside the owner shared mmap. */
int peer_sync_events_new_at(struct peer_sync_engine *, void *, struct peer_sync_events **);
struct ibv_pd *peer_sync_engine_pd(struct peer_sync_engine *);
int peer_sync_engine_fd(struct peer_sync_engine *);
/* The owner observed this engine's notification FD readable. Does not poll
 * or consume payload. The next progress clears/rearms the one-shot PE. */
void peer_sync_engine_notification(struct peer_sync_engine *);
/* Fast nonblocking drain; no event-channel syscalls. Before sleeping the
 * owner must call arm, then progress/get again to close the arrival race. */
int peer_sync_engine_progress(struct peer_sync_engine *);
int peer_sync_engine_arm(struct peer_sync_engine *);
int peer_sync_engine_free(struct peer_sync_engine *);
/* Two cache-line-separated, aligned uint64_t values: RX tail and TX credit.
 * Their explicit mmap address/key is for an authenticated peer's single
 * writer on the SAME protected RC QP as its payload WRITE. */
int peer_sync_events_new(struct peer_sync_engine *, struct peer_sync_events **);
uint64_t peer_sync_events_address(struct peer_sync_events *, unsigned which);
uint32_t peer_sync_events_key(struct peer_sync_events *);
int peer_sync_events_get(struct peer_sync_events *, uint64_t *tail, uint64_t *head);
/* Call only after the associated QP has been destroyed/fenced. On failure
 * retain the object, mmap and backing memory and retry retirement. */
int peer_sync_events_free(struct peer_sync_events *);
#endif
