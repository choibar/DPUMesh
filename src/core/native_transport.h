/* Private native carrier boundary. Public API layouts do not depend on DOCA. */
#ifndef DMESH_NATIVE_TRANSPORT_H
#define DMESH_NATIVE_TRANSPORT_H
#include "dmesh_core.h"
struct dmesh_native_transport;
struct dmesh_native_config {
    size_t bytes;
    /* K is the Host-to-DPU forward-ring count. L is the independently
     * owned DPU-to-Host landing-stripe count. */
    int slot_size, rings, rx_stripes;
    const char *service_name;
    void *tx, *rx;
    size_t rx_bytes;              /* receive region size; 0 means the TX size */
    int pod_id, service_id, stripes;
    /* Descriptors one flow can have submitted and unacknowledged, a power of
     * two: within it a submit never finds the flow's ring full. */
    uint32_t flow_depth;
};
enum { DMESH_NATIVE_RX = 1, DMESH_NATIVE_ACK, DMESH_NATIVE_ERROR };
struct dmesh_native_event {
    int kind, error;
    sw_descriptor_t desc;
    uint16_t port, seq;
    uint32_t seq_count;
};
/* open succeeds only after registered memory and remote readiness are live. */
int dmesh_native_open(struct dmesh_native_transport **, struct dmesh_native_config *);
/* close retains ownership on failure; no mapping may be freed before quiesce. */
int dmesh_native_close(struct dmesh_native_transport *);
/* submit borrows the registered TX range until a custody ACK, including FIN.
 * A FIN with DMESH_DESC_HALF_CLOSE ends only the forward stream. */
int dmesh_native_submit(struct dmesh_native_transport *, const sw_descriptor_t *);
/* A stripe has one consumer at a time; the core provides per-stripe exclusion. */
/* Advance the channel's shared control PE once before scanning its stripes.
 * Errors are retained and delivered by each affected flow's poll. */
/* Returns a snapshot of stripes needing drain, including closed flows whose
 * custody/FIN events still need retirement. New flows appear on the next pass. */
uint32_t dmesh_native_progress(struct dmesh_native_transport *);
/* The errno of a failed session, or 0. Sticky: a lost session never recovers,
 * so every flow, dial and accept of the channel is dead with it. */
int dmesh_native_lost(struct dmesh_native_transport *);
/* Fills up to max events of one stripe, after publishing what its flow wake
 * completed (dmesh_native_flow_wake), all under one hold of the stripe;
 * returns the count. Consecutive custody ACKs come as one event (seq_count). */
int dmesh_native_poll(struct dmesh_native_transport *, int stripe, struct dmesh_native_event *, int max);
void dmesh_native_release(struct dmesh_native_transport *, int byte_offset);
/* Doorbell of a stripe: an fd readable while the stripe has signalled work, or
 * -1 when the stripe has none. Stable for the carrier's lifetime. */
int dmesh_native_stripe_fd(struct dmesh_native_transport *, int stripe);
/* The stripe carrying a port's stream, or -1. */
int dmesh_native_stripe_of(struct dmesh_native_transport *, uint16_t port);
/* Arms the stripe's doorbell before its consumer sleeps; `rang` acknowledges
 * a doorbell that fired since the last arm. Returns 0 when
 * dmesh_native_idle_arm plus the doorbell suffice; otherwise what still needs
 * polling (custody ACKs have no doorbell): DMESH_STRIPE_CUSTODY for an open
 * flow's outstanding custody, DMESH_STRIPE_RETIRING for a closed flow whose
 * custody or FIN still has to be retired. */
enum { DMESH_STRIPE_CUSTODY = 1, DMESH_STRIPE_RETIRING = 2 };
int dmesh_native_stripe_arm(struct dmesh_native_transport *, int stripe, int rang);
/* Flow wake (every signaled channel): every stripe has a Comch consumer of
 * its own, whose fd is the stripe's doorbell. Its receives are published only
 * by dmesh_native_poll, and its control messages (CLOSED, ERROR) are reported
 * by dmesh_native_take_news to the session's progress, since the doorbell
 * carries receives alone. Fixed once the carrier is open. */
int dmesh_native_flow_wake(struct dmesh_native_transport *);
uint32_t dmesh_native_take_news(struct dmesh_native_transport *);
/* Nonzero when every receive raises the channel wake fd by itself (the
 * Comch fast path): an idle consumer then needs neither an ARM round trip
 * nor polling to see receives. Fixed once the carrier is open. */
int dmesh_native_rx_signaled(struct dmesh_native_transport *);
/* Channel-wide idle wake: the fd (or -1) is raised by control messages once
 * dmesh_native_idle_arm succeeded, which also asks the DPU to ring for
 * receives without a doorbell. The arm does not progress the channel: the
 * consumer calls dmesh_native_progress after it, so nothing that landed
 * before the arm is slept through. A failed arm means the consumer must keep
 * polling. The next arm clears a raised fd. */
int dmesh_native_wake_fd(struct dmesh_native_transport *);
int dmesh_native_idle_arm(struct dmesh_native_transport *);
/* ARM messages sent and DOORBELLs received, for diagnostics. */
void dmesh_native_wake_counters(struct dmesh_native_transport *, uint64_t *arms, uint64_t *doorbells);
int dmesh_native_resolve(struct dmesh_native_transport *, const char *name, uint32_t addr, uint16_t port);
/* A client port's stream exists from connect until its FIN or disconnect; a
 * half-close FIN ends only its forward direction. Its receive buffers outlive
 * it until released; close fails with EBUSY while one is held. */
int dmesh_native_connect(struct dmesh_native_transport *, uint16_t port, int service_id);
int dmesh_native_disconnect(struct dmesh_native_transport *, uint16_t port);
#endif
