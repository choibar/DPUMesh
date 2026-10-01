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
/* submit borrows the registered TX range until a custody ACK, including FIN. */
int dmesh_native_submit(struct dmesh_native_transport *, const sw_descriptor_t *);
/* Progress channel-wide control once before polling the landing stripes. */
void dmesh_native_progress(struct dmesh_native_transport *);
/* A stripe has one consumer at a time; the core provides per-stripe exclusion. */
int dmesh_native_poll(struct dmesh_native_transport *, int stripe, struct dmesh_native_event *);
void dmesh_native_release(struct dmesh_native_transport *, int byte_offset);
/* Doorbell of a stripe: an fd readable while the stripe has signalled work, or
 * -1 when the stripe has none. Stable for the carrier's lifetime. */
int dmesh_native_stripe_fd(struct dmesh_native_transport *, int stripe);
/* The stripe carrying a port's stream, or -1. */
int dmesh_native_stripe_of(struct dmesh_native_transport *, uint16_t port);
/* Arms the stripe's doorbell before its consumer sleeps. Returns 1 while the
 * stripe still needs polling (custody ACKs outstanding: they have no doorbell),
 * 0 when dmesh_native_idle_arm plus the doorbell suffice. */
int dmesh_native_stripe_arm(struct dmesh_native_transport *, int stripe);
/* Acknowledges a signalled doorbell; the stripe is then polled. */
void dmesh_native_stripe_clear(struct dmesh_native_transport *, int stripe);
/* Channel-wide idle wake: the fd (or -1) is raised by control messages once
 * dmesh_native_idle_arm succeeded, which also asks the DPU to ring for
 * receives without a doorbell. A failed arm means the consumer must keep
 * polling. dmesh_native_wake_clear acknowledges a raised fd. */
int dmesh_native_wake_fd(struct dmesh_native_transport *);
int dmesh_native_idle_arm(struct dmesh_native_transport *);
void dmesh_native_wake_clear(struct dmesh_native_transport *);
/* ARM messages sent and DOORBELLs received, for diagnostics. */
void dmesh_native_wake_counters(struct dmesh_native_transport *, uint64_t *arms, uint64_t *doorbells);
int dmesh_native_resolve(struct dmesh_native_transport *, const char *name, uint32_t addr, uint16_t port);
/* A client port's stream exists from connect until its FIN or disconnect. */
int dmesh_native_connect(struct dmesh_native_transport *, uint16_t port, int service_id);
int dmesh_native_disconnect(struct dmesh_native_transport *, uint16_t port);
#endif
