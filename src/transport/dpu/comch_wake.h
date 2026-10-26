#ifndef COMCH_WAKE_H
#define COMCH_WAKE_H

/*
 * Comch fast-path wake (DPU side). A push flow whose session has a host
 * consumer publishes its 16B descriptor as the immediate data of a Comch
 * producer message instead of a desc copy. The message lands as a
 * completion in the host's control PE, which is the host's idle wake: an
 * idle host sleeps on that PE's notification with no ARM round trip.
 *
 * Each worker keeps one producer per session it serves such flows of, on its
 * own PE (consumer_pe); each send names the flow's consumer: the session's,
 * or with flow wake the host flow slot's own. Producer create/start/stop/destroy go through the
 * session's connection, which the dispatcher thread progresses, so they run
 * under the dispatcher's SDK lock; sends and completions are worker-local.
 */
#include <stdbool.h>
#include <stdint.h>
#include <doca_comch.h>
#include <doca_error.h>

struct objects;
struct dmesh_conn;

/* DMESH_COMCH_WAKE=0 on the DPU keeps every flow on the desc copy + ARM path. */
bool dmesh_wake_enabled(void);

/* Bind a newly admitted flow to its worker's producer for the session,
 * creating and starting the producer on first use. Worker thread. */
doca_error_t dmesh_wake_attach(struct dmesh_conn *c, struct doca_comch_connection *connection,
                               uint32_t consumer);

/* Unbind a flow being torn down; the last flow of a session stops and
 * destroys the producer before returning (its connection may be closed
 * right after the flow retires). Worker thread. */
void dmesh_wake_detach(struct dmesh_conn *c);

/* Send the flow's descriptor. DOCA_ERROR_AGAIN: the producer is still
 * starting or cannot take a task yet; retry later. On success the send
 * completion calls dmesh_dma_push_desc_sent (or _failed). */
doca_error_t dmesh_wake_send_desc(struct dmesh_conn *c, uint64_t seq, uint32_t pos, uint32_t len);

/* Worker cleanup: destroy whatever producers remain. */
void dmesh_wake_cleanup(struct objects *o);

#endif /* COMCH_WAKE_H */
