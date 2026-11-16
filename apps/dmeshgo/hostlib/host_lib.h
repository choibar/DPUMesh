#ifndef DMESH_HOST_LIB_H
#define DMESH_HOST_LIB_H

/*
 * libdmesh_host: the host-side DMA channel as a byte-stream library, so an
 * application (e.g. a Go gRPC service through cgo) can dial/serve through the
 * DPU proxy directly instead of splicing through a TCP bridge process.
 *
 * A channel wraps one comch connection + one push-transport DMA channel
 * (안 2: no host DPA, freely replicable per process/thread). Modes:
 *   2 (CLIENT): client semantics — the DPU proxy serves this flow
 *     through its outbound L7 stack (dial side).
 *   1 (BACKEND): this process is a backend provider for (dst_ip, dst_port);
 *     the proxy connects through the channel instead of dialing TCP.
 *
 * Threading: one channel may be used by one reader thread and one writer
 * thread concurrently (an internal mutex serializes the shared progress
 * engine). Read/write are non-blocking; the caller drives its own backoff.
 */

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

struct dmesh_chan;

/* Connect a channel. Returns NULL on failure (details in the DOCA log).
 * pci_addr: host PCI function for comch (e.g. "94:00.1").
 * server_name: DPU comch server (e.g. "DPUMesh0").
 * src/dst: flow identity (dotted-quad strings, host-order ports).
 * workload: source workload identity string (attested over PCIe).
 * mode: 1 = BACKEND provider, 2 = CLIENT client. */
struct dmesh_chan *dmesh_chan_connect(const char *pci_addr,
				      const char *server_name,
				      const char *src_ip, uint16_t src_port,
				      const char *dst_ip, uint16_t dst_port,
				      const char *workload, uint32_t mode);

/* Non-blocking read of proxied bytes. Returns >0 bytes copied into buf,
 * 0 when no data is currently available, -1 when the channel is dead. */
ssize_t dmesh_chan_read(struct dmesh_chan *c, void *buf, size_t len);

/* Non-blocking write. Returns the number of bytes accepted (possibly 0 when
 * the staging ring is momentarily full), -1 when the channel is dead. */
ssize_t dmesh_chan_write(struct dmesh_chan *c, const void *buf, size_t len);

/* 1 once any bytes have arrived from the DPU on this channel (a BACKEND
 * channel has been claimed by a proxied flow), else 0. */
int dmesh_chan_claimed(struct dmesh_chan *c);

/* Tear down the channel and its DOCA resources. */
void dmesh_chan_close(struct dmesh_chan *c);

#ifdef __cplusplus
}
#endif

#endif /* DMESH_HOST_LIB_H */
