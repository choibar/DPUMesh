#ifndef DPUMESH_GRPCIO_H
#define DPUMESH_GRPCIO_H

/* gRPC C-core over DPUMesh, built into grpcio's cygrpc (see ../README.md).
 * Every function opens the process's DPUMesh runtime on first use, from
 * libdpumesh_stream.so or DPUMESH_STREAM_LIBRARY. On failure each writes a
 * message to `err`. */

#include <stddef.h>

#include <grpc/grpc.h>
#include <grpc/grpc_security.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A channel whose connections are DPUMesh streams to the "<host>:<port>"
 * service `target`; gRPC reconnects through them as over TCP. NULL on
 * failure. */
grpc_channel *dpumesh_channel_create(const char *target,
                                     grpc_channel_credentials *creds,
                                     const grpc_channel_args *args, char *err,
                                     size_t err_len);

/* Adds a DPUMesh listener to `server`, which must not be started yet. Returns
 * a handle for dpumesh_server_serve(), or NULL. */
void *dpumesh_server_add_listener(grpc_server *server, char *err,
                                  size_t err_len);

/* After grpc_server_start(): hands the streams the DPU routes to
 * DPUMESH_SERVICE to the server. Returns 0, or -1. */
int dpumesh_server_serve(void *listener, char *err, size_t err_len);

/* Stops accepting streams for the listener. */
void dpumesh_server_stop(void *listener);

/* Closes the process's DPUMesh runtime; a process calls it before it exits,
 * or the DPU sees its channel vanish mid-stream. */
void dpumesh_close(void);

#ifdef __cplusplus
}
#endif

#endif /* DPUMESH_GRPCIO_H */
