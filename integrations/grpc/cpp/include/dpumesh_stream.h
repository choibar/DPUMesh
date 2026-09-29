#ifndef DPUMESH_STREAM_H
#define DPUMESH_STREAM_H

/* Byte streams over DPUMesh for language bindings: the C ABI of the C++
 * adapter's EQ reactor. A runtime owns the process's DPUMesh channel, one
 * reactor thread that is the EQ's only consumer, and one callback thread. A
 * stream is one native QP carrying one HTTP/2 connection's bytes.
 *
 * Threads. Stream callbacks run on the reactor thread; connect and accept
 * results run on the callback thread. No callback runs inside a dms_* call,
 * except `released`, which may run inside dms_stream_close() or
 * dms_stream_abort(). A callback may call any dms_stream_* function.
 *
 * Lifetime. A stream handle is valid from its connect or accept delivery until
 * the binding calls dms_stream_close() or dms_stream_abort(), which frees it.
 * Callbacks already under way may still run after that; `released` is the last
 * call for a bound stream, after which the binding may free its context.
 */

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DMS_ABI_VERSION 1

typedef struct dms_runtime dms_runtime;
typedef struct dms_stream dms_stream;

/* Values `recv` returns. */
enum {
  DMS_RECV_OK = 0,
  /* The binding holds more unread bytes than it wants; the stream withholds
   * receive credit, so the peer stops sending, until dms_stream_resume(). */
  DMS_RECV_HOLD = 1,
};

typedef struct dms_stream_callbacks {
  /* Returns a destination for `len` received bytes, or NULL to fail the
   * stream. */
  void *(*recv_alloc)(void *ctx, size_t len);
  /* `buf`, from recv_alloc, now holds `len` bytes and belongs to the binding.
   * Returns DMS_RECV_OK or DMS_RECV_HOLD. */
  int (*recv)(void *ctx, void *buf, size_t len);
  /* A write that returned -EAGAIN may be retried. */
  void (*writable)(void *ctx);
  /* The peer finished sending. */
  void (*eof)(void *ctx);
  /* The stream failed and delivers nothing more. `message` is valid only
   * during the call. */
  void (*error)(void *ctx, int err, const char *message);
  /* The last call for this stream. */
  void (*released)(void *ctx);
} dms_stream_callbacks;

/* `stream` is NULL on failure, with a positive errno value in `err`. */
typedef void (*dms_connect_fn)(void *ctx, dms_stream *stream, int err,
                               const char *message);
typedef void (*dms_accept_fn)(void *ctx, dms_stream *stream);

int dms_abi_version(void);

/* Opens the process's DPUMesh channel from its DPUMESH_* configuration.
 * Returns NULL on failure and writes a message to `err` when it is not NULL. */
dms_runtime *dms_runtime_open(char *err, size_t err_len);
/* Fails live streams, stops the runtime's threads and closes the channel.
 * `rt` must not be used again, and no dms_connect() or dms_listen() may be in
 * progress. Open stream handles stay valid: their calls fail, each is still
 * closed as usual, and `released` still arrives for every bound stream. A
 * process closes its runtime before it exits, or the DPU sees the channel
 * vanish mid-stream. */
void dms_runtime_close(dms_runtime *rt);
/* The largest single native post; dms_stream_write() splits larger writes. */
size_t dms_runtime_post_max(const dms_runtime *rt);

/* Opens a stream to a "<host>:<port>" service target. `done` runs once. */
void dms_connect(dms_runtime *rt, const char *service, dms_connect_fn done,
                 void *ctx);
/* Delivers the streams the DPU routes to this process's DPUMESH_SERVICE to
 * `accept`, or rejects them when `accept` is NULL. Returns once the change
 * applies: 0, or a negative errno value. */
int dms_listen(dms_runtime *rt, dms_accept_fn accept, void *ctx);

/* Starts deliveries to `cb` with `ctx`. Bytes that arrived before binding are
 * delivered first. Call at most once; returns 0 or a negative errno value. */
int dms_stream_bind(dms_stream *s, const dms_stream_callbacks *cb, void *ctx);
/* Writes up to `len` bytes. Returns the count accepted, which may be short;
 * -EAGAIN when no transmit credit is free, after which `writable` follows; or
 * another negative errno value once the stream is closed or failed. */
ssize_t dms_stream_write(dms_stream *s, const void *data, size_t len);
/* Reserves `len` bytes of native transmit space, at most
 * dms_runtime_post_max(), has `fill` write all of them and posts them: a write
 * without a staging copy. Returns `len`; -EAGAIN when no transmit credit is
 * free, after which `writable` follows; or another negative errno value.
 * `fill` runs only when the result is `len`, and must not call into the
 * stream. */
ssize_t dms_stream_post(dms_stream *s, size_t len,
                        void (*fill)(void *ctx, void *dst, size_t len),
                        void *ctx);
/* Returns the receive credit withheld after DMS_RECV_HOLD. */
void dms_stream_resume(dms_stream *s);
/* Closes the stream after the bytes already written and frees the handle. */
void dms_stream_close(dms_stream *s);
/* Resets the stream and frees the handle. */
void dms_stream_abort(dms_stream *s);

#ifdef __cplusplus
}
#endif

#endif /* DPUMESH_STREAM_H */
