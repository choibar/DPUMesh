#include "dpumesh_stream.h"

#include <errno.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "dmesh_api_ops.h"
#include "dmesh_runtime.h"
#include "dmesh_stream_internal.h"
#include "endpoint_transport.h"

namespace dpumesh::grpc {
namespace {

int ErrnoOf(const absl::Status& status) {
  switch (status.code()) {
    case absl::StatusCode::kOk:
      return 0;
    case absl::StatusCode::kUnavailable:
      return EPIPE;
    case absl::StatusCode::kResourceExhausted:
      return ENOBUFS;
    case absl::StatusCode::kInvalidArgument:
      return EINVAL;
    case absl::StatusCode::kCancelled:
      return ECANCELED;
    case absl::StatusCode::kDeadlineExceeded:
      return ETIMEDOUT;
    default:
      return EIO;
  }
}

// Delivers one stream's reactor events to a binding's callbacks. The reactor
// holds it weakly and locks it for each event, so the destructor, which sends
// `released`, runs only once no callback is under way.
class CallbackSink final : public ConnectionSink {
 public:
  CallbackSink(const dms_stream_callbacks& callbacks, void* ctx)
      : callbacks_(callbacks), ctx_(ctx) {}

  ~CallbackSink() override {
    if (callbacks_.released != nullptr) callbacks_.released(ctx_);
  }

  // Stops deliveries once the binding closed its handle. A callback that
  // already passed the check still completes.
  void Detach() { detached_.store(true, std::memory_order_release); }

  ReceiveOutcome OnIncomingData(
      size_t length, absl::FunctionRef<void(uint8_t*)> fill) override {
    if (detached()) return ReceiveOutcome{absl::OkStatus(), false};
    void* buffer = callbacks_.recv_alloc(ctx_, length);
    if (buffer == nullptr) {
      return ReceiveOutcome{
          absl::ResourceExhaustedError("binding refused a receive buffer"),
          false};
    }
    fill(static_cast<uint8_t*>(buffer));
    const int result = callbacks_.recv(ctx_, buffer, length);
    return ReceiveOutcome{absl::OkStatus(), result == DMS_RECV_HOLD};
  }

  void OnWritable() override {
    if (!detached()) callbacks_.writable(ctx_);
  }

  void OnRemoteEof() override {
    if (!detached()) callbacks_.eof(ctx_);
  }

  void OnTransportError(absl::Status status) override {
    if (detached()) return;
    const std::string message(status.message());
    callbacks_.error(ctx_, ErrnoOf(status), message.c_str());
  }

 private:
  bool detached() const { return detached_.load(std::memory_order_acquire); }

  const dms_stream_callbacks callbacks_;
  void* const ctx_;
  std::atomic<bool> detached_{false};
};

}  // namespace
}  // namespace dpumesh::grpc

using dpumesh::grpc::CallbackSink;
using dpumesh::grpc::DmeshReactor;
using dpumesh::grpc::DmeshRuntime;
using dpumesh::grpc::PostCode;
using dpumesh::grpc::PostResult;
using dpumesh::grpc::Reservation;

struct dms_runtime {
  std::shared_ptr<DmeshRuntime> runtime;
  size_t post_max;
};

struct dms_stream {
  explicit dms_stream(DmeshReactor::ConnectedTransport connected, size_t max)
      : transport(std::move(connected.transport)),
        abort(std::move(connected.release)),
        post_max(max) {}

  std::unique_ptr<dpumesh::grpc::EndpointTransport> transport;
  std::function<void()> abort;
  std::shared_ptr<CallbackSink> sink;
  const size_t post_max;
};

namespace dpumesh::grpc {

dms_runtime* OpenStreamRuntime(std::unique_ptr<DmeshApiOps> ops,
                               std::string* error) {
  auto runtime = DmeshRuntime::Create(std::move(ops));
  if (!runtime.ok()) {
    if (error != nullptr) *error = runtime.status().ToString();
    errno = ErrnoOf(runtime.status());
    return nullptr;
  }
  const int post_max = (*runtime)->post_max();
  return new dms_runtime{*std::move(runtime), static_cast<size_t>(post_max)};
}

}  // namespace dpumesh::grpc

extern "C" {

int dms_abi_version(void) { return DMS_ABI_VERSION; }

void dms_runtime_close(dms_runtime* rt) { delete rt; }

size_t dms_runtime_post_max(const dms_runtime* rt) { return rt->post_max; }

void dms_connect(dms_runtime* rt, const char* service, dms_connect_fn done,
                 void* ctx) {
  const size_t post_max = rt->post_max;
  rt->runtime->Connect(
      service != nullptr ? service : "",
      [done, ctx, post_max](
          absl::StatusOr<DmeshReactor::ConnectedTransport> result) mutable {
        if (!result.ok()) {
          const std::string message(result.status().message());
          int err = dpumesh::grpc::ErrnoOf(result.status());
          if (err == EPIPE) err = ECONNREFUSED;
          done(ctx, nullptr, err, message.c_str());
          return;
        }
        done(ctx, new dms_stream(*std::move(result), post_max), 0, nullptr);
      });
}

int dms_listen(dms_runtime* rt, dms_accept_fn accept, void* ctx) {
  DmeshReactor::AcceptCallback callback;
  if (accept != nullptr) {
    const size_t post_max = rt->post_max;
    callback = [accept, ctx,
                post_max](DmeshReactor::ConnectedTransport connected) {
      accept(ctx, new dms_stream(std::move(connected), post_max));
    };
  }
  const absl::Status status = rt->runtime->SetAcceptCallback(callback);
  return status.ok() ? 0 : -dpumesh::grpc::ErrnoOf(status);
}

int dms_stream_bind(dms_stream* s, const dms_stream_callbacks* cb, void* ctx) {
  if (s->sink != nullptr) return -EALREADY;
  if (cb == nullptr || cb->recv_alloc == nullptr || cb->recv == nullptr ||
      cb->writable == nullptr || cb->eof == nullptr || cb->error == nullptr) {
    return -EINVAL;
  }
  s->sink = std::make_shared<CallbackSink>(*cb, ctx);
  s->transport->BindSink(s->sink);
  return 0;
}

ssize_t dms_stream_write(dms_stream* s, const void* data, size_t len) {
  if (s->sink == nullptr) return -EINVAL;
  len = std::min(len, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
  const uint8_t* bytes = static_cast<const uint8_t*>(data);
  size_t done = 0;
  while (done < len) {
    const size_t n = std::min(len - done, s->post_max);
    const PostResult result =
        s->transport->Post(n, [bytes, done, n](Reservation reservation) {
          std::memcpy(reservation.data, bytes + done, n);
        });
    switch (result.code) {
      case PostCode::kAccepted:
        done += n;
        break;
      case PostCode::kWouldBlock:
        return done > 0 ? static_cast<ssize_t>(done) : -EAGAIN;
      case PostCode::kClosed:
        return done > 0 ? static_cast<ssize_t>(done) : -EPIPE;
      case PostCode::kError:
        return done > 0 ? static_cast<ssize_t>(done)
                        : -dpumesh::grpc::ErrnoOf(result.status);
    }
  }
  return static_cast<ssize_t>(done);
}

ssize_t dms_stream_post(dms_stream* s, size_t len,
                        void (*fill)(void* ctx, void* dst, size_t len),
                        void* ctx) {
  if (s->sink == nullptr || fill == nullptr) return -EINVAL;
  if (len == 0 || len > s->post_max) return -EMSGSIZE;
  const PostResult result =
      s->transport->Post(len, [fill, ctx, len](Reservation reservation) {
        fill(ctx, reservation.data, len);
      });
  switch (result.code) {
    case PostCode::kAccepted:
      return static_cast<ssize_t>(len);
    case PostCode::kWouldBlock:
      return -EAGAIN;
    case PostCode::kClosed:
      return -EPIPE;
    case PostCode::kError:
      return -dpumesh::grpc::ErrnoOf(result.status);
  }
  return -EIO;
}

void dms_stream_resume(dms_stream* s) { s->transport->ResumeReceive(); }

void dms_stream_close(dms_stream* s) {
  if (s->sink != nullptr) s->sink->Detach();
  s->transport->Close();
  delete s;
}

void dms_stream_abort(dms_stream* s) {
  if (s->sink != nullptr) s->sink->Detach();
  if (s->abort) s->abort();
  delete s;
}

}  // extern "C"
