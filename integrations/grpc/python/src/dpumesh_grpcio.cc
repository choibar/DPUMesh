// gRPC C-core over DPUMesh for grpcio. The C++ adapter's EventEngine endpoint
// (integrations/grpc/cpp/src/dmesh_endpoint.cc) runs over the stream C ABI,
// which is opened with dlopen: grpcio built with this file needs no DPUMesh
// library until a DPUMesh channel or listener is used.
//
// A client channel gets its own EventEngine that delegates everything but
// Connect() to the default engine; Connect() opens a DPUMesh stream, so gRPC
// keeps its own connection management and reconnects. A server takes the
// streams the DPU routes to it through a C-core passive listener.
#include "dpumesh_grpcio.h"

#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <grpc/event_engine/event_engine.h>
#include <grpc/event_engine/internal/memory_allocator_impl.h>
#include <grpc/event_engine/memory_request.h>
#include <grpc/impl/channel_arg_names.h>
#include <grpc/passive_listener.h>
#include <grpc/slice.h>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "dmesh_endpoint.h"
#include "dpumesh_stream.h"
#include "src/core/ext/transport/chttp2/server/chttp2_server.h"
#include "src/core/server/server.h"

namespace dpumesh::grpcio {
namespace {

using dpumesh::grpc::ConnectionSink;
using dpumesh::grpc::DmeshEndpoint;
using dpumesh::grpc::EndpointTransport;
using dpumesh::grpc::Executor;
using dpumesh::grpc::PostResult;
using dpumesh::grpc::ReceiveOutcome;
using dpumesh::grpc::Reservation;
using EventEngine = grpc_event_engine::experimental::EventEngine;
using MemoryAllocator = grpc_event_engine::experimental::MemoryAllocator;

constexpr char kSyntheticTarget[] = "ipv4:127.0.0.1:1";

void CopyError(char* out, size_t out_len, const std::string& message) {
  if (out == nullptr || out_len == 0) return;
  const size_t n = std::min(out_len - 1, message.size());
  std::memcpy(out, message.data(), n);
  out[n] = '\0';
}

// ---- the stream C ABI, loaded on first use ----

struct Api {
  int (*abi_version)(void);
  dms_runtime* (*runtime_open)(char*, size_t);
  void (*runtime_close)(dms_runtime*);
  size_t (*runtime_post_max)(const dms_runtime*);
  void (*connect)(dms_runtime*, const char*, dms_connect_fn, void*);
  int (*listen)(dms_runtime*, dms_accept_fn, void*);
  int (*stream_bind)(dms_stream*, const dms_stream_callbacks*, void*);
  ssize_t (*stream_post)(dms_stream*, size_t, void (*)(void*, void*, size_t), void*);
  void (*stream_resume)(dms_stream*);
  void (*stream_close)(dms_stream*);
  void (*stream_abort)(dms_stream*);
};

Api g_api;
std::mutex g_runtime_mu;
dms_runtime* g_runtime = nullptr;  // guarded by g_runtime_mu
bool g_closed = false;             // guarded by g_runtime_mu
size_t g_post_max = 0;

bool LoadApi(std::string* error) {
  const char* path = std::getenv("DPUMESH_STREAM_LIBRARY");
  if (path == nullptr || *path == '\0') path = "libdpumesh_stream.so";
  void* lib = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (lib == nullptr) {
    *error = std::string("cannot load the DPUMesh stream library: ") + dlerror();
    return false;
  }
#define LOAD(field, name)                                                   \
  g_api.field = reinterpret_cast<decltype(g_api.field)>(dlsym(lib, name)); \
  if (g_api.field == nullptr) {                                             \
    *error = std::string("missing ") + name;                                \
    return false;                                                           \
  }
  LOAD(abi_version, "dms_abi_version")
  LOAD(runtime_open, "dms_runtime_open")
  LOAD(runtime_close, "dms_runtime_close")
  LOAD(runtime_post_max, "dms_runtime_post_max")
  LOAD(connect, "dms_connect")
  LOAD(listen, "dms_listen")
  LOAD(stream_bind, "dms_stream_bind")
  LOAD(stream_post, "dms_stream_post")
  LOAD(stream_resume, "dms_stream_resume")
  LOAD(stream_close, "dms_stream_close")
  LOAD(stream_abort, "dms_stream_abort")
#undef LOAD
  if (g_api.abi_version() != DMS_ABI_VERSION) {
    *error = "libdpumesh_stream ABI mismatch";
    return false;
  }
  return true;
}

dms_runtime* Runtime(std::string* error) {
  std::lock_guard<std::mutex> lock(g_runtime_mu);
  if (g_runtime != nullptr) return g_runtime;
  if (g_closed) {
    *error = "the DPUMesh runtime is closed";
    return nullptr;
  }
  if (g_api.abi_version == nullptr && !LoadApi(error)) return nullptr;
  char err[256] = {0};
  g_runtime = g_api.runtime_open(err, sizeof(err));
  if (g_runtime == nullptr) {
    *error = std::string("DPUMesh runtime: ") + err;
    return nullptr;
  }
  g_post_max = g_api.runtime_post_max(g_runtime);
  return g_runtime;
}

absl::Status ErrnoStatus(const char* operation, int err) {
  const std::string message =
      absl::StrCat(operation, " failed: ", std::strerror(err));
  return err == EPIPE || err == ECONNREFUSED || err == ECONNRESET
             ? absl::UnavailableError(message)
             : absl::InternalError(message);
}

// ---- one stream as the endpoint's transport ----

// A stream's callbacks, which report to the endpoint's driver. Freed by the
// stream's `released` callback.
struct Bridge {
  std::weak_ptr<ConnectionSink> sink;
};

void* BridgeRecvAlloc(void*, size_t len) { return std::malloc(len); }

int BridgeRecv(void* ctx, void* buf, size_t len) {
  auto sink = static_cast<Bridge*>(ctx)->sink.lock();
  if (sink == nullptr) {
    std::free(buf);
    return DMS_RECV_OK;
  }
  // The driver adopts the buffer as a gRPC slice and frees it with the slice.
  const ReceiveOutcome outcome = sink->OnIncomingBuffer(
      static_cast<uint8_t*>(buf), len, std::free, buf);
  return outcome.status.ok() && outcome.hold_credit ? DMS_RECV_HOLD : DMS_RECV_OK;
}

void BridgeWritable(void* ctx) {
  if (auto sink = static_cast<Bridge*>(ctx)->sink.lock()) sink->OnWritable();
}

void BridgeEof(void* ctx) {
  if (auto sink = static_cast<Bridge*>(ctx)->sink.lock()) sink->OnRemoteEof();
}

void BridgeError(void* ctx, int err, const char* message) {
  if (auto sink = static_cast<Bridge*>(ctx)->sink.lock()) {
    sink->OnTransportError(absl::UnavailableError(
        absl::StrCat("DPUMesh stream failed: ", message, " (errno ", err, ")")));
  }
}

void BridgeReleased(void* ctx) { delete static_cast<Bridge*>(ctx); }

const dms_stream_callbacks kBridgeCallbacks = {
    BridgeRecvAlloc, BridgeRecv, BridgeWritable, BridgeEof, BridgeError, BridgeReleased};

class StreamTransport final : public EndpointTransport {
 public:
  explicit StreamTransport(dms_stream* handle) : handle_(handle) {}

  ~StreamTransport() override { Close(); }

  void BindSink(std::weak_ptr<ConnectionSink> sink) override {
    auto* bridge = new Bridge{std::move(sink)};
    std::lock_guard<std::mutex> lock(mu_);
    if (handle_ == nullptr || g_api.stream_bind(handle_, &kBridgeCallbacks, bridge) != 0) {
      delete bridge;
    }
  }

  size_t MaxPostSize() const override { return g_post_max; }

  PostResult Post(size_t length, absl::FunctionRef<void(Reservation)> fill) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (handle_ == nullptr) {
      return PostResult::Closed(absl::UnavailableError("DPUMesh stream is closed"));
    }
    const ssize_t n = g_api.stream_post(
        handle_, length,
        [](void* ctx, void* dst, size_t len) {
          (*static_cast<absl::FunctionRef<void(Reservation)>*>(ctx))(
              Reservation{static_cast<uint8_t*>(dst), len});
        },
        &fill);
    if (n == static_cast<ssize_t>(length)) return PostResult::Accepted();
    if (n == -EAGAIN) return PostResult::WouldBlock();
    if (n == -EPIPE) {
      return PostResult::Closed(absl::UnavailableError("DPUMesh stream is closed"));
    }
    return PostResult::Error(ErrnoStatus("dms_stream_post", static_cast<int>(-n)));
  }

  // Posts are already in the library's custody; its batching owns flushes.
  absl::Status Flush() override {
    std::lock_guard<std::mutex> lock(mu_);
    return handle_ == nullptr ? absl::UnavailableError("DPUMesh stream is closed")
                              : absl::OkStatus();
  }

  void ResumeReceive() override {
    std::lock_guard<std::mutex> lock(mu_);
    if (handle_ != nullptr) g_api.stream_resume(handle_);
  }

  void Close() override { End(false); }

  // Resets the stream instead of closing it after the written bytes.
  void Abort() { End(true); }

 private:
  void End(bool abort) {
    dms_stream* handle;
    {
      std::lock_guard<std::mutex> lock(mu_);
      handle = handle_;
      handle_ = nullptr;
    }
    if (handle == nullptr) return;
    if (abort) g_api.stream_abort(handle);
    else g_api.stream_close(handle);
  }

  std::mutex mu_;
  dms_stream* handle_;
};

// Deferred endpoint completions run on the default EventEngine's threads.
class EngineExecutor final : public Executor {
 public:
  explicit EngineExecutor(std::shared_ptr<EventEngine> engine) : engine_(std::move(engine)) {}
  void Run(absl::AnyInvocable<void()> task) override { engine_->Run(std::move(task)); }

 private:
  const std::shared_ptr<EventEngine> engine_;
};

// Unquota'd receive memory: exact-size malloc slices.
class SliceMallocAllocator final
    : public grpc_event_engine::experimental::internal::MemoryAllocatorImpl {
 public:
  size_t Reserve(grpc_event_engine::experimental::MemoryRequest request) override {
    return request.max();
  }
  grpc_slice MakeSlice(grpc_event_engine::experimental::MemoryRequest request) override {
    return grpc_slice_malloc(request.max());
  }
  void Release(size_t) override {}
  void Shutdown() override {}
};

EventEngine::ResolvedAddress LoopbackAddress() {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  return EventEngine::ResolvedAddress(reinterpret_cast<const sockaddr*>(&address),
                                      sizeof(address));
}

std::unique_ptr<DmeshEndpoint> MakeEndpoint(std::unique_ptr<EndpointTransport> transport,
                                            MemoryAllocator allocator) {
  static const auto executor = std::make_shared<EngineExecutor>(
      grpc_event_engine::experimental::GetDefaultEventEngine());
  return std::make_unique<DmeshEndpoint>(std::move(transport), executor,
                                         std::move(allocator), LoopbackAddress(),
                                         LoopbackAddress());
}

// ---- client: an EventEngine whose Connect() opens DPUMesh streams ----

// Retires a client stream once, from its endpoint's transport or from the
// channel's release, whichever comes first.
class ConnectionLease final {
 public:
  explicit ConnectionLease(StreamTransport* transport) : transport_(transport) {}

  void Release() {
    std::lock_guard<std::mutex> lock(mu_);
    if (transport_ != nullptr) transport_->Abort();
    transport_ = nullptr;
  }

  void Forget() {
    std::lock_guard<std::mutex> lock(mu_);
    transport_ = nullptr;
  }

 private:
  std::mutex mu_;
  StreamTransport* transport_;
};

// The transport a client endpoint owns: dropping the endpoint means gRPC has
// abandoned the connection, so the stream is reset rather than closed.
class LeasedTransport final : public EndpointTransport {
 public:
  LeasedTransport(std::unique_ptr<StreamTransport> stream,
                  std::shared_ptr<ConnectionLease> lease, std::function<void()> on_destroy)
      : stream_(std::move(stream)), lease_(std::move(lease)), on_destroy_(std::move(on_destroy)) {}

  ~LeasedTransport() override {
    lease_->Forget();
    stream_->Abort();
    if (on_destroy_) on_destroy_();
  }

  void BindSink(std::weak_ptr<ConnectionSink> sink) override { stream_->BindSink(std::move(sink)); }
  size_t MaxPostSize() const override { return stream_->MaxPostSize(); }
  PostResult Post(size_t length, absl::FunctionRef<void(Reservation)> fill) override {
    return stream_->Post(length, fill);
  }
  absl::Status Flush() override { return stream_->Flush(); }
  void ResumeReceive() override { stream_->ResumeReceive(); }
  void Close() override { stream_->Abort(); }

 private:
  const std::unique_ptr<StreamTransport> stream_;
  const std::shared_ptr<ConnectionLease> lease_;
  const std::function<void()> on_destroy_;
};

class DmeshClientEventEngine final : public EventEngine {
 public:
  explicit DmeshClientEventEngine(std::string target)
      : target_(std::move(target)),
        delegate_(grpc_event_engine::experimental::GetDefaultEventEngine()) {}

  ~DmeshClientEventEngine() override {
    std::vector<std::shared_ptr<ConnectionLease>> leases;
    std::unordered_map<uint64_t, PendingConnect> pending;
    {
      std::lock_guard<std::mutex> lock(mu_);
      closing_ = true;
      for (auto& entry : active_) {
        if (auto lease = entry.second.lock()) leases.push_back(std::move(lease));
      }
      active_.clear();
      pending.swap(pending_);
    }
    for (auto& lease : leases) lease->Release();
    for (auto& entry : pending) {
      if (entry.second.timer != TaskHandle::kInvalid) (void)delegate_->Cancel(entry.second.timer);
    }
  }

  bool IsWorkerThread() override { return delegate_->IsWorkerThread(); }

  absl::StatusOr<std::unique_ptr<DNSResolver>> GetDNSResolver(
      const DNSResolver::ResolverOptions& options) override {
    return delegate_->GetDNSResolver(options);
  }

  absl::StatusOr<std::unique_ptr<Listener>> CreateListener(
      Listener::AcceptCallback on_accept, absl::AnyInvocable<void(absl::Status)> on_shutdown,
      const grpc_event_engine::experimental::EndpointConfig& config,
      std::unique_ptr<grpc_event_engine::experimental::MemoryAllocatorFactory> factory) override {
    return delegate_->CreateListener(std::move(on_accept), std::move(on_shutdown), config,
                                     std::move(factory));
  }

  ConnectionHandle Connect(OnConnectCallback on_connect, const ResolvedAddress&,
                           const grpc_event_engine::experimental::EndpointConfig&,
                           MemoryAllocator memory_allocator, Duration timeout) override {
    std::string error;
    dms_runtime* rt = Runtime(&error);
    if (rt == nullptr) {
      delegate_->Run([on_connect = std::move(on_connect), error]() mutable {
        on_connect(absl::UnavailableError(error));
      });
      return ConnectionHandle::kInvalid;
    }
    const uint64_t id = next_id_.fetch_add(1, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (closing_) {
        delegate_->Run([on_connect = std::move(on_connect)]() mutable {
          on_connect(absl::CancelledError("DPUMesh channel is being released"));
        });
        return ConnectionHandle::kInvalid;
      }
      pending_.emplace(id, PendingConnect{std::move(on_connect), TaskHandle::kInvalid});
    }
    std::weak_ptr<DmeshClientEventEngine> weak =
        std::static_pointer_cast<DmeshClientEventEngine>(shared_from_this());
    const TaskHandle timer = delegate_->RunAfter(timeout, [weak, id] {
      if (auto self = weak.lock()) self->FinishTimeout(id);
    });
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto found = pending_.find(id);
      if (found != pending_.end()) found->second.timer = timer;
      else if (timer != TaskHandle::kInvalid) (void)delegate_->Cancel(timer);
    }
    auto* context = new ConnectContext{weak, id, std::move(memory_allocator)};
    g_api.connect(rt, target_.c_str(), OnConnected, context);
    return ConnectionHandle{{reinterpret_cast<intptr_t>(this), static_cast<intptr_t>(id)}};
  }

  bool CancelConnect(ConnectionHandle handle) override {
    if (handle.keys[0] != reinterpret_cast<intptr_t>(this) || handle.keys[1] <= 0) return false;
    PendingConnect pending;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto found = pending_.find(static_cast<uint64_t>(handle.keys[1]));
      if (found == pending_.end()) return false;
      pending = std::move(found->second);
      pending_.erase(found);
    }
    if (pending.timer != TaskHandle::kInvalid) (void)delegate_->Cancel(pending.timer);
    pending.on_connect = {};
    return true;
  }

  void Run(Closure* closure) override { delegate_->Run(closure); }
  void Run(absl::AnyInvocable<void()> closure) override { delegate_->Run(std::move(closure)); }
  TaskHandle RunAfter(Duration when, Closure* closure) override {
    return delegate_->RunAfter(when, closure);
  }
  TaskHandle RunAfter(Duration when, absl::AnyInvocable<void()> closure) override {
    return delegate_->RunAfter(when, std::move(closure));
  }
  bool Cancel(TaskHandle handle) override { return delegate_->Cancel(handle); }

 private:
  struct PendingConnect {
    OnConnectCallback on_connect;
    TaskHandle timer = TaskHandle::kInvalid;
  };

  struct ConnectContext {
    std::weak_ptr<DmeshClientEventEngine> engine;
    uint64_t id;
    MemoryAllocator allocator;
  };

  // On the runtime's callback thread.
  static void OnConnected(void* ctx, dms_stream* stream, int err, const char* message) {
    std::unique_ptr<ConnectContext> context(static_cast<ConnectContext*>(ctx));
    auto self = context->engine.lock();
    if (self == nullptr) {
      if (stream != nullptr) g_api.stream_abort(stream);
      return;
    }
    absl::StatusOr<dms_stream*> result(stream);
    if (stream == nullptr) {
      result = absl::UnavailableError(
          absl::StrCat("DPUMesh connect failed: ", message, " (errno ", err, ")"));
    }
    self->FinishConnect(context->id, std::move(context->allocator), result);
  }

  void FinishTimeout(uint64_t id) {
    PendingConnect pending;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto found = pending_.find(id);
      if (found == pending_.end()) return;
      pending = std::move(found->second);
      pending_.erase(found);
    }
    if (pending.on_connect) {
      pending.on_connect(
          absl::DeadlineExceededError("DPUMesh connection attempt exceeded the gRPC deadline"));
    }
  }

  void FinishConnect(uint64_t id, MemoryAllocator allocator, absl::StatusOr<dms_stream*> stream) {
    PendingConnect pending;
    bool abandoned = false;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto found = pending_.find(id);
      if (found == pending_.end() || closing_) {
        abandoned = true;
      } else {
        pending = std::move(found->second);
        pending_.erase(found);
      }
    }
    if (pending.timer != TaskHandle::kInvalid) (void)delegate_->Cancel(pending.timer);
    // A cancel, timeout or release won: no endpoint will own the stream.
    if (abandoned || !pending.on_connect) {
      if (stream.ok()) g_api.stream_abort(*stream);
      return;
    }
    if (!stream.ok()) {
      pending.on_connect(stream.status());
      return;
    }
    auto transport = std::make_unique<StreamTransport>(*stream);
    auto lease = std::make_shared<ConnectionLease>(transport.get());
    {
      std::lock_guard<std::mutex> lock(mu_);
      active_.emplace(id, lease);
    }
    std::weak_ptr<DmeshClientEventEngine> weak =
        std::static_pointer_cast<DmeshClientEventEngine>(shared_from_this());
    auto leased = std::make_unique<LeasedTransport>(std::move(transport), lease, [weak, id] {
      if (auto self = weak.lock()) {
        std::lock_guard<std::mutex> lock(self->mu_);
        self->active_.erase(id);
      }
    });
    pending.on_connect(MakeEndpoint(std::move(leased), std::move(allocator)));
  }

  const std::string target_;
  const std::shared_ptr<EventEngine> delegate_;
  std::mutex mu_;
  std::unordered_map<uint64_t, PendingConnect> pending_;
  std::unordered_map<uint64_t, std::weak_ptr<ConnectionLease>> active_;
  bool closing_ = false;
  std::atomic<uint64_t> next_id_{1};
};

// ---- server: a passive listener fed with accepted streams ----

struct ServerListener {
  std::shared_ptr<grpc_core::experimental::PassiveListenerImpl> passive;
  std::atomic<bool> serving{false};
};

// On the runtime's callback thread.
void OnAccepted(void* ctx, dms_stream* stream) {
  auto* listener = static_cast<ServerListener*>(ctx);
  if (!listener->serving.load()) {
    g_api.stream_abort(stream);
    return;
  }
  auto endpoint = MakeEndpoint(std::make_unique<StreamTransport>(stream),
                               MemoryAllocator(std::make_shared<SliceMallocAllocator>()));
  const absl::Status status = listener->passive->AcceptConnectedEndpoint(std::move(endpoint));
  (void)status;  // a stopped server refuses; the endpoint then closes its stream
}

}  // namespace
}  // namespace dpumesh::grpcio

using namespace dpumesh::grpcio;

extern "C" grpc_channel* dpumesh_channel_create(const char* target,
                                                grpc_channel_credentials* creds,
                                                const grpc_channel_args* args, char* err,
                                                size_t err_len) {
  std::string error;
  if (Runtime(&error) == nullptr) {
    CopyError(err, err_len, error);
    return nullptr;
  }
  std::vector<grpc_arg> merged;
  bool has_authority = false;
  if (args != nullptr) {
    for (size_t i = 0; i < args->num_args; ++i) {
      if (std::strcmp(args->args[i].key, GRPC_ARG_EVENT_ENGINE) == 0) {
        CopyError(err, err_len, "a DPUMesh channel owns GRPC_ARG_EVENT_ENGINE");
        return nullptr;
      }
      has_authority |= std::strcmp(args->args[i].key, GRPC_ARG_DEFAULT_AUTHORITY) == 0;
      merged.push_back(args->args[i]);
    }
  }
  std::shared_ptr<EventEngine> engine = std::make_shared<DmeshClientEventEngine>(target);
  merged.push_back(grpc_channel_arg_pointer_create(
      const_cast<char*>(GRPC_ARG_EVENT_ENGINE), &engine,
      grpc_event_engine::experimental::grpc_event_engine_arg_vtable()));
  if (!has_authority) {
    merged.push_back(grpc_channel_arg_string_create(const_cast<char*>(GRPC_ARG_DEFAULT_AUTHORITY),
                                                    const_cast<char*>(target)));
  }
  const grpc_channel_args channel_args{merged.size(), merged.data()};
  return grpc_channel_create(kSyntheticTarget, creds, &channel_args);
}

extern "C" void* dpumesh_server_add_listener(grpc_server* server, char* err, size_t err_len) {
  auto listener = std::make_unique<ServerListener>();
  listener->passive = std::make_shared<grpc_core::experimental::PassiveListenerImpl>();
  grpc_server_credentials* creds = grpc_insecure_server_credentials_create();
  const absl::Status status =
      grpc_server_add_passive_listener(grpc_core::Server::FromC(server), creds, listener->passive);
  grpc_server_credentials_release(creds);
  if (!status.ok()) {
    CopyError(err, err_len, status.ToString());
    return nullptr;
  }
  // Kept for the process's life: an accept already queued may still name it.
  return listener.release();
}

extern "C" int dpumesh_server_serve(void* handle, char* err, size_t err_len) {
  std::string error;
  dms_runtime* rt = Runtime(&error);
  if (rt == nullptr) {
    CopyError(err, err_len, error);
    return -1;
  }
  auto* listener = static_cast<ServerListener*>(handle);
  listener->serving.store(true);
  const int rc = g_api.listen(rt, OnAccepted, listener);
  if (rc != 0) {
    listener->serving.store(false);
    CopyError(err, err_len, std::string("dms_listen failed: ") + std::strerror(-rc));
    return -1;
  }
  return 0;
}

extern "C" void dpumesh_server_stop(void* handle) {
  auto* listener = static_cast<ServerListener*>(handle);
  listener->serving.store(false);
  std::lock_guard<std::mutex> lock(g_runtime_mu);
  if (g_runtime != nullptr) g_api.listen(g_runtime, nullptr, nullptr);
}

extern "C" void dpumesh_close(void) {
  std::lock_guard<std::mutex> lock(g_runtime_mu);
  g_closed = true;
  if (g_runtime == nullptr) return;
  g_api.runtime_close(g_runtime);
  g_runtime = nullptr;
}
