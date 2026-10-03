// N-API binding of the stream C ABI (integrations/grpc/cpp/include/
// dpumesh_stream.h). The library is opened with dlopen: DPUMESH_STREAM_LIBRARY
// names another build of the same ABI, such as the in-process loopback the
// tests use. Stream events arrive on the runtime's threads and reach
// JavaScript in order through one threadsafe function.
#include <dlfcn.h>
#include <errno.h>
#include <node_api.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

#include "dpumesh_stream.h"

namespace {

// Bytes handed to JavaScript but not yet pushed into its socket, above which a
// stream withholds receive credit even before the socket asks it to.
constexpr size_t kInflightHighWater = 1 << 20;

struct Api {
  int (*abi_version)(void);
  dms_runtime* (*runtime_open)(char*, size_t);
  void (*runtime_close)(dms_runtime*);
  size_t (*runtime_post_max)(const dms_runtime*);
  void (*connect)(dms_runtime*, const char*, dms_connect_fn, void*);
  int (*listen)(dms_runtime*, dms_accept_fn, void*);
  int (*stream_bind)(dms_stream*, const dms_stream_callbacks*, void*);
  ssize_t (*stream_write)(dms_stream*, const void*, size_t);
  void (*stream_resume)(dms_stream*);
  void (*stream_close)(dms_stream*);
  void (*stream_abort)(dms_stream*);
};

enum EventKind {
  kAccept = 1,
  kConnect = 2,
  kData = 3,
  kWritable = 4,
  kEof = 5,
  kError = 6,
  kReleased = 7,
};

struct Event {
  EventKind kind = kAccept;
  uint32_t id = 0;      // stream id; for kConnect, the request id
  uint32_t stream = 0;  // kConnect: the new stream's id, or 0
  void* data = nullptr;
  size_t len = 0;
  int err = 0;
  std::string message;
};

// Freed on the JavaScript thread once its `released` event is dispatched,
// which is also the only thread that looks streams up.
struct Stream {
  uint32_t id = 0;
  std::mutex mu;  // guards handle
  dms_stream* handle = nullptr;
  std::atomic<size_t> inflight{0};
  std::atomic<bool> paused{false};
  std::atomic<bool> held{false};
};

Event* NewEvent(EventKind kind, uint32_t id) {
  auto* event = new Event;
  event->kind = kind;
  event->id = id;
  return event;
}

Api g_api;
bool g_loaded = false;
std::string g_load_error;
dms_runtime* g_rt = nullptr;
napi_threadsafe_function g_tsfn = nullptr;
std::mutex g_streams_mu;
std::unordered_map<uint32_t, Stream*> g_streams;
std::atomic<uint32_t> g_next_id{1};

bool Load() {
  if (g_loaded) return true;
  const char* path = std::getenv("DPUMESH_STREAM_LIBRARY");
  if (path == nullptr || *path == '\0') path = "libdpumesh_stream.so";
  void* lib = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (lib == nullptr) {
    g_load_error = dlerror();
    return false;
  }
#define LOAD(field, name)                                              \
  g_api.field = reinterpret_cast<decltype(g_api.field)>(dlsym(lib, name)); \
  if (g_api.field == nullptr) {                                        \
    g_load_error = std::string("missing ") + name;                     \
    return false;                                                      \
  }
  LOAD(abi_version, "dms_abi_version")
  LOAD(runtime_open, "dms_runtime_open")
  LOAD(runtime_close, "dms_runtime_close")
  LOAD(runtime_post_max, "dms_runtime_post_max")
  LOAD(connect, "dms_connect")
  LOAD(listen, "dms_listen")
  LOAD(stream_bind, "dms_stream_bind")
  LOAD(stream_write, "dms_stream_write")
  LOAD(stream_resume, "dms_stream_resume")
  LOAD(stream_close, "dms_stream_close")
  LOAD(stream_abort, "dms_stream_abort")
#undef LOAD
  if (g_api.abi_version() != DMS_ABI_VERSION) {
    g_load_error = "libdpumesh_stream ABI mismatch";
    return false;
  }
  g_loaded = true;
  return true;
}

void Post(Event* event) {
  if (g_tsfn == nullptr ||
      napi_call_threadsafe_function(g_tsfn, event, napi_tsfn_nonblocking) != napi_ok) {
    std::free(event->data);
    delete event;
  }
}

Stream* Find(uint32_t id) {
  std::lock_guard<std::mutex> lock(g_streams_mu);
  auto found = g_streams.find(id);
  return found == g_streams.end() ? nullptr : found->second;
}

// Stream callbacks, on the reactor thread.
void* OnRecvAlloc(void*, size_t len) { return std::malloc(len); }

int OnRecv(void* ctx, void* buf, size_t len) {
  auto* s = static_cast<Stream*>(ctx);
  const size_t inflight = s->inflight.fetch_add(len) + len;
  Event* event = NewEvent(kData, s->id);
  event->data = buf;
  event->len = len;
  Post(event);
  if (s->paused.load() || inflight > kInflightHighWater) {
    s->held.store(true);
    return DMS_RECV_HOLD;
  }
  return DMS_RECV_OK;
}

void OnWritable(void* ctx) { Post(NewEvent(kWritable, static_cast<Stream*>(ctx)->id)); }
void OnEof(void* ctx) { Post(NewEvent(kEof, static_cast<Stream*>(ctx)->id)); }

void OnError(void* ctx, int err, const char* message) {
  auto* event = NewEvent(kError, static_cast<Stream*>(ctx)->id);
  event->err = err;
  event->message = message != nullptr ? message : "";
  Post(event);
}

void OnReleased(void* ctx) { Post(NewEvent(kReleased, static_cast<Stream*>(ctx)->id)); }

const dms_stream_callbacks kCallbacks = {OnRecvAlloc, OnRecv, OnWritable,
                                         OnEof, OnError, OnReleased};

// Registers and binds a delivered stream. `announce` is posted first, so
// JavaScript learns of the stream before any of its events.
void Adopt(dms_stream* handle, Event* announce) {
  auto* s = new Stream;
  s->id = g_next_id.fetch_add(1);
  s->handle = handle;
  {
    std::lock_guard<std::mutex> lock(g_streams_mu);
    g_streams.emplace(s->id, s);
  }
  if (announce->kind == kConnect) {
    announce->stream = s->id;
  } else {
    announce->id = s->id;
  }
  Post(announce);
  if (g_api.stream_bind(handle, &kCallbacks, s) != 0) {
    // Unbound: no `released` follows, so the stream is dropped here.
    {
      std::lock_guard<std::mutex> lock(g_streams_mu);
      g_streams.erase(s->id);
    }
    g_api.stream_abort(handle);
    Post(NewEvent(kReleased, s->id));
    delete s;
  }
}

void OnAccept(void*, dms_stream* handle) { Adopt(handle, NewEvent(kAccept, 0)); }

void OnConnect(void* ctx, dms_stream* handle, int err, const char* message) {
  const auto request = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(ctx));
  auto* event = NewEvent(kConnect, request);
  if (handle == nullptr) {
    event->err = err;
    event->message = message != nullptr ? message : "";
    Post(event);
    return;
  }
  Adopt(handle, event);
}

// On the JavaScript thread: dispatch(kind, id, value, extra).
void CallJs(napi_env env, napi_value dispatch, void*, void* data) {
  auto* event = static_cast<Event*>(data);
  if (env == nullptr) {
    std::free(event->data);
    delete event;
    return;
  }
  napi_value argv[4];
  napi_create_uint32(env, event->kind, &argv[0]);
  napi_create_uint32(env, event->id, &argv[1]);
  napi_get_undefined(env, &argv[2]);
  napi_get_undefined(env, &argv[3]);
  switch (event->kind) {
    case kData: {
      if (Stream* s = Find(event->id)) s->inflight.fetch_sub(event->len);
      void* bytes = event->data;
      event->data = nullptr;
      napi_status status = napi_create_external_buffer(
          env, event->len, bytes,
          [](napi_env, void* finalize_data, void*) { std::free(finalize_data); },
          nullptr, &argv[2]);
      if (status != napi_ok) {
        void* copy;
        napi_create_buffer_copy(env, event->len, bytes, &copy, &argv[2]);
        std::free(bytes);
      }
      break;
    }
    case kConnect:
      napi_create_uint32(env, event->stream, &argv[2]);
      if (event->stream == 0) {
        napi_create_string_utf8(env, event->message.c_str(), NAPI_AUTO_LENGTH, &argv[3]);
      }
      break;
    case kError:
      napi_create_int32(env, event->err, &argv[2]);
      napi_create_string_utf8(env, event->message.c_str(), NAPI_AUTO_LENGTH, &argv[3]);
      break;
    case kReleased: {
      Stream* s = nullptr;
      {
        std::lock_guard<std::mutex> lock(g_streams_mu);
        auto found = g_streams.find(event->id);
        if (found != g_streams.end()) {
          s = found->second;
          g_streams.erase(found);
        }
      }
      delete s;
      break;
    }
    default:
      break;
  }
  delete event;
  napi_value global;
  napi_get_global(env, &global);
  napi_call_function(env, global, dispatch, 4, argv, nullptr);
}

// ---- JavaScript API ----

napi_value Throw(napi_env env, const std::string& message) {
  napi_throw_error(env, nullptr, message.c_str());
  return nullptr;
}

uint32_t ArgU32(napi_env env, napi_value value) {
  uint32_t out = 0;
  napi_get_value_uint32(env, value, &out);
  return out;
}

// open(dispatch) -> post_max
napi_value Open(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
  if (g_rt != nullptr) return Throw(env, "DPUMesh runtime already open");
  if (!Load()) return Throw(env, "cannot load the DPUMesh stream library: " + g_load_error);
  napi_value name;
  napi_create_string_utf8(env, "dpumesh", NAPI_AUTO_LENGTH, &name);
  if (napi_create_threadsafe_function(env, argv[0], nullptr, name, 0, 1, nullptr,
                                      nullptr, nullptr, CallJs, &g_tsfn) != napi_ok) {
    return Throw(env, "napi_create_threadsafe_function failed");
  }
  // JavaScript refs it while anything that can still receive events is live.
  napi_unref_threadsafe_function(env, g_tsfn);
  char err[256] = {0};
  g_rt = g_api.runtime_open(err, sizeof(err));
  if (g_rt == nullptr) {
    napi_release_threadsafe_function(g_tsfn, napi_tsfn_abort);
    g_tsfn = nullptr;
    return Throw(env, std::string("DPUMesh runtime: ") + err);
  }
  napi_value result;
  napi_create_uint32(env, static_cast<uint32_t>(g_api.runtime_post_max(g_rt)), &result);
  return result;
}

// close(): fails live streams and closes the channel.
napi_value Close(napi_env env, napi_callback_info) {
  if (g_rt != nullptr) {
    g_api.runtime_close(g_rt);
    g_rt = nullptr;
  }
  napi_value undefined;
  napi_get_undefined(env, &undefined);
  return undefined;
}

// listen(enabled)
napi_value Listen(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
  bool enabled = false;
  napi_get_value_bool(env, argv[0], &enabled);
  if (g_rt == nullptr) return Throw(env, "DPUMesh runtime is not open");
  const int rc = g_api.listen(g_rt, enabled ? OnAccept : nullptr, nullptr);
  if (rc != 0) return Throw(env, "dms_listen failed: " + std::string(std::strerror(-rc)));
  napi_value undefined;
  napi_get_undefined(env, &undefined);
  return undefined;
}

// ref(enabled): whether pending events keep the process alive.
napi_value Ref(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
  bool enabled = false;
  napi_get_value_bool(env, argv[0], &enabled);
  if (g_tsfn != nullptr) {
    if (enabled) napi_ref_threadsafe_function(env, g_tsfn);
    else napi_unref_threadsafe_function(env, g_tsfn);
  }
  napi_value undefined;
  napi_get_undefined(env, &undefined);
  return undefined;
}

// connect(service, requestId)
napi_value Connect(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
  char service[512];
  size_t length = 0;
  napi_get_value_string_utf8(env, argv[0], service, sizeof(service), &length);
  const uint32_t request = ArgU32(env, argv[1]);
  if (g_rt == nullptr) return Throw(env, "DPUMesh runtime is not open");
  g_api.connect(g_rt, service, OnConnect,
                reinterpret_cast<void*>(static_cast<uintptr_t>(request)));
  napi_value undefined;
  napi_get_undefined(env, &undefined);
  return undefined;
}

// write(id, buffer, offset) -> bytes accepted, or a negative errno value
napi_value Write(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
  Stream* s = Find(ArgU32(env, argv[0]));
  void* data = nullptr;
  size_t len = 0;
  napi_get_buffer_info(env, argv[1], &data, &len);
  const uint32_t offset = ArgU32(env, argv[2]);
  ssize_t n = -EPIPE;
  if (s != nullptr && offset <= len) {
    std::lock_guard<std::mutex> lock(s->mu);
    if (s->handle != nullptr) {
      n = g_api.stream_write(s->handle, static_cast<char*>(data) + offset, len - offset);
    }
  }
  napi_value result;
  napi_create_int64(env, n, &result);
  return result;
}

// pause(id, paused): a paused socket holds the next receive's credit;
// unpausing returns credit already held.
napi_value Pause(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
  bool paused = false;
  napi_get_value_bool(env, argv[1], &paused);
  if (Stream* s = Find(ArgU32(env, argv[0]))) {
    s->paused.store(paused);
    if (!paused && s->held.exchange(false)) {
      std::lock_guard<std::mutex> lock(s->mu);
      if (s->handle != nullptr) g_api.stream_resume(s->handle);
    }
  }
  napi_value undefined;
  napi_get_undefined(env, &undefined);
  return undefined;
}

// end(id, abort): closes after the written bytes, or resets.
napi_value End(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, nullptr, nullptr);
  bool abort = false;
  napi_get_value_bool(env, argv[1], &abort);
  if (Stream* s = Find(ArgU32(env, argv[0]))) {
    std::lock_guard<std::mutex> lock(s->mu);
    if (s->handle != nullptr) {
      dms_stream* handle = s->handle;
      s->handle = nullptr;
      if (abort) g_api.stream_abort(handle);
      else g_api.stream_close(handle);
    }
  }
  napi_value undefined;
  napi_get_undefined(env, &undefined);
  return undefined;
}

napi_value Init(napi_env env, napi_value exports) {
  const napi_property_descriptor methods[] = {
      {"open", nullptr, Open, nullptr, nullptr, nullptr, napi_default, nullptr},
      {"close", nullptr, Close, nullptr, nullptr, nullptr, napi_default, nullptr},
      {"listen", nullptr, Listen, nullptr, nullptr, nullptr, napi_default, nullptr},
      {"ref", nullptr, Ref, nullptr, nullptr, nullptr, napi_default, nullptr},
      {"connect", nullptr, Connect, nullptr, nullptr, nullptr, napi_default, nullptr},
      {"write", nullptr, Write, nullptr, nullptr, nullptr, napi_default, nullptr},
      {"pause", nullptr, Pause, nullptr, nullptr, nullptr, napi_default, nullptr},
      {"end", nullptr, End, nullptr, nullptr, nullptr, napi_default, nullptr},
  };
  napi_define_properties(env, exports, sizeof(methods) / sizeof(methods[0]), methods);
  return exports;
}

}  // namespace

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
