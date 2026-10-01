// channel_bench: the C++ peer of integrations/grpc/go/cmd/channel-bench. It
// speaks the same wire (/dmesh.ChannelBench/Echo, a 64-byte raw request echoed
// unchanged, bytes 0-7 the little-endian sequence number and 8-15 the worker
// ID), takes the same flags and prints the same JSON lines, over the DPUMesh
// C++ adapter or, with -tcp host:port, kernel TCP. -start-file is not
// supported. With -tcp, native_dials is empty: gRPC C++ exposes no dial
// count, so a TCP reconnect is not detected.
#include <signal.h>
#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/generic/callback_generic_service.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/passive_listener.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/slice.h>

#include "dmesh_api_ops.h"
#include "dmesh_grpc_runtime.h"
#include "dmesh_runtime.h"

namespace {

using Clock = std::chrono::steady_clock;
using Wall = std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::nanoseconds;

constexpr char kMethod[] = "/dmesh.ChannelBench/Echo";
constexpr size_t kPayloadBytes = 64;

// ---- native accounting ----------------------------------------------------

// Counts native dials and records failed QP/channel teardown, like the Go
// fixture's dial counter and close tracker.
struct NativeCounters {
  std::atomic<int64_t> dials{0};
  std::atomic<int64_t> closes{0};
  std::mutex mu;
  std::string close_errors;

  void Failed(const char* what, int rc) {
    std::lock_guard<std::mutex> lock(mu);
    if (!close_errors.empty()) close_errors += "; ";
    close_errors += std::string(what) + ": " + std::strerror(rc < 0 ? -rc : rc);
  }
  std::string Errors() {
    std::lock_guard<std::mutex> lock(mu);
    return close_errors;
  }
};

class CountingOps final : public dpumesh::grpc::DmeshApiOps {
 public:
  CountingOps(std::unique_ptr<DmeshApiOps> inner,
              std::shared_ptr<NativeCounters> counters)
      : inner_(std::move(inner)), counters_(std::move(counters)) {}

  dmesh_channel_t* CreateChannel() override { return inner_->CreateChannel(); }
  int DestroyChannel(dmesh_channel_t* channel) override {
    const int rc = inner_->DestroyChannel(channel);
    if (rc != 0) counters_->Failed("destroy channel", rc);
    return rc;
  }
  dmesh_eq_t* CreateEq(dmesh_channel_t* channel) override {
    return inner_->CreateEq(channel);
  }
  int DestroyEq(dmesh_eq_t* eq) override { return inner_->DestroyEq(eq); }
  int EqFd(dmesh_eq_t* eq) override { return inner_->EqFd(eq); }
  dmesh_qp_t* CreateQp(dmesh_eq_t* eq, const char* service) override {
    dmesh_qp_t* qp = inner_->CreateQp(eq, service);
    if (qp != nullptr) counters_->dials.fetch_add(1);
    return qp;
  }
  int DestroyQp(dmesh_qp_t* qp) override {
    const int rc = inner_->DestroyQp(qp);
    counters_->closes.fetch_add(1);
    if (rc != 0) counters_->Failed("destroy QP", rc);
    return rc;
  }
  int AbortQp(dmesh_qp_t* qp) override {
    const int rc = inner_->AbortQp(qp);
    counters_->closes.fetch_add(1);
    if (rc != 0) counters_->Failed("abort QP", rc);
    return rc;
  }
  void* Alloc(dmesh_qp_t* qp, uint32_t len) override {
    return inner_->Alloc(qp, len);
  }
  int PostSend(dmesh_qp_t* qp, const void* buffer, uint32_t len) override {
    return inner_->PostSend(qp, buffer, len);
  }
  int Flush(dmesh_qp_t* qp) override { return inner_->Flush(qp); }
  int PollEq(dmesh_eq_t* eq, dmesh_event_t* events, int max_events) override {
    return inner_->PollEq(eq, events, max_events);
  }
  void ReleaseRxBuffer(dmesh_channel_t* channel,
                       dmesh_event_t* event) override {
    inner_->ReleaseRxBuffer(channel, event);
  }
  int PostMax(dmesh_channel_t* channel) override {
    return inner_->PostMax(channel);
  }
  int PodId(dmesh_channel_t* channel) override { return inner_->PodId(channel); }
  int64_t EqNextDeadlineNs(dmesh_eq_t* eq) override {
    return inner_->EqNextDeadlineNs(eq);
  }

 private:
  std::unique_ptr<DmeshApiOps> inner_;
  std::shared_ptr<NativeCounters> counters_;
};

std::shared_ptr<dpumesh::grpc::DmeshRuntime> CreateRuntime(
    size_t reactors, const std::shared_ptr<NativeCounters>& counters,
    std::string* error) {
  dpumesh::grpc::DmeshRuntime::Options options;
  options.reactor_count = reactors;
  auto runtime = dpumesh::grpc::DmeshRuntime::Create(
      std::make_unique<CountingOps>(dpumesh::grpc::MakeNativeDmeshApiOps(),
                                    counters),
      options);
  if (!runtime.ok()) {
    *error = "runtime: " + runtime.status().ToString();
    return nullptr;
  }
  return std::move(*runtime);
}

// ---- flags ----------------------------------------------------------------

struct Config {
  std::string mode = "client";
  int connections = 1;
  int concurrency = 64;
  nanoseconds warmup = std::chrono::seconds(3);
  nanoseconds duration = std::chrono::seconds(10);
  nanoseconds rpc_timeout = std::chrono::seconds(5);
  nanoseconds timeout = std::chrono::seconds(90);
  std::string tcp;
  size_t reactors = 1;
};

// Go time.ParseDuration for the forms the harness uses: a decimal number and
// one unit of ns, us, ms, s, m or h.
bool ParseDuration(const std::string& text, nanoseconds* out) {
  static const std::map<std::string, double> kUnits = {
      {"ns", 1}, {"us", 1e3}, {"ms", 1e6}, {"s", 1e9}, {"m", 60e9}, {"h", 3600e9}};
  size_t unit = text.find_first_not_of("0123456789.");
  if (unit == 0 || unit == std::string::npos) return false;
  auto it = kUnits.find(text.substr(unit));
  if (it == kUnits.end()) return false;
  char* end = nullptr;
  const std::string number = text.substr(0, unit);
  const double value = std::strtod(number.c_str(), &end);
  if (end == nullptr || *end != '\0') return false;
  *out = nanoseconds(static_cast<int64_t>(value * it->second));
  return true;
}

bool ParseInt(const std::string& text, long long* out) {
  char* end = nullptr;
  *out = std::strtoll(text.c_str(), &end, 10);
  return !text.empty() && end != nullptr && *end == '\0';
}

// Accepts -name value, -name=value and the -- forms, as Go's flag package does.
bool ParseFlags(int argc, char** argv, Config* c, std::string* error) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--", 0) == 0) arg.erase(0, 1);
    if (arg.size() < 2 || arg[0] != '-') {
      *error = "unexpected argument " + arg;
      return false;
    }
    std::string name = arg.substr(1), value;
    const size_t eq = name.find('=');
    if (eq != std::string::npos) {
      value = name.substr(eq + 1);
      name.resize(eq);
    } else if (i + 1 < argc) {
      value = argv[++i];
    } else {
      *error = "flag -" + name + " needs a value";
      return false;
    }
    long long n = 0;
    bool ok = true;
    if (name == "mode") {
      c->mode = value;
    } else if (name == "tcp") {
      c->tcp = value;
    } else if (name == "connections") {
      ok = ParseInt(value, &n) && (c->connections = static_cast<int>(n), true);
    } else if (name == "concurrency") {
      ok = ParseInt(value, &n) && (c->concurrency = static_cast<int>(n), true);
    } else if (name == "reactors") {
      ok = ParseInt(value, &n) && n > 0 && (c->reactors = static_cast<size_t>(n), true);
    } else if (name == "warmup") {
      ok = ParseDuration(value, &c->warmup);
    } else if (name == "duration") {
      ok = ParseDuration(value, &c->duration);
    } else if (name == "rpc-timeout") {
      ok = ParseDuration(value, &c->rpc_timeout);
    } else if (name == "timeout") {
      ok = ParseDuration(value, &c->timeout);
    } else {
      *error = "unknown flag -" + name;
      return false;
    }
    if (!ok) {
      *error = "invalid value for -" + name + ": " + value;
      return false;
    }
  }
  return true;
}

// ---- output ---------------------------------------------------------------

// RFC 3339 with nanoseconds and trailing zeros trimmed, as Go marshals a
// time.Time.
std::string Rfc3339(Wall::time_point t) {
  const auto ns = duration_cast<nanoseconds>(t.time_since_epoch()).count();
  const time_t secs = static_cast<time_t>(ns / 1000000000);
  long frac = static_cast<long>(ns % 1000000000);
  struct tm tm;
  gmtime_r(&secs, &tm);
  char base[32];
  std::strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &tm);
  std::string out = base;
  if (frac != 0) {
    char digits[16];
    std::snprintf(digits, sizeof(digits), "%09ld", frac);
    std::string f = digits;
    f.erase(f.find_last_not_of('0') + 1);
    out += "." + f;
  }
  return out + "Z";
}

// Wall-clock time of a steady-clock instant, for the markers.
Wall::time_point ToWall(Clock::time_point t) {
  return Wall::now() + duration_cast<Wall::duration>(t - Clock::now());
}

std::string JsonString(const std::string& s) {
  std::string out = "\"";
  for (char ch : s) {
    switch (ch) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\t': out += "\\t"; break;
      default:
        if (static_cast<unsigned char>(ch) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", ch);
          out += buf;
        } else {
          out += ch;
        }
    }
  }
  return out + "\"";
}

template <typename T>
std::string JsonArray(const std::vector<T>& values) {
  std::string out = "[";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i) out += ",";
    out += std::to_string(values[i]);
  }
  return out + "]";
}

void Marker(const char* event, Clock::time_point at) {
  std::printf("{\"event\":\"%s\",\"timestamp\":\"%s\",\"emitted_at\":\"%s\"}\n",
              event, Rfc3339(ToWall(at)).c_str(), Rfc3339(Wall::now()).c_str());
  std::fflush(stdout);
}

double ProcessCpuSeconds() {
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  return usage.ru_utime.tv_sec + usage.ru_stime.tv_sec +
         (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) / 1e6;
}

// ---- gRPC helpers ---------------------------------------------------------

grpc::ByteBuffer ToBuffer(const std::string& bytes) {
  grpc::Slice slice(bytes);
  return grpc::ByteBuffer(&slice, 1);
}

bool Flatten(const grpc::ByteBuffer& buffer, std::string* out) {
  std::vector<grpc::Slice> slices;
  if (!buffer.Dump(&slices).ok()) return false;
  out->clear();
  for (const auto& s : slices) {
    out->append(reinterpret_cast<const char*>(s.begin()), s.size());
  }
  return true;
}

void PutLE64(std::string* payload, size_t offset, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    (*payload)[offset + i] = static_cast<char>((v >> (8 * i)) & 0xff);
  }
}

// ---- server ---------------------------------------------------------------

class EchoReactor final : public grpc::ServerGenericBidiReactor {
 public:
  explicit EchoReactor(grpc::GenericCallbackServerContext* context) {
    if (context->method() != kMethod) {
      Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "unknown method"));
      return;
    }
    StartRead(&request_);
  }
  void OnReadDone(bool ok) override {
    if (!ok) {
      Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "no request"));
      return;
    }
    StartWriteAndFinish(&request_, grpc::WriteOptions(), grpc::Status::OK);
  }
  void OnDone() override { delete this; }

 private:
  grpc::ByteBuffer request_;
};

class EchoService final : public grpc::CallbackGenericService {
  grpc::ServerGenericBidiReactor* CreateReactor(
      grpc::GenericCallbackServerContext* context) override {
    return new EchoReactor(context);
  }
};

int RunServer(const Config& c, const sigset_t& stop_signals) {
  auto counters = std::make_shared<NativeCounters>();
  std::string error;
  std::shared_ptr<dpumesh::grpc::DmeshRuntime> runtime;
  if (c.tcp.empty()) {
    runtime = CreateRuntime(c.reactors, counters, &error);
    if (!runtime) {
      std::fprintf(stderr, "%s\n", error.c_str());
      return 1;
    }
  }
  EchoService service;
  grpc::ServerBuilder builder;
  builder.RegisterCallbackGenericService(&service);
  std::unique_ptr<grpc::experimental::PassiveListener> listener;
  if (c.tcp.empty()) {
    builder.experimental().AddPassiveListener(grpc::InsecureServerCredentials(),
                                              listener);
  } else {
    builder.AddListeningPort(c.tcp, grpc::InsecureServerCredentials());
  }
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (server == nullptr) {
    std::fprintf(stderr, "server did not start\n");
    return 1;
  }
  std::unique_ptr<dpumesh::grpc::DmeshGrpcServerAttachment> attachment;
  if (c.tcp.empty()) {
    auto attached = dpumesh::grpc::AttachDmeshGrpcServer(
        runtime, listener.get(), {}, [](const absl::Status& status) {
          std::fprintf(stderr, "accept: %s\n", status.ToString().c_str());
        });
    if (!attached.ok()) {
      std::fprintf(stderr, "attach: %s\n", attached.status().ToString().c_str());
      server->Shutdown();
      return 1;
    }
    attachment = std::move(*attached);
  }
  const char* service_name = std::getenv("DPUMESH_SERVICE");
  std::fprintf(stderr, "CHANNEL_BENCH_SERVER_READY service=%s\n",
               c.tcp.empty() ? (service_name ? service_name : "") : c.tcp.c_str());

  int sig = 0;
  sigwait(&stop_signals, &sig);
  if (attachment) attachment->Detach();
  server->Shutdown(Wall::now() + std::chrono::seconds(5));
  server->Wait();
  server.reset();
  attachment.reset();
  listener.reset();
  runtime.reset();
  const std::string close_errors = counters->Errors();
  if (!close_errors.empty()) {
    std::fprintf(stderr, "close: %s\n", close_errors.c_str());
    return 1;
  }
  std::fprintf(stderr, "CHANNEL_BENCH_SERVER_CLOSED\n");
  return 0;
}

// ---- client ---------------------------------------------------------------

struct Result {
  bool ok = false;
  std::vector<int> per_conn;
  double elapsed = 0, cpu_pct = 0, cpu_seconds = 0;
  Clock::time_point start, end;
  uint64_t completed = 0, total_completed = 0, rpc_errors = 0;
  std::vector<int64_t> dials;
  int64_t reconnects = 0;
  double qps = 0, mean_us = 0, p50_us = 0, p99_us = 0;
  std::string error;
};

void AddError(std::string* error, const std::string& more) {
  if (more.empty()) return;
  if (!error->empty()) *error += "\n";
  *error += more;
}

void EmitResult(const Config& c, const Result& r) {
  std::string out = "{\"event\":\"result\",\"ok\":";
  out += r.ok ? "true" : "false";
  out += ",\"connections\":" + std::to_string(c.connections);
  out += ",\"concurrency\":" + std::to_string(c.concurrency);
  out += ",\"concurrency_per_conn\":" + JsonArray(r.per_conn);
  out += ",\"payload_bytes\":" + std::to_string(kPayloadBytes);
  char buf[512];
  std::snprintf(buf, sizeof(buf),
                ",\"warmup_seconds\":%g,\"duration_seconds\":%g,\"elapsed_secs\":%g,"
                "\"client_process_cpu_pct\":%.6f,\"client_cpu_sample_seconds\":%.6f",
                std::chrono::duration<double>(c.warmup).count(),
                std::chrono::duration<double>(c.duration).count(), r.elapsed,
                r.cpu_pct, r.cpu_seconds);
  out += buf;
  out += ",\"measurement_start\":\"" + Rfc3339(ToWall(r.start)) + "\"";
  out += ",\"measurement_end\":\"" + Rfc3339(ToWall(r.end)) + "\"";
  out += ",\"completed\":" + std::to_string(r.completed);
  out += ",\"total_completed_including_warmup_and_drain\":" +
         std::to_string(r.total_completed);
  out += ",\"rpc_errors\":" + std::to_string(r.rpc_errors);
  out += ",\"native_dials\":" + JsonArray(r.dials);
  out += ",\"reconnects\":" + std::to_string(r.reconnects);
  std::snprintf(buf, sizeof(buf),
                ",\"qps\":%.1f,\"latency_mean_us\":%.3f,\"latency_p50_us\":%.3f,"
                "\"latency_p99_us\":%.3f",
                r.qps, r.mean_us, r.p50_us, r.p99_us);
  out += buf;
  if (!r.error.empty()) out += ",\"error\":" + JsonString(r.error);
  out += "}\n";
  std::fputs(out.c_str(), stdout);
  std::fflush(stdout);
}

struct Peer {
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<grpc::GenericStub> stub;
};

// One closed RPC loop. Calls alternate between two slots so the context and
// buffers of the call whose completion is running stay alive while the next
// call starts from inside that completion.
struct Worker {
  struct Slot {
    std::unique_ptr<grpc::ClientContext> context;
    grpc::ByteBuffer request, response;
  };
  int id = 0;
  grpc::GenericStub* stub = nullptr;
  std::string payload;
  uint64_t sequence = 0;
  Slot slots[2];
  Clock::time_point started;
  std::vector<int64_t> latencies_ns;
  uint64_t completed = 0, errors = 0;
  std::string error;
  std::string echoed;
};

class Load {
 public:
  Load(const Config& c, std::shared_ptr<NativeCounters> counters, int64_t dials)
      : c_(c), counters_(std::move(counters)), expected_dials_(dials) {}

  void Start(std::vector<std::unique_ptr<Worker>>* workers, Clock::time_point start,
             Clock::time_point end) {
    start_ = start;
    end_ = end;
    running_ = static_cast<int>(workers->size());
    for (auto& w : *workers) Issue(w.get());
  }

  // Waits for every worker to stop; false on the overall deadline.
  bool Wait(Clock::time_point deadline) {
    std::unique_lock<std::mutex> lock(mu_);
    return done_cv_.wait_until(lock, deadline, [this] { return running_ == 0; });
  }

  void Cancel() { cancel_.store(true); }

 private:
  void Issue(Worker* w) {
    if (cancel_.load() || Clock::now() >= end_) {
      Finish();
      return;
    }
    Worker::Slot& slot = w->slots[w->sequence & 1];
    ++w->sequence;
    PutLE64(&w->payload, 0, w->sequence);
    slot.context = std::make_unique<grpc::ClientContext>();
    slot.context->set_deadline(Wall::now() + c_.rpc_timeout);
    slot.request = ToBuffer(w->payload);
    slot.response.Clear();
    w->started = Clock::now();
    w->stub->UnaryCall(slot.context.get(), kMethod, grpc::StubOptions(),
                       &slot.request, &slot.response,
                       [this, w, &slot](grpc::Status status) {
                         Complete(w, slot, status);
                       });
  }

  void Complete(Worker* w, Worker::Slot& slot, const grpc::Status& status) {
    const Clock::time_point completed = Clock::now();
    std::string error;
    if (!status.ok()) {
      error = "rpc error: code = " + std::to_string(status.error_code()) +
              " desc = " + status.error_message();
    } else if (!Flatten(slot.response, &w->echoed) || w->echoed != w->payload) {
      error = "payload mismatch: " + std::to_string(kPayloadBytes) +
              " bytes sent, " + std::to_string(w->echoed.size()) + " received";
    } else if (expected_dials_ > 0 && counters_->dials.load() != expected_dials_) {
      error = "unexpected reconnect: " + std::to_string(counters_->dials.load()) +
              " native dials for " + std::to_string(expected_dials_) + " connections";
    }
    if (!error.empty()) {
      w->errors++;
      w->error = "worker " + std::to_string(w->id) + " request " +
                 std::to_string(w->sequence) + ": " + error;
      cancel_.store(true);
      Finish();
      return;
    }
    w->completed++;
    if (completed >= start_ && completed < end_) {
      w->latencies_ns.push_back(duration_cast<nanoseconds>(completed - w->started).count());
    }
    Issue(w);
  }

  void Finish() {
    std::lock_guard<std::mutex> lock(mu_);
    if (--running_ == 0) done_cv_.notify_all();
  }

  const Config& c_;
  std::shared_ptr<NativeCounters> counters_;
  const int64_t expected_dials_;
  Clock::time_point start_, end_;
  std::atomic<bool> cancel_{false};
  std::mutex mu_;
  std::condition_variable done_cv_;
  int running_ = 0;
};

// A blocking verified call, for the per-connection preflight.
std::string Preflight(grpc::GenericStub* stub, const std::string& payload,
                      nanoseconds limit) {
  grpc::ClientContext context;
  context.set_deadline(Wall::now() + limit);
  grpc::ByteBuffer request = ToBuffer(payload), response;
  std::promise<grpc::Status> done;
  stub->UnaryCall(&context, kMethod, grpc::StubOptions(), &request, &response,
                  [&done](grpc::Status status) { done.set_value(status); });
  const grpc::Status status = done.get_future().get();
  if (!status.ok()) return "rpc error: " + status.error_message();
  std::string echoed;
  if (!Flatten(response, &echoed) || echoed != payload) return "payload mismatch";
  return "";
}

std::vector<int> Distribute(int total, int connections) {
  std::vector<int> d(connections);
  for (int i = 0; i < connections; ++i) {
    d[i] = total / connections + (i < total % connections ? 1 : 0);
  }
  return d;
}

int RunClient(const Config& c, const std::string& target) {
  Result r;
  r.per_conn = Distribute(c.concurrency, c.connections);
  const Clock::time_point overall = Clock::now() + c.timeout;
  auto counters = std::make_shared<NativeCounters>();
  std::shared_ptr<dpumesh::grpc::DmeshRuntime> runtime;
  if (c.tcp.empty()) {
    runtime = CreateRuntime(c.reactors, counters, &r.error);
  }
  std::vector<Peer> peers;
  std::vector<std::unique_ptr<Worker>> workers;
  std::unique_ptr<Load> load;

  // Preflight every connection, one native dial each.
  for (int i = 0; r.error.empty() && i < c.connections; ++i) {
    grpc::ChannelArguments args;
    // A separate subchannel per channel, so each opens its own connection.
    args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    args.SetInt("dmesh.channel_bench.connection", i);
    args.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
    Peer peer;
    if (c.tcp.empty()) {
      auto channel = dpumesh::grpc::CreateDmeshChannel(
          runtime, target, grpc::InsecureChannelCredentials(), args);
      if (!channel.ok()) {
        r.error = "channel: " + channel.status().ToString();
        break;
      }
      peer.channel = std::move(*channel);
    } else {
      peer.channel = grpc::CreateCustomChannel(
          c.tcp, grpc::InsecureChannelCredentials(), args);
    }
    peer.stub = std::make_unique<grpc::GenericStub>(peer.channel);
    const int64_t before = counters->dials.load();
    const std::string err =
        Preflight(peer.stub.get(), std::string(kPayloadBytes, static_cast<char>(i + 1)),
                  c.rpc_timeout);
    if (!err.empty()) {
      r.rpc_errors++;
      r.error = "connection " + std::to_string(i + 1) + " preflight: " + err;
    }
    if (c.tcp.empty()) r.dials.push_back(counters->dials.load() - before);
    peers.push_back(std::move(peer));
  }

  if (r.error.empty()) {
    std::fprintf(stderr, "PREFLIGHT_OK connections=%d concurrency=%s payload=%zuB\n",
                 c.connections, JsonArray(r.per_conn).c_str(), kPayloadBytes);
    int id = 0;
    for (int conn = 0; conn < c.connections; ++conn) {
      for (int j = 0; j < r.per_conn[conn]; ++j, ++id) {
        auto w = std::make_unique<Worker>();
        w->id = id;
        w->stub = peers[conn].stub.get();
        w->payload.assign(kPayloadBytes, static_cast<char>(id + 1));
        PutLE64(&w->payload, 8, static_cast<uint64_t>(id));
        w->latencies_ns.reserve(16384);
        workers.push_back(std::move(w));
      }
    }
    load = std::make_unique<Load>(c, counters, c.tcp.empty() ? c.connections : 0);
    const Clock::time_point load_start = Clock::now();
    r.start = load_start + duration_cast<Clock::duration>(c.warmup);
    r.end = r.start + duration_cast<Clock::duration>(c.duration);
    load->Start(&workers, r.start, r.end);

    std::this_thread::sleep_until(r.start);
    const double cpu_start = ProcessCpuSeconds();
    const Clock::time_point cpu_start_at = Clock::now();
    Marker("measure_start", r.start);
    std::this_thread::sleep_until(r.end);
    const double cpu_end = ProcessCpuSeconds();
    r.cpu_seconds = std::chrono::duration<double>(Clock::now() - cpu_start_at).count();
    if (r.cpu_seconds > 0) r.cpu_pct = 100 * (cpu_end - cpu_start) / r.cpu_seconds;
    Marker("measure_end", r.end);
    if (!load->Wait(overall)) {
      load->Cancel();
      AddError(&r.error, "workers still running at the overall deadline");
      // Calls end within -rpc-timeout once cancelled.
      if (!load->Wait(Clock::now() + c.rpc_timeout + std::chrono::seconds(5))) {
        // Calls still reference the workers: report and exit without teardown.
        r.ok = false;
        EmitResult(c, r);
        std::fprintf(stderr, "%s\n", r.error.c_str());
        std::_Exit(1);
      }
    }

    std::vector<int64_t> latencies;
    for (auto& w : workers) {
      r.total_completed += w->completed;
      r.rpc_errors += w->errors;
      latencies.insert(latencies.end(), w->latencies_ns.begin(), w->latencies_ns.end());
      AddError(&r.error, w->error);
    }
    r.completed = latencies.size();
    r.elapsed = std::chrono::duration<double>(r.end - r.start).count();
    r.qps = r.completed / r.elapsed;
    if (!latencies.empty()) {
      std::sort(latencies.begin(), latencies.end());
      double sum = 0;
      for (int64_t l : latencies) sum += static_cast<double>(l);
      auto pct = [&](double p) {
        return latencies[static_cast<size_t>(std::ceil(p * latencies.size())) - 1] / 1e3;
      };
      r.mean_us = sum / latencies.size() / 1e3;
      r.p50_us = pct(0.5);
      r.p99_us = pct(0.99);
    } else {
      AddError(&r.error, "no successful RPC completions in measurement window");
    }
  }

  // Close every connection and the native channel before reporting.
  workers.clear();
  load.reset();
  peers.clear();
  if (runtime) {
    const Clock::time_point closes_due = Clock::now() + std::chrono::seconds(30);
    while (counters->closes.load() < counters->dials.load() && Clock::now() < closes_due) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (counters->closes.load() < counters->dials.load()) {
      AddError(&r.error, std::to_string(counters->dials.load() - counters->closes.load()) +
                             " native closes pending");
    }
    runtime.reset();
    AddError(&r.error, counters->Errors());
    for (int64_t d : r.dials) {
      if (d != 1) AddError(&r.error, "expected one native dial per connection, got " +
                                         std::to_string(d));
    }
    r.reconnects = std::max<int64_t>(0, counters->dials.load() - c.connections);
    if (r.reconnects > 0) {
      AddError(&r.error, "unexpected reconnect: " + std::to_string(counters->dials.load()) +
                             " native dials for " + std::to_string(c.connections) +
                             " connections");
    }
  }
  r.ok = r.error.empty();
  EmitResult(c, r);
  if (!r.ok) std::fprintf(stderr, "%s\n", r.error.c_str());
  return r.ok ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  Config c;
  std::string error;
  if (!ParseFlags(argc, argv, &c, &error)) {
    std::fprintf(stderr, "%s\n", error.c_str());
    return 2;
  }
  const char* ip = std::getenv("DPUMESH_SERVICE_IP");
  const char* port = std::getenv("DPUMESH_SERVICE_PORT");
  long long port_number = 0;
  const bool have_target = ip && *ip && port && ParseInt(port, &port_number) &&
                           port_number >= 1 && port_number <= 65535;
  if ((c.mode == "client" && c.tcp.empty() && !have_target) || c.connections < 1 ||
      c.connections > 4 || c.concurrency < c.connections || c.warmup.count() < 0 ||
      c.duration.count() <= 0 || c.rpc_timeout.count() <= 0 || c.timeout.count() <= 0) {
    std::fprintf(stderr,
                 "set DPUMESH_SERVICE_IP/PORT for the client, connections 1..4, "
                 "concurrency >= connections, and valid durations\n");
    return 2;
  }
  if (c.mode == "server") {
    // Block the stop signals before gRPC starts threads; the server waits
    // for them with sigwait.
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);
    return RunServer(c, stop_signals);
  }
  if (c.mode == "client") {
    return RunClient(c, have_target ? std::string(ip) + ":" + port : std::string());
  }
  std::fprintf(stderr, "unknown mode %s\n", c.mode.c_str());
  return 2;
}
