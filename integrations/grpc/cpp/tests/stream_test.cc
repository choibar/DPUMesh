// The stream C ABI over the reactor and fake native operations.
#include <errno.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "dmesh_stream_internal.h"
#include "dpumesh_stream.h"
#include "fake_dmesh_ops.h"

namespace dpumesh::grpc::testing {
namespace {

using namespace std::chrono_literals;

struct TestFailure {
  std::string message;
};

#define CHECK_TRUE(condition)                                               \
  do {                                                                      \
    if (!(condition)) {                                                     \
      throw TestFailure{std::string("check failed: ") + #condition +       \
                        " at line " + std::to_string(__LINE__)};            \
    }                                                                       \
  } while (false)

#define CHECK_EQ(left, right)                                               \
  do {                                                                      \
    const auto& check_left = (left);                                        \
    const auto& check_right = (right);                                      \
    if (!(check_left == check_right)) {                                     \
      throw TestFailure{std::string("check failed: ") + #left + " == " +  \
                        #right + " at line " + std::to_string(__LINE__)};   \
    }                                                                       \
  } while (false)

bool WaitFor(const std::function<bool()>& done,
             std::chrono::milliseconds timeout = 2s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!done()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(100us);
  }
  return true;
}

// Records one stream's callbacks, as a binding would.
struct Recorder {
  std::mutex mu;
  std::string received;
  std::atomic<int> writable{0};
  std::atomic<int> eof{0};
  std::atomic<int> errors{0};
  std::atomic<int> last_error{0};
  std::atomic<int> released{0};
  std::atomic<int> calls_after_release{0};
  std::atomic<bool> hold{false};

  std::string Received() {
    std::lock_guard<std::mutex> lock(mu);
    return received;
  }

  void Touch() {
    if (released.load() != 0) calls_after_release.fetch_add(1);
  }

  static void* RecvAlloc(void* ctx, size_t len) {
    static_cast<Recorder*>(ctx)->Touch();
    return std::malloc(len);
  }
  static int Recv(void* ctx, void* buf, size_t len) {
    auto* self = static_cast<Recorder*>(ctx);
    self->Touch();
    {
      std::lock_guard<std::mutex> lock(self->mu);
      self->received.append(static_cast<const char*>(buf), len);
    }
    std::free(buf);
    return self->hold.load() ? DMS_RECV_HOLD : DMS_RECV_OK;
  }
  static void Writable(void* ctx) {
    auto* self = static_cast<Recorder*>(ctx);
    self->Touch();
    self->writable.fetch_add(1);
  }
  static void Eof(void* ctx) {
    auto* self = static_cast<Recorder*>(ctx);
    self->Touch();
    self->eof.fetch_add(1);
  }
  static void Error(void* ctx, int err, const char* /*message*/) {
    auto* self = static_cast<Recorder*>(ctx);
    self->Touch();
    self->last_error.store(err);
    self->errors.fetch_add(1);
  }
  static void Released(void* ctx) {
    static_cast<Recorder*>(ctx)->released.fetch_add(1);
  }

  static constexpr dms_stream_callbacks kCallbacks = {
      RecvAlloc, Recv, Writable, Eof, Error, Released};
};

// Receives one connect or accept delivery.
struct Delivery {
  std::atomic<dms_stream*> stream{nullptr};
  std::atomic<int> err{0};
  std::atomic<int> count{0};

  static void OnConnect(void* ctx, dms_stream* s, int err,
                        const char* /*message*/) {
    auto* self = static_cast<Delivery*>(ctx);
    self->stream.store(s);
    self->err.store(err);
    self->count.fetch_add(1);
  }
  static void OnAccept(void* ctx, dms_stream* s) {
    auto* self = static_cast<Delivery*>(ctx);
    self->stream.store(s);
    self->count.fetch_add(1);
  }
};

struct Fixture {
  explicit Fixture(int post_max = 65536)
      : state(std::make_shared<FakeDmeshState>()) {
    state->SetPostMax(post_max);
    std::string error;
    rt = OpenStreamRuntime(MakeFakeDmeshApiOps(state), &error);
    CHECK_TRUE(rt != nullptr);
  }

  ~Fixture() {
    if (rt != nullptr) dms_runtime_close(rt);
  }

  // Connects and binds a client stream to `recorder`.
  dms_stream* Connect(Recorder* recorder) {
    Delivery delivery;
    dms_connect(rt, "svc.test:80", Delivery::OnConnect, &delivery);
    CHECK_TRUE(WaitFor([&] { return delivery.count.load() == 1; }));
    dms_stream* s = delivery.stream.load();
    CHECK_TRUE(s != nullptr);
    CHECK_EQ(dms_stream_bind(s, &Recorder::kCallbacks, recorder), 0);
    qp = state->ClientQps().back();
    return s;
  }

  std::shared_ptr<FakeDmeshState> state;
  dms_runtime* rt = nullptr;
  dmesh_qp_t* qp = nullptr;
};

std::string Joined(const std::vector<std::vector<uint8_t>>& posts) {
  std::string all;
  for (const auto& post : posts) all.append(post.begin(), post.end());
  return all;
}

void TestWriteSplitsAtPostMax() {
  Fixture fixture(4);
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  CHECK_EQ(dms_runtime_post_max(fixture.rt), size_t{4});
  CHECK_EQ(dms_stream_write(s, "0123456789", 10), ssize_t{10});
  const auto posts = fixture.state->Posts(fixture.qp);
  CHECK_EQ(posts.size(), size_t{3});
  CHECK_EQ(posts[2].size(), size_t{2});
  CHECK_EQ(Joined(posts), std::string("0123456789"));
  dms_stream_close(s);
  CHECK_TRUE(fixture.state->WaitForDestroyCount(1, 2s));
  CHECK_TRUE(WaitFor([&] { return recorder.released.load() == 1; }));
}

void FillDigits(void* ctx, void* dst, size_t len) {
  ++*static_cast<int*>(ctx);
  for (size_t i = 0; i < len; ++i) static_cast<char*>(dst)[i] = '0' + i % 10;
}

void TestPostFillsReservation() {
  Fixture fixture(8);
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  int fills = 0;
  CHECK_EQ(dms_stream_post(s, 8, FillDigits, &fills), ssize_t{8});
  CHECK_EQ(dms_stream_post(s, 9, FillDigits, &fills), ssize_t{-EMSGSIZE});
  fixture.state->FailNextAlloc(fixture.qp, EAGAIN);
  CHECK_EQ(dms_stream_post(s, 3, FillDigits, &fills), ssize_t{-EAGAIN});
  CHECK_EQ(fills, 1);
  CHECK_EQ(Joined(fixture.state->Posts(fixture.qp)), std::string("01234567"));
  dms_stream_close(s);
}

void TestReceivesArriveInOrder() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  fixture.state->InjectReceive(fixture.qp, "hello ");
  fixture.state->InjectReceive(fixture.qp, "stream");
  CHECK_TRUE(WaitFor([&] { return recorder.Received() == "hello stream"; }));
  CHECK_TRUE(fixture.state->WaitForReleaseCount(2, 2s));
  dms_stream_close(s);
}

void TestHoldKeepsCreditUntilResume() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  recorder.hold.store(true);
  fixture.state->InjectReceive(fixture.qp, "held");
  CHECK_TRUE(WaitFor([&] { return recorder.Received() == "held"; }));
  std::this_thread::sleep_for(20ms);
  CHECK_EQ(fixture.state->release_count(), size_t{0});
  recorder.hold.store(false);
  dms_stream_resume(s);
  CHECK_TRUE(fixture.state->WaitForReleaseCount(1, 2s));
  dms_stream_close(s);
}

void TestEagainThenWritable() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  fixture.state->SetAllocError(fixture.qp, EAGAIN);
  CHECK_EQ(dms_stream_write(s, "x", 1), ssize_t{-EAGAIN});
  fixture.state->SetAllocError(fixture.qp, 0);
  fixture.state->InjectTxReady(fixture.qp);
  CHECK_TRUE(WaitFor([&] { return recorder.writable.load() == 1; }));
  CHECK_EQ(dms_stream_write(s, "x", 1), ssize_t{1});
  dms_stream_close(s);
}

void TestShortWriteReportsAcceptedBytes() {
  Fixture fixture(4);
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  // The first chunk is accepted; the second finds no credit.
  CHECK_EQ(dms_stream_write(s, "abcd", 4), ssize_t{4});
  fixture.state->FailNextAlloc(fixture.qp, EAGAIN);
  CHECK_EQ(dms_stream_write(s, "efghij", 6), ssize_t{-EAGAIN});
  CHECK_EQ(Joined(fixture.state->Posts(fixture.qp)), std::string("abcd"));
  CHECK_EQ(dms_stream_write(s, "efghij", 6), ssize_t{6});
  CHECK_EQ(Joined(fixture.state->Posts(fixture.qp)), std::string("abcdefghij"));
  dms_stream_close(s);
}

void TestFinThenClose() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  fixture.state->InjectReceive(fixture.qp, "last");
  fixture.state->InjectFin(fixture.qp);
  CHECK_TRUE(WaitFor([&] { return recorder.eof.load() == 1; }));
  CHECK_EQ(recorder.Received(), std::string("last"));
  CHECK_EQ(dms_stream_write(s, "late", 4), ssize_t{4});
  dms_stream_close(s);
  CHECK_TRUE(fixture.state->WaitForDestroyCount(1, 2s));
  CHECK_EQ(fixture.state->abort_count(), size_t{0});
  CHECK_TRUE(WaitFor([&] { return recorder.released.load() == 1; }));
  CHECK_EQ(recorder.errors.load(), 0);
}

void TestAcceptReplaysEarlyBytes() {
  Fixture fixture;
  Delivery delivery;
  CHECK_EQ(dms_listen(fixture.rt, Delivery::OnAccept, &delivery), 0);
  dmesh_qp_t* qp = fixture.state->InjectConnectionRequest("PRI * HTTP/2.0");
  CHECK_TRUE(qp != nullptr);
  CHECK_TRUE(WaitFor([&] { return delivery.count.load() == 1; }));
  // Bytes that land between accept and bind are held for the binding.
  fixture.state->InjectReceive(qp, "\r\n");
  std::this_thread::sleep_for(20ms);
  Recorder recorder;
  dms_stream* s = delivery.stream.load();
  CHECK_EQ(dms_stream_bind(s, &Recorder::kCallbacks, &recorder), 0);
  CHECK_TRUE(
      WaitFor([&] { return recorder.Received() == "PRI * HTTP/2.0\r\n"; }));
  CHECK_EQ(dms_stream_write(s, "ok", 2), ssize_t{2});
  CHECK_EQ(Joined(fixture.state->Posts(qp)), std::string("ok"));
  dms_stream_close(s);
  CHECK_TRUE(fixture.state->WaitForDestroyCount(1, 2s));
}

void TestListenNullRejects() {
  Fixture fixture;
  Delivery delivery;
  CHECK_EQ(dms_listen(fixture.rt, Delivery::OnAccept, &delivery), 0);
  CHECK_EQ(dms_listen(fixture.rt, nullptr, nullptr), 0);
  fixture.state->InjectConnectionRequest("");
  CHECK_TRUE(fixture.state->WaitForDestroyCount(1, 2s));
  CHECK_EQ(delivery.count.load(), 0);
}

void TestAbortResets() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  dms_stream_abort(s);
  CHECK_TRUE(WaitFor([&] { return fixture.state->abort_count() == 1; }));
  CHECK_TRUE(WaitFor([&] { return recorder.released.load() == 1; }));
  CHECK_EQ(recorder.errors.load(), 0);
}

void TestTransmitErrorFailsStream() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  fixture.state->InjectTxError(fixture.qp);
  CHECK_TRUE(WaitFor([&] { return recorder.errors.load() == 1; }));
  CHECK_EQ(recorder.last_error.load(), EPIPE);
  CHECK_EQ(dms_stream_write(s, "x", 1), ssize_t{-EPIPE});
  dms_stream_close(s);
  CHECK_TRUE(WaitFor([&] { return recorder.released.load() == 1; }));
}

void TestNoCallbackAfterClose() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  dms_stream_close(s);
  fixture.state->InjectReceive(fixture.qp, "dropped");
  fixture.state->InjectFin(fixture.qp);
  CHECK_TRUE(WaitFor([&] { return recorder.released.load() == 1; }));
  std::this_thread::sleep_for(20ms);
  CHECK_EQ(recorder.Received(), std::string());
  CHECK_EQ(recorder.eof.load(), 0);
  CHECK_EQ(recorder.calls_after_release.load(), 0);
  CHECK_EQ(recorder.released.load(), 1);
}

void TestUnboundCloseDestroysQp() {
  Fixture fixture;
  Delivery delivery;
  dms_connect(fixture.rt, "svc.test:80", Delivery::OnConnect, &delivery);
  CHECK_TRUE(WaitFor([&] { return delivery.count.load() == 1; }));
  dms_stream_close(delivery.stream.load());
  CHECK_TRUE(fixture.state->WaitForDestroyCount(1, 2s));
}

void TestRuntimeCloseFailsLiveStreams() {
  Fixture fixture;
  Recorder recorder;
  dms_stream* s = fixture.Connect(&recorder);
  dms_runtime_close(fixture.rt);
  fixture.rt = nullptr;
  CHECK_EQ(recorder.errors.load(), 1);
  CHECK_EQ(recorder.last_error.load(), ECANCELED);
  CHECK_EQ(fixture.state->channel_destroy_count(), size_t{1});
  CHECK_EQ(dms_stream_write(s, "x", 1), ssize_t{-EPIPE});
  dms_stream_close(s);
  CHECK_EQ(recorder.released.load(), 1);
}

void TestConnectFailureReportsErrno() {
  Fixture fixture;
  fixture.state->FailNextCreateQp(ENOENT);
  Delivery delivery;
  dms_connect(fixture.rt, "missing.test:80", Delivery::OnConnect, &delivery);
  CHECK_TRUE(WaitFor([&] { return delivery.count.load() == 1; }));
  CHECK_TRUE(delivery.stream.load() == nullptr);
  CHECK_EQ(delivery.err.load(), ECONNREFUSED);
}

void TestBindRules() {
  Fixture fixture;
  Delivery delivery;
  dms_connect(fixture.rt, "svc.test:80", Delivery::OnConnect, &delivery);
  CHECK_TRUE(WaitFor([&] { return delivery.count.load() == 1; }));
  dms_stream* s = delivery.stream.load();
  CHECK_EQ(dms_stream_write(s, "x", 1), ssize_t{-EINVAL});
  dms_stream_callbacks partial = Recorder::kCallbacks;
  partial.recv = nullptr;
  Recorder recorder;
  CHECK_EQ(dms_stream_bind(s, &partial, &recorder), -EINVAL);
  CHECK_EQ(dms_stream_bind(s, &Recorder::kCallbacks, &recorder), 0);
  CHECK_EQ(dms_stream_bind(s, &Recorder::kCallbacks, &recorder), -EALREADY);
  dms_stream_close(s);
  CHECK_TRUE(WaitFor([&] { return recorder.released.load() == 1; }));
}

struct TestCase {
  const char* name;
  void (*run)();
};

}  // namespace
}  // namespace dpumesh::grpc::testing

int main() {
  using namespace dpumesh::grpc::testing;
  CHECK_EQ(dms_abi_version(), DMS_ABI_VERSION);
  const TestCase tests[] = {
      {"write splits at post max", TestWriteSplitsAtPostMax},
      {"post fills the reservation", TestPostFillsReservation},
      {"receives arrive in order", TestReceivesArriveInOrder},
      {"hold keeps credit until resume", TestHoldKeepsCreditUntilResume},
      {"EAGAIN then writable", TestEagainThenWritable},
      {"short write reports accepted bytes", TestShortWriteReportsAcceptedBytes},
      {"FIN then close", TestFinThenClose},
      {"accept replays early bytes", TestAcceptReplaysEarlyBytes},
      {"listen NULL rejects", TestListenNullRejects},
      {"abort resets", TestAbortResets},
      {"transmit error fails stream", TestTransmitErrorFailsStream},
      {"no callback after close", TestNoCallbackAfterClose},
      {"unbound close destroys QP", TestUnboundCloseDestroysQp},
      {"runtime close fails live streams", TestRuntimeCloseFailsLiveStreams},
      {"connect failure reports errno", TestConnectFailureReportsErrno},
      {"bind rules", TestBindRules},
  };
  int failures = 0;
  for (const TestCase& test : tests) {
    try {
      test.run();
      std::cout << "PASS " << test.name << "\n";
    } catch (const TestFailure& failure) {
      ++failures;
      std::cout << "FAIL " << test.name << ": " << failure.message << "\n";
    }
  }
  return failures == 0 ? 0 : 1;
}
