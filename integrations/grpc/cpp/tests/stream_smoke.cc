// Manual hardware smoke for the stream C ABI; not registered with CTest.
//   server: echoes every stream the DPU routes to DPUMESH_SERVICE until SIGTERM.
//   client <target> <count> <size>: ping-pongs <count> messages of <size>
//   bytes with the echo at <target>, checks every byte and prints the RTTs.
#include <errno.h>
#include <signal.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "dpumesh_stream.h"

namespace {

using Clock = std::chrono::steady_clock;

std::atomic<bool> g_stop{false};
std::atomic<int> g_live{0};

void* Alloc(void*, size_t len) { return std::malloc(len); }

struct Echo {
  dms_stream* stream = nullptr;
  std::mutex mu;
  std::string pending;
  bool eof = false;
  bool closed = false;

  void Flush() {
    while (!pending.empty()) {
      const ssize_t n = dms_stream_write(stream, pending.data(), pending.size());
      if (n == -EAGAIN) return;
      if (n < 0) {
        pending.clear();
        eof = true;
        break;
      }
      pending.erase(0, static_cast<size_t>(n));
    }
    if (eof && !closed) {
      closed = true;
      dms_stream_close(stream);
    }
  }
  static int Recv(void* ctx, void* buf, size_t len) {
    auto* self = static_cast<Echo*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->pending.append(static_cast<char*>(buf), len);
    std::free(buf);
    self->Flush();
    return DMS_RECV_OK;
  }
  static void Writable(void* ctx) {
    auto* self = static_cast<Echo*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->Flush();
  }
  static void Eof(void* ctx) {
    auto* self = static_cast<Echo*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->eof = true;
    self->Flush();
  }
  static void Error(void* ctx, int err, const char* message) {
    std::fprintf(stderr, "echo stream error %d: %s\n", err, message);
    auto* self = static_cast<Echo*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->eof = true;
    self->pending.clear();
    self->Flush();
  }
  static void Released(void* ctx) {
    delete static_cast<Echo*>(ctx);
    g_live.fetch_sub(1);
  }
  static constexpr dms_stream_callbacks kCallbacks = {
      Alloc, Recv, Writable, Eof, Error, Released};

  static void OnAccept(void*, dms_stream* s) {
    auto* echo = new Echo;
    echo->stream = s;
    g_live.fetch_add(1);
    std::lock_guard<std::mutex> lock(echo->mu);
    dms_stream_bind(s, &kCallbacks, echo);
  }
};

int Server() {
  char err[256] = {0};
  dms_runtime* rt = dms_runtime_open(err, sizeof(err));
  if (rt == nullptr) {
    std::fprintf(stderr, "dms_runtime_open: %s\n", err);
    return 1;
  }
  if (dms_listen(rt, Echo::OnAccept, nullptr) != 0) return 1;
  std::printf("STREAM_SMOKE_SERVER_READY post_max=%zu\n",
              dms_runtime_post_max(rt));
  std::fflush(stdout);
  while (!g_stop.load()) std::this_thread::sleep_for(std::chrono::milliseconds(50));
  dms_listen(rt, nullptr, nullptr);
  dms_runtime_close(rt);
  std::printf("STREAM_SMOKE_SERVER_DONE live=%d\n", g_live.load());
  return 0;
}

struct Pinger {
  std::mutex mu;
  std::string received;
  std::atomic<bool> connected{false};
  std::atomic<bool> failed{false};
  std::atomic<bool> released{false};
  dms_stream* stream = nullptr;

  static int Recv(void* ctx, void* buf, size_t len) {
    auto* self = static_cast<Pinger*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->received.append(static_cast<char*>(buf), len);
    std::free(buf);
    return DMS_RECV_OK;
  }
  static void Writable(void*) {}
  static void Eof(void* ctx) { static_cast<Pinger*>(ctx)->failed.store(true); }
  static void Error(void* ctx, int err, const char* message) {
    std::fprintf(stderr, "client stream error %d: %s\n", err, message);
    static_cast<Pinger*>(ctx)->failed.store(true);
  }
  static void Released(void* ctx) {
    static_cast<Pinger*>(ctx)->released.store(true);
  }
  static constexpr dms_stream_callbacks kCallbacks = {
      Alloc, Recv, Writable, Eof, Error, Released};

  static void OnConnect(void* ctx, dms_stream* s, int err, const char* msg) {
    auto* self = static_cast<Pinger*>(ctx);
    if (s == nullptr) {
      std::fprintf(stderr, "connect failed %d: %s\n", err, msg);
      self->failed.store(true);
      return;
    }
    self->stream = s;
    dms_stream_bind(s, &kCallbacks, self);
    self->connected.store(true);
  }

  size_t Size() {
    std::lock_guard<std::mutex> lock(mu);
    return received.size();
  }
};

int Client(const char* target, int count, size_t size) {
  char err[256] = {0};
  dms_runtime* rt = dms_runtime_open(err, sizeof(err));
  if (rt == nullptr) {
    std::fprintf(stderr, "dms_runtime_open: %s\n", err);
    return 1;
  }
  Pinger pinger;
  const auto connect_start = Clock::now();
  dms_connect(rt, target, Pinger::OnConnect, &pinger);
  while (!pinger.connected.load() && !pinger.failed.load()) {
    if (Clock::now() - connect_start > std::chrono::seconds(10)) break;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  if (!pinger.connected.load()) return 1;
  const double connect_us =
      std::chrono::duration<double, std::micro>(Clock::now() - connect_start)
          .count();

  std::string message(size, '\0');
  std::vector<double> rtts;
  size_t expected = 0;
  bool ok = true;
  for (int i = 0; i < count && ok; ++i) {
    for (size_t j = 0; j < size; ++j) {
      message[j] = static_cast<char>((i * 31 + j * 7) & 0xff);
    }
    const auto start = Clock::now();
    size_t sent = 0;
    while (sent < size) {
      const ssize_t n =
          dms_stream_write(pinger.stream, message.data() + sent, size - sent);
      if (n == -EAGAIN) continue;
      if (n < 0) {
        std::fprintf(stderr, "write failed: %zd\n", n);
        ok = false;
        break;
      }
      sent += static_cast<size_t>(n);
    }
    expected += size;
    while (ok && pinger.Size() < expected) {
      if (pinger.failed.load() ||
          Clock::now() - start > std::chrono::seconds(10)) {
        std::fprintf(stderr, "message %d: no echo\n", i);
        ok = false;
      }
    }
    rtts.push_back(
        std::chrono::duration<double, std::micro>(Clock::now() - start).count());
    std::lock_guard<std::mutex> lock(pinger.mu);
    if (ok && pinger.received.compare(expected - size, size, message) != 0) {
      std::fprintf(stderr, "message %d: echo differs\n", i);
      ok = false;
    }
  }
  dms_stream_close(pinger.stream);
  while (!pinger.released.load()) std::this_thread::yield();
  dms_runtime_close(rt);
  if (!ok) return 1;
  std::sort(rtts.begin(), rtts.end());
  std::printf(
      "STREAM_SMOKE_CLIENT_DONE count=%d size=%zu connect_us=%.0f "
      "rtt_p50_us=%.1f rtt_p99_us=%.1f rtt_max_us=%.1f\n",
      count, size, connect_us, rtts[rtts.size() / 2],
      rtts[std::min(rtts.size() - 1, rtts.size() * 99 / 100)], rtts.back());
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  signal(SIGTERM, [](int) { g_stop.store(true); });
  signal(SIGINT, [](int) { g_stop.store(true); });
  if (argc >= 2 && std::strcmp(argv[1], "server") == 0) return Server();
  if (argc >= 5 && std::strcmp(argv[1], "client") == 0) {
    return Client(argv[2], std::atoi(argv[3]),
                  static_cast<size_t>(std::atol(argv[4])));
  }
  std::fprintf(stderr,
               "usage: %s server | client <target> <count> <size>\n", argv[0]);
  return 2;
}
