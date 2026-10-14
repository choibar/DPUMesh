// An echo server and a client over libdpumesh_stream_loopback.so, through the
// public C ABI only. The client withholds its receive credit while it sends
// more than the transport window, so the echo also exercises EAGAIN,
// TX_READY and resume.
#include <errno.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "dpumesh_stream.h"

namespace {

using namespace std::chrono_literals;

bool WaitFor(const std::atomic<bool>& flag,
             std::chrono::milliseconds timeout = 10s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!flag.load()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(200us);
  }
  return true;
}

void* Alloc(void*, size_t len) { return std::malloc(len); }

// Echoes every byte back and closes after the peer's FIN.
struct EchoPeer {
  dms_stream* stream = nullptr;
  std::mutex mu;
  std::string pending;
  bool eof = false;
  bool closed = false;
  std::atomic<int> eagain{0};
  std::atomic<bool> released{false};

  // Requires mu.
  void Flush() {
    while (!pending.empty()) {
      const ssize_t n = dms_stream_write(stream, pending.data(), pending.size());
      if (n == -EAGAIN) {
        eagain.fetch_add(1);
        return;
      }
      if (n < 0) return;
      pending.erase(0, static_cast<size_t>(n));
    }
    if (eof && !closed) {
      closed = true;
      dms_stream_close(stream);
    }
  }

  static int Recv(void* ctx, void* buf, size_t len) {
    auto* self = static_cast<EchoPeer*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->pending.append(static_cast<char*>(buf), len);
    std::free(buf);
    self->Flush();
    return DMS_RECV_OK;
  }
  static void Writable(void* ctx) {
    auto* self = static_cast<EchoPeer*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->Flush();
  }
  static void Eof(void* ctx) {
    auto* self = static_cast<EchoPeer*>(ctx);
    std::lock_guard<std::mutex> lock(self->mu);
    self->eof = true;
    self->Flush();
  }
  static void Error(void*, int, const char* message) {
    std::cerr << "echo error: " << message << "\n";
  }
  static void Released(void* ctx) {
    static_cast<EchoPeer*>(ctx)->released.store(true);
  }
  static constexpr dms_stream_callbacks kCallbacks = {
      Alloc, Recv, Writable, Eof, Error, Released};

  static void OnAccept(void* ctx, dms_stream* s) {
    auto* self = static_cast<EchoPeer*>(ctx);
    self->stream = s;
    dms_stream_bind(s, &kCallbacks, self);
  }
};

struct Client {
  dms_stream* stream = nullptr;
  std::mutex mu;
  std::string received;
  std::atomic<bool> hold{true};
  std::atomic<int> holds{0};
  std::atomic<bool> writable{false};
  std::atomic<bool> eof{false};
  std::atomic<bool> connected{false};
  std::atomic<bool> released{false};

  size_t Received() {
    std::lock_guard<std::mutex> lock(mu);
    return received.size();
  }

  static int Recv(void* ctx, void* buf, size_t len) {
    auto* self = static_cast<Client*>(ctx);
    {
      std::lock_guard<std::mutex> lock(self->mu);
      self->received.append(static_cast<char*>(buf), len);
    }
    std::free(buf);
    if (!self->hold.load()) return DMS_RECV_OK;
    self->holds.fetch_add(1);
    return DMS_RECV_HOLD;
  }
  static void Writable(void* ctx) {
    static_cast<Client*>(ctx)->writable.store(true);
  }
  static void Eof(void* ctx) { static_cast<Client*>(ctx)->eof.store(true); }
  static void Error(void*, int, const char* message) {
    std::cerr << "client error: " << message << "\n";
  }
  static void Released(void* ctx) {
    static_cast<Client*>(ctx)->released.store(true);
  }
  static constexpr dms_stream_callbacks kCallbacks = {
      Alloc, Recv, Writable, Eof, Error, Released};

  static void OnConnect(void* ctx, dms_stream* s, int err, const char* msg) {
    auto* self = static_cast<Client*>(ctx);
    if (s == nullptr) {
      std::cerr << "connect failed: " << err << " " << msg << "\n";
      return;
    }
    self->stream = s;
    dms_stream_bind(s, &kCallbacks, self);
    self->connected.store(true);
  }
};

#define CHECK(condition)                                               \
  do {                                                                 \
    if (!(condition)) {                                                \
      std::cerr << "FAIL: " #condition " at line " << __LINE__ << "\n"; \
      return 1;                                                        \
    }                                                                  \
  } while (false)

}  // namespace

int main() {
  char err[256] = {0};
  dms_runtime* rt = dms_runtime_open(err, sizeof(err));
  CHECK(rt != nullptr);

  EchoPeer echo;
  CHECK(dms_listen(rt, EchoPeer::OnAccept, &echo) == 0);

  Client client;
  dms_connect(rt, "echo.test:7", Client::OnConnect, &client);
  CHECK(WaitFor(client.connected));

  // 1 MiB exceeds 64 credits x 8064 bytes, so the echo stalls on the held
  // receive credit until the client resumes.
  std::string payload(1 << 20, '\0');
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>((i * 131 + 7) & 0xff);
  }
  size_t sent = 0;
  const auto deadline = std::chrono::steady_clock::now() + 10s;
  while (sent < payload.size()) {
    CHECK(std::chrono::steady_clock::now() < deadline);
    const ssize_t n = dms_stream_write(client.stream, payload.data() + sent,
                                       payload.size() - sent);
    if (n == -EAGAIN) {
      std::this_thread::sleep_for(100us);
      continue;
    }
    CHECK(n > 0);
    sent += static_cast<size_t>(n);
  }
  std::this_thread::sleep_for(50ms);
  CHECK(client.holds.load() > 0);
  CHECK(echo.eagain.load() > 0);

  client.hold.store(false);
  dms_stream_resume(client.stream);
  const auto echo_deadline = std::chrono::steady_clock::now() + 10s;
  while (client.Received() < payload.size()) {
    CHECK(std::chrono::steady_clock::now() < echo_deadline);
    std::this_thread::sleep_for(200us);
    // Credit withheld by late HOLD answers is returned here.
    dms_stream_resume(client.stream);
  }
  {
    std::lock_guard<std::mutex> lock(client.mu);
    CHECK(client.received == payload);
  }

  // The client's FIN closes the echo, whose FIN reaches the client.
  dms_stream_close(client.stream);
  CHECK(WaitFor(echo.released));
  CHECK(WaitFor(client.released));

  dms_runtime_close(rt);
  std::cout << "PASS loopback echo of " << payload.size() << " bytes ("
            << client.holds.load() << " held receives, "
            << echo.eagain.load() << " echo EAGAINs)\n";
  return 0;
}
