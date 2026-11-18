// In-memory native operations that connect a process to itself: every
// dmesh_create_qp() becomes an inbound stream the same process accepts.
// libdpumesh_stream_loopback.so exports the stream C ABI over them, so the
// language adapters run real gRPC clients and servers in their tests without
// a DOCA device.
//
// It keeps the native contract the reactor relies on: the channel's fd and
// each QP's fd are readable while an accept or a poll has something to return,
// and settle when it returns nothing; a RECV holds one credit until
// dmesh_release_rx_buffer(); a sender whose peer holds `window` credits gets
// EAGAIN and, once one returns, TX_READY; a destroyed QP reclaims its queued
// events and sends its peer RECV_FIN. DPUMESH_LOOPBACK_POST_MAX and
// DPUMESH_LOOPBACK_WINDOW override the post size (8064) and window (64).
#include "loopback_dmesh_ops.h"

#include <errno.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace dpumesh::grpc {
namespace {

int EnvInt(const char* name, int fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') return fallback;
  const int parsed = std::atoi(value);
  return parsed > 0 ? parsed : fallback;
}

void Raise(int fd) {
  const uint64_t one = 1;
  const ssize_t result = ::write(fd, &one, sizeof(one));
  (void)result;
}

void Settle(int fd) {
  uint64_t value;
  const ssize_t result = ::read(fd, &value, sizeof(value));
  (void)result;
}

class LoopbackDmeshApiOps final : public DmeshApiOps {
 public:
  LoopbackDmeshApiOps()
      : post_max_(EnvInt("DPUMESH_LOOPBACK_POST_MAX", 8064)),
        window_(static_cast<size_t>(EnvInt("DPUMESH_LOOPBACK_WINDOW", 64))),
        channel_fd_(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) {
    channel_.block_size = post_max_;
  }

  ~LoopbackDmeshApiOps() override {
    for (auto& entry : qps_) {
      if (entry.second->fd >= 0) ::close(entry.second->fd);
    }
    if (channel_fd_ >= 0) ::close(channel_fd_);
  }

  dmesh_channel_t* CreateChannel() override {
    if (channel_fd_ < 0) return nullptr;
    return &channel_;
  }
  int DestroyChannel(dmesh_channel_t*) override { return 0; }
  int PostMax(dmesh_channel_t*) override { return post_max_; }
  int ChannelFd(dmesh_channel_t*) override { return channel_fd_; }

  dmesh_qp_t* Accept(dmesh_channel_t*) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (inbound_.empty()) {
      Settle(channel_fd_);
      errno = EAGAIN;
      return nullptr;
    }
    Qp* server = inbound_.front();
    inbound_.pop_front();
    return &server->value;
  }

  dmesh_qp_t* CreateQp(dmesh_channel_t*, const char* target) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (target == nullptr || target[0] == '\0') {
      errno = EINVAL;
      return nullptr;
    }
    Qp* client = NewQp(DMESH_ROLE_CLIENT);
    Qp* server = NewQp(DMESH_ROLE_SERVER);
    if (client == nullptr || server == nullptr) {
      errno = EMFILE;
      return nullptr;
    }
    client->peer = server;
    server->peer = client;
    server->value.remote_pod = 1;  // this process, the loopback's only pod
    server->value.remote_port = client->value.local_port;
    inbound_.push_back(server);
    Raise(channel_fd_);
    return &client->value;
  }

  int QpFd(dmesh_qp_t* qp) override {
    std::lock_guard<std::mutex> lock(mu_);
    Qp* q = Find(qp);
    return q != nullptr && q->alive ? q->fd : -1;
  }

  int DestroyQp(dmesh_qp_t* qp) override { return Close(qp); }
  int AbortQp(dmesh_qp_t* qp) override { return Close(qp); }

  void* Alloc(dmesh_qp_t* qp, uint32_t len) override {
    std::lock_guard<std::mutex> lock(mu_);
    Qp* q = Find(qp);
    if (q == nullptr || !q->alive || len == 0 ||
        len > static_cast<uint32_t>(post_max_)) {
      errno = EINVAL;
      return nullptr;
    }
    Qp* peer = q->peer;
    if (peer != nullptr && peer->alive && peer->held >= window_) {
      q->blocked = true;
      errno = EAGAIN;
      return nullptr;
    }
    q->reservation.assign(len, 0);
    return q->reservation.data();
  }

  int PostSend(dmesh_qp_t* qp, const void* buffer, uint32_t len) override {
    std::lock_guard<std::mutex> lock(mu_);
    Qp* q = Find(qp);
    if (q == nullptr || !q->alive || buffer != q->reservation.data() ||
        len == 0 || len > q->reservation.size()) {
      errno = EINVAL;
      return -1;
    }
    Qp* peer = q->peer;
    // Bytes for a departed peer vanish, as on a reset stream.
    if (peer == nullptr || !peer->alive) return 0;
    const int32_t token = next_token_++;
    auto& payload = payloads_[token];
    payload.receiver = peer;
    payload.bytes.assign(q->reservation.begin(), q->reservation.begin() + len);
    ++peer->held;
    dmesh_event_t event = Event(peer, DMESH_EVENT_RECV);
    event.buf = payload.bytes.data();
    event.len = len;
    event._rx_token = token;
    Push(peer, event);
    return 0;
  }

  int Flush(dmesh_qp_t*) override { return 0; }

  int PollQp(dmesh_qp_t* qp, dmesh_event_t* events, int max_events) override {
    std::lock_guard<std::mutex> lock(mu_);
    Qp* q = Find(qp);
    if (q == nullptr || !q->alive) {
      errno = EINVAL;
      return -1;
    }
    int count = 0;
    while (count < max_events && !q->events.empty()) {
      events[count++] = q->events.front();
      q->events.pop_front();
    }
    if (count == 0) Settle(q->fd);
    return count;
  }

  void ReleaseRxBuffer(dmesh_channel_t*, dmesh_event_t* event) override {
    std::lock_guard<std::mutex> lock(mu_);
    Release(event->_rx_token);
    event->_rx_token = -1;
  }

 private:
  struct Qp {
    dmesh_qp_t value{};
    int fd = -1;
    Qp* peer = nullptr;
    bool alive = true;
    bool blocked = false;
    size_t held = 0;
    std::deque<dmesh_event_t> events;
    std::vector<uint8_t> reservation;
  };

  struct Payload {
    Qp* receiver = nullptr;
    std::vector<uint8_t> bytes;
  };

  static dmesh_event_t Event(Qp* qp, dmesh_event_type_t type) {
    dmesh_event_t event{};
    event.qp = &qp->value;
    event.type = type;
    event._rx_token = -1;
    return event;
  }

  Qp* NewQp(int role) {
    auto qp = std::make_unique<Qp>();
    qp->fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (qp->fd < 0) return nullptr;
    qp->value.ep = &channel_;
    qp->value.role = role;
    qp->value.local_port = next_port_++;
    qp->value.rx_slot = -1;
    Qp* raw = qp.get();
    qps_.emplace(&raw->value, std::move(qp));
    return raw;
  }

  Qp* Find(dmesh_qp_t* qp) {
    auto found = qps_.find(qp);
    return found == qps_.end() ? nullptr : found->second.get();
  }

  void Push(Qp* qp, const dmesh_event_t& event) {
    if (!qp->alive) return;
    qp->events.push_back(event);
    Raise(qp->fd);
  }

  // Returns a receive credit; a sender waiting on it gets TX_READY.
  void Release(int32_t token) {
    auto found = payloads_.find(token);
    if (found == payloads_.end()) return;
    Qp* receiver = found->second.receiver;
    payloads_.erase(found);
    if (receiver->held > 0) --receiver->held;
    Qp* sender = receiver->peer;
    if (sender != nullptr && sender->alive && sender->blocked &&
        receiver->held < window_) {
      sender->blocked = false;
      Push(sender, Event(sender, DMESH_EVENT_TX_READY));
    }
  }

  int Close(dmesh_qp_t* qp) {
    std::lock_guard<std::mutex> lock(mu_);
    Qp* q = Find(qp);
    if (q == nullptr || !q->alive) return 0;
    q->alive = false;
    // Queued events are reclaimed with their credits.
    for (const dmesh_event_t& event : q->events) {
      if (event._rx_token >= 0) Release(event._rx_token);
    }
    q->events.clear();
    ::close(q->fd);
    q->fd = -1;
    Qp* peer = q->peer;
    if (peer != nullptr && peer->alive) {
      Push(peer, Event(peer, DMESH_EVENT_RECV_FIN));
      if (peer->blocked) {
        peer->blocked = false;
        Push(peer, Event(peer, DMESH_EVENT_TX_READY));
      }
    }
    return 0;
  }

  std::mutex mu_;
  dmesh_channel_t channel_{};
  const int post_max_;
  const size_t window_;
  const int channel_fd_;
  uint16_t next_port_ = 1;
  int32_t next_token_ = 1;
  // Streams dialed and not yet accepted.
  std::deque<Qp*> inbound_;
  std::unordered_map<dmesh_qp_t*, std::unique_ptr<Qp>> qps_;
  std::unordered_map<int32_t, Payload> payloads_;
};

}  // namespace

std::unique_ptr<DmeshApiOps> MakeLoopbackDmeshApiOps() {
  return std::make_unique<LoopbackDmeshApiOps>();
}

}  // namespace dpumesh::grpc

// dms_runtime_open() over the loopback, for libdpumesh_stream_loopback.so.
extern "C" dms_runtime* dms_runtime_open(char* err, size_t err_len) {
  std::string error;
  dms_runtime* rt = dpumesh::grpc::OpenStreamRuntime(
      dpumesh::grpc::MakeLoopbackDmeshApiOps(), &error);
  if (rt == nullptr && err != nullptr && err_len != 0) {
    const size_t n = std::min(err_len - 1, error.size());
    std::memcpy(err, error.data(), n);
    err[n] = '\0';
  }
  return rt;
}
