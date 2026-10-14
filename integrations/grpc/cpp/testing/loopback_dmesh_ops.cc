// In-memory native operations that connect a process to itself: every
// dmesh_create_qp() becomes an inbound stream on the same process's first EQ.
// libdpumesh_stream_loopback.so exports the stream C ABI over them, so the
// language adapters run real gRPC clients and servers in their tests without
// a DOCA device.
//
// It keeps the native contract the reactor relies on: a RECV holds one
// credit until dmesh_release_rx_buffer(); a sender whose peer holds `window`
// credits gets EAGAIN and, once one returns, TX_READY; a destroyed QP reclaims
// its queued events and sends its peer RECV_FIN. DPUMESH_LOOPBACK_POST_MAX and
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

class LoopbackDmeshApiOps final : public DmeshApiOps {
 public:
  LoopbackDmeshApiOps()
      : post_max_(EnvInt("DPUMESH_LOOPBACK_POST_MAX", 8064)),
        window_(static_cast<size_t>(EnvInt("DPUMESH_LOOPBACK_WINDOW", 64))) {
    channel_.pod_id = 1;
    channel_.slot_size = post_max_;
    channel_.block_size = post_max_;
  }

  ~LoopbackDmeshApiOps() override {
    for (auto& eq : eqs_) {
      if (eq->fd >= 0) ::close(eq->fd);
    }
  }

  dmesh_channel_t* CreateChannel() override { return &channel_; }
  int DestroyChannel(dmesh_channel_t*) override { return 0; }

  dmesh_eq_t* CreateEq(dmesh_channel_t*) override {
    std::lock_guard<std::mutex> lock(mu_);
    auto eq = std::make_unique<Eq>();
    eq->fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (eq->fd < 0) return nullptr;
    Eq* raw = eq.get();
    eqs_.push_back(std::move(eq));
    return reinterpret_cast<dmesh_eq_t*>(raw);
  }

  int DestroyEq(dmesh_eq_t* eq) override {
    std::lock_guard<std::mutex> lock(mu_);
    Eq* e = reinterpret_cast<Eq*>(eq);
    if (e->fd >= 0) ::close(e->fd);
    e->fd = -1;
    e->destroyed = true;
    return 0;
  }

  int EqFd(dmesh_eq_t* eq) override {
    std::lock_guard<std::mutex> lock(mu_);
    return reinterpret_cast<Eq*>(eq)->fd;
  }

  dmesh_qp_t* CreateQp(dmesh_eq_t* eq, const char* service) override {
    std::lock_guard<std::mutex> lock(mu_);
    if (service == nullptr || service[0] == '\0') {
      errno = EINVAL;
      return nullptr;
    }
    Eq* listening = FirstLiveEq();
    if (listening == nullptr) {
      errno = ENOENT;
      return nullptr;
    }
    Qp* client = NewQp(reinterpret_cast<Eq*>(eq), DMESH_ROLE_CLIENT);
    Qp* server = NewQp(listening, DMESH_ROLE_SERVER);
    client->peer = server;
    server->peer = client;
    server->value.remote_pod = static_cast<int16_t>(channel_.pod_id);
    server->value.remote_port = client->value.local_port;
    Push(listening, Event(server, DMESH_EVENT_CONN_REQ));
    return &client->value;
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
    Push(peer->eq, event);
    return 0;
  }

  int Flush(dmesh_qp_t*) override { return 0; }

  int PollEq(dmesh_eq_t* eq, dmesh_event_t* events, int max_events) override {
    std::lock_guard<std::mutex> lock(mu_);
    Eq* e = reinterpret_cast<Eq*>(eq);
    int count = 0;
    while (count < max_events && !e->events.empty()) {
      events[count++] = e->events.front();
      e->events.pop_front();
    }
    return count;
  }

  void ReleaseRxBuffer(dmesh_channel_t*, dmesh_event_t* event) override {
    std::lock_guard<std::mutex> lock(mu_);
    Release(event->_rx_token);
    event->_rx_token = -1;
  }

  int PostMax(dmesh_channel_t*) override { return post_max_; }
  int PodId(dmesh_channel_t*) override { return channel_.pod_id; }
  int64_t EqNextDeadlineNs(dmesh_eq_t*) override { return -1; }

 private:
  struct Qp;

  struct Eq {
    int fd = -1;
    bool destroyed = false;
    std::deque<dmesh_event_t> events;
  };

  struct Qp {
    dmesh_qp_t value{};
    Eq* eq = nullptr;
    Qp* peer = nullptr;
    bool alive = true;
    bool blocked = false;
    size_t held = 0;
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

  Eq* FirstLiveEq() {
    for (auto& eq : eqs_) {
      if (!eq->destroyed) return eq.get();
    }
    return nullptr;
  }

  Qp* NewQp(Eq* eq, int role) {
    auto qp = std::make_unique<Qp>();
    qp->eq = eq;
    qp->value.ep = &channel_;
    qp->value.eq = reinterpret_cast<dmesh_eq_t*>(eq);
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

  void Push(Eq* eq, const dmesh_event_t& event) {
    if (eq == nullptr || eq->destroyed) return;
    eq->events.push_back(event);
    const uint64_t one = 1;
    const ssize_t result = ::write(eq->fd, &one, sizeof(one));
    (void)result;
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
      Push(sender->eq, Event(sender, DMESH_EVENT_TX_READY));
    }
  }

  int Close(dmesh_qp_t* qp) {
    std::lock_guard<std::mutex> lock(mu_);
    Qp* q = Find(qp);
    if (q == nullptr || !q->alive) return 0;
    q->alive = false;
    // Queued events for this QP are reclaimed with their credits.
    auto& queue = q->eq->events;
    for (auto it = queue.begin(); it != queue.end();) {
      if (it->qp == qp) {
        if (it->_rx_token >= 0) Release(it->_rx_token);
        it = queue.erase(it);
      } else {
        ++it;
      }
    }
    Qp* peer = q->peer;
    if (peer != nullptr && peer->alive) {
      Push(peer->eq, Event(peer, DMESH_EVENT_RECV_FIN));
      if (peer->blocked) {
        peer->blocked = false;
        Push(peer->eq, Event(peer, DMESH_EVENT_TX_READY));
      }
    }
    return 0;
  }

  std::mutex mu_;
  dmesh_channel_t channel_{};
  const int post_max_;
  const size_t window_;
  uint16_t next_port_ = 1;
  int32_t next_token_ = 1;
  std::vector<std::unique_ptr<Eq>> eqs_;
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
