#include "fake_dmesh_ops.h"

#include <errno.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dpumesh::grpc::testing {
namespace {

template <typename Predicate>
bool WaitUntil(std::mutex* mu, std::chrono::milliseconds timeout,
               Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  for (;;) {
    {
      std::lock_guard<std::mutex> lock(*mu);
      if (predicate()) return true;
    }
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

}  // namespace

class FakeDmeshState::Impl final {
 public:
  struct Event {
    dmesh_event_t value{};
  };

  struct Qp {
    dmesh_qp_t value{};
    int fd = -1;
    bool alive = true;
    bool batch_active = false;
    std::thread::id poll_thread;
    int next_poll_error = 0;
    std::deque<Event> events;
    size_t alloc_calls = 0;
    size_t flush_calls = 0;
    std::deque<int> alloc_failures;
    int sticky_alloc_error = 0;
    std::deque<int> post_failures;
    std::vector<uint8_t> allocation;
    std::vector<std::vector<uint8_t>> posts;
  };

  Impl() : channel_fd(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) {}

  ~Impl() {
    for (auto& entry : qps) {
      if (entry.second->fd >= 0) ::close(entry.second->fd);
    }
    if (channel_fd >= 0) ::close(channel_fd);
  }

  Qp* FindQp(dmesh_qp_t* qp) const {
    auto found = qps.find(qp);
    return found == qps.end() ? nullptr : found->second.get();
  }

  Qp* NewQp(int role) {
    auto qp = std::make_unique<Qp>();
    qp->fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (qp->fd < 0) return nullptr;
    qp->value.ep = &channel;
    qp->value.role = role;
    qp->value.local_port = next_port++;
    qp->value.rx_slot = -1;
    dmesh_qp_t* result = &qp->value;
    qps.emplace(result, std::move(qp));
    if (role == DMESH_ROLE_CLIENT) {
      client_qps.push_back(result);
    }
    return FindQp(result);
  }

  static void Raise(int fd) {
    if (fd < 0) return;
    const uint64_t one = 1;
    const ssize_t result = ::write(fd, &one, sizeof(one));
    (void)result;
  }

  static void Settle(int fd) {
    uint64_t value;
    const ssize_t result = ::read(fd, &value, sizeof(value));
    (void)result;
  }

  void Push(Qp* qp, const Event& event) {
    qp->events.push_back(event);
    Raise(qp->fd);
  }

  void QueueReceive(Qp* qp, const std::string& bytes) {
    const int32_t slot = next_rx_slot++;
    auto payload = std::make_shared<std::vector<uint8_t>>(bytes.begin(),
                                                          bytes.end());
    rx_payloads.emplace(slot, payload);
    Event event;
    event.value.qp = &qp->value;
    event.value.type = DMESH_EVENT_RECV;
    event.value.buf = payload->data();
    event.value.len = static_cast<uint32_t>(payload->size());
    event.value._rx_token = slot;
    Push(qp, event);
  }

  // As the native teardown: the receives never polled return their credit,
  // and the fd closes.
  void Kill(Qp* qp) {
    if (qp->batch_active) ++mid_batch_destroys;
    for (const Event& event : qp->events) {
      if (event.value._rx_token < 0) continue;
      rx_payloads.erase(event.value._rx_token);
      ++releases;
    }
    qp->events.clear();
    qp->alive = false;
    qp->allocation.clear();
    if (qp->fd >= 0) ::close(qp->fd);
    qp->fd = -1;
    ++destroys;
  }

  mutable std::mutex mu;
  dmesh_channel_t channel{nullptr, 65536};
  const int channel_fd;
  // Streams no accept has taken yet.
  std::deque<dmesh_qp_t*> inbound;
  int post_max = 65536;
  int next_create_qp_error = 0;
  uint16_t next_port = 1;
  int32_t next_rx_slot = 1;
  std::vector<dmesh_qp_t*> client_qps;
  std::vector<std::string> client_targets;
  std::unordered_map<dmesh_qp_t*, std::unique_ptr<Qp>> qps;
  std::unordered_map<int32_t, std::shared_ptr<std::vector<uint8_t>>>
      rx_payloads;
  size_t releases = 0;
  size_t destroys = 0;
  size_t aborts = 0;
  size_t mid_batch_destroys = 0;
  size_t poll_thread_violations = 0;
  size_t channel_destroys = 0;
};

class FakeDmeshApiOps final : public DmeshApiOps {
 public:
  explicit FakeDmeshApiOps(std::shared_ptr<FakeDmeshState> state)
      : state_(std::move(state)), impl_(state_->impl_.get()) {}

  dmesh_channel_t* CreateChannel() override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    if (impl_->channel_fd < 0) return nullptr;
    return &impl_->channel;
  }

  // Like the native one, it refuses while a QP it handed out is live.
  int DestroyChannel(dmesh_channel_t* channel) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    if (channel != &impl_->channel) {
      errno = EINVAL;
      return -1;
    }
    for (const auto& entry : impl_->qps) {
      if (entry.second->alive &&
          std::find(impl_->inbound.begin(), impl_->inbound.end(),
                    entry.first) == impl_->inbound.end()) {
        errno = EBUSY;
        return -1;
      }
    }
    ++impl_->channel_destroys;
    return 0;
  }

  int PostMax(dmesh_channel_t* /*channel*/) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->post_max;
  }

  int ChannelFd(dmesh_channel_t* channel) override {
    if (channel != &impl_->channel) {
      errno = EINVAL;
      return -1;
    }
    return impl_->channel_fd;
  }

  dmesh_qp_t* Accept(dmesh_channel_t* channel) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    if (channel != &impl_->channel) {
      errno = EINVAL;
      return nullptr;
    }
    if (impl_->inbound.empty()) {
      FakeDmeshState::Impl::Settle(impl_->channel_fd);
      errno = EAGAIN;
      return nullptr;
    }
    dmesh_qp_t* qp = impl_->inbound.front();
    impl_->inbound.pop_front();
    return qp;
  }

  dmesh_qp_t* CreateQp(dmesh_channel_t* channel, const char* target) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    if (channel != &impl_->channel || target == nullptr ||
        target[0] == '\0') {
      errno = EINVAL;
      return nullptr;
    }
    if (impl_->next_create_qp_error != 0) {
      errno = impl_->next_create_qp_error;
      impl_->next_create_qp_error = 0;
      return nullptr;
    }
    auto* fake = impl_->NewQp(DMESH_ROLE_CLIENT);
    if (fake == nullptr) return nullptr;
    impl_->client_targets.emplace_back(target);
    return &fake->value;
  }

  // Raises the fd once, as the native call does, so the first wait polls.
  int QpFd(dmesh_qp_t* qp) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || !fake->alive) {
      errno = EINVAL;
      return -1;
    }
    FakeDmeshState::Impl::Raise(fake->fd);
    return fake->fd;
  }

  int DestroyQp(dmesh_qp_t* qp) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || !fake->alive) return 0;
    impl_->Kill(fake);
    return 0;
  }

  int AbortQp(dmesh_qp_t* qp) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || !fake->alive) return 0;
    impl_->Kill(fake);
    ++impl_->aborts;
    return 0;
  }

  void* Alloc(dmesh_qp_t* qp, uint32_t len) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || !fake->alive || len == 0 ||
        len > static_cast<uint32_t>(impl_->post_max)) {
      errno = EINVAL;
      return nullptr;
    }
    ++fake->alloc_calls;
    if (fake->sticky_alloc_error != 0) {
      errno = fake->sticky_alloc_error;
      return nullptr;
    }
    if (!fake->alloc_failures.empty()) {
      errno = fake->alloc_failures.front();
      fake->alloc_failures.pop_front();
      return nullptr;
    }
    fake->allocation.assign(len, 0);
    return fake->allocation.data();
  }

  int PostSend(dmesh_qp_t* qp, const void* buffer, uint32_t len) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || !fake->alive || buffer == nullptr ||
        len == 0 || len > fake->allocation.size() ||
        buffer != fake->allocation.data()) {
      errno = EINVAL;
      return -1;
    }
    if (!fake->post_failures.empty()) {
      errno = fake->post_failures.front();
      fake->post_failures.pop_front();
      fake->allocation.clear();
      return -1;
    }
    fake->posts.emplace_back(fake->allocation.begin(),
                             fake->allocation.begin() + len);
    fake->allocation.clear();
    return 0;
  }

  int Flush(dmesh_qp_t* qp) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || !fake->alive) {
      errno = EINVAL;
      return -1;
    }
    ++fake->flush_calls;
    return 0;
  }

  int PollQp(dmesh_qp_t* qp, dmesh_event_t* events, int max_events) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || !fake->alive || events == nullptr ||
        max_events <= 0) {
      errno = EINVAL;
      return -1;
    }
    const std::thread::id caller = std::this_thread::get_id();
    if (fake->poll_thread == std::thread::id()) {
      fake->poll_thread = caller;
    } else if (fake->poll_thread != caller) {
      ++impl_->poll_thread_violations;
    }
    if (fake->next_poll_error != 0) {
      errno = fake->next_poll_error;
      fake->next_poll_error = 0;
      fake->batch_active = false;
      return -1;
    }

    int count = 0;
    while (count < max_events && !fake->events.empty()) {
      events[count++] = fake->events.front().value;
      fake->events.pop_front();
    }
    fake->batch_active = count != 0;
    if (count == 0) FakeDmeshState::Impl::Settle(fake->fd);
    return count;
  }

  void ReleaseRxBuffer(dmesh_channel_t* channel, dmesh_event_t* event) override {
    std::lock_guard<std::mutex> lock(impl_->mu);
    if (channel != &impl_->channel || event == nullptr ||
        event->_rx_token < 0) {
      return;
    }
    impl_->rx_payloads.erase(event->_rx_token);
    event->_rx_token = -1;
    event->buf = nullptr;
    ++impl_->releases;
  }

 private:
  std::shared_ptr<FakeDmeshState> state_;
  FakeDmeshState::Impl* const impl_;
};

FakeDmeshState::FakeDmeshState() : impl_(std::make_unique<Impl>()) {}

FakeDmeshState::~FakeDmeshState() = default;

void FakeDmeshState::SetPostMax(int post_max) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  impl_->post_max = post_max;
  impl_->channel.block_size = post_max;
}

void FakeDmeshState::FailNextCreateQp(int error_number) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  impl_->next_create_qp_error = error_number;
}

void FakeDmeshState::FailNextAlloc(dmesh_qp_t* qp, int error_number) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  if (auto* fake = impl_->FindQp(qp)) {
    fake->alloc_failures.push_back(error_number);
  }
}

void FakeDmeshState::SetAllocError(dmesh_qp_t* qp, int error_number) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  if (auto* fake = impl_->FindQp(qp)) {
    fake->sticky_alloc_error = error_number;
  }
}

void FakeDmeshState::FailNextPost(dmesh_qp_t* qp, int error_number) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  if (auto* fake = impl_->FindQp(qp)) {
    fake->post_failures.push_back(error_number);
  }
}

void FakeDmeshState::FailNextPoll(dmesh_qp_t* qp, int error_number) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  if (fake == nullptr || !fake->alive) return;
  fake->next_poll_error = error_number;
  Impl::Raise(fake->fd);
}

bool FakeDmeshState::WaitForClientQpCount(
    size_t count, std::chrono::milliseconds timeout) {
  return WaitUntil(&impl_->mu, timeout,
                   [this, count] { return impl_->client_qps.size() >= count; });
}

std::vector<dmesh_qp_t*> FakeDmeshState::ClientQps() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->client_qps;
}

std::vector<std::string> FakeDmeshState::ClientTargets() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->client_targets;
}

bool FakeDmeshState::WaitForPostCount(dmesh_qp_t* qp, size_t count,
                                      std::chrono::milliseconds timeout) {
  return WaitUntil(&impl_->mu, timeout, [this, qp, count] {
    auto* fake = impl_->FindQp(qp);
    return fake != nullptr && fake->posts.size() >= count;
  });
}

bool FakeDmeshState::WaitForFlushCount(dmesh_qp_t* qp, size_t count,
                                       std::chrono::milliseconds timeout) {
  return WaitUntil(&impl_->mu, timeout, [this, qp, count] {
    auto* fake = impl_->FindQp(qp);
    return fake != nullptr && fake->flush_calls >= count;
  });
}

bool FakeDmeshState::WaitForAllocCallCount(
    dmesh_qp_t* qp, size_t count, std::chrono::milliseconds timeout) {
  return WaitUntil(&impl_->mu, timeout, [this, qp, count] {
    auto* fake = impl_->FindQp(qp);
    return fake != nullptr && fake->alloc_calls >= count;
  });
}

bool FakeDmeshState::WaitForReleaseCount(
    size_t count, std::chrono::milliseconds timeout) {
  return WaitUntil(&impl_->mu, timeout,
                   [this, count] { return impl_->releases >= count; });
}

bool FakeDmeshState::WaitForDestroyCount(
    size_t count, std::chrono::milliseconds timeout) {
  return WaitUntil(&impl_->mu, timeout,
                   [this, count] { return impl_->destroys >= count; });
}

void FakeDmeshState::InjectReceive(dmesh_qp_t* qp, const std::string& bytes) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  if (fake == nullptr || !fake->alive) return;
  impl_->QueueReceive(fake, bytes);
}

void FakeDmeshState::InjectReceiveBatch(
    dmesh_qp_t* qp,
    const std::vector<std::string>& receives) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  if (fake == nullptr || !fake->alive) return;
  for (const auto& receive : receives) impl_->QueueReceive(fake, receive);
}

void FakeDmeshState::InjectFin(dmesh_qp_t* qp) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  if (fake == nullptr || !fake->alive) return;
  Impl::Event event;
  event.value.qp = qp;
  event.value.type = DMESH_EVENT_RECV_FIN;
  event.value._rx_token = -1;
  impl_->Push(fake, event);
}

void FakeDmeshState::InjectTxReady(dmesh_qp_t* qp) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  if (fake == nullptr || !fake->alive) return;
  Impl::Event event;
  event.value.qp = qp;
  event.value.type = DMESH_EVENT_TX_READY;
  event.value._rx_token = -1;
  impl_->Push(fake, event);
}

void FakeDmeshState::InjectTxError(dmesh_qp_t* qp) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  if (fake == nullptr || !fake->alive) return;
  Impl::Event event;
  event.value.qp = qp;
  event.value.type = DMESH_EVENT_TX_ERROR;
  event.value._rx_token = -1;
  impl_->Push(fake, event);
}

dmesh_qp_t* FakeDmeshState::InjectConnectionRequest(
    const std::string& first_bytes) {
  std::lock_guard<std::mutex> lock(impl_->mu);
  Impl::Qp* fake = impl_->NewQp(DMESH_ROLE_SERVER);
  if (fake == nullptr) return nullptr;
  if (!first_bytes.empty()) impl_->QueueReceive(fake, first_bytes);
  impl_->inbound.push_back(&fake->value);
  Impl::Raise(impl_->channel_fd);
  return &fake->value;
}

std::vector<std::vector<uint8_t>> FakeDmeshState::Posts(
    dmesh_qp_t* qp) const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  return fake == nullptr ? std::vector<std::vector<uint8_t>>() : fake->posts;
}

size_t FakeDmeshState::alloc_calls(dmesh_qp_t* qp) const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  return fake == nullptr ? 0 : fake->alloc_calls;
}

size_t FakeDmeshState::flush_calls(dmesh_qp_t* qp) const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  auto* fake = impl_->FindQp(qp);
  return fake == nullptr ? 0 : fake->flush_calls;
}

size_t FakeDmeshState::release_count() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->releases;
}

size_t FakeDmeshState::destroy_count() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->destroys;
}

size_t FakeDmeshState::abort_count() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->aborts;
}

size_t FakeDmeshState::mid_batch_destroy_count() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->mid_batch_destroys;
}

size_t FakeDmeshState::poll_thread_violation_count() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->poll_thread_violations;
}

size_t FakeDmeshState::channel_destroy_count() const {
  std::lock_guard<std::mutex> lock(impl_->mu);
  return impl_->channel_destroys;
}

bool FakeDmeshState::WaitForChannelDestroyCount(
    size_t count, std::chrono::milliseconds timeout) {
  return WaitUntil(&impl_->mu, timeout,
                   [this, count] { return impl_->channel_destroys >= count; });
}

bool FakeDmeshState::WaitForPollThread(dmesh_qp_t* qp,
                                       std::chrono::milliseconds timeout,
                                       std::thread::id* thread) {
  return WaitUntil(&impl_->mu, timeout, [this, qp, thread] {
    auto* fake = impl_->FindQp(qp);
    if (fake == nullptr || fake->poll_thread == std::thread::id()) {
      return false;
    }
    *thread = fake->poll_thread;
    return true;
  });
}

std::unique_ptr<DmeshApiOps> MakeFakeDmeshApiOps(
    std::shared_ptr<FakeDmeshState> state) {
  return std::make_unique<FakeDmeshApiOps>(std::move(state));
}

}  // namespace dpumesh::grpc::testing
