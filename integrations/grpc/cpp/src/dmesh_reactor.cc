#include "dmesh_reactor.h"

#include <errno.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <deque>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "endpoint_transport.h"

namespace dpumesh::grpc {
namespace {

// Receive events one connection may hold credit for. The credits belong to a
// landing ring shared with the other connections on this shard, so a stalled
// reader keeps its backpressure bounded and cannot withhold the ring.
constexpr size_t kMaxHeldReceives = 64;

// Bytes accepted before gRPC has constructed and bound the Endpoint. Connect
// and accept delivery are asynchronous, so a stalled callback executor must
// not turn that gap into an unbounded per-connection queue.
constexpr size_t kMaxPrebindBytes = kReceiveHighWaterBytes;

// Poll batches one loop iteration consumes from a QP before it returns to the
// command queue, so a saturated QP still yields to commands, the other QPs
// and the stop request. Its fd stays readable, so the next wait returns it.
constexpr int kQpBatchBudget = 8;

// Ready fds one epoll_wait returns.
constexpr int kReadyBatch = 64;

absl::Status ErrnoStatus(const char* operation, int error_number) {
  if (error_number == 0) error_number = EIO;
  const std::string message = absl::StrCat(
      operation, " failed: errno=", error_number, " (",
      std::generic_category().message(error_number), ")");
  switch (error_number) {
    case ENOENT:
      return absl::UnavailableError(message);
    case ENOMEM:
    case EMFILE:
    case ENOSPC:
      return absl::ResourceExhaustedError(message);
    case EBADMSG:
    case EIO:
      return absl::UnavailableError(message);
    case EINVAL:
      return absl::InternalError(message);
    default:
      return absl::UnknownError(message);
  }
}

void DrainCounterFd(int fd) {
  // One read resets an eventfd counter to zero.
  uint64_t value;
  const ssize_t result = ::read(fd, &value, sizeof(value));
  (void)result;
}

}  // namespace

class DmeshReactor::Impl final
    : public std::enable_shared_from_this<DmeshReactor::Impl> {
 public:
  struct Connection : std::enable_shared_from_this<Connection> {
    struct QueuedReceive {
      std::vector<uint8_t> bytes;
    };

    enum class Terminal {
      kNone,
      kRemoteEof,
      kError,
    };

    // Serializes transmit (Endpoint::Write caller or TX_READY owner) against
    // QP lifecycle (owner). Guards qp, closing and remote_eof; the owner
    // thread is the only writer of qp and closing, so its own unlocked reads
    // of them stay consistent. Never held across a driver call: the endpoint
    // takes its own state lock first, so the reverse order deadlocks.
    std::mutex tx_mu;
    dmesh_qp_t* qp = nullptr;
    // The QP's fd in the owner's epoll set; -1 once unwatched. Owner only.
    int fd = -1;
    std::weak_ptr<ConnectionSink> driver;
    std::deque<QueuedReceive> prebind_receives;
    size_t prebind_bytes = 0;
    // Receives whose credit is withheld while the endpoint queue is above its
    // high-water mark.
    std::deque<dmesh_event_t> held_receives;
    Terminal prebind_terminal = Terminal::kNone;
    absl::Status prebind_error = absl::OkStatus();
    std::atomic<bool> close_enqueued{false};
    bool closing = false;
    bool abort = false;
    bool remote_eof = false;
  };

  class Transport final : public EndpointTransport {
   public:
    Transport(std::weak_ptr<Impl> impl, std::shared_ptr<Connection> connection,
              size_t post_max)
        : impl_(std::move(impl)),
          connection_(std::move(connection)),
          post_max_(post_max) {}

    ~Transport() override { Close(); }

    void BindSink(std::weak_ptr<ConnectionSink> driver) override {
      if (auto impl = impl_.lock()) {
        impl->AttachDriver(connection_, std::move(driver));
      }
    }

    size_t MaxPostSize() const override { return post_max_; }

    PostResult Post(size_t length,
                    absl::FunctionRef<void(Reservation)> fill) override {
      if (auto impl = impl_.lock()) {
        return impl->Post(connection_, length, fill);
      }
      return PostResult::Closed(
          absl::UnavailableError("DPUmesh reactor no longer exists"));
    }

    absl::Status Flush() override {
      if (auto impl = impl_.lock()) {
        return impl->Flush(connection_);
      }
      return absl::UnavailableError("DPUmesh reactor no longer exists");
    }

    void ResumeReceive() override {
      if (auto impl = impl_.lock()) impl->ResumeReceive(connection_);
    }

    void Close() override {
      if (auto impl = impl_.lock()) impl->RequestClose(connection_);
    }

   private:
    std::weak_ptr<Impl> impl_;
    std::shared_ptr<Connection> connection_;
    const size_t post_max_;
  };

  Impl(DmeshApiOps* ops, dmesh_channel_t* channel, int post_max,
       std::shared_ptr<Executor> callback_executor, Options options)
      : ops_(ops),
        channel_(channel),
        post_max_(post_max),
        callback_executor_(std::move(callback_executor)),
        options_(options) {}

  ~Impl() { Shutdown(); }

  Stats stats() const {
    Stats snapshot;
    snapshot.receive_credit_hold_dropped =
        receive_credit_hold_dropped_.load(std::memory_order_relaxed);
    snapshot.drain_budget_exhausted =
        drain_budget_exhausted_.load(std::memory_order_relaxed);
    return snapshot;
  }

  absl::Status Start() {
    if (ops_ == nullptr || channel_ == nullptr ||
        callback_executor_ == nullptr) {
      return absl::InvalidArgumentError(
          "DPUmesh reactor requires ops, channel and an executor");
    }
    if (post_max_ <= 0 || options_.event_batch_size == 0 ||
        options_.event_batch_size > static_cast<size_t>(std::numeric_limits<int>::max())) {
      return absl::InvalidArgumentError("invalid DPUmesh reactor options");
    }

    event_buffer_.resize(options_.event_batch_size);
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) return ErrnoStatus("epoll_create1", errno);
    command_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (command_fd_ < 0) {
      const absl::Status status = ErrnoStatus("eventfd", errno);
      CloseFds();
      return status;
    }
    if (!Watch(command_fd_, &command_fd_)) {
      const absl::Status status = ErrnoStatus("epoll_ctl", errno);
      CloseFds();
      return status;
    }
    accepting_.store(true, std::memory_order_release);
    try {
      owner_thread_ = std::thread([self = shared_from_this()] {
        // Named so per-thread CPU shows the owner.
        pthread_setname_np(pthread_self(), "dmesh-reactor");
        self->ThreadMain();
      });
    } catch (const std::system_error& error) {
      accepting_.store(false, std::memory_order_release);
      CloseFds();
      return absl::InternalError(
          absl::StrCat("failed to start DPUmesh reactor thread: ",
                       error.what()));
    }
    return absl::OkStatus();
  }

  bool Enqueue(absl::AnyInvocable<void()> task) {
    if (!accepting_.load(std::memory_order_acquire)) return false;
    std::lock_guard<std::mutex> lock(command_mu_);
    if (!accepting_.load(std::memory_order_relaxed)) return false;
    const bool was_empty = commands_.empty();
    commands_.push_back(std::move(task));
    /* The eventfd counter stays signalled until the owner drains it, so only
     * the empty→non-empty transition needs a wake. The wake and Shutdown()'s
     * close both run under command_mu_. */
    if (was_empty) WakeCommandFdLocked();
    return true;
  }

  void Connect(std::string service, ConnectCallback callback) {
    if (service.empty()) {
      DeliverConnect(
          std::move(callback),
          absl::InvalidArgumentError("DPUmesh service name is empty"));
      return;
    }
    if (!accepting_.load(std::memory_order_acquire)) {
      DeliverConnect(
          std::move(callback),
          absl::UnavailableError("DPUmesh reactor is shutting down"));
      return;
    }
    auto callback_holder =
        std::make_shared<ConnectCallback>(std::move(callback));
    if (!Enqueue([self = shared_from_this(), service = std::move(service),
                  callback_holder]() mutable {
          self->ConnectOwner(std::move(service),
                             std::move(*callback_holder));
        })) {
      DeliverConnect(
          std::move(*callback_holder),
          absl::UnavailableError("DPUmesh reactor is shutting down"));
    }
  }

  absl::Status Listen(InboundRouter route) {
    auto applied = std::make_shared<std::promise<absl::Status>>();
    auto ready = applied->get_future();
    if (!Enqueue([self = shared_from_this(), route = std::move(route),
                  applied]() mutable {
          applied->set_value(self->ListenOwner(std::move(route)));
        })) {
      return absl::UnavailableError("DPUmesh reactor is shutting down");
    }
    return ready.get();
  }

  bool Adopt(dmesh_qp_t* qp) {
    return Enqueue([self = shared_from_this(), qp] { self->AdoptOwner(qp); });
  }

  absl::Status SetAcceptCallback(AcceptCallback callback) {
    auto applied = std::make_shared<std::promise<void>>();
    auto ready = applied->get_future();
    if (!Enqueue([self = shared_from_this(), callback = std::move(callback),
                  applied = std::move(applied)]() mutable {
          self->accept_callback_ = std::move(callback);
          applied->set_value();
        })) {
      return absl::UnavailableError("DPUmesh reactor is shutting down");
    }
    ready.wait();
    return absl::OkStatus();
  }

  void AttachDriver(std::shared_ptr<Connection> connection,
                    std::weak_ptr<ConnectionSink> driver) {
    std::weak_ptr<ConnectionSink> driver_on_failure = driver;
    if (!Enqueue([self = shared_from_this(), connection = std::move(connection),
                  driver = std::move(driver)]() mutable {
          self->AttachDriverOwner(connection, std::move(driver));
        })) {
      callback_executor_->Run(
          [driver = std::move(driver_on_failure)]() mutable {
            if (auto bound = driver.lock()) {
              bound->OnTransportError(absl::UnavailableError(
                  "DPUmesh reactor is shutting down"));
            }
          });
    }
  }

  PostResult Post(const std::shared_ptr<Connection>& connection, size_t length,
                  absl::FunctionRef<void(Reservation)> fill) {
    if (!accepting_.load(std::memory_order_acquire)) {
      return PostResult::Closed(
          absl::UnavailableError("DPUmesh reactor is shutting down"));
    }
    if (length == 0 || length > static_cast<size_t>(post_max_) ||
        length > std::numeric_limits<uint32_t>::max()) {
      return PostResult::Error(
          absl::InternalError("invalid DPUmesh post length"));
    }
    /* The lock spans reserve, fill and submit: a QP holds one live reservation
     * at a time, and the owner takes the same lock before destroying it. */
    std::lock_guard<std::mutex> lock(connection->tx_mu);
    if (connection->closing || connection->qp == nullptr) {
      return PostResult::Closed(
          absl::UnavailableError("DPUmesh connection is closed"));
    }

    /* Cleared so the EAGAIN test below reads this call's errno. */
    errno = 0;
    void* destination =
        ops_->Alloc(connection->qp, static_cast<uint32_t>(length));
    if (destination == nullptr) {
      const int error_number = errno;
      if (error_number == EAGAIN) {
        /* A closed peer returns no more credits, so the armed TX_READY would
         * never fire. */
        if (connection->remote_eof) {
          return PostResult::Error(absl::UnavailableError(
              "DPUmesh peer closed while transmit was blocked"));
        }
        return PostResult::WouldBlock();
      }
      return PostResult::Error(ErrnoStatus("dmesh_alloc", error_number));
    }

    fill(Reservation{static_cast<uint8_t*>(destination), length});

    errno = 0;
    if (ops_->PostSend(connection->qp, destination,
                       static_cast<uint32_t>(length)) != 0) {
      return PostResult::Error(ErrnoStatus("dmesh_post_send", errno));
    }
    return PostResult::Accepted();
  }

  /* PostSend already transferred custody. A Write boundary (headers, DATA,
   * trailers) is not a physical flush: libdpumesh publishes an idle tail
   * immediately and holds a busy tail to its own bounded deadline. */
  absl::Status Flush(const std::shared_ptr<Connection>& connection) {
    std::lock_guard<std::mutex> lock(connection->tx_mu);
    if (connection->closing || connection->qp == nullptr)
      return absl::UnavailableError("DPUmesh connection is closed");
    return absl::OkStatus();
  }

  void ResumeReceive(std::shared_ptr<Connection> connection) {
    Enqueue([self = shared_from_this(), connection = std::move(connection)] {
      self->ReleaseHeldReceives(connection);
    });
  }

  void RequestClose(const std::shared_ptr<Connection>& connection) {
    if (connection->close_enqueued.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    Enqueue([self = shared_from_this(), connection] {
      self->RequestCloseOwner(connection);
    });
  }

  void RequestAbort(const std::shared_ptr<Connection>& connection) {
    if (connection->close_enqueued.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    Enqueue([self = shared_from_this(), connection] {
      self->RequestAbortOwner(connection);
    });
  }

  void Shutdown() {
    std::lock_guard<std::mutex> shutdown_lock(shutdown_mu_);
    bool expected = true;
    if (!accepting_.compare_exchange_strong(
            expected, false, std::memory_order_acq_rel)) {
      if (owner_thread_.joinable() &&
          std::this_thread::get_id() != owner_thread_.get_id()) {
        owner_thread_.join();
      }
      return;
    }

    /* Commands accepted before accepting_ fell still run: ShutdownOwner()
     * drains the queue. */
    {
      std::lock_guard<std::mutex> lock(command_mu_);
      stop_requested_.store(true, std::memory_order_release);
      WakeCommandFdLocked();
    }
    if (owner_thread_.joinable()) owner_thread_.join();

    std::lock_guard<std::mutex> lock(command_mu_);
    CloseFds();
  }

 private:
  // The owner's epoll set holds the command fd, each live QP's fd and, on
  // the listening reactor, the channel's fd; `tag` tells them apart.
  bool Watch(int fd, void* tag) {
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.ptr = tag;
    return ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == 0;
  }

  void Unwatch(int fd) {
    if (fd >= 0) (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
  }

  // Requires command_mu_ once the owner thread exists.
  void CloseFds() {
    if (command_fd_ >= 0) ::close(command_fd_);
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
    command_fd_ = epoll_fd_ = -1;
  }

  void ReleaseHeldReceives(const std::shared_ptr<Connection>& connection) {
    for (dmesh_event_t& event : connection->held_receives) {
      ops_->ReleaseRxBuffer(channel_, &event);
    }
    connection->held_receives.clear();
  }

  // Requires command_mu_.
  void WakeCommandFdLocked() {
    if (command_fd_ < 0) return;
    const uint64_t one = 1;
    const ssize_t result = ::write(command_fd_, &one, sizeof(one));
    (void)result;
  }

  void DeliverConnect(ConnectCallback callback,
                      absl::StatusOr<ConnectedTransport> result) {
    callback_executor_->Run(
        [callback = std::move(callback), result = std::move(result)]() mutable {
          callback(std::move(result));
        });
  }

  void DeliverAccept(ConnectedTransport connected) {
    AcceptCallback callback = accept_callback_;
    callback_executor_->Run(
        [callback = std::move(callback),
         connected = std::move(connected)]() mutable {
          callback(std::move(connected));
        });
  }

  void ConnectOwner(std::string service, ConnectCallback callback) {
    if (!accepting_.load(std::memory_order_acquire) ||
        stop_requested_.load(std::memory_order_acquire)) {
      DeliverConnect(
          std::move(callback),
          absl::UnavailableError("DPUmesh reactor is shutting down"));
      return;
    }

    errno = 0;
    dmesh_qp_t* qp = ops_->CreateQp(channel_, service.c_str());
    if (qp == nullptr) {
      DeliverConnect(std::move(callback),
                     ErrnoStatus("dmesh_create_qp", errno));
      return;
    }
    auto connection = AddConnection(qp);
    if (!connection.ok()) {
      DeliverConnect(std::move(callback), connection.status());
      return;
    }
    DeliverConnect(std::move(callback), MakeConnectedTransport(*connection));
  }

  // Owns a new QP and watches its fd; on failure the QP is destroyed.
  absl::StatusOr<std::shared_ptr<Connection>> AddConnection(dmesh_qp_t* qp) {
    errno = 0;
    const int fd = ops_->QpFd(qp);
    if (fd < 0) {
      const absl::Status status = ErrnoStatus("dmesh_qp_fd", errno);
      ops_->DestroyQp(qp);
      return status;
    }
    auto connection = std::make_shared<Connection>();
    if (!Watch(fd, connection.get())) {
      const absl::Status status = ErrnoStatus("epoll_ctl", errno);
      ops_->DestroyQp(qp);
      return status;
    }
    connection->qp = qp;
    connection->fd = fd;
    connections_.emplace(qp, connection);
    return connection;
  }

  absl::Status ListenOwner(InboundRouter route) {
    if (channel_fd_ >= 0) return absl::OkStatus();
    errno = 0;
    const int fd = ops_->ChannelFd(channel_);
    if (fd < 0) return ErrnoStatus("dmesh_channel_fd", errno);
    if (!Watch(fd, &channel_fd_)) return ErrnoStatus("epoll_ctl", errno);
    channel_fd_ = fd;
    route_ = std::move(route);
    return absl::OkStatus();
  }

  // The channel's work and its inbound streams. A session that is gone stops
  // the watch: each QP's own poll then reports the loss.
  void AcceptAll() {
    for (;;) {
      errno = 0;
      dmesh_qp_t* qp = ops_->Accept(channel_);
      if (qp == nullptr) {
        if (errno != EAGAIN) {
          Unwatch(channel_fd_);
          channel_fd_ = -1;
        }
        return;
      }
      route_(qp);
    }
  }

  void AdoptOwner(dmesh_qp_t* qp) {
    if (!accept_callback_ || stop_requested_.load(std::memory_order_acquire)) {
      ops_->DestroyQp(qp);
      return;
    }
    auto connection = AddConnection(qp);
    if (connection.ok()) DeliverAccept(MakeConnectedTransport(*connection));
  }

  ConnectedTransport MakeConnectedTransport(
      const std::shared_ptr<Connection>& connection) {
    ConnectedTransport connected;
    connected.transport = std::make_unique<Transport>(
        weak_from_this(), connection, static_cast<size_t>(post_max_));
    connected.release = [impl = weak_from_this(), connection] {
      if (auto self = impl.lock()) self->RequestAbort(connection);
    };
    connected.callback_executor = callback_executor_;
    connected.local_port = connection->qp->local_port;
    connected.peer_pod = connection->qp->remote_pod;
    connected.peer_port = connection->qp->remote_port;
    return connected;
  }

  void AttachDriverOwner(const std::shared_ptr<Connection>& connection,
                         std::weak_ptr<ConnectionSink> driver) {
    connection->driver = std::move(driver);
    auto bound_driver = connection->driver.lock();
    if (bound_driver == nullptr) return;

    while (!connection->prebind_receives.empty()) {
      auto receive = std::move(connection->prebind_receives.front());
      connection->prebind_receives.pop_front();
      connection->prebind_bytes -= receive.bytes.size();
      const ReceiveOutcome outcome = bound_driver->OnIncomingData(
          absl::MakeConstSpan(receive.bytes));
      if (!outcome.status.ok()) {
        if (!connection->closing) {
          FailConnectionOwner(connection, outcome.status);
        }
        return;
      }
    }

    if (connection->prebind_terminal == Connection::Terminal::kRemoteEof) {
      bound_driver->OnRemoteEof();
    } else if (connection->prebind_terminal == Connection::Terminal::kError) {
      bound_driver->OnTransportError(connection->prebind_error);
    } else if (connection->closing || connection->qp == nullptr) {
      bound_driver->OnTransportError(
          absl::UnavailableError("DPUmesh connection closed before binding"));
    }
    connection->prebind_terminal = Connection::Terminal::kNone;
    connection->prebind_error = absl::OkStatus();
  }

  void RequestCloseOwner(const std::shared_ptr<Connection>& connection) {
    connection->close_enqueued.store(true, std::memory_order_release);
    if (connection->closing || connection->qp == nullptr) return;
    {
      std::lock_guard<std::mutex> lock(connection->tx_mu);
      connection->closing = true;
    }
    deferred_closes_.push_back(connection);
  }

  void RequestAbortOwner(const std::shared_ptr<Connection>& connection) {
    connection->close_enqueued.store(true, std::memory_order_release);
    if (connection->closing || connection->qp == nullptr) return;
    {
      std::lock_guard<std::mutex> lock(connection->tx_mu);
      connection->closing = true;
      connection->abort = true;
    }
    deferred_closes_.push_back(connection);
  }

  void FailConnectionOwner(const std::shared_ptr<Connection>& connection,
                           absl::Status status) {
    if (connection->closing || connection->qp == nullptr) return;
    if (status.ok()) status = absl::UnknownError("DPUmesh connection failed");
    connection->close_enqueued.store(true, std::memory_order_release);
    {
      std::lock_guard<std::mutex> lock(connection->tx_mu);
      connection->closing = true;
    }
    if (auto driver = connection->driver.lock()) {
      driver->OnTransportError(status);
    } else {
      connection->prebind_terminal = Connection::Terminal::kError;
      connection->prebind_error = std::move(status);
    }
    deferred_closes_.push_back(connection);
  }

  // Leading events that are receives: one run is handed to the endpoint as
  // one write.
  int ReceiveRunLength(const dmesh_event_t* events, int available) const {
    int length = 0;
    while (length < available && events[length].type == DMESH_EVENT_RECV) {
      ++length;
    }
    return length;
  }

  void ReleaseRun(dmesh_event_t* events, int count) {
    for (int i = 0; i < count; ++i) ops_->ReleaseRxBuffer(channel_, &events[i]);
  }

  bool QueuePrebindReceive(const std::shared_ptr<Connection>& connection,
                           dmesh_event_t* event) {
    if (event->len > kMaxPrebindBytes - connection->prebind_bytes) {
      ops_->ReleaseRxBuffer(channel_, event);
      FailConnectionOwner(
          connection,
          absl::ResourceExhaustedError(
              "DPUmesh endpoint binding did not keep up with receive data"));
      return false;
    }
    Connection::QueuedReceive queued;
    try {
      queued.bytes.assign(event->buf, event->buf + event->len);
      connection->prebind_receives.push_back(std::move(queued));
      connection->prebind_bytes += event->len;
    } catch (const std::bad_alloc&) {
      ops_->ReleaseRxBuffer(channel_, event);
      FailConnectionOwner(
          connection,
          absl::ResourceExhaustedError(
              "failed to buffer DPUmesh receive before endpoint binding"));
      return false;
    }
    ops_->ReleaseRxBuffer(channel_, event);
    return true;
  }

  void HandleReceiveRun(const std::shared_ptr<Connection>& connection,
                        dmesh_event_t* events, int count) {
    if (connection->closing || connection->remote_eof) {
      ReleaseRun(events, count);
      if (!connection->closing) {
        FailConnectionOwner(
            connection,
            absl::UnavailableError("DPUmesh delivered data after remote FIN"));
      }
      return;
    }

    size_t length = 0;
    for (int i = 0; i < count; ++i) {
      if (events[i].len == 0 || events[i].buf == nullptr) {
        ReleaseRun(events, count);
        FailConnectionOwner(
            connection,
            absl::InternalError("invalid DPUmesh receive event"));
        return;
      }
      length += events[i].len;
    }

    auto driver = connection->driver.lock();
    if (driver == nullptr) {
      for (int i = 0; i < count; ++i) {
        if (!QueuePrebindReceive(connection, &events[i])) {
          ReleaseRun(events + i + 1, count - i - 1);
          return;
        }
      }
      return;
    }

    const ReceiveOutcome outcome =
        driver->OnIncomingData(length, [events, count](uint8_t* destination) {
          for (int i = 0; i < count; ++i) {
            std::memcpy(destination, events[i].buf, events[i].len);
            destination += events[i].len;
          }
        });

    /* A withheld credit stops the transport landing more bytes for this
     * connection until a read drains the endpoint queue. */
    const bool hold = outcome.hold_credit && outcome.status.ok();
    bool credit_overflow = false;
    for (int i = 0; i < count; ++i) {
      if (hold && connection->held_receives.size() < kMaxHeldReceives) {
        connection->held_receives.push_back(events[i]);
      } else {
        if (hold) {
          receive_credit_hold_dropped_.fetch_add(1, std::memory_order_relaxed);
          credit_overflow = true;
        }
        ops_->ReleaseRxBuffer(channel_, &events[i]);
      }
    }
    if (!outcome.status.ok()) {
      FailConnectionOwner(connection, outcome.status);
    } else if (credit_overflow) {
      /* Returning credits while the endpoint remains above its high-water mark
       * would let a non-cooperating peer grow the gRPC slice queue without a
       * bound. The retention cap is a fail-closed guard. */
      FailConnectionOwner(
          connection,
          absl::ResourceExhaustedError(
              "DPUmesh receive backpressure retention limit exceeded"));
    }
  }

  void HandleEvent(const std::shared_ptr<Connection>& connection,
                   const dmesh_event_t* event) {
    if (event->type == DMESH_EVENT_TX_READY) {
      if (connection->closing || connection->qp == nullptr) return;
      /* Native EAGAIN arms one one-shot event. The endpoint knows whether a
       * write is actually parked, so the hint is forwarded as-is and a stale
       * one is dropped there. */
      if (auto driver = connection->driver.lock()) driver->OnWritable();
      return;
    }

    if (event->type == DMESH_EVENT_TX_ERROR) {
      FailConnectionOwner(
          connection,
          absl::UnavailableError("DPUmesh background transmit failed"));
      return;
    }

    if (event->type == DMESH_EVENT_RECV_FIN) {
      if (connection->closing || connection->remote_eof) return;
      {
        /* Under tx_mu: a post that blocks after this point fails rather than
         * parks. A write already parked is failed by the endpoint from
         * OnRemoteEof. */
        std::lock_guard<std::mutex> lock(connection->tx_mu);
        connection->remote_eof = true;
      }
      if (auto driver = connection->driver.lock()) {
        driver->OnRemoteEof();
      } else {
        connection->prebind_terminal = Connection::Terminal::kRemoteEof;
      }
      return;
    }

    /* The DPU's migration and scaling requests ask for streams this adapter
     * does not open: without a spare the DPU drains the stream (GOAWAY) and
     * gRPC reconnects; a SCALE goes unapplied. */
    if (event->type == DMESH_EVENT_PREPARE || event->type == DMESH_EVENT_SCALE) return;

    FailConnectionOwner(
        connection, absl::InternalError("unknown DPUmesh event type"));
  }

  // Consumes at most kQpBatchBudget poll batches. An empty poll settles the
  // QP's fd for the next wait.
  void DrainQp(const std::shared_ptr<Connection>& connection) {
    std::vector<dmesh_event_t>& events = event_buffer_;
    for (int batch = 0; batch < kQpBatchBudget; ++batch) {
      errno = 0;
      const int count = ops_->PollQp(
          connection->qp, events.data(), static_cast<int>(events.size()));
      if (count == 0) return;
      if (count < 0) {
        FailConnectionOwner(connection, ErrnoStatus("dmesh_poll_qp", errno));
        return;
      }
      if (count > static_cast<int>(events.size())) {
        FailConnectionOwner(
            connection,
            absl::InternalError("dmesh_poll_qp exceeded event capacity"));
        return;
      }
      for (int i = 0; i < count;) {
        const int run = ReceiveRunLength(&events[i], count - i);
        if (run > 0) {
          HandleReceiveRun(connection, &events[i], run);
          i += run;
        } else {
          HandleEvent(connection, &events[i]);
          ++i;
        }
      }
    }
    drain_budget_exhausted_.fetch_add(1, std::memory_order_relaxed);
  }

  void SweepDeferredCloses() {
    for (const auto& connection : deferred_closes_) {
      dmesh_qp_t* qp = connection->qp;
      if (qp == nullptr) continue;
      ReleaseHeldReceives(connection);
      connection->driver.reset();
      connections_.erase(qp);
      Unwatch(connection->fd);
      connection->fd = -1;
      {
        /* tx_mu spans the destroy, so a transmit racing the close either
         * completes in full against a live QP or observes qp == nullptr. */
        std::lock_guard<std::mutex> lock(connection->tx_mu);
        connection->qp = nullptr;
        if (connection->abort) {
          ops_->AbortQp(qp);
        } else {
          ops_->DestroyQp(qp);
        }
      }
    }
    deferred_closes_.clear();
  }

  void DrainCommands() {
    /* One batch per loop cycle: tasks enqueued while this batch runs execute
     * on the next cycle, after their empty→non-empty wake is observed by
     * epoll_wait(). */
    std::deque<absl::AnyInvocable<void()>> batch;
    {
      std::lock_guard<std::mutex> lock(command_mu_);
      batch.swap(commands_);
    }
    for (auto& task : batch) task();
  }

  void ShutdownOwner() {
    /* Run every command accepted before shutdown; queued lambdas hold owning
     * references to this Impl and must not outlive the thread. */
    for (;;) {
      bool empty;
      {
        std::lock_guard<std::mutex> lock(command_mu_);
        empty = commands_.empty();
      }
      if (empty) break;
      DrainCommands();
    }

    const absl::Status status =
        absl::CancelledError("DPUmesh reactor shutdown");
    std::vector<std::shared_ptr<Connection>> connections;
    connections.reserve(connections_.size());
    for (const auto& entry : connections_) connections.push_back(entry.second);
    for (const auto& connection : connections) {
      if (connection->closing) continue;
      connection->close_enqueued.store(true, std::memory_order_release);
      {
        std::lock_guard<std::mutex> lock(connection->tx_mu);
        connection->closing = true;
      }
      if (auto driver = connection->driver.lock()) {
        driver->OnTransportError(status);
      } else {
        connection->prebind_terminal = Connection::Terminal::kError;
        connection->prebind_error = status;
      }
      deferred_closes_.push_back(connection);
    }
    SweepDeferredCloses();
  }

  void ThreadMain() {
    epoll_event ready[kReadyBatch];
    while (!stop_requested_.load(std::memory_order_acquire)) {
      const int count = ::epoll_wait(epoll_fd_, ready, kReadyBatch, -1);
      if (count < 0) {
        if (errno == EINTR) continue;
        stop_requested_.store(true, std::memory_order_release);
        break;
      }

      bool channel_ready = false;
      for (int i = 0; i < count; ++i) {
        void* const tag = ready[i].data.ptr;
        if (tag == &command_fd_) {
          DrainCounterFd(command_fd_);
        } else if (tag == &channel_fd_) {
          channel_ready = true;
        } else {
          ready_qps_.push_back(
              static_cast<Connection*>(tag)->shared_from_this());
        }
      }

      DrainCommands();
      if (stop_requested_.load(std::memory_order_acquire)) break;

      if (channel_ready && channel_fd_ >= 0) AcceptAll();
      for (const auto& connection : ready_qps_) {
        if (connection->qp != nullptr) DrainQp(connection);
      }
      ready_qps_.clear();
      SweepDeferredCloses();
    }

    ShutdownOwner();
  }

  DmeshApiOps* const ops_;
  dmesh_channel_t* const channel_;
  const int post_max_;
  const std::shared_ptr<Executor> callback_executor_;
  const Options options_;

  std::atomic<uint64_t> receive_credit_hold_dropped_{0};
  std::atomic<uint64_t> drain_budget_exhausted_{0};

  int epoll_fd_ = -1;
  int command_fd_ = -1;
  // The channel's fd while this reactor listens, else -1. Owner only.
  int channel_fd_ = -1;
  InboundRouter route_;
  std::thread owner_thread_;
  std::atomic<bool> accepting_{false};
  // Set by Shutdown() on any thread, consumed by the owner loop.
  std::atomic<bool> stop_requested_{false};

  std::mutex shutdown_mu_;
  std::mutex command_mu_;
  std::deque<absl::AnyInvocable<void()>> commands_;

  std::vector<dmesh_event_t> event_buffer_;
  std::unordered_map<dmesh_qp_t*, std::shared_ptr<Connection>> connections_;
  // The QPs the last wait returned; reused across iterations.
  std::vector<std::shared_ptr<Connection>> ready_qps_;
  std::vector<std::shared_ptr<Connection>> deferred_closes_;
  AcceptCallback accept_callback_;
};

absl::StatusOr<std::unique_ptr<DmeshReactor>> DmeshReactor::Create(
    DmeshApiOps* ops, dmesh_channel_t* channel, int post_max,
    std::shared_ptr<Executor> callback_executor, Options options) {
  auto impl = std::make_shared<Impl>(ops, channel, post_max,
                                     std::move(callback_executor), options);
  auto reactor = std::unique_ptr<DmeshReactor>(new DmeshReactor(impl));
  const absl::Status status = impl->Start();
  if (!status.ok()) return status;
  return reactor;
}

DmeshReactor::DmeshReactor(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

DmeshReactor::~DmeshReactor() { Shutdown(); }

void DmeshReactor::Connect(std::string service, ConnectCallback callback) {
  impl_->Connect(std::move(service), std::move(callback));
}

absl::Status DmeshReactor::Listen(InboundRouter route) {
  return impl_->Listen(std::move(route));
}

bool DmeshReactor::Adopt(dmesh_qp_t* qp) { return impl_->Adopt(qp); }

absl::Status DmeshReactor::SetAcceptCallback(AcceptCallback callback) {
  return impl_->SetAcceptCallback(std::move(callback));
}

void DmeshReactor::Shutdown() {
  if (impl_ != nullptr) impl_->Shutdown();
}

DmeshReactor::Stats DmeshReactor::stats() const {
  return impl_ != nullptr ? impl_->stats() : Stats{};
}

}  // namespace dpumesh::grpc
