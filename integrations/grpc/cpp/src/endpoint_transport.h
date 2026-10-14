#ifndef DPUMESH_GRPC_ENDPOINT_TRANSPORT_H
#define DPUMESH_GRPC_ENDPOINT_TRANSPORT_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/types/span.h"

namespace dpumesh::grpc {

// Queued receive bytes above which a sink asks its reactor to hold the native
// receive credit.
inline constexpr size_t kReceiveHighWaterBytes = 1024 * 1024;

// Result of handing one native receive to a sink.
struct ReceiveOutcome {
  absl::Status status;
  // True while the sink holds more queued bytes than its high-water mark. The
  // reactor then keeps that event's receive credit until a read drains the
  // queue, and the transport lands no further bytes on this connection.
  bool hold_credit = false;
};

// Receives one connection's events from its reactor: the gRPC endpoint driver,
// or a language binding through the stream C ABI. The reactor holds a sink
// weakly and locks it for each event, and never calls it while holding the
// connection's transmit lock.
class ConnectionSink {
 public:
  virtual ~ConnectionSink() = default;

  // Copies `length` bytes through `fill`, which writes exactly `length` bytes
  // at the pointer it receives. One call carries a whole run of receives.
  virtual ReceiveOutcome OnIncomingData(
      size_t length, absl::FunctionRef<void(uint8_t*)> fill) = 0;
  ReceiveOutcome OnIncomingData(absl::Span<const uint8_t> bytes) {
    return OnIncomingData(bytes.size(), [bytes](uint8_t* destination) {
      std::memcpy(destination, bytes.data(), bytes.size());
    });
  }
  // Hands over `length` received bytes already in memory; `release(arg)`
  // frees it once the sink is done with it. The default copies them.
  virtual ReceiveOutcome OnIncomingBuffer(uint8_t* data, size_t length,
                                          void (*release)(void*), void* arg) {
    ReceiveOutcome outcome =
        OnIncomingData(absl::Span<const uint8_t>(data, length));
    release(arg);
    return outcome;
  }
  virtual void OnWritable() = 0;
  virtual void OnRemoteEof() = 0;
  virtual void OnTransportError(absl::Status status) = 0;
};

enum class PostCode {
  kAccepted,
  kWouldBlock,
  kClosed,
  kError,
};

struct PostResult {
  PostCode code;
  absl::Status status;

  static PostResult Accepted() { return {PostCode::kAccepted, absl::OkStatus()}; }
  static PostResult WouldBlock() {
    return {PostCode::kWouldBlock, absl::OkStatus()};
  }
  static PostResult Closed(absl::Status status) {
    return {PostCode::kClosed, std::move(status)};
  }
  static PostResult Error(absl::Status status) {
    return {PostCode::kError, std::move(status)};
  }
};

// Registered transmit space, valid only for the duration of the fill callback.
struct Reservation {
  uint8_t* data = nullptr;
  size_t length = 0;
};

// Seam between a connection's consumer (the EventEngine endpoint state machine
// or a stream binding) and the EQ reactor. Post() and Flush() run on whichever
// thread pumps the write: initially the writer and, after native backpressure,
// the EQ owner that delivers TX_READY. Post(), Close() and BindSink() must not
// invoke the sink inline; reactor events are delivered separately.
class EndpointTransport {
 public:
  virtual ~EndpointTransport() = default;
  virtual void BindSink(std::weak_ptr<ConnectionSink> sink) = 0;
  virtual size_t MaxPostSize() const = 0;
  // Reserve `length` bytes of registered transmit space, invoke `fill` on it,
  // and submit it, holding the connection's transmit lock throughout. `fill`
  // must write every byte of the reservation and must not re-enter the
  // transport. It runs on kAccepted only; every other result leaves the
  // transport untouched and `fill` uncalled.
  virtual PostResult Post(size_t length,
                          absl::FunctionRef<void(Reservation)> fill) = 0;
  // Complete the logical write boundary. The DPUmesh transport validates that
  // the connection is still live but does not physically flush: Post already
  // transferred custody, and libdpumesh's idle/deadline policy owns batching.
  virtual absl::Status Flush() = 0;
  // Return the receive credit withheld above the endpoint's high-water mark.
  virtual void ResumeReceive() = 0;
  virtual void Close() = 0;
};

}  // namespace dpumesh::grpc

#endif  // DPUMESH_GRPC_ENDPOINT_TRANSPORT_H
