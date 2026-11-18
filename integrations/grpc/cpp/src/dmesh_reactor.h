#ifndef DPUMESH_GRPC_DMESH_REACTOR_H
#define DPUMESH_GRPC_DMESH_REACTOR_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "dmesh_api_ops.h"
#include "endpoint_transport.h"
#include "executor.h"

namespace dpumesh::grpc {

// One shard of QPs and its single owner thread. The owner waits on its QPs'
// fds, is the sole caller of dmesh_poll_qp() for them and owns their
// lifecycle; no QP is destroyed while a returned event batch can still name
// it. One reactor also listens: it waits on the channel's fd and accepts.
// Transmit operations run on the thread pumping the endpoint write -- the
// Endpoint::Write() caller or the owner delivering TX_READY -- under a
// per-connection lock, which the owner also takes before destroying that
// connection's QP.
class DmeshReactor final {
 public:
  struct Options {
    size_t event_batch_size = 64;
  };

  // Each field is sampled independently.
  struct Stats {
    // Receives that exceeded the per-connection credit-retention cap. The
    // connection is failed closed so its gRPC receive queue stays bounded.
    uint64_t receive_credit_hold_dropped = 0;
    // Drains that ended on the per-iteration poll budget with the QP non-empty.
    uint64_t drain_budget_exhausted = 0;
  };

  struct ConnectedTransport {
    std::unique_ptr<EndpointTransport> transport;
    // Retires this native connection independently of gRPC's Endpoint
    // ownership.  Client channels use it as an incarnation fence when the
    // last public channel reference is dropped: gRPC may retain an orphaned
    // Endpoint on its asynchronous cleanup schedule, but the native QP must
    // not remain live for that implementation detail.
    std::function<void()> release;
    // Carries the completions a gRPC call raises itself.
    std::shared_ptr<Executor> callback_executor;
    // Native identity of the two ends. The peer pair stays unset on a client
    // QP: the DPU selects and pins a backend for that stream, so the host
    // learns no peer pod.
    uint16_t local_port = 0;
    int peer_pod = -1;
    uint16_t peer_port = 0;
  };

  using ConnectCallback = absl::AnyInvocable<void(
      absl::StatusOr<ConnectedTransport>)>;
  using AcceptCallback = std::function<void(ConnectedTransport)>;
  // Places a stream the channel accepted on a reactor.
  using InboundRouter = std::function<void(dmesh_qp_t*)>;

  static absl::StatusOr<std::unique_ptr<DmeshReactor>> Create(
      DmeshApiOps* ops, dmesh_channel_t* channel, int post_max,
      std::shared_ptr<Executor> callback_executor, Options options);

  ~DmeshReactor();

  DmeshReactor(const DmeshReactor&) = delete;
  DmeshReactor& operator=(const DmeshReactor&) = delete;

  // QP creation and callback delivery are both asynchronous. The callback is
  // delivered on callback_executor, never on the owner thread.
  void Connect(std::string service, ConnectCallback callback);

  // Makes this the reactor that waits on the channel's fd: the session's
  // control work and the inbound streams, each handed to `route` on the owner
  // thread. Returns once the owner watches the fd.
  absl::Status Listen(InboundRouter route);

  // Takes an accepted QP: serves it with the accept callback, or destroys it
  // when none is installed. False when the reactor is shutting down; the QP
  // stays the caller's.
  bool Adopt(dmesh_qp_t* qp);

  // Installs or replaces the callback for native inbound connections. Returns
  // only after the owner has applied the change, so callers may advertise
  // readiness immediately after success. The callback runs on
  // callback_executor and receives the same transport shape as Connect(). An
  // empty callback rejects future connection requests.
  absl::Status SetAcceptCallback(AcceptCallback callback);

  // Idempotent. Existing connections receive a terminal error and their QPs
  // are closed on the owner thread before it exits.
  void Shutdown();

  Stats stats() const;

 private:
  class Impl;

  explicit DmeshReactor(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
};

}  // namespace dpumesh::grpc

#endif  // DPUMESH_GRPC_DMESH_REACTOR_H
