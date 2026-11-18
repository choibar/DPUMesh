#ifndef DPUMESH_GRPC_TESTS_FAKE_DMESH_OPS_H
#define DPUMESH_GRPC_TESTS_FAKE_DMESH_OPS_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dmesh_api_ops.h"

namespace dpumesh::grpc::testing {

class FakeDmeshApiOps;

// Thread-safe fake of the public DPUmesh API. The channel's fd and each QP's
// fd are real eventfds, readable while an accept or a poll has something to
// return, so reactor tests exercise the production epoll owner-thread loop
// rather than a separate test-only scheduler.
class FakeDmeshState final {
 public:
  FakeDmeshState();
  ~FakeDmeshState();

  FakeDmeshState(const FakeDmeshState&) = delete;
  FakeDmeshState& operator=(const FakeDmeshState&) = delete;

  void SetPostMax(int post_max);
  void FailNextCreateQp(int error_number);
  void FailNextAlloc(dmesh_qp_t* qp, int error_number);
  void SetAllocError(dmesh_qp_t* qp, int error_number);
  void FailNextPost(dmesh_qp_t* qp, int error_number);
  void FailNextPoll(dmesh_qp_t* qp, int error_number);

  bool WaitForClientQpCount(size_t count, std::chrono::milliseconds timeout);
  std::vector<dmesh_qp_t*> ClientQps() const;
  std::vector<std::string> ClientTargets() const;
  bool WaitForPostCount(dmesh_qp_t* qp, size_t count,
                        std::chrono::milliseconds timeout);
  bool WaitForFlushCount(dmesh_qp_t* qp, size_t count,
                         std::chrono::milliseconds timeout);
  bool WaitForAllocCallCount(dmesh_qp_t* qp, size_t count,
                             std::chrono::milliseconds timeout);
  bool WaitForReleaseCount(size_t count, std::chrono::milliseconds timeout);
  bool WaitForDestroyCount(size_t count, std::chrono::milliseconds timeout);
  bool WaitForChannelDestroyCount(size_t count,
                                  std::chrono::milliseconds timeout);
  // The thread that polled the QP first; empty until it has been polled.
  bool WaitForPollThread(dmesh_qp_t* qp, std::chrono::milliseconds timeout,
                         std::thread::id* thread);

  void InjectReceive(dmesh_qp_t* qp, const std::string& bytes);
  void InjectReceiveBatch(dmesh_qp_t* qp,
                          const std::vector<std::string>& receives);
  void InjectFin(dmesh_qp_t* qp);
  void InjectTxReady(dmesh_qp_t* qp);
  void InjectTxError(dmesh_qp_t* qp);
  // A stream for the channel to accept, holding first_bytes as a receive.
  dmesh_qp_t* InjectConnectionRequest(const std::string& first_bytes);

  std::vector<std::vector<uint8_t>> Posts(dmesh_qp_t* qp) const;
  size_t alloc_calls(dmesh_qp_t* qp) const;
  size_t flush_calls(dmesh_qp_t* qp) const;
  size_t release_count() const;
  size_t destroy_count() const;
  size_t abort_count() const;
  size_t mid_batch_destroy_count() const;
  size_t poll_thread_violation_count() const;
  size_t channel_destroy_count() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  friend std::unique_ptr<DmeshApiOps> MakeFakeDmeshApiOps(
      std::shared_ptr<FakeDmeshState> state);
  friend class FakeDmeshApiOps;
};

std::unique_ptr<DmeshApiOps> MakeFakeDmeshApiOps(
    std::shared_ptr<FakeDmeshState> state);

}  // namespace dpumesh::grpc::testing

#endif  // DPUMESH_GRPC_TESTS_FAKE_DMESH_OPS_H
