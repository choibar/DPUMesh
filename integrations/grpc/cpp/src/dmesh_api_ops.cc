#include "dmesh_api_ops.h"

namespace dpumesh::grpc {
namespace {

class NativeDmeshApiOps final : public DmeshApiOps {
 public:
  dmesh_channel_t* CreateChannel() override { return dmesh_create_channel(); }
  int DestroyChannel(dmesh_channel_t* channel) override {
    return dmesh_destroy_channel(channel);
  }
  int PostMax(dmesh_channel_t* channel) override {
    return dmesh_post_max(channel);
  }
  int ChannelFd(dmesh_channel_t* channel) override {
    return dmesh_channel_fd(channel);
  }
  dmesh_qp_t* Accept(dmesh_channel_t* channel) override {
    return dmesh_accept(channel);
  }
  dmesh_qp_t* CreateQp(dmesh_channel_t* channel, const char* target) override {
    return dmesh_create_qp(channel, target);
  }
  int QpFd(dmesh_qp_t* qp) override { return dmesh_qp_fd(qp); }
  int DestroyQp(dmesh_qp_t* qp) override { return dmesh_destroy_qp(qp); }
  int AbortQp(dmesh_qp_t* qp) override { return dmesh_abort_qp(qp); }
  void* Alloc(dmesh_qp_t* qp, uint32_t len) override {
    return dmesh_alloc(qp, len);
  }
  int PostSend(dmesh_qp_t* qp, const void* buffer, uint32_t len) override {
    return dmesh_post_send(qp, buffer, len);
  }
  int Flush(dmesh_qp_t* qp) override { return dmesh_flush(qp); }
  int PollQp(dmesh_qp_t* qp, dmesh_event_t* events,
             int max_events) override {
    return dmesh_poll_qp(qp, events, max_events);
  }
  void ReleaseRxBuffer(dmesh_channel_t* channel,
                       dmesh_event_t* event) override {
    dmesh_release_rx_buffer(channel, event);
  }
};

}  // namespace

std::unique_ptr<DmeshApiOps> MakeNativeDmeshApiOps() {
  return std::make_unique<NativeDmeshApiOps>();
}

}  // namespace dpumesh::grpc
