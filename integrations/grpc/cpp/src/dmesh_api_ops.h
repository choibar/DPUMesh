#ifndef DPUMESH_GRPC_DMESH_API_OPS_H
#define DPUMESH_GRPC_DMESH_API_OPS_H

#include <cstdint>
#include <memory>

#include <dpumesh/dmesh.h>

namespace dpumesh::grpc {

// The only native-API seam used by the adapter, held at the public dmesh.h
// level.
class DmeshApiOps {
 public:
  virtual ~DmeshApiOps() = default;

  virtual dmesh_channel_t* CreateChannel() = 0;
  virtual int DestroyChannel(dmesh_channel_t* channel) = 0;
  virtual int PostMax(dmesh_channel_t* channel) = 0;
  virtual int ChannelFd(dmesh_channel_t* channel) = 0;
  virtual dmesh_qp_t* Accept(dmesh_channel_t* channel) = 0;
  virtual dmesh_qp_t* CreateQp(dmesh_channel_t* channel,
                               const char* target) = 0;
  virtual int QpFd(dmesh_qp_t* qp) = 0;
  virtual int DestroyQp(dmesh_qp_t* qp) = 0;
  virtual int AbortQp(dmesh_qp_t* qp) = 0;
  virtual void* Alloc(dmesh_qp_t* qp, uint32_t len) = 0;
  virtual int PostSend(dmesh_qp_t* qp, const void* buffer, uint32_t len) = 0;
  virtual int Flush(dmesh_qp_t* qp) = 0;
  virtual int PollQp(dmesh_qp_t* qp, dmesh_event_t* events,
                     int max_events) = 0;
  virtual void ReleaseRxBuffer(dmesh_channel_t* channel,
                               dmesh_event_t* event) = 0;
};

std::unique_ptr<DmeshApiOps> MakeNativeDmeshApiOps();

}  // namespace dpumesh::grpc

#endif  // DPUMESH_GRPC_DMESH_API_OPS_H
