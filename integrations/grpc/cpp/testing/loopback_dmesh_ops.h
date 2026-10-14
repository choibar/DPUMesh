#ifndef DPUMESH_GRPC_TESTING_LOOPBACK_DMESH_OPS_H
#define DPUMESH_GRPC_TESTING_LOOPBACK_DMESH_OPS_H

#include <memory>

#include "dmesh_api_ops.h"
#include "dmesh_stream_internal.h"

namespace dpumesh::grpc {

// Native operations that route every stream a process opens back to itself.
std::unique_ptr<DmeshApiOps> MakeLoopbackDmeshApiOps();

}  // namespace dpumesh::grpc

#endif  // DPUMESH_GRPC_TESTING_LOOPBACK_DMESH_OPS_H
