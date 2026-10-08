#ifndef DPUMESH_GRPC_DMESH_STREAM_INTERNAL_H
#define DPUMESH_GRPC_DMESH_STREAM_INTERNAL_H

#include <memory>
#include <string>

#include "dmesh_api_ops.h"
#include "dpumesh_stream.h"

namespace dpumesh::grpc {

// dms_runtime_open() over caller-supplied native operations, for tests.
// Returns nullptr and sets `error` on failure.
dms_runtime* OpenStreamRuntime(std::unique_ptr<DmeshApiOps> ops,
                               std::string* error);

}  // namespace dpumesh::grpc

#endif  // DPUMESH_GRPC_DMESH_STREAM_INTERNAL_H
