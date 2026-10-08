// dms_runtime_open() over the native library, apart from dmesh_stream.cc so
// tests can link the stream ABI against fake operations.
#include <errno.h>

#include <algorithm>
#include <cstring>
#include <string>

#include "dmesh_api_ops.h"
#include "dmesh_stream_internal.h"
#include "dpumesh_stream.h"

extern "C" dms_runtime* dms_runtime_open(char* err, size_t err_len) {
  std::string error;
  dms_runtime* rt = dpumesh::grpc::OpenStreamRuntime(
      dpumesh::grpc::MakeNativeDmeshApiOps(), &error);
  if (rt == nullptr && err != nullptr && err_len != 0) {
    const size_t n = std::min(err_len - 1, error.size());
    std::memcpy(err, error.data(), n);
    err[n] = '\0';
  }
  return rt;
}
