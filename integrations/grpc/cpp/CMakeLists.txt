cmake_minimum_required(VERSION 3.20)

project(grpc_dpumesh VERSION 0.3.0 LANGUAGES C CXX)

include(CTest)

set(DPUMESH_GRPC_REQUIRED_VERSION "1.80.0")
set(DPUMESH_GRPC_SOURCE_DIR "" CACHE PATH
    "Path to an exact gRPC v1.80.0 source checkout")
set(DPUMESH_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../.." CACHE PATH
    "Path to the DPUmesh source tree")
option(DPUMESH_GRPC_ENABLE_SANITIZERS
       "Enable AddressSanitizer and UndefinedBehaviorSanitizer for the full build"
       OFF)
option(DPUMESH_GRPC_ENABLE_TSAN
       "Enable ThreadSanitizer for the full build"
       OFF)
option(DPUMESH_GRPC_BUILD_EXAMPLES "Build the hello gRPC example" ON)

# libdpumesh_stream.so links the reactor and abseil statically.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(DPUMESH_GRPC_ENABLE_SANITIZERS AND DPUMESH_GRPC_ENABLE_TSAN)
  message(FATAL_ERROR
    "ASan/UBSan and ThreadSanitizer must use separate build configurations")
endif()
if(DPUMESH_GRPC_ENABLE_TSAN AND NOT DPUMESH_GRPC_SOURCE_DIR)
  message(FATAL_ERROR
    "ThreadSanitizer requires DPUMESH_GRPC_SOURCE_DIR so gRPC is instrumented")
endif()
if(DPUMESH_GRPC_ENABLE_SANITIZERS AND NOT DPUMESH_GRPC_SOURCE_DIR)
  message(WARNING
    "DPUMESH_GRPC_SOURCE_DIR is unset; the prebuilt gRPC library cannot be instrumented")
endif()

# Sanitizer options are directory-wide so add_subdirectory() also instruments
# the vendored gRPC source. With a prebuilt gRPC package, only this project's
# targets can be instrumented.
if(DPUMESH_GRPC_ENABLE_SANITIZERS)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(DPUMESH_GRPC_ENABLE_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()

if(DPUMESH_GRPC_SOURCE_DIR)
  if(NOT EXISTS "${DPUMESH_GRPC_SOURCE_DIR}/include/grpc/event_engine/event_engine.h")
    message(FATAL_ERROR
      "DPUMESH_GRPC_SOURCE_DIR does not contain the EventEngine headers")
  endif()

  file(STRINGS "${DPUMESH_GRPC_SOURCE_DIR}/CMakeLists.txt"
       _grpc_version_line REGEX "^set\\(PACKAGE_VERSION[ \\t]+\"[^\"]+\"\\)")
  string(REGEX REPLACE ".*\"([^\"]+)\".*" "\\1"
         _grpc_source_version "${_grpc_version_line}")
  if(NOT _grpc_source_version VERSION_EQUAL DPUMESH_GRPC_REQUIRED_VERSION)
    message(FATAL_ERROR
      "gRPC ${DPUMESH_GRPC_REQUIRED_VERSION} is required; source is ${_grpc_source_version}")
  endif()

  set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  if(DPUMESH_GRPC_BUILD_EXAMPLES)
    set(gRPC_BUILD_CODEGEN ON CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_CPP_PLUGIN ON CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
  else()
    set(gRPC_BUILD_CODEGEN OFF CACHE BOOL "" FORCE)
  endif()
  set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
  set(gRPC_SSL_PROVIDER package CACHE STRING "" FORCE)
  set(gRPC_ZLIB_PROVIDER package CACHE STRING "" FORCE)
  set(gRPC_ABSL_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_CARES_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_PROTOBUF_PROVIDER module CACHE STRING "" FORCE)
  set(gRPC_RE2_PROVIDER module CACHE STRING "" FORCE)
  set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  set(utf8_range_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
  set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)
  add_subdirectory("${DPUMESH_GRPC_SOURCE_DIR}"
                   "${CMAKE_CURRENT_BINARY_DIR}/third_party/grpc"
                   EXCLUDE_FROM_ALL)
  if(TARGET grpc++ AND NOT TARGET gRPC::grpc++)
    add_library(gRPC::grpc++ ALIAS grpc++)
  endif()
  if(TARGET grpc AND NOT TARGET gRPC::grpc)
    add_library(gRPC::grpc ALIAS grpc)
  endif()
else()
  find_package(gRPC ${DPUMESH_GRPC_REQUIRED_VERSION} EXACT CONFIG REQUIRED)
endif()

find_package(Threads REQUIRED)

add_library(dpumesh_executor STATIC src/thread_executor.cc)
target_compile_features(dpumesh_executor PUBLIC cxx_std_17)
target_compile_options(dpumesh_executor PRIVATE -Wall -Wextra)
target_include_directories(dpumesh_executor
  PRIVATE "${CMAKE_CURRENT_LIST_DIR}/src")
target_link_libraries(dpumesh_executor
  PUBLIC absl::status absl::any_invocable
  PRIVATE Threads::Threads)

add_library(grpc_dpumesh_endpoint STATIC src/dmesh_endpoint.cc)
target_compile_features(grpc_dpumesh_endpoint PUBLIC cxx_std_17)
target_compile_options(grpc_dpumesh_endpoint PRIVATE -Wall -Wextra)
target_include_directories(grpc_dpumesh_endpoint
  PRIVATE "${CMAKE_CURRENT_LIST_DIR}/src")
target_link_libraries(grpc_dpumesh_endpoint
  PUBLIC gRPC::grpc++ dpumesh_executor
  PRIVATE Threads::Threads)

find_path(DPUMESH_INCLUDE_DIR dpumesh/dmesh.h
  HINTS "${DPUMESH_ROOT}/include" NO_DEFAULT_PATH)
find_library(DPUMESH_LIBRARY NAMES dpumesh
  HINTS "${DPUMESH_ROOT}/build/lib" NO_DEFAULT_PATH)

if(DPUMESH_INCLUDE_DIR)
  # The EQ reactor and runtime need no gRPC: the gRPC endpoint and the stream
  # C ABI are two sinks over them.
  add_library(dpumesh_reactor_core STATIC
    src/dmesh_reactor.cc
    src/dmesh_runtime.cc)
  target_compile_features(dpumesh_reactor_core PUBLIC cxx_std_17)
  target_compile_options(dpumesh_reactor_core PRIVATE -Wall -Wextra)
  target_include_directories(dpumesh_reactor_core
    PUBLIC "${DPUMESH_INCLUDE_DIR}"
    PRIVATE "${CMAKE_CURRENT_LIST_DIR}/src")
  target_link_libraries(dpumesh_reactor_core
    PUBLIC dpumesh_executor absl::status absl::statusor absl::strings
           absl::any_invocable absl::function_ref absl::span
    PRIVATE Threads::Threads)

  add_library(grpc_dpumesh_reactor STATIC src/dmesh_grpc_runtime.cc)
  target_compile_features(grpc_dpumesh_reactor PUBLIC cxx_std_17)
  target_compile_options(grpc_dpumesh_reactor PRIVATE -Wall -Wextra)
  target_include_directories(grpc_dpumesh_reactor
    PRIVATE
      "${CMAKE_CURRENT_LIST_DIR}/src"
      "${DPUMESH_INCLUDE_DIR}")
  target_link_libraries(grpc_dpumesh_reactor
    PUBLIC dpumesh_reactor_core grpc_dpumesh_endpoint
    PRIVATE Threads::Threads)

  # The stream C ABI (include/dpumesh_stream.h) for the language adapters.
  add_library(dpumesh_stream_core STATIC src/dmesh_stream.cc)
  target_compile_features(dpumesh_stream_core PUBLIC cxx_std_17)
  target_compile_options(dpumesh_stream_core PRIVATE -Wall -Wextra)
  target_include_directories(dpumesh_stream_core
    PUBLIC "${CMAKE_CURRENT_LIST_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_LIST_DIR}/src")
  target_link_libraries(dpumesh_stream_core PUBLIC dpumesh_reactor_core)

  # The same ABI over an in-process loopback, for the adapters' tests.
  add_library(dpumesh_stream_loopback SHARED
    src/dmesh_stream.cc testing/loopback_dmesh_ops.cc)
  target_compile_features(dpumesh_stream_loopback PRIVATE cxx_std_17)
  target_compile_options(dpumesh_stream_loopback PRIVATE -Wall -Wextra)
  target_include_directories(dpumesh_stream_loopback
    PUBLIC "${CMAKE_CURRENT_LIST_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_LIST_DIR}/src" "${CMAKE_CURRENT_LIST_DIR}/testing")
  target_link_libraries(dpumesh_stream_loopback PRIVATE dpumesh_reactor_core)
  target_link_options(dpumesh_stream_loopback PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_LIST_DIR}/src/dpumesh_stream.map"
    "-Wl,--exclude-libs,ALL" "-Wl,--no-undefined")
else()
  message(STATUS
    "DPUmesh public headers were not found; the reactor target will not be built")
endif()

if(DPUMESH_INCLUDE_DIR AND DPUMESH_LIBRARY)
  add_library(dpumesh_native UNKNOWN IMPORTED)
  set_target_properties(dpumesh_native PROPERTIES
    IMPORTED_LOCATION "${DPUMESH_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${DPUMESH_INCLUDE_DIR}")

  add_library(grpc_dpumesh_native STATIC src/dmesh_api_ops.cc)
  target_compile_features(grpc_dpumesh_native PUBLIC cxx_std_17)
  target_compile_options(grpc_dpumesh_native PRIVATE -Wall -Wextra)
  target_include_directories(grpc_dpumesh_native
    PRIVATE "${CMAKE_CURRENT_LIST_DIR}/src" "${DPUMESH_ROOT}")
  target_link_libraries(grpc_dpumesh_native PUBLIC dpumesh_native grpc_dpumesh_reactor)

  # Exports only dms_*; the reactor and abseil stay private to the library.
  add_library(dpumesh_stream SHARED
    src/dmesh_stream.cc src/dmesh_stream_open.cc src/dmesh_api_ops.cc)
  target_compile_features(dpumesh_stream PRIVATE cxx_std_17)
  target_compile_options(dpumesh_stream PRIVATE -Wall -Wextra)
  target_include_directories(dpumesh_stream
    PUBLIC "${CMAKE_CURRENT_LIST_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_LIST_DIR}/src")
  target_link_libraries(dpumesh_stream PRIVATE dpumesh_reactor_core dpumesh_native)
  target_link_options(dpumesh_stream PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_LIST_DIR}/src/dpumesh_stream.map"
    "-Wl,--exclude-libs,ALL" "-Wl,--no-undefined")
  set_target_properties(dpumesh_stream PROPERTIES
    LINK_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/src/dpumesh_stream.map")

  add_library(grpc_dpumesh INTERFACE)
  # Export bootstrap headers through the aggregate target.
  target_include_directories(grpc_dpumesh INTERFACE
    "${CMAKE_CURRENT_LIST_DIR}/src")
  target_link_libraries(grpc_dpumesh INTERFACE
    grpc_dpumesh_endpoint grpc_dpumesh_reactor grpc_dpumesh_native)

  # The C++ peer of the Go channel-bench: 64-byte verified unary echo over
  # DPUMesh or, with -tcp, kernel TCP (bench/channel_bench.cc).
  add_executable(dpumesh_channel_bench bench/channel_bench.cc)
  target_compile_features(dpumesh_channel_bench PRIVATE cxx_std_17)
  target_compile_options(dpumesh_channel_bench PRIVATE -Wall -Wextra)
  target_link_libraries(dpumesh_channel_bench PRIVATE grpc_dpumesh Threads::Threads)

  if(DPUMESH_GRPC_BUILD_EXAMPLES AND DPUMESH_GRPC_SOURCE_DIR)
    if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/../../../examples/grpc/CMakeLists.txt")
      add_subdirectory("${CMAKE_CURRENT_LIST_DIR}/../../../examples/grpc"
                       "${CMAKE_CURRENT_BINARY_DIR}/examples/grpc")
    endif()
  endif()
else()
  message(STATUS
    "libdpumesh was not found; the native wrapper target will not be built")
endif()

if(BUILD_TESTING)
  add_executable(grpc_dpumesh_endpoint_test
    tests/endpoint_test.cc
    tests/fake_endpoint_transport.cc)
  target_compile_features(grpc_dpumesh_endpoint_test PRIVATE cxx_std_17)
  target_compile_options(grpc_dpumesh_endpoint_test
    PRIVATE -Wall -Wextra)
  target_include_directories(grpc_dpumesh_endpoint_test PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/src"
    "${CMAKE_CURRENT_LIST_DIR}/tests")
  target_link_libraries(grpc_dpumesh_endpoint_test
    PRIVATE grpc_dpumesh_endpoint Threads::Threads)
  add_test(NAME grpc_dpumesh_endpoint_test
           COMMAND grpc_dpumesh_endpoint_test)

  add_executable(grpc_dpumesh_channel_test
    tests/grpc_channel_test.cc)
  target_compile_features(grpc_dpumesh_channel_test PRIVATE cxx_std_17)
  target_compile_options(grpc_dpumesh_channel_test PRIVATE -Wall -Wextra)
  target_include_directories(grpc_dpumesh_channel_test PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/src")
  target_link_libraries(grpc_dpumesh_channel_test PRIVATE
    grpc_dpumesh_endpoint Threads::Threads)
  add_test(NAME grpc_dpumesh_channel_test
           COMMAND grpc_dpumesh_channel_test)

  if(DPUMESH_INCLUDE_DIR)
    add_executable(dpumesh_stream_test
      tests/stream_test.cc
      tests/fake_dmesh_ops.cc)
    target_compile_features(dpumesh_stream_test PRIVATE cxx_std_17)
    target_compile_options(dpumesh_stream_test PRIVATE -Wall -Wextra)
    target_include_directories(dpumesh_stream_test PRIVATE
      "${CMAKE_CURRENT_LIST_DIR}/src"
      "${CMAKE_CURRENT_LIST_DIR}/tests")
    target_link_libraries(dpumesh_stream_test PRIVATE
      dpumesh_stream_core Threads::Threads)
    add_test(NAME dpumesh_stream_test COMMAND dpumesh_stream_test)

    add_executable(dpumesh_stream_loopback_test tests/stream_loopback_test.cc)
    target_compile_features(dpumesh_stream_loopback_test PRIVATE cxx_std_17)
    target_compile_options(dpumesh_stream_loopback_test PRIVATE -Wall -Wextra)
    target_link_libraries(dpumesh_stream_loopback_test PRIVATE
      dpumesh_stream_loopback Threads::Threads)
    add_test(NAME dpumesh_stream_loopback_test
             COMMAND dpumesh_stream_loopback_test)

    add_executable(grpc_dpumesh_reactor_test
      tests/reactor_test.cc
      tests/fake_dmesh_ops.cc
      tests/fake_endpoint_transport.cc)
    target_compile_features(grpc_dpumesh_reactor_test PRIVATE cxx_std_17)
    target_compile_options(grpc_dpumesh_reactor_test
      PRIVATE -Wall -Wextra)
    target_include_directories(grpc_dpumesh_reactor_test PRIVATE
      "${CMAKE_CURRENT_LIST_DIR}/src"
      "${CMAKE_CURRENT_LIST_DIR}/tests"
      "${DPUMESH_INCLUDE_DIR}")
    target_link_libraries(grpc_dpumesh_reactor_test PRIVATE
      grpc_dpumesh_reactor Threads::Threads)
    add_test(NAME grpc_dpumesh_reactor_test
             COMMAND grpc_dpumesh_reactor_test)
  endif()

  if(DPUMESH_INCLUDE_DIR AND DPUMESH_LIBRARY)
    add_executable(grpc_dpumesh_native_link_test
      tests/native_api_link_test.cc)
    target_compile_features(grpc_dpumesh_native_link_test PRIVATE cxx_std_17)
    target_include_directories(grpc_dpumesh_native_link_test PRIVATE
      "${CMAKE_CURRENT_LIST_DIR}/src")
    target_link_libraries(grpc_dpumesh_native_link_test
      PRIVATE grpc_dpumesh)
    add_test(NAME grpc_dpumesh_native_link_test
             COMMAND grpc_dpumesh_native_link_test)

    # This executable deliberately is not registered with CTest. It opens a real
    # DPUmesh channel and therefore must only be run in a BlueField-enabled pod.
    add_executable(grpc_dpumesh_native_runtime_smoke
      tests/native_runtime_smoke.cc)
    target_compile_features(grpc_dpumesh_native_runtime_smoke PRIVATE cxx_std_17)
    target_compile_options(grpc_dpumesh_native_runtime_smoke
      PRIVATE -Wall -Wextra)
    target_include_directories(grpc_dpumesh_native_runtime_smoke PRIVATE
      "${CMAKE_CURRENT_LIST_DIR}/src")
    target_link_libraries(grpc_dpumesh_native_runtime_smoke
      PRIVATE grpc_dpumesh Threads::Threads)

    # Manual hardware test of the stream C ABI: an echo server and a
    # ping-pong client in two processes.
    add_executable(dpumesh_stream_smoke tests/stream_smoke.cc)
    target_compile_features(dpumesh_stream_smoke PRIVATE cxx_std_17)
    target_compile_options(dpumesh_stream_smoke PRIVATE -Wall -Wextra)
    target_link_libraries(dpumesh_stream_smoke PRIVATE dpumesh_stream Threads::Threads)

    # Manual two-pod hardware test. Run one copy in server mode and another in
    # client mode; unlike native_runtime_smoke this executes a real gRPC RPC.
    add_executable(grpc_dpumesh_native_grpc_smoke
      tests/native_grpc_smoke.cc)
    target_compile_features(grpc_dpumesh_native_grpc_smoke PRIVATE cxx_std_17)
    target_compile_options(grpc_dpumesh_native_grpc_smoke
      PRIVATE -Wall -Wextra)
    target_include_directories(grpc_dpumesh_native_grpc_smoke PRIVATE
      "${CMAKE_CURRENT_LIST_DIR}/src")
    target_link_libraries(grpc_dpumesh_native_grpc_smoke
      PRIVATE grpc_dpumesh Threads::Threads)
  endif()
endif()
