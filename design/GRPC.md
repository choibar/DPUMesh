# gRPC integration

`integrations/grpc/cpp` adapts gRPC 1.80.0 to the native API through an
EventEngine endpoint beneath chttp2. HTTP/2 framing, message serialization,
deadlines and RPC semantics stay in gRPC; the endpoint moves bytes.

## Runtime and ownership

`DmeshRuntime` owns one channel and a configurable set of reactor shards. The
first reactor also waits on the channel's fd and accepts; dialed and accepted
QPs alike are spread across shards round-robin. Each reactor waits on its QPs'
fds and owns their polling and receive/TX-ready progress; a shared executor
delivers deferred callbacks.
Endpoint destruction and transport failure complete pending operations once.

Reads copy native fragments into gRPC slices; a reactor may retain receive
credit while an endpoint is above its high-water mark, bounded per connection.
Writes reserve native memory, commit bytes and resume on `TX_READY` after
backpressure.

## Build and verification

```sh
make lib
cmake -S integrations/grpc/cpp -B build/grpc \
  -DDPUMESH_GRPC_SOURCE_DIR=/path/to/grpc-v1.80.0 -DBUILD_TESTING=ON
cmake --build build/grpc --parallel 2
ctest --test-dir build/grpc --output-on-failure
```

An installed gRPC of the same version is accepted when the source directory is
omitted. The CTest programs use in-memory transports and native doubles and
open no device; the device smoke programs are run by hand.
