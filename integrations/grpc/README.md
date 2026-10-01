# gRPC over DPUMesh

Each adapter carries a gRPC library's HTTP/2 bytes over DPUMesh streams at
the library's own transport hook. Generated stubs, handlers, deadlines and
metadata stay unchanged; a service opts in with a few lines and
`DPUMESH_ENABLE=1`, and runs over TCP without it.

| Language | gRPC library | Hook | Directory |
|---|---|---|---|
| Go | grpc-go | `net.Listener` / `grpc.WithContextDialer` | [go](go/README.md) |
| C++ | gRPC C++ 1.80 | EventEngine endpoint, passive listener | [cpp](cpp/README.md) |

The Go and C++ adapters drive the native API directly. Adapters for other
runtimes share the **stream C ABI**
([`cpp/include/dpumesh_stream.h`](cpp/include/dpumesh_stream.h)),
`libdpumesh_stream.so`: the C++ adapter's EQ reactor behind a byte-stream
interface — connect, listen, write, and callbacks for data, transmit credit,
end of stream and errors. The reactor keeps the native rules (one EQ consumer,
receive leases, TX_READY, serialized QP teardown), so a binding only maps the
callbacks onto its runtime's I/O object.

## Build

The stream library and its in-process loopback build with the C++ adapter.
They link only abseil, which comes from a gRPC 1.80.0 source tree:

```sh
make lib
cmake -S integrations/grpc/cpp -B build/grpc -G Ninja \
  -DDPUMESH_GRPC_SOURCE_DIR=/path/to/grpc-v1.80.0 -DBUILD_TESTING=ON
cmake --build build/grpc --target dpumesh_stream dpumesh_stream_loopback
```

Processes find `libdpumesh_stream.so` and `libdpumesh.so.5` on
`LD_LIBRARY_PATH` (`build/grpc`, `build/lib`). `DPUMESH_STREAM_LIBRARY` names
another build of the ABI. A binding's tests can set it to
`build/grpc/libdpumesh_stream_loopback.so`, which routes a process's streams
to its own listener, so they run real gRPC clients and servers without a
DOCA device.

A process should close its DPUMesh channel before it exits. A channel that
vanishes mid-stream stalls the DPU proxy for several seconds.
