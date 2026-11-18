# Python gRPC

grpcio does its I/O inside gRPC C-core, and the grpcio wheels on PyPI hide
every C-core symbol, so DPUMesh support is compiled into grpcio itself:
[`patches/grpcio-1.80.0-dpumesh.patch`](patches/grpcio-1.80.0-dpumesh.patch)
adds [`src/dpumesh_grpcio.cc`](src/dpumesh_grpcio.cc) and the C++ adapter's
EventEngine endpoint to the build. The endpoint runs over the
[stream C ABI](../README.md), loaded with `dlopen` on first use, so the wheel
behaves as stock grpcio until a DPUMesh channel or listener is used.

- A channel target `dpumesh:<host>:<port>` gets its own EventEngine that
  delegates everything but `Connect()`, which opens a DPUMesh stream; gRPC
  keeps its connection management and reconnects.
- A server takes the streams the DPU routes to `DPUMESH_PORT` on the Pod IP (or
  `DPUMESH_SERVICE`) through a
  C-core passive listener.
- Received bytes become gRPC slices without a further copy; writes fill native
  transmit space directly (`dms_stream_post`).

`dpumesh_grpc` is the Python API:

```python
import dpumesh_grpc

channel = dpumesh_grpc.insecure_channel("10.96.0.5:3550")  # grpc.insecure_channel when disabled
dpumesh_grpc.serve(server, "[::]:8080")  # replaces add_insecure_port() and start()
```

Both use DPUMesh only with `DPUMESH_ENABLE=1`. The runtime closes at exit and
on a SIGTERM that nothing else handles. `grpc.aio` is not wired yet.

Build the wheel (a few minutes; needs a C++17 compiler and Python headers) and
install it over the pinned grpcio, then the package:

```sh
integrations/grpc/python/build_wheel.sh            # build/grpcio/wheels/
pip install --force-reinstall --no-deps build/grpcio/wheels/grpcio-1.80.0-*.whl
pip install --no-deps integrations/grpc/python
python -m unittest discover integrations/grpc/python/tests   # needs the loopback
```

The tests serve and call through `dpumesh_grpc` over the loopback: message
sizes up to 4 MiB, 16 MiB of server streaming, 64 concurrent calls, deadlines
and repeated channels.

## Channel benchmark

`bench/channel_bench.py` is the grpcio peer of the Go
[channel-bench](../go/cmd/channel-bench/README.md): the same wire, flags and
JSON result, over DPUMesh with the patched grpcio or, with `-tcp host:port`,
kernel TCP. The server is a synchronous `grpc.server` with 10 worker threads,
as Online Boutique's Python services run; the client drives each RPC loop from
future callbacks. `-start-file` is not supported, and `native_dials` counts
each channel's transitions to READY.

```sh
python integrations/grpc/python/bench/channel_bench.py -mode server
python integrations/grpc/python/bench/channel_bench.py -mode client -connections 4 -concurrency 64
```
