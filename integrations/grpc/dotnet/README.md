# .NET gRPC

`Dpumesh.Grpc` (.NET 8+) serves ASP.NET Core gRPC over DPUMesh through a
Kestrel transport and gives Grpc.Net.Client a handler whose connections are
DPUMesh streams. It uses the [stream C ABI](../README.md) through P/Invoke.

```csharp
// Server: with DPUMESH_ENABLE=1, Kestrel serves DPUMESH_PORT on the Pod IP (or
// DPUMESH_SERVICE) over DPUMesh
// (HTTP/2) instead of its configured addresses; otherwise nothing changes.
webBuilder.UseDpumesh();

// Client: "<host>:<port>" of the request URI is the service address.
var channel = GrpcChannel.ForAddress("http://10.96.0.5:7070",
    new GrpcChannelOptions { HttpHandler = DpumeshHttpHandler.Create() });
```

`AddDpumeshTransport()` and `KestrelServerOptions.ListenDpumesh()` are the two
halves of `UseDpumesh()` for hosts configured by hand. The transport coexists
with Kestrel's socket transport, which keeps every other endpoint.

- Received bytes land in the connection's input pipe directly from the native
  receive, one copy. Above 1 MiB of unread input the stream withholds its
  receive credit until the application reads.
- A write that finds no transmit credit waits for the stream's writable
  callback.
- The runtime opens on first use and closes on process exit.

Build and test (the tests need the loopback, see [the overview](../README.md#build)):

```sh
dotnet build src/Dpumesh.Grpc -c Release
dotnet test tests/Dpumesh.Grpc.Tests
```

The tests run a Kestrel gRPC server and a Grpc.Net.Client channel over the
loopback: message sizes around the post limit up to 4 MiB, bidirectional
streaming, 64 concurrent calls, deadlines, cancellation, server restart, and a
raw stream whose reader stalls past the credit threshold.

## Channel benchmark

`bench/ChannelBench` is the .NET peer of the Go
[channel-bench](../go/cmd/channel-bench/README.md): the same wire, flags and
JSON result, with an ASP.NET Core server on Kestrel and a Grpc.Net.Client
client, over DPUMesh or, with `-tcp host:port`, kernel TCP. `-start-file` is
not supported.

```sh
dotnet build -c Release bench/ChannelBench
bench/ChannelBench/bin/Release/net10.0/ChannelBench -mode server
bench/ChannelBench/bin/Release/net10.0/ChannelBench -mode client -connections 4 -concurrency 64
```
