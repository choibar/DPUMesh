# Node.js gRPC

`@dpumesh/grpc-js` serves a `@grpc/grpc-js` server (1.10 or later) over
DPUMesh: the streams the DPU routes to `DPUMESH_SERVICE` become the server's
HTTP/2 connections through grpc-js's connection injector. An N-API addon
binds the [stream C ABI](../README.md), loaded with `dlopen`.

```js
if (process.env.DPUMESH_ENABLE === '1') {
  require('@dpumesh/grpc-js').serve(server, grpc.ServerCredentials.createInsecure());
} else {
  server.bindAsync(`[::]:${port}`, grpc.ServerCredentials.createInsecure(), () => {});
}
```

`connect("<host>:<port>")` opens a DPUMesh stream as a `Duplex` socket, and
`listen(onSocket)` hands over raw inbound streams; grpc-js has no client
transport hook, so a grpc-js client cannot use DPUMesh yet.

- Received bytes reach JavaScript as external `Buffer`s, one copy. When the
  socket's read buffer is full, or more than 1 MiB waits for it, the stream
  withholds its receive credit until the socket reads again.
- Node's HTTP/2 runs over a non-`net.Socket` stream through its JavaScript
  stream wrapper, which costs CPU that a TCP socket does not; see the
  [measurements](../../../bench-results/2026-09-29_online-boutique-e2e.md).
- The runtime opens on first use and closes on `exit` and on a SIGTERM that
  nothing else handles.

Build and test (the tests need the loopback, see [the overview](../README.md#build)):

```sh
npm install          # runs node-gyp for the addon
npm test
```

The tests run a grpc-js server and speak gRPC over `node:http2` on DPUMesh
sockets: message sizes up to 4 MiB, 16 MiB of server streaming, 64 concurrent
calls, deadlines, repeated connections, and a paused reader past the credit
threshold.

## Channel benchmark

`bench/channel-bench.js` is the grpc-js server of the Go
[channel-bench](../go/cmd/channel-bench/README.md): the same wire and flags,
over DPUMesh or, with `-tcp host:port`, kernel TCP. There is no client mode,
since grpc-js cannot dial over DPUMesh; drive it with the Go or C++ client,
for example through [bench/grpc](../../../bench/grpc/README.md):

```sh
python3 bench/grpc/run.py --tag node-1x64 --l7 --lib build/lib:build/grpc \
    --server "node integrations/grpc/node/bench/channel-bench.js" \
    --client integrations/grpc/go/bin/channel-bench
```
