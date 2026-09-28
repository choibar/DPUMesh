# Examples

Each program uses the host library over a running DPUMesh proxy. Services are
named by `<host>:<port>` targets that resolve through DNS
([naming](../design/API.md#naming)); a server process sets `DPUMESH_SERVICE`
to its target, a client leaves it unset. `.env.example` at the repository
root lists every variable; see [configuration](../design/HOST.md#configuration).

## Native C

`native/hello_dpumesh_server.c` accepts one stream at a time and echoes it;
`native/hello_dpumesh.c` sends one message and prints the reply.

```sh
make examples
DPUMESH_SERVICE=hello-dpumesh:9095 build/bin/hello_dpumesh_server
build/bin/hello_dpumesh hello-dpumesh:9095 hello      # another configured process
```

## POSIX preload

`preload/tcp_echo.c` and `preload/tcp_client.c` are ordinary socket programs.
Under `LD_PRELOAD` the shim carries the client's connections to the targets in
`DPUMESH_TARGETS` over the transport; the server also sets `DPUMESH_PORT` to
its listening port.

```sh
LD_PRELOAD=build/lib/libdpumesh_preload.so DPUMESH_SERVICE=hello-dpumesh:9095 DPUMESH_PORT=9095 build/bin/tcp_echo 9095
printf 'RUN 100 64 1\nQUIT\n' | LD_PRELOAD=build/lib/libdpumesh_preload.so DPUMESH_TARGETS=hello-dpumesh:9095 build/bin/tcp_client <service-ip> 9095
```

## C++ gRPC

`grpc/hello_grpc_server.cc` attaches an ordinary generated service to the
runtime; `grpc/hello_grpc_client.cc` calls it. Both build with the
[C++ adapter](../integrations/grpc/cpp/README.md) and run from `build/grpc/examples/grpc`.

## Go gRPC

`integrations/grpc/go/cmd/echo-server` and `cmd/echo-client` run the gRPC
health RPC over `net.Conn`. Both read the service address from
`DPUMESH_SERVICE_IP` and `DPUMESH_SERVICE_PORT`; the server's `DPUMESH_SERVICE`
must resolve to that address. See the [Go adapter](../integrations/grpc/go/README.md).
