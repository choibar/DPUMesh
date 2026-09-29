# Java gRPC

`dpumesh-grpc.jar` runs grpc-java over DPUMesh through grpc-netty with a
DPUMesh Netty channel. With the jar on the classpath and `DPUMESH_ENABLE=1`,
its `ServerProvider` and `ManagedChannelProvider` answer
`ServerBuilder.forPort(...)` and `ManagedChannelBuilder.forTarget(...)`, so a
service needs no code change; without `DPUMESH_ENABLE` they stay unused.

```java
// Explicit use:
Server server = DpumeshGrpc.serverBuilder().addService(service).build().start();
ManagedChannel channel = DpumeshGrpc.channelBuilder("10.96.0.5:9555").build();
```

The server serves `DPUMESH_SERVICE` (the `forPort` port is unused); a channel
target is a `"<host>:<port>"` service address. A JNI library, packed in the
jar, binds the [stream C ABI](../README.md), loaded with `dlopen`.

- `DpumeshChannel` and `DpumeshServerChannel` run on any Netty event loop;
  stream events arrive on the DPUMesh runtime's thread and run on the
  channel's loop. Received bytes land in pooled direct `ByteBuf`s, one copy.
- When a channel with auto-read off holds more than 1 MiB of unread input,
  the stream withholds its receive credit until the channel reads.
- The runtime opens on first use and closes in a JVM shutdown hook.

Build with any Gradle 8 (Java 11+ bytecode; the tests need the loopback, see
[the overview](../README.md#build)), for example a consuming project's
wrapper:

```sh
sh <project>/gradlew -p integrations/grpc/java test jar   # build/libs/dpumesh-grpc.jar
```

The tests serve and call through the providers over the loopback: message
sizes up to 4 MiB, 16 MiB of server streaming, 64 concurrent calls, deadlines,
repeated channels, and a raw Netty channel that stalls past the credit
threshold.
