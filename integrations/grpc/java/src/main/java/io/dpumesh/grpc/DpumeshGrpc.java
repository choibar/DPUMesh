package io.dpumesh.grpc;

import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * gRPC-java over DPUMesh. With DPUMESH_ENABLE=1 on the classpath, the bundled
 * providers make {@code ServerBuilder.forPort} and
 * {@code ManagedChannelBuilder.forTarget} use these builders, so a service
 * needs no code change; the builders can also be used directly.
 */
public final class DpumeshGrpc {
  private static volatile EventLoopGroup group;

  private DpumeshGrpc() {}

  /** True when DPUMESH_ENABLE is "1". */
  public static boolean enabled() {
    return "1".equals(System.getenv("DPUMESH_ENABLE"));
  }

  /** A server builder for this process's target: DPUMESH_SERVICE, or DPUMESH_PORT on the Pod IP. */
  public static NettyServerBuilder serverBuilder() {
    return NettyServerBuilder.forAddress(new DpumeshAddress(servedTarget()))
        .channelType(DpumeshServerChannel.class)
        .bossEventLoopGroup(group())
        .workerEventLoopGroup(group());
  }

  // The server channel's label only: the native library serves the target it
  // finds when it opens the channel.
  private static String servedTarget() {
    String service = System.getenv("DPUMESH_SERVICE");
    if (service != null && !service.isEmpty()) return service;
    String port = System.getenv("DPUMESH_PORT");
    if (port == null || port.isEmpty()) {
      throw new IllegalStateException(
          "a DPUMesh server needs DPUMESH_PORT or a DPUMESH_SERVICE \"<host>:<port>\" target");
    }
    return ":" + port;
  }

  /** A plaintext channel builder for a {@code "<host>:<port>"} service address. */
  public static NettyChannelBuilder channelBuilder(String target) {
    return NettyChannelBuilder.forAddress(new DpumeshAddress(target))
        .channelType(DpumeshChannel.class)
        .eventLoopGroup(group())
        .usePlaintext();
  }

  // DPUMesh channels use their event loop only to run their events, so one
  // small shared group serves every server and client in the process.
  @SuppressWarnings("deprecation")
  static EventLoopGroup group() {
    EventLoopGroup current = group;
    if (current != null) return current;
    synchronized (DpumeshGrpc.class) {
      if (group == null) {
        int threads = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors()));
        group = new NioEventLoopGroup(threads, new DefaultThreadFactory("dpumesh-grpc", true));
      }
      return group;
    }
  }
}
