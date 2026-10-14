package io.dpumesh.grpc;

import io.grpc.ServerBuilder;
import io.grpc.ServerProvider;

/** Selected by {@code ServerBuilder.forPort} when DPUMESH_ENABLE=1; the port is unused. */
public final class DpumeshServerProvider extends ServerProvider {
  @Override
  protected boolean isAvailable() {
    return DpumeshGrpc.enabled();
  }

  @Override
  protected int priority() {
    return 10; // above grpc-netty's 5
  }

  @Override
  protected ServerBuilder<?> builderForPort(int port) {
    return DpumeshGrpc.serverBuilder();
  }
}
