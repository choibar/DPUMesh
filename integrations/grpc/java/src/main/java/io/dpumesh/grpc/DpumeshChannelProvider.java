package io.dpumesh.grpc;

import io.grpc.ManagedChannelBuilder;
import io.grpc.ManagedChannelProvider;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;

/** Selected by {@code ManagedChannelBuilder.forTarget} when DPUMESH_ENABLE=1. */
public final class DpumeshChannelProvider extends ManagedChannelProvider {
  @Override
  protected boolean isAvailable() {
    return DpumeshGrpc.enabled();
  }

  @Override
  protected int priority() {
    return 10; // above grpc-netty's 5
  }

  @Override
  protected ManagedChannelBuilder<?> builderForAddress(String name, int port) {
    return DpumeshGrpc.channelBuilder(name + ":" + port);
  }

  @Override
  protected ManagedChannelBuilder<?> builderForTarget(String target) {
    return DpumeshGrpc.channelBuilder(target);
  }

  @Override
  protected Collection<Class<? extends SocketAddress>> getSupportedSocketAddressTypes() {
    return Collections.singleton(DpumeshAddress.class);
  }
}
