package io.dpumesh.grpc;

import io.netty.channel.AbstractServerChannel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.EventLoop;
import java.net.SocketAddress;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.LongConsumer;

/**
 * A Netty server channel for this process's DPUMESH_SERVICE: binding it to a
 * {@link DpumeshAddress} starts accepting the streams the DPU routes to the
 * service. One DPUMesh server channel listens at a time.
 */
public final class DpumeshServerChannel extends AbstractServerChannel {
  private final DefaultChannelConfig config = new DefaultChannelConfig(this);
  private final LongConsumer acceptor = this::onAccept;
  private volatile boolean open = true;
  private volatile boolean active;
  private volatile SocketAddress local;

  /** For {@code NettyServerBuilder.channelType}. */
  public DpumeshServerChannel() {}

  @Override
  protected boolean isCompatible(EventLoop loop) {
    return true;
  }

  @Override
  public ChannelConfig config() {
    return config;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  protected SocketAddress localAddress0() {
    return local;
  }

  @Override
  protected void doBind(SocketAddress address) throws Exception {
    if (!(address instanceof DpumeshAddress)) {
      throw new IllegalArgumentException(address + " is not a DpumeshAddress");
    }
    local = address;
    NativeBridge.listen(acceptor);
    active = true;
  }

  @Override
  protected void doClose() throws Exception {
    open = false;
    active = false;
    NativeBridge.stopListening(acceptor);
  }

  @Override
  protected void doBeginRead() throws Exception {
    // Accepted streams are delivered as they arrive.
  }

  // On the runtime's callback thread.
  private void onAccept(long stream) {
    try {
      eventLoop()
          .execute(
              () -> {
                if (!isOpen()) {
                  NativeBridge.nativeAbortUnbound(stream);
                  return;
                }
                pipeline().fireChannelRead(new DpumeshChannel(this, stream, local));
                pipeline().fireChannelReadComplete();
              });
    } catch (RejectedExecutionException e) {
      NativeBridge.nativeAbortUnbound(stream);
    }
  }
}
