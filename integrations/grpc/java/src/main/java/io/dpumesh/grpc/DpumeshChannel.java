package io.dpumesh.grpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Netty channel over one DPUMesh stream: a client channel that connects to a
 * {@link DpumeshAddress}, or a stream a {@link DpumeshServerChannel} accepted.
 * The stream's events arrive on the DPUMesh runtime's thread and run on the
 * channel's event loop, so the channel works on any event loop.
 */
public final class DpumeshChannel extends AbstractChannel {
  private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
  // Unread inbound bytes above which the stream withholds receive credit, as
  // the C++ endpoint does, so a reader that stops reading stops the peer.
  private static final long HOLD_BYTES = 1 << 20;

  private final DefaultChannelConfig config = new StreamConfig(this);
  private volatile boolean open = true;
  private volatile boolean active;
  private SocketAddress local;
  private SocketAddress remote;

  // Event loop only.
  private long unbound; // an accepted stream, bound once registered
  private long stream; // the bound stream; 0 once ended
  private final ArrayDeque<ByteBuf> inbound = new ArrayDeque<>();
  private boolean readPending;
  private boolean eofPending;
  private boolean writePending;

  private final AtomicLong queued = new AtomicLong();
  private final AtomicBoolean held = new AtomicBoolean();

  /** A client channel, for {@code NettyChannelBuilder.channelType}. */
  public DpumeshChannel() {
    super(null);
  }

  DpumeshChannel(Channel parent, long accepted, SocketAddress local) {
    super(parent);
    this.unbound = accepted;
    this.local = local;
    this.active = true;
  }

  @Override
  protected AbstractUnsafe newUnsafe() {
    return new DpumeshUnsafe();
  }

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
  public ChannelMetadata metadata() {
    return METADATA;
  }

  @Override
  protected SocketAddress localAddress0() {
    return local;
  }

  @Override
  protected SocketAddress remoteAddress0() {
    return remote;
  }

  @Override
  protected void doRegister() throws Exception {
    if (unbound != 0) {
      long accepted = unbound;
      unbound = 0;
      attach(accepted);
    }
  }

  @Override
  protected void doBind(SocketAddress address) throws Exception {
    throw new UnsupportedOperationException("a DPUMesh stream has no local address to bind");
  }

  @Override
  protected void doDisconnect() throws Exception {
    doClose();
  }

  @Override
  protected void doClose() throws Exception {
    open = false;
    active = false;
    if (unbound != 0) {
      NativeBridge.nativeAbortUnbound(unbound);
      unbound = 0;
    }
    end(false);
    ByteBuf buffer;
    while ((buffer = inbound.poll()) != null) {
      queued.addAndGet(-buffer.readableBytes());
      ReferenceCountUtil.release(buffer);
    }
  }

  @Override
  protected void doBeginRead() throws Exception {
    readPending = true;
    drain();
  }

  @Override
  protected void doWrite(ChannelOutboundBuffer in) throws Exception {
    if (stream == 0) throw new ClosedChannelException();
    while (true) {
      Object message = in.current();
      if (message == null) return;
      if (!(message instanceof ByteBuf)) {
        in.remove(new UnsupportedOperationException("DPUMesh writes only ByteBuf"));
        continue;
      }
      ByteBuf buffer = (ByteBuf) message;
      if (!buffer.isReadable()) {
        in.remove();
        continue;
      }
      int n = write(buffer);
      if (n > 0) {
        in.removeBytes(n);
      } else if (n == -NativeBridge.EAGAIN) {
        writePending = true; // flushed again on the stream's writable event
        return;
      } else {
        throw new IOException("DPUMesh write failed: errno " + -n);
      }
    }
  }

  // Posts the start of `buffer`; returns the count accepted or -errno.
  private int write(ByteBuf buffer) {
    int index = buffer.readerIndex();
    int readable = buffer.readableBytes();
    if (buffer.hasArray()) {
      return NativeBridge.nativeWriteArray(
          stream, buffer.array(), buffer.arrayOffset() + index, readable);
    }
    ByteBuffer part =
        buffer.nioBufferCount() == 1
            ? buffer.internalNioBuffer(index, readable)
            : buffer.nioBuffers(index, readable)[0];
    if (part.isDirect()) {
      return NativeBridge.nativeWriteDirect(stream, part, part.position(), part.remaining());
    }
    return NativeBridge.nativeWriteArray(
        stream, part.array(), part.arrayOffset() + part.position(), part.remaining());
  }

  private void attach(long accepted) throws IOException {
    stream = NativeBridge.nativeBind(accepted, new Sink());
  }

  private void end(boolean abort) {
    if (stream == 0) return;
    long ending = stream;
    stream = 0;
    NativeBridge.nativeEnd(ending, abort);
  }

  // Delivers queued inbound bytes while a read is pending, then the end of
  // input once they are all delivered.
  private void drain() {
    if (readPending && !inbound.isEmpty()) {
      if (!config.isAutoRead()) readPending = false;
      ByteBuf buffer;
      while ((buffer = inbound.poll()) != null) {
        queued.addAndGet(-buffer.readableBytes());
        pipeline().fireChannelRead(buffer);
      }
      pipeline().fireChannelReadComplete();
    }
    if (held.get() && queued.get() < HOLD_BYTES / 2 && stream != 0) {
      held.set(false);
      NativeBridge.nativeResume(stream);
    }
    if (eofPending && inbound.isEmpty()) {
      eofPending = false;
      unsafe().close(unsafe().voidPromise());
    }
  }

  private void onEventLoop(Runnable task) {
    try {
      eventLoop().execute(task);
    } catch (RejectedExecutionException e) {
      // The event loop is gone; so is everything the task would touch.
    }
  }

  // Accepts and ignores the TCP options gRPC sets: a DPUMesh stream has no
  // TCP socket.
  static final class StreamConfig extends DefaultChannelConfig {
    StreamConfig(Channel channel) {
      super(channel);
    }

    @Override
    public <T> boolean setOption(ChannelOption<T> option, T value) {
      if (option == ChannelOption.SO_KEEPALIVE || option == ChannelOption.TCP_NODELAY) {
        return true;
      }
      return super.setOption(option, value);
    }
  }

  private final class DpumeshUnsafe extends AbstractUnsafe {
    @Override
    public void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
      if (!promise.setUncancellable() || !ensureOpen(promise)) return;
      if (!(remoteAddress instanceof DpumeshAddress)) {
        promise.setFailure(new IllegalArgumentException(remoteAddress + " is not a DpumeshAddress"));
        return;
      }
      NativeBridge.connect(((DpumeshAddress) remoteAddress).service())
          .whenComplete(
              (connected, error) ->
                  onEventLoop(
                      () -> {
                        if (error != null) {
                          promise.tryFailure(error);
                          close(voidPromise());
                          return;
                        }
                        if (!isOpen()) {
                          NativeBridge.nativeAbortUnbound(connected);
                          promise.tryFailure(new ClosedChannelException());
                          return;
                        }
                        try {
                          attach(connected);
                        } catch (IOException e) {
                          promise.tryFailure(e);
                          close(voidPromise());
                          return;
                        }
                        remote = remoteAddress;
                        local = remoteAddress;
                        active = true;
                        if (promise.trySuccess()) pipeline().fireChannelActive();
                      }));
    }

    void flushAgain() {
      flush0();
    }
  }

  // The stream's native events, on the runtime's reactor thread.
  private final class Sink implements NativeBridge.StreamSink {
    private ByteBuf allocating;

    @Override
    public ByteBuffer allocate(int len) {
      allocating = config.getAllocator().directBuffer(len);
      return allocating.nioBuffer(0, len);
    }

    @Override
    public boolean received(int len) {
      ByteBuf buffer = allocating;
      allocating = null;
      buffer.writerIndex(len);
      boolean hold = queued.addAndGet(len) > HOLD_BYTES;
      if (hold) held.set(true);
      try {
        eventLoop()
            .execute(
                () -> {
                  if (!open) {
                    queued.addAndGet(-buffer.readableBytes());
                    buffer.release();
                    return;
                  }
                  inbound.add(buffer);
                  drain();
                });
      } catch (RejectedExecutionException e) {
        buffer.release();
        return false;
      }
      return hold;
    }

    @Override
    public void writable() {
      onEventLoop(
          () -> {
            if (!writePending) return;
            writePending = false;
            ((DpumeshUnsafe) unsafe()).flushAgain();
          });
    }

    @Override
    public void eof() {
      onEventLoop(
          () -> {
            eofPending = true;
            drain();
          });
    }

    @Override
    public void error(int err, String message) {
      onEventLoop(
          () -> {
            pipeline()
                .fireExceptionCaught(
                    new IOException("DPUMesh stream failed: " + message + " (errno " + err + ")"));
            unsafe().close(unsafe().voidPromise());
          });
    }

    @Override
    public void released() {}
  }
}
