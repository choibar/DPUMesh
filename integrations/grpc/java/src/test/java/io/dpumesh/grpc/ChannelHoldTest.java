package io.dpumesh.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

// Raw Netty channels over the loopback, below HTTP/2, whose flow control keeps
// a gRPC connection's input drained: a server child that does not read drives
// the receive credit hold, and reading again must resume it.
class ChannelHoldTest {
  @Test
  void stalledReaderHoldsCreditThenReceivesEverything() throws Exception {
    byte[] payload = new byte[8 << 20];
    new Random(7).nextBytes(payload);
    CompletableFuture<Channel> accepted = new CompletableFuture<>();
    ByteArrayOutputStream received = new ByteArrayOutputStream();
    CountDownLatch closed = new CountDownLatch(1);

    Channel listener = new ServerBootstrap()
        .group(DpumeshGrpc.group())
        .channel(DpumeshServerChannel.class)
        .childOption(ChannelOption.AUTO_READ, false)
        .childHandler(new ChannelInitializer<Channel>() {
          @Override
          protected void initChannel(Channel child) {
            child.pipeline().addLast(new ChannelInboundHandlerAdapter() {
              @Override
              public void channelActive(ChannelHandlerContext ctx) {
                accepted.complete(ctx.channel());
              }

              @Override
              public void channelRead(ChannelHandlerContext ctx, Object message) {
                ByteBuf buffer = (ByteBuf) message;
                synchronized (received) {
                  byte[] bytes = new byte[buffer.readableBytes()];
                  buffer.readBytes(bytes);
                  received.write(bytes, 0, bytes.length);
                }
                buffer.release();
              }

              @Override
              public void channelInactive(ChannelHandlerContext ctx) {
                closed.countDown();
              }
            });
          }
        })
        .bind(new DpumeshAddress("raw.test:1")).sync().channel();
    try {
      Channel client = new Bootstrap()
          .group(DpumeshGrpc.group())
          .channel(DpumeshChannel.class)
          .handler(new ChannelInboundHandlerAdapter())
          .connect(new DpumeshAddress("raw.test:1")).sync().channel();
      Channel server = accepted.get(5, TimeUnit.SECONDS);

      ChannelFuture write = client.writeAndFlush(Unpooled.wrappedBuffer(payload));
      Thread.sleep(300);
      assertFalse(write.isDone(), "the writer finished although the reader held its credit");

      server.config().setAutoRead(true);
      server.read();
      assertTrue(write.await(20, TimeUnit.SECONDS) && write.isSuccess());
      client.close().sync();
      assertTrue(closed.await(10, TimeUnit.SECONDS));
      synchronized (received) {
        assertArrayEquals(payload, received.toByteArray());
      }
    } finally {
      listener.close().sync();
    }
  }
}
