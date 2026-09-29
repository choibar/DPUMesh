package io.dpumesh.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// A grpc-java server and channel built through ServerBuilder.forPort and
// ManagedChannelBuilder.forTarget, which the DPUMesh providers answer
// (DPUMESH_ENABLE=1 in the test environment), over the in-process loopback.
class EchoGrpcTest {
  private static final MethodDescriptor.Marshaller<byte[]> BYTES =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
          return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
          try {
            return stream.readAllBytes();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      };

  private static MethodDescriptor<byte[], byte[]> method(String name, MethodDescriptor.MethodType type) {
    return MethodDescriptor.<byte[], byte[]>newBuilder()
        .setType(type)
        .setFullMethodName("test.Echo/" + name)
        .setRequestMarshaller(BYTES)
        .setResponseMarshaller(BYTES)
        .build();
  }

  private static final MethodDescriptor<byte[], byte[]> UNARY =
      method("Unary", MethodDescriptor.MethodType.UNARY);
  // Request: count and size, two ints. Streams `count` messages of `size` bytes.
  private static final MethodDescriptor<byte[], byte[]> SOURCE =
      method("Source", MethodDescriptor.MethodType.SERVER_STREAMING);
  // Request: a delay in milliseconds.
  private static final MethodDescriptor<byte[], byte[]> SLEEP =
      method("Sleep", MethodDescriptor.MethodType.UNARY);

  private static final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor();
  private static Server server;
  private static ManagedChannel channel;

  @BeforeAll
  static void start() throws IOException {
    ServerServiceDefinition echo =
        ServerServiceDefinition.builder("test.Echo")
            .addMethod(UNARY, ServerCalls.asyncUnaryCall((request, response) -> {
              response.onNext(request);
              response.onCompleted();
            }))
            .addMethod(SOURCE, ServerCalls.asyncServerStreamingCall((request, response) -> {
              ByteBuffer spec = ByteBuffer.wrap(request);
              int count = spec.getInt();
              byte[] chunk = pattern(spec.getInt());
              for (int i = 0; i < count; i++) response.onNext(chunk);
              response.onCompleted();
            }))
            .addMethod(SLEEP, ServerCalls.asyncUnaryCall((request, response) ->
                timers.schedule(() -> {
                  response.onNext(request);
                  response.onCompleted();
                }, ByteBuffer.wrap(request).getInt(), TimeUnit.MILLISECONDS)))
            .build();
    server = ServerBuilder.forPort(0).addService(echo).maxInboundMessageSize(Integer.MAX_VALUE)
        .build().start();
    channel = ManagedChannelBuilder.forTarget("echo.test:50051")
        .maxInboundMessageSize(Integer.MAX_VALUE).build();
  }

  @AfterAll
  static void stop() throws InterruptedException {
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }

  private static byte[] pattern(int size) {
    byte[] bytes = new byte[size];
    new Random(size).nextBytes(bytes);
    return bytes;
  }

  @Test
  void providersServeOverDpumesh() {
    assertEquals(1, server.getListenSockets().size());
    assertInstanceOf(DpumeshAddress.class, server.getListenSockets().get(0));
  }

  @Test
  void unaryEchoesAcrossSizes() {
    for (int size : new int[] {0, 1, 8063, 8064, 8065, 65536, 1 << 20, 4 << 20}) {
      byte[] data = pattern(size);
      assertArrayEquals(data, ClientCalls.blockingUnaryCall(channel, UNARY, CallOptions.DEFAULT, data),
          "size " + size);
    }
  }

  @Test
  void serverStreamingDeliversSixteenMiB() {
    byte[] request = ByteBuffer.allocate(8).putInt(64).putInt(256 * 1024).array();
    Iterator<byte[]> replies = ClientCalls.blockingServerStreamingCall(channel, SOURCE, CallOptions.DEFAULT, request);
    byte[] chunk = pattern(256 * 1024);
    int count = 0;
    while (replies.hasNext()) {
      assertArrayEquals(chunk, replies.next());
      count++;
    }
    assertEquals(64, count);
  }

  @Test
  void concurrentCallsShareTheConnection() throws Exception {
    List<CompletableFuture<Void>> calls = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      final int size = 64 + i;
      calls.add(CompletableFuture.runAsync(() -> {
        for (int j = 0; j < 20; j++) {
          byte[] data = pattern(size);
          assertArrayEquals(data, ClientCalls.blockingUnaryCall(channel, UNARY, CallOptions.DEFAULT, data));
        }
      }));
    }
    CompletableFuture.allOf(calls.toArray(new CompletableFuture<?>[0])).get(60, TimeUnit.SECONDS);
  }

  @Test
  void deadlineThenReuse() {
    StatusRuntimeException late = assertThrows(StatusRuntimeException.class, () ->
        ClientCalls.blockingUnaryCall(channel, SLEEP,
            CallOptions.DEFAULT.withDeadlineAfter(100, TimeUnit.MILLISECONDS),
            ByteBuffer.allocate(4).putInt(5000).array()));
    assertEquals(Status.Code.DEADLINE_EXCEEDED, late.getStatus().getCode());
    byte[] data = pattern(100);
    assertArrayEquals(data, ClientCalls.blockingUnaryCall(channel, UNARY, CallOptions.DEFAULT, data));
  }

  @Test
  void manyChannelsOpenAndClose() throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      ManagedChannel fresh = ManagedChannelBuilder.forTarget("echo.test:50051").build();
      byte[] data = pattern(1000);
      assertArrayEquals(data, ClientCalls.blockingUnaryCall(fresh, UNARY, CallOptions.DEFAULT, data));
      fresh.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
