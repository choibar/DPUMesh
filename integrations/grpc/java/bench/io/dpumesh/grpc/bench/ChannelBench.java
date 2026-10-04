package io.dpumesh.grpc.bench;

import io.dpumesh.grpc.DpumeshGrpc;
import io.grpc.CallOptions;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * channel-bench for grpc-java: the peer of integrations/grpc/go/cmd/channel-bench.
 * It speaks the same wire (/dmesh.ChannelBench/Echo, a 64-byte raw request
 * echoed unchanged, bytes 0-7 the little-endian sequence number and 8-15 the
 * worker ID), takes the same flags and prints the same JSON lines, over
 * DPUMesh or, with -tcp host:port, kernel TCP. -start-file is not supported.
 * native_dials counts each connection's transitions to READY: grpc-java
 * exposes no dial hook.
 */
public final class ChannelBench {
  private static final int PAYLOAD_BYTES = 64;

  private static final MethodDescriptor.Marshaller<byte[]> RAW =
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
            throw Status.INTERNAL.withCause(e).asRuntimeException();
          }
        }
      };

  private static final MethodDescriptor<byte[], byte[]> ECHO =
      MethodDescriptor.<byte[], byte[]>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("dmesh.ChannelBench/Echo")
          .setRequestMarshaller(RAW)
          .setResponseMarshaller(RAW)
          .build();

  private ChannelBench() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> flags;
    try {
      flags = parseFlags(args);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      System.exit(2);
      return;
    }
    String tcp = flags.getOrDefault("tcp", "");
    String mode = flags.getOrDefault("mode", "client");
    if (mode.equals("server")) {
      runServer(tcp);
      return;
    }
    if (!mode.equals("client")) {
      System.err.println("unknown mode " + mode);
      System.exit(2);
    }
    int connections = Integer.parseInt(flags.getOrDefault("connections", "1"));
    int concurrency = Integer.parseInt(flags.getOrDefault("concurrency", "64"));
    long warmup = durationNanos(flags.getOrDefault("warmup", "3s"));
    long duration = durationNanos(flags.getOrDefault("duration", "10s"));
    long rpcTimeout = durationNanos(flags.getOrDefault("rpc-timeout", "5s"));
    String ip = System.getenv("DPUMESH_SERVICE_IP");
    String port = System.getenv("DPUMESH_SERVICE_PORT");
    if ((tcp.isEmpty() && (ip == null || ip.isEmpty() || port == null || port.isEmpty()))
        || connections < 1 || connections > 4 || concurrency < connections || duration <= 0) {
      System.err.println("set DPUMESH_SERVICE_IP/PORT for the client, connections 1..4, "
          + "concurrency >= connections, and valid durations");
      System.exit(2);
    }
    String target = tcp.isEmpty() ? ip + ":" + port : tcp;
    int code = new Client(connections, concurrency, warmup, duration, rpcTimeout, tcp, target).run();
    System.exit(code);
  }

  // Accepts -name value, -name=value and the -- forms, as Go's flag package does.
  static Map<String, String> parseFlags(String[] args) {
    Map<String, String> flags = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (!args[i].startsWith("-")) throw new IllegalArgumentException("unexpected argument " + args[i]);
      String body = args[i].replaceFirst("^--?", "");
      int eq = body.indexOf('=');
      if (eq >= 0) {
        flags.put(body.substring(0, eq), body.substring(eq + 1));
      } else if (i + 1 < args.length) {
        flags.put(body, args[++i]);
      } else {
        throw new IllegalArgumentException("flag -" + body + " needs a value");
      }
    }
    return flags;
  }

  // Go time.ParseDuration for one number and one unit of ns, us, ms, s, m or h.
  static long durationNanos(String text) {
    String unit = text.replaceFirst("^[0-9.]+", "");
    double value = Double.parseDouble(text.substring(0, text.length() - unit.length()));
    double scale;
    switch (unit) {
      case "ns": scale = 1; break;
      case "us": scale = 1e3; break;
      case "ms": scale = 1e6; break;
      case "s": scale = 1e9; break;
      case "m": scale = 60e9; break;
      case "h": scale = 3600e9; break;
      default: throw new IllegalArgumentException("invalid duration " + text);
    }
    return (long) (value * scale);
  }

  private static void runServer(String tcp) throws Exception {
    ServerServiceDefinition service = ServerServiceDefinition.builder("dmesh.ChannelBench")
        .addMethod(ECHO, ServerCalls.asyncUnaryCall((byte[] request, StreamObserver<byte[]> reply) -> {
          reply.onNext(request);
          reply.onCompleted();
        }))
        .build();
    NettyServerBuilder builder = tcp.isEmpty()
        ? DpumeshGrpc.serverBuilder()
        : NettyServerBuilder.forAddress(hostPort(tcp));
    Server server = builder.addService(service).build().start();
    System.err.println("CHANNEL_BENCH_SERVER_READY service="
        + (tcp.isEmpty() ? System.getenv("DPUMESH_SERVICE") : tcp));
    // Exit 0 on SIGTERM and SIGINT after a graceful stop; the DPUMesh runtime
    // closes in its shutdown hook.
    CountDownLatch stop = new CountDownLatch(1);
    for (String name : new String[] {"TERM", "INT"}) {
      sun.misc.Signal.handle(new sun.misc.Signal(name), signal -> stop.countDown());
    }
    stop.await();
    server.shutdown();
    if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
    System.err.println("CHANNEL_BENCH_SERVER_CLOSED");
    System.exit(0);
  }

  static InetSocketAddress hostPort(String address) {
    int colon = address.lastIndexOf(':');
    return new InetSocketAddress(address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
  }

  private static final class Client {
    private final int connections;
    private final int concurrency;
    private final long warmup;
    private final long duration;
    private final long rpcTimeout;
    private final String tcp;
    private final String target;
    private final AtomicBoolean cancel = new AtomicBoolean();
    private final List<String> errors = new ArrayList<>();
    private long start;
    private long end;
    private CountDownLatch running;

    Client(int connections, int concurrency, long warmup, long duration, long rpcTimeout,
        String tcp, String target) {
      this.connections = connections;
      this.concurrency = concurrency;
      this.warmup = warmup;
      this.duration = duration;
      this.rpcTimeout = rpcTimeout;
      this.tcp = tcp;
      this.target = target;
    }

    final class Worker {
      final int id;
      final ManagedChannel channel;
      final AtomicLong dials;
      final byte[] payload = new byte[PAYLOAD_BYTES];
      long sequence;
      long started;
      long completed;
      long rpcErrors;
      long[] latencies = new long[16384];
      int count;
      String error;

      Worker(int id, ManagedChannel channel, AtomicLong dials) {
        this.id = id;
        this.channel = channel;
        this.dials = dials;
        Arrays.fill(payload, (byte) (id + 1));
        ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).putLong(8, id);
      }

      // One closed RPC loop: each completion issues the next call.
      void issue() {
        if (cancel.get() || System.nanoTime() >= end) {
          running.countDown();
          return;
        }
        sequence++;
        ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).putLong(0, sequence);
        started = System.nanoTime();
        ClientCalls.asyncUnaryCall(
            channel.newCall(ECHO, CallOptions.DEFAULT.withDeadlineAfter(rpcTimeout, TimeUnit.NANOSECONDS)),
            payload.clone(),
            new StreamObserver<byte[]>() {
              byte[] reply;

              @Override
              public void onNext(byte[] value) {
                reply = value;
              }

              @Override
              public void onError(Throwable t) {
                fail(Status.fromThrowable(t).toString());
              }

              @Override
              public void onCompleted() {
                long done = System.nanoTime();
                if (!Arrays.equals(reply, payload)) {
                  fail("payload mismatch: " + PAYLOAD_BYTES + " bytes sent, "
                      + (reply == null ? 0 : reply.length) + " received");
                  return;
                }
                if (dials.get() != 1) {
                  fail("unexpected reconnect: " + dials.get() + " connections");
                  return;
                }
                completed++;
                if (done >= start && done < end) {
                  if (count == latencies.length) latencies = Arrays.copyOf(latencies, count * 2);
                  latencies[count++] = done - started;
                }
                issue();
              }
            });
      }

      void fail(String message) {
        rpcErrors++;
        error = "worker " + id + " request " + sequence + ": " + message;
        cancel.set(true);
        running.countDown();
      }
    }

    // Counts the channel's transitions to READY, its connection count.
    static void watchReady(ManagedChannel channel, ConnectivityState last, AtomicLong ready) {
      ConnectivityState now = channel.getState(false);
      if (now == ConnectivityState.READY && last != ConnectivityState.READY) ready.incrementAndGet();
      if (now == ConnectivityState.SHUTDOWN) return;
      channel.notifyWhenStateChanged(now, () -> watchReady(channel, now, ready));
    }

    int run() throws Exception {
      int[] perConn = new int[connections];
      for (int i = 0; i < connections; i++) {
        perConn[i] = concurrency / connections + (i < concurrency % connections ? 1 : 0);
      }
      List<ManagedChannel> channels = new ArrayList<>();
      List<AtomicLong> dials = new ArrayList<>();
      long rpcErrors = 0;
      for (int i = 0; i < connections && errors.isEmpty(); i++) {
        NettyChannelBuilder builder = tcp.isEmpty()
            ? DpumeshGrpc.channelBuilder(target)
            : NettyChannelBuilder.forAddress(hostPort(tcp));
        ManagedChannel channel = builder.usePlaintext().disableRetry().build();
        AtomicLong ready = new AtomicLong();
        watchReady(channel, ConnectivityState.IDLE, ready);
        channels.add(channel);
        dials.add(ready);
        byte[] payload = new byte[PAYLOAD_BYTES];
        Arrays.fill(payload, (byte) (i + 1));
        try {
          byte[] reply = ClientCalls.blockingUnaryCall(channel, ECHO,
              CallOptions.DEFAULT.withDeadlineAfter(rpcTimeout, TimeUnit.NANOSECONDS), payload);
          if (!Arrays.equals(reply, payload)) throw new IllegalStateException("payload mismatch");
        } catch (RuntimeException e) {
          rpcErrors++;
          errors.add("connection " + (i + 1) + " preflight: " + e.getMessage());
        }
      }
      List<Worker> workers = new ArrayList<>();
      double cpuPct = 0;
      double cpuSeconds = 0;
      if (errors.isEmpty()) {
        System.err.println("PREFLIGHT_OK connections=" + connections + " concurrency="
            + Arrays.toString(perConn) + " payload=" + PAYLOAD_BYTES + "B");
        int id = 0;
        for (int conn = 0; conn < connections; conn++) {
          for (int j = 0; j < perConn[conn]; j++) {
            workers.add(new Worker(id++, channels.get(conn), dials.get(conn)));
          }
        }
        running = new CountDownLatch(workers.size());
        long loadStart = System.nanoTime();
        start = loadStart + warmup;
        end = start + duration;
        for (Worker w : workers) w.issue();
        sleepUntil(start);
        com.sun.management.OperatingSystemMXBean os =
            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long cpuStart = os.getProcessCpuTime();
        long cpuStartAt = System.nanoTime();
        marker("measure_start", start);
        sleepUntil(end);
        cpuSeconds = (System.nanoTime() - cpuStartAt) / 1e9;
        cpuPct = 100 * (os.getProcessCpuTime() - cpuStart) / 1e9 / cpuSeconds;
        marker("measure_end", end);
        if (!running.await(rpcTimeout + TimeUnit.SECONDS.toNanos(30), TimeUnit.NANOSECONDS)) {
          errors.add("workers still running after the measurement");
        }
      }
      long total = 0;
      int samples = 0;
      for (Worker w : workers) samples += w.count;
      long[] latencies = new long[samples];
      int at = 0;
      for (Worker w : workers) {
        total += w.completed;
        rpcErrors += w.rpcErrors;
        if (w.error != null) errors.add(w.error);
        System.arraycopy(w.latencies, 0, latencies, at, w.count);
        at += w.count;
      }
      Arrays.sort(latencies);
      for (ManagedChannel channel : channels) channel.shutdown();
      for (ManagedChannel channel : channels) channel.awaitTermination(10, TimeUnit.SECONDS);
      long[] dialCounts = dials.stream().mapToLong(AtomicLong::get).toArray();
      long reconnects = 0;
      for (long d : dialCounts) {
        if (d != 1) errors.add("expected one connection per channel, got " + d);
        reconnects += Math.max(0, d - 1);
      }
      double elapsed = (end - start) / 1e9;
      if (errors.isEmpty() && latencies.length == 0) {
        errors.add("no successful RPC completions in measurement window");
      }
      double mean = 0;
      for (long l : latencies) mean += l;
      mean = latencies.length == 0 ? 0 : mean / latencies.length / 1e3;
      StringBuilder out = new StringBuilder("{\"event\":\"result\"");
      out.append(",\"ok\":").append(errors.isEmpty());
      out.append(",\"connections\":").append(connections);
      out.append(",\"concurrency\":").append(concurrency);
      out.append(",\"concurrency_per_conn\":").append(Arrays.toString(perConn).replace(" ", ""));
      out.append(",\"payload_bytes\":").append(PAYLOAD_BYTES);
      out.append(",\"warmup_seconds\":").append(warmup / 1e9);
      out.append(",\"duration_seconds\":").append(duration / 1e9);
      out.append(",\"elapsed_secs\":").append(elapsed);
      out.append(",\"client_process_cpu_pct\":").append(cpuPct);
      out.append(",\"client_cpu_sample_seconds\":").append(cpuSeconds);
      out.append(",\"measurement_start\":\"").append(wall(start)).append('"');
      out.append(",\"measurement_end\":\"").append(wall(end)).append('"');
      out.append(",\"completed\":").append(latencies.length);
      out.append(",\"total_completed_including_warmup_and_drain\":").append(total);
      out.append(",\"rpc_errors\":").append(rpcErrors);
      out.append(",\"native_dials\":").append(Arrays.toString(dialCounts).replace(" ", ""));
      out.append(",\"reconnects\":").append(reconnects);
      out.append(",\"qps\":").append(elapsed > 0 ? latencies.length / elapsed : 0);
      out.append(",\"latency_mean_us\":").append(mean);
      out.append(",\"latency_p50_us\":").append(percentile(latencies, 0.5));
      out.append(",\"latency_p99_us\":").append(percentile(latencies, 0.99));
      if (!errors.isEmpty()) out.append(",\"error\":").append(jsonString(String.join("\n", errors)));
      out.append('}');
      System.out.println(out);
      System.out.flush();
      if (!errors.isEmpty()) System.err.println(String.join("\n", errors));
      return errors.isEmpty() ? 0 : 1;
    }

    static double percentile(long[] sorted, double p) {
      if (sorted.length == 0) return 0;
      return sorted[(int) Math.ceil(p * sorted.length) - 1] / 1e3;
    }

    static void sleepUntil(long deadline) throws InterruptedException {
      long left;
      while ((left = deadline - System.nanoTime()) > 0) TimeUnit.NANOSECONDS.sleep(left);
    }

    static String wall(long nanoTime) {
      Instant now = Instant.now();
      return now.plusNanos(nanoTime - System.nanoTime()).toString();
    }

    static void marker(String event, long at) {
      System.out.println("{\"event\":\"" + event + "\",\"timestamp\":\"" + wall(at)
          + "\",\"emitted_at\":\"" + Instant.now() + "\"}");
      System.out.flush();
    }

    static String jsonString(String s) {
      return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
  }
}
