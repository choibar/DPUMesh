package io.dpumesh.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

// The process's DPUMesh runtime over the stream C ABI
// (integrations/grpc/cpp/include/dpumesh_stream.h), through libdpumesh_jni.
// DPUMESH_STREAM_LIBRARY names another build of the ABI, such as the
// in-process loopback the tests use. The runtime opens on first use and closes
// at JVM shutdown: a process that exits with its channel open leaves the DPU
// to find it gone mid-stream.
final class NativeBridge {
  static final int EAGAIN = 11;

  // A bound stream's events, delivered on the runtime's reactor thread.
  interface StreamSink {
    // Returns direct memory for `len` received bytes, or null to fail the stream.
    ByteBuffer allocate(int len);

    // The memory from allocate now holds `len` bytes. Returns true to withhold
    // the stream's receive credit until nativeResume.
    boolean received(int len);

    void writable();

    void eof();

    void error(int err, String message);

    // The last event: the stream's native state is gone.
    void released();
  }

  private static final Object lock = new Object();
  private static long runtime; // guarded by lock
  private static boolean closed; // guarded by lock
  private static int postMax;
  private static volatile LongConsumer acceptor;
  private static final Map<Long, CompletableFuture<Long>> connects = new ConcurrentHashMap<>();
  private static final AtomicLong nextRequest = new AtomicLong(1);

  private NativeBridge() {}

  static long runtime() {
    synchronized (lock) {
      if (runtime != 0) return runtime;
      if (closed) throw new IllegalStateException("the DPUMesh runtime is closed");
      loadJni();
      try {
        runtime = nativeOpen(System.getenv("DPUMESH_STREAM_LIBRARY"));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      postMax = nativePostMax(runtime);
      Runtime.getRuntime().addShutdownHook(new Thread(NativeBridge::close, "dpumesh-close"));
      return runtime;
    }
  }

  static void close() {
    synchronized (lock) {
      closed = true;
      if (runtime == 0) return;
      acceptor = null;
      nativeClose(runtime);
      runtime = 0;
    }
  }

  // Routes the streams the DPU delivers for DPUMESH_SERVICE to `onStream`.
  static void listen(LongConsumer onStream) {
    synchronized (lock) {
      long rt = runtime();
      acceptor = onStream;
      int rc = nativeListen(rt, true);
      if (rc != 0) throw new IllegalStateException("dms_listen failed: errno " + -rc);
    }
  }

  static void stopListening(LongConsumer onStream) {
    synchronized (lock) {
      if (acceptor != onStream) return;
      acceptor = null;
      if (runtime != 0) nativeListen(runtime, false);
    }
  }

  // Opens a stream to a "<host>:<port>" service target. A stream that arrives
  // after the future was cancelled is reset.
  static CompletableFuture<Long> connect(String service) {
    long rt = runtime();
    long request = nextRequest.getAndIncrement();
    CompletableFuture<Long> future = new CompletableFuture<>();
    connects.put(request, future);
    nativeConnect(rt, service, request);
    return future;
  }

  // Upcall, on the runtime's callback thread.
  private static void onAccept(long stream) {
    LongConsumer target = acceptor;
    if (target == null) {
      nativeAbortUnbound(stream);
    } else {
      target.accept(stream);
    }
  }

  // Upcall, on the runtime's callback thread.
  private static void onConnect(long request, long stream, int err, String message) {
    CompletableFuture<Long> future = connects.remove(request);
    if (stream == 0) {
      if (future != null) {
        future.completeExceptionally(
            new IOException("DPUMesh connect failed: " + message + " (errno " + err + ")"));
      }
      return;
    }
    if (future == null || !future.complete(stream)) nativeAbortUnbound(stream);
  }

  private static void loadJni() {
    String resource = "/native/linux-x86_64/libdpumesh_jni.so";
    try (InputStream in = NativeBridge.class.getResourceAsStream(resource)) {
      if (in != null) {
        Path file = Files.createTempFile("libdpumesh_jni", ".so");
        file.toFile().deleteOnExit();
        Files.copy(in, file, StandardCopyOption.REPLACE_EXISTING);
        System.load(file.toString());
        return;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    System.loadLibrary("dpumesh_jni");
  }

  static int postMax() {
    runtime();
    return postMax;
  }

  private static native long nativeOpen(String library) throws IOException;

  private static native void nativeClose(long runtime);

  private static native int nativePostMax(long runtime);

  private static native int nativeListen(long runtime, boolean enabled);

  private static native void nativeConnect(long runtime, String service, long request);

  static native long nativeBind(long stream, StreamSink sink) throws IOException;

  static native int nativeWriteDirect(long jstream, ByteBuffer buffer, int offset, int len);

  static native int nativeWriteArray(long jstream, byte[] array, int offset, int len);

  static native void nativeResume(long jstream);

  // `jstream` is freed after its sink's released(), which may run inside.
  static native void nativeEnd(long jstream, boolean abort);

  static native void nativeAbortUnbound(long stream);
}
