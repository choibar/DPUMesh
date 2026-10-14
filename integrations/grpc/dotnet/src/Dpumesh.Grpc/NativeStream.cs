using System.Buffers;
using System.IO.Pipelines;
using System.Runtime.InteropServices;

namespace Dpumesh.Grpc;

// One DPUMesh stream as a duplex pipe. Received bytes land in Input directly
// from the native receive, one copy; bytes written to Output are posted by a
// pump that waits for transmit credit. Completing Output closes the stream
// after the written bytes; Abort resets it.
internal sealed class NativeStream : IDuplexPipe, IAsyncDisposable
{
    // Unread input above which the stream withholds receive credit, as the C++
    // endpoint does, so a slow reader stops the peer instead of queueing.
    private const long PauseThreshold = 1024 * 1024;
    private const long ResumeThreshold = 512 * 1024;
    private const int EPIPE = 32;

    private static readonly unsafe Native.Callbacks* s_callbacks = CreateCallbacks();

    private readonly Pipe _input = new(new PipeOptions(
        pauseWriterThreshold: PauseThreshold, resumeWriterThreshold: ResumeThreshold,
        readerScheduler: PipeScheduler.ThreadPool, writerScheduler: PipeScheduler.Inline,
        useSynchronizationContext: false));
    private readonly Pipe _output = new(new PipeOptions(
        pauseWriterThreshold: PauseThreshold, resumeWriterThreshold: ResumeThreshold,
        readerScheduler: PipeScheduler.ThreadPool, writerScheduler: PipeScheduler.ThreadPool,
        useSynchronizationContext: false));

    // Guards the input writer, which the reactor thread and flush
    // continuations share. Held from recv_alloc until recv on the reactor
    // thread, while the native side fills the pinned memory.
    private readonly object _rxLock = new();
    private MemoryHandle _rxPin;
    private unsafe void* _scratch;
    private bool _flushPending;
    private bool _inputDone;

    // Guards the native handle: every call on it, and its close.
    private readonly object _handleLock = new();
    private IntPtr _handle;

    private readonly SemaphoreSlim _writable = new(0);
    private readonly CancellationTokenSource _closed = new();
    private Exception? _failure;
    private readonly Task _pump;

    internal unsafe NativeStream(IntPtr handle)
    {
        _handle = handle;
        var self = GCHandle.Alloc(this);
        var rc = Native.StreamBind(handle, s_callbacks, GCHandle.ToIntPtr(self));
        if (rc != 0)
        {
            self.Free();
            Native.StreamAbort(handle);
            throw new IOException($"dms_stream_bind failed: errno {-rc}");
        }
        _pump = Task.Run(PumpAsync);
    }

    public PipeReader Input => _input.Reader;
    public PipeWriter Output => _output.Writer;

    // Fires once the stream fails, is reset or is closed.
    public CancellationToken Closed => _closed.Token;

    // Resets the stream; pending and later reads and writes fail with `reason`.
    public void Abort(Exception? reason = null)
    {
        Fail(reason ?? new IOException("DPUMesh stream aborted"));
        CloseNative(abort: true);
    }

    // Closes the stream after the bytes already written.
    public async ValueTask DisposeAsync()
    {
        await _output.Writer.CompleteAsync().ConfigureAwait(false);
        await _pump.ConfigureAwait(false);
    }

    private async Task PumpAsync()
    {
        var reader = _output.Reader;
        Exception? error = null;
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync().ConfigureAwait(false);
                if (Volatile.Read(ref _failure) is { } failure) throw failure;
                var buffer = result.Buffer;
                foreach (var segment in buffer)
                {
                    await WriteAsync(segment).ConfigureAwait(false);
                }
                reader.AdvanceTo(buffer.End);
                if (result.IsCompleted) break;
            }
        }
        catch (Exception e)
        {
            error = e;
        }
        await reader.CompleteAsync(error).ConfigureAwait(false);
        CloseNative(abort: error != null);
        lock (_rxLock) CompleteInput(error);
    }

    private async ValueTask WriteAsync(ReadOnlyMemory<byte> data)
    {
        while (!data.IsEmpty)
        {
            var n = WriteSome(data.Span);
            if (n > 0)
            {
                data = data.Slice((int)n);
                continue;
            }
            if (n != -Native.EAGAIN)
            {
                throw Volatile.Read(ref _failure)
                    ?? new IOException($"DPUMesh write failed: errno {-n}");
            }
            // Each EAGAIN arms one TX_READY; a stale wake only retries.
            await _writable.WaitAsync().ConfigureAwait(false);
            if (Volatile.Read(ref _failure) is { } failure) throw failure;
        }
    }

    private unsafe nint WriteSome(ReadOnlySpan<byte> span)
    {
        lock (_handleLock)
        {
            if (_handle == IntPtr.Zero) return -EPIPE;
            fixed (byte* p = span)
            {
                return Native.StreamWrite(_handle, p, (nuint)span.Length);
            }
        }
    }

    private void CloseNative(bool abort)
    {
        lock (_handleLock)
        {
            var handle = _handle;
            if (handle == IntPtr.Zero) return;
            _handle = IntPtr.Zero;
            if (abort) Native.StreamAbort(handle);
            else Native.StreamClose(handle);
        }
        _closed.Cancel();
    }

    private void Fail(Exception error)
    {
        Interlocked.CompareExchange(ref _failure, error, null);
        lock (_rxLock) CompleteInput(error);
        _writable.Release();
        _output.Reader.CancelPendingRead();
    }

    // Requires _rxLock.
    private void CompleteInput(Exception? error)
    {
        if (_inputDone) return;
        _inputDone = true;
        _input.Writer.Complete(error);
    }

    // Requires _rxLock. Publishes the bytes advanced while a flush was held.
    private void AwaitFlush(ValueTask<FlushResult> flush)
    {
        _flushPending = true;
        flush.AsTask().ContinueWith(static (_, state) => ((NativeStream)state!).OnFlushed(),
            this, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private void OnFlushed()
    {
        lock (_rxLock)
        {
            if (!_inputDone)
            {
                var flush = _input.Writer.FlushAsync();
                if (!flush.IsCompleted)
                {
                    AwaitFlush(flush);
                    return;
                }
                if (flush.Result.IsCompleted) CompleteInput(null);
            }
            _flushPending = false;
        }
        lock (_handleLock)
        {
            if (_handle != IntPtr.Zero) Native.StreamResume(_handle);
        }
    }

    private static NativeStream From(IntPtr ctx) => (NativeStream)GCHandle.FromIntPtr(ctx).Target!;

    private static unsafe Native.Callbacks* CreateCallbacks()
    {
        var cb = (Native.Callbacks*)NativeMemory.Alloc((nuint)sizeof(Native.Callbacks));
        cb->RecvAlloc = &OnRecvAlloc;
        cb->Recv = &OnRecv;
        cb->Writable = &OnWritable;
        cb->Eof = &OnEof;
        cb->Error = &OnError;
        cb->Released = &OnReleased;
        return cb;
    }

    [UnmanagedCallersOnly]
    private static unsafe IntPtr OnRecvAlloc(IntPtr ctx, nuint len)
    {
        var self = From(ctx);
        Monitor.Enter(self._rxLock);
        try
        {
            if (self._inputDone)
            {
                // Nobody reads any more; the bytes are dropped in OnRecv.
                self._scratch = NativeMemory.Alloc(len);
                return (IntPtr)self._scratch;
            }
            var memory = self._input.Writer.GetMemory(checked((int)len));
            self._rxPin = memory.Pin();
            return (IntPtr)self._rxPin.Pointer;
        }
        catch
        {
            Monitor.Exit(self._rxLock);
            return IntPtr.Zero;
        }
    }

    [UnmanagedCallersOnly]
    private static unsafe int OnRecv(IntPtr ctx, IntPtr buf, nuint len)
    {
        var self = From(ctx);
        try
        {
            if (self._scratch != null)
            {
                NativeMemory.Free(self._scratch);
                self._scratch = null;
                return Native.RecvOk;
            }
            self._rxPin.Dispose();
            var writer = self._input.Writer;
            writer.Advance((int)len);
            if (self._flushPending) return Native.RecvHold;
            var flush = writer.FlushAsync();
            if (flush.IsCompleted)
            {
                if (flush.Result.IsCompleted) self.CompleteInput(null);
                return Native.RecvOk;
            }
            self.AwaitFlush(flush);
            return Native.RecvHold;
        }
        catch
        {
            return Native.RecvOk;
        }
        finally
        {
            Monitor.Exit(self._rxLock);
        }
    }

    [UnmanagedCallersOnly]
    private static void OnWritable(IntPtr ctx)
    {
        try { From(ctx)._writable.Release(); } catch { }
    }

    [UnmanagedCallersOnly]
    private static void OnEof(IntPtr ctx)
    {
        try
        {
            var self = From(ctx);
            lock (self._rxLock) self.CompleteInput(null);
        }
        catch { }
    }

    [UnmanagedCallersOnly]
    private static void OnError(IntPtr ctx, int err, IntPtr message)
    {
        try
        {
            var text = Marshal.PtrToStringUTF8(message);
            From(ctx).Fail(new IOException($"DPUMesh stream failed: {text} (errno {err})"));
        }
        catch { }
    }

    [UnmanagedCallersOnly]
    private static void OnReleased(IntPtr ctx)
    {
        try { GCHandle.FromIntPtr(ctx).Free(); } catch { }
    }
}
