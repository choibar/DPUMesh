using System.Runtime.InteropServices;
using System.Text;

namespace Dpumesh.Grpc;

/// <summary>
/// The process's DPUMesh runtime: one channel opened from the DPUMESH_*
/// configuration on first use and closed when the process exits.
/// </summary>
public sealed unsafe class DpumeshRuntime
{
    private static readonly Lazy<DpumeshRuntime> s_shared = new(() => new DpumeshRuntime());

    private readonly object _lock = new();
    private IntPtr _rt;
    private Action<IntPtr>? _accept;

    private DpumeshRuntime()
    {
        if (Native.AbiVersionOf() != Native.AbiVersion)
        {
            throw new InvalidOperationException(
                $"libdpumesh_stream ABI {Native.AbiVersionOf()} is not {Native.AbiVersion}");
        }
        var err = stackalloc byte[256];
        _rt = Native.RuntimeOpen(err, 256);
        if (_rt == IntPtr.Zero)
        {
            throw new IOException("DPUMesh runtime: " + Marshal.PtrToStringUTF8((IntPtr)err));
        }
        PostMax = (int)Native.RuntimePostMax(_rt);
        AppDomain.CurrentDomain.ProcessExit += (_, _) => Close();
    }

    /// <summary>True when DPUMESH_ENABLE is "1".</summary>
    public static bool Enabled => Environment.GetEnvironmentVariable("DPUMESH_ENABLE") == "1";

    internal static DpumeshRuntime Shared => s_shared.Value;

    internal int PostMax { get; }

    // Opens a stream to a "<host>:<port>" service target. A stream that
    // arrives after `cancellationToken` fired is reset.
    internal Task<NativeStream> ConnectAsync(string service, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var pending = new PendingConnect(cancellationToken);
        var handle = GCHandle.Alloc(pending);
        var bytes = Encoding.UTF8.GetBytes(service + "\0");
        lock (_lock)
        {
            if (_rt == IntPtr.Zero)
            {
                handle.Free();
                throw new ObjectDisposedException(nameof(DpumeshRuntime));
            }
            fixed (byte* p = bytes)
            {
                Native.Connect(_rt, p, &OnConnected, GCHandle.ToIntPtr(handle));
            }
        }
        return pending.Task;
    }

    // Routes the streams the DPU delivers for DPUMESH_SERVICE to `accept`.
    internal void Listen(Action<IntPtr> accept)
    {
        lock (_lock)
        {
            if (_rt == IntPtr.Zero) throw new ObjectDisposedException(nameof(DpumeshRuntime));
            _accept = accept;
            var rc = Native.Listen(_rt, &OnAccepted, IntPtr.Zero);
            if (rc != 0) throw new IOException($"dms_listen failed: errno {-rc}");
        }
    }

    internal void StopListening(Action<IntPtr> accept)
    {
        lock (_lock)
        {
            if (_accept != accept) return;
            _accept = null;
            if (_rt != IntPtr.Zero) Native.Listen(_rt, null, IntPtr.Zero);
        }
    }

    private void Close()
    {
        lock (_lock)
        {
            if (_rt == IntPtr.Zero) return;
            _accept = null;
            Native.RuntimeClose(_rt);
            _rt = IntPtr.Zero;
        }
    }

    [UnmanagedCallersOnly]
    private static void OnConnected(IntPtr ctx, IntPtr stream, int err, IntPtr message)
    {
        try
        {
            var handle = GCHandle.FromIntPtr(ctx);
            var pending = (PendingConnect)handle.Target!;
            handle.Free();
            if (stream == IntPtr.Zero)
            {
                pending.Fail(new IOException(
                    $"DPUMesh connect failed: {Marshal.PtrToStringUTF8(message)} (errno {err})"));
            }
            else
            {
                pending.Complete(stream);
            }
        }
        catch { }
    }

    [UnmanagedCallersOnly]
    private static void OnAccepted(IntPtr ctx, IntPtr stream)
    {
        try
        {
            var accept = s_shared.Value._accept;
            if (accept == null) Native.StreamAbort(stream);
            else accept(stream);
        }
        catch
        {
            Native.StreamAbort(stream);
        }
    }

    private sealed class PendingConnect
    {
        private readonly TaskCompletionSource<NativeStream> _tcs =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenRegistration _registration;

        public PendingConnect(CancellationToken cancellationToken)
        {
            _registration = cancellationToken.Register(
                static state => ((PendingConnect)state!)._tcs.TrySetCanceled(), this);
        }

        public Task<NativeStream> Task => _tcs.Task;

        public void Complete(IntPtr handle)
        {
            _registration.Dispose();
            if (_tcs.Task.IsCompleted)
            {
                Native.StreamAbort(handle);
                return;
            }
            NativeStream stream;
            try
            {
                stream = new NativeStream(handle);
            }
            catch (Exception e)
            {
                _tcs.TrySetException(e);
                return;
            }
            if (!_tcs.TrySetResult(stream)) stream.Abort();
        }

        public void Fail(Exception error)
        {
            _registration.Dispose();
            _tcs.TrySetException(error);
        }
    }
}
