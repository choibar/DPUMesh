using System.Reflection;
using System.Runtime.InteropServices;

namespace Dpumesh.Grpc;

// The stream C ABI (integrations/grpc/cpp/include/dpumesh_stream.h).
// DPUMESH_STREAM_LIBRARY names another library with the same ABI, such as the
// in-process loopback the tests use.
internal static unsafe partial class Native
{
    private const string Library = "dpumesh_stream";
    internal const int AbiVersion = 1;
    internal const int RecvOk = 0;
    internal const int RecvHold = 1;
    internal const int EAGAIN = 11;

    static Native()
    {
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, Resolve);
    }

    private static IntPtr Resolve(string name, Assembly assembly, DllImportSearchPath? path)
    {
        if (name != Library) return IntPtr.Zero;
        var overridePath = Environment.GetEnvironmentVariable("DPUMESH_STREAM_LIBRARY");
        if (!string.IsNullOrEmpty(overridePath)) return NativeLibrary.Load(overridePath);
        return NativeLibrary.TryLoad("libdpumesh_stream.so", assembly, path, out var handle)
            ? handle : IntPtr.Zero;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct Callbacks
    {
        public delegate* unmanaged<IntPtr, nuint, IntPtr> RecvAlloc;
        public delegate* unmanaged<IntPtr, IntPtr, nuint, int> Recv;
        public delegate* unmanaged<IntPtr, void> Writable;
        public delegate* unmanaged<IntPtr, void> Eof;
        public delegate* unmanaged<IntPtr, int, IntPtr, void> Error;
        public delegate* unmanaged<IntPtr, void> Released;
    }

    [LibraryImport(Library, EntryPoint = "dms_abi_version")]
    internal static partial int AbiVersionOf();

    [LibraryImport(Library, EntryPoint = "dms_runtime_open")]
    internal static partial IntPtr RuntimeOpen(byte* err, nuint errLen);

    [LibraryImport(Library, EntryPoint = "dms_runtime_close")]
    internal static partial void RuntimeClose(IntPtr rt);

    [LibraryImport(Library, EntryPoint = "dms_runtime_post_max")]
    internal static partial nuint RuntimePostMax(IntPtr rt);

    [LibraryImport(Library, EntryPoint = "dms_connect")]
    internal static partial void Connect(IntPtr rt, byte* service,
        delegate* unmanaged<IntPtr, IntPtr, int, IntPtr, void> done, IntPtr ctx);

    [LibraryImport(Library, EntryPoint = "dms_listen")]
    internal static partial int Listen(IntPtr rt,
        delegate* unmanaged<IntPtr, IntPtr, void> accept, IntPtr ctx);

    [LibraryImport(Library, EntryPoint = "dms_stream_bind")]
    internal static partial int StreamBind(IntPtr s, Callbacks* cb, IntPtr ctx);

    [LibraryImport(Library, EntryPoint = "dms_stream_write")]
    internal static partial nint StreamWrite(IntPtr s, byte* data, nuint len);

    [LibraryImport(Library, EntryPoint = "dms_stream_resume")]
    internal static partial void StreamResume(IntPtr s);

    [LibraryImport(Library, EntryPoint = "dms_stream_close")]
    internal static partial void StreamClose(IntPtr s);

    [LibraryImport(Library, EntryPoint = "dms_stream_abort")]
    internal static partial void StreamAbort(IntPtr s);
}
