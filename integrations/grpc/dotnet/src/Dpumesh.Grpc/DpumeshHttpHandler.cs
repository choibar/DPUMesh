using System.IO.Pipelines;

namespace Dpumesh.Grpc;

/// <summary>
/// HTTP handlers for gRPC clients over DPUMesh, such as
/// <c>GrpcChannel.ForAddress("http://10.96.0.5:7070", new GrpcChannelOptions { HttpHandler = DpumeshHttpHandler.Create() })</c>.
/// </summary>
public static class DpumeshHttpHandler
{
    /// <summary>
    /// A handler whose connections are DPUMesh streams to the request's
    /// "&lt;host&gt;:&lt;port&gt;" service address.
    /// </summary>
    public static SocketsHttpHandler Create() => new()
    {
        ConnectCallback = static async (context, cancellationToken) =>
        {
            var endpoint = context.DnsEndPoint;
            var stream = await DpumeshRuntime.Shared
                .ConnectAsync($"{endpoint.Host}:{endpoint.Port}", cancellationToken)
                .ConfigureAwait(false);
            return new DuplexPipeStream(stream);
        },
    };

    // A Stream over a NativeStream's two pipes. Disposing it closes the stream
    // after the written bytes.
    private sealed class DuplexPipeStream : Stream
    {
        private readonly NativeStream _stream;
        private readonly Stream _reader;
        private readonly Stream _writer;

        public DuplexPipeStream(NativeStream stream)
        {
            _stream = stream;
            _reader = stream.Input.AsStream();
            _writer = stream.Output.AsStream();
        }

        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => _reader.Read(buffer, offset, count);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            _reader.ReadAsync(buffer, cancellationToken);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _reader.ReadAsync(buffer, offset, count, cancellationToken);
        public override void Write(byte[] buffer, int offset, int count) => _writer.Write(buffer, offset, count);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
            _writer.WriteAsync(buffer, cancellationToken);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            _writer.WriteAsync(buffer, offset, count, cancellationToken);
        public override void Flush() => _writer.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _writer.FlushAsync(cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) _stream.DisposeAsync().AsTask().GetAwaiter().GetResult();
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _stream.DisposeAsync().ConfigureAwait(false);
            await base.DisposeAsync().ConfigureAwait(false);
        }
    }
}
