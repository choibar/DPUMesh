using System.IO.Pipelines;
using System.Net;
using System.Threading.Channels;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Http.Features;

namespace Dpumesh.Grpc;

// Kestrel's transport for a DpumeshEndPoint. It coexists with the socket
// transport, which keeps every other endpoint.
internal sealed class DpumeshTransportFactory : IConnectionListenerFactory, IConnectionListenerFactorySelector
{
    public bool CanBind(EndPoint endpoint) => endpoint is DpumeshEndPoint;

    public ValueTask<IConnectionListener> BindAsync(EndPoint endpoint, CancellationToken cancellationToken = default)
    {
        if (endpoint is not DpumeshEndPoint dpumesh)
        {
            throw new NotSupportedException($"{endpoint} is not a DPUMesh endpoint");
        }
        return new ValueTask<IConnectionListener>(new DpumeshConnectionListener(dpumesh));
    }
}

internal sealed class DpumeshConnectionListener : IConnectionListener
{
    private readonly Channel<NativeStream> _accepted = Channel.CreateUnbounded<NativeStream>(
        new UnboundedChannelOptions { SingleWriter = true });
    private readonly Action<IntPtr> _accept;

    public DpumeshConnectionListener(DpumeshEndPoint endpoint)
    {
        EndPoint = endpoint;
        _accept = OnAccept;
        DpumeshRuntime.Shared.Listen(_accept);
    }

    public EndPoint EndPoint { get; }

    // Runs on the runtime's callback thread. Binding at once keeps the bytes
    // that arrive before Kestrel accepts.
    private void OnAccept(IntPtr handle)
    {
        NativeStream stream;
        try
        {
            stream = new NativeStream(handle);
        }
        catch
        {
            return;
        }
        if (!_accepted.Writer.TryWrite(stream)) stream.Abort();
    }

    public async ValueTask<ConnectionContext?> AcceptAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var stream = await _accepted.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            return new DpumeshConnectionContext(stream, EndPoint);
        }
        catch (ChannelClosedException)
        {
            return null;
        }
    }

    public ValueTask UnbindAsync(CancellationToken cancellationToken = default)
    {
        DpumeshRuntime.Shared.StopListening(_accept);
        _accepted.Writer.TryComplete();
        return default;
    }

    public async ValueTask DisposeAsync()
    {
        await UnbindAsync().ConfigureAwait(false);
        while (_accepted.Reader.TryRead(out var stream)) stream.Abort();
    }
}

internal sealed class DpumeshConnectionContext : ConnectionContext
{
    private static long s_nextId;
    private readonly NativeStream _stream;

    public DpumeshConnectionContext(NativeStream stream, EndPoint local)
    {
        _stream = stream;
        Transport = stream;
        LocalEndPoint = local;
        ConnectionId = "dpumesh-" + Interlocked.Increment(ref s_nextId);
        ConnectionClosed = stream.Closed;
    }

    public override string ConnectionId { get; set; }
    public override IFeatureCollection Features { get; } = new FeatureCollection();
    public override IDictionary<object, object?> Items { get; set; } = new Dictionary<object, object?>();
    public override IDuplexPipe Transport { get; set; }
    public override CancellationToken ConnectionClosed { get; set; }

    public override void Abort(ConnectionAbortedException abortReason) => _stream.Abort(abortReason);

    public override ValueTask DisposeAsync() => _stream.DisposeAsync();
}
