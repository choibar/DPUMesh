using Google.Protobuf;
using Grpc.Core;
using Dpumesh.Test;

namespace Dpumesh.Grpc.Tests;

public sealed class EchoService : Echo.EchoBase
{
    public override Task<Payload> Unary(Payload request, ServerCallContext context) =>
        Task.FromResult(request);

    public override async Task Bidi(IAsyncStreamReader<Payload> requestStream,
        IServerStreamWriter<Payload> responseStream, ServerCallContext context)
    {
        await foreach (var payload in requestStream.ReadAllAsync(context.CancellationToken))
        {
            await responseStream.WriteAsync(payload);
        }
    }

    public override async Task<Payload> Sink(IAsyncStreamReader<Payload> requestStream,
        ServerCallContext context)
    {
        long bytes = 0;
        await foreach (var payload in requestStream.ReadAllAsync(context.CancellationToken))
        {
            bytes += payload.Data.Length;
        }
        return new Payload { Count = bytes };
    }

    public override async Task Source(Payload request, IServerStreamWriter<Payload> responseStream,
        ServerCallContext context)
    {
        for (long i = 0; i < request.Count; ++i)
        {
            await responseStream.WriteAsync(new Payload { Data = request.Data, Count = i });
        }
    }

    public override async Task<Payload> Sleep(Payload request, ServerCallContext context)
    {
        await Task.Delay(request.DelayMs, context.CancellationToken);
        return request;
    }
}
