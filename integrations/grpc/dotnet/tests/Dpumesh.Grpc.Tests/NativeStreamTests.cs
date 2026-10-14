using System.Buffers;
using System.IO.Pipelines;
using System.Security.Cryptography;
using System.Threading.Channels;
using Xunit;

namespace Dpumesh.Grpc.Tests;

// NativeStream pairs over the loopback, below HTTP/2: HTTP/2 flow control
// keeps a gRPC connection's input drained, so only a raw reader that stops
// reading drives the input above its threshold and the receive credit hold.
[Collection("native")]
public sealed class NativeStreamTests
{
    private static async Task<(NativeStream client, NativeStream server)> PairAsync()
    {
        System.Runtime.CompilerServices.RuntimeHelpers.RunClassConstructor(typeof(LoopbackServer).TypeHandle);
        var accepted = Channel.CreateUnbounded<NativeStream>();
        void Accept(IntPtr handle) => accepted.Writer.TryWrite(new NativeStream(handle));
        DpumeshRuntime.Shared.Listen(Accept);
        try
        {
            var client = await DpumeshRuntime.Shared.ConnectAsync("raw.test:1", CancellationToken.None);
            var server = await accepted.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
            return (client, server);
        }
        finally
        {
            DpumeshRuntime.Shared.StopListening(Accept);
        }
    }

    private static async Task<byte[]> ReadExactlyAsync(PipeReader reader, int length)
    {
        var received = new byte[length];
        var at = 0;
        while (at < length)
        {
            var result = await reader.ReadAsync();
            foreach (var segment in result.Buffer)
            {
                segment.Span.CopyTo(received.AsSpan(at));
                at += segment.Length;
            }
            reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted && at < length) throw new EndOfStreamException($"{at} of {length}");
        }
        return received;
    }

    [Fact]
    public async Task StalledReaderHoldsCreditThenReceivesEverything()
    {
        var (client, server) = await PairAsync();
        var payload = new byte[8 << 20];
        RandomNumberGenerator.Fill(payload);

        // The server does not read while the client writes: its input passes
        // the pause threshold, the stream withholds credit and the client's
        // writes wait for transmit credit.
        var write = Task.Run(async () =>
        {
            await client.Output.WriteAsync(payload);
            await client.DisposeAsync();
        });
        await Task.Delay(300);
        Assert.False(write.IsCompleted);

        var received = await ReadExactlyAsync(server.Input, payload.Length).WaitAsync(TimeSpan.FromSeconds(20));
        Assert.Equal(payload, received);
        await write.WaitAsync(TimeSpan.FromSeconds(5));

        // The client's close arrives as the end of the server's input.
        var end = await server.Input.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(end.IsCompleted);
        Assert.True(end.Buffer.IsEmpty);
        await server.DisposeAsync();
    }

    [Fact]
    public async Task AbortFailsTheLocalSideAndEndsThePeer()
    {
        var (client, server) = await PairAsync();
        client.Abort();
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await client.Output.WriteAsync(new byte[16]);
            await client.Input.ReadAsync();
        });
        var end = await server.Input.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(end.IsCompleted);
        Assert.True(client.Closed.IsCancellationRequested);
        await server.DisposeAsync();
    }
}
