using System.Security.Cryptography;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Dpumesh.Test;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Dpumesh.Grpc.Tests;

// A Kestrel gRPC server and a Grpc.Net.Client channel over
// libdpumesh_stream_loopback.so, which routes the process's streams to its own
// listener. Build it first: integrations/grpc/cpp (see the README).
public sealed class LoopbackServer : IAsyncLifetime
{
    static LoopbackServer()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DPUMESH_STREAM_LIBRARY")))
        {
            Environment.SetEnvironmentVariable("DPUMESH_STREAM_LIBRARY", FindLoopbackLibrary());
        }
        Environment.SetEnvironmentVariable("DPUMESH_SERVICE", "echo.test:50051");
    }

    public WebApplication? App { get; private set; }

    public GrpcChannel Channel { get; } = GrpcChannel.ForAddress("http://echo.test:50051",
        new GrpcChannelOptions
        {
            HttpHandler = DpumeshHttpHandler.Create(),
            MaxReceiveMessageSize = null,
            MaxSendMessageSize = null,
        });

    public Echo.EchoClient Client => new(Channel);

    public async Task InitializeAsync() => App = await StartAsync();

    public async Task DisposeAsync()
    {
        Channel.Dispose();
        if (App != null) await App.DisposeAsync();
    }

    public static async Task<WebApplication> StartAsync()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddGrpc(options =>
        {
            options.MaxReceiveMessageSize = null;
            options.MaxSendMessageSize = null;
        });
        builder.Services.AddDpumeshTransport();
        builder.WebHost.ConfigureKestrel(options => options.ListenDpumesh());
        var app = builder.Build();
        app.MapGrpcService<EchoService>();
        await app.StartAsync();
        return app;
    }

    private static string FindLoopbackLibrary()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "build", "grpc", "libdpumesh_stream_loopback.so");
            if (File.Exists(candidate)) return candidate;
        }
        throw new FileNotFoundException(
            "libdpumesh_stream_loopback.so not found; build integrations/grpc/cpp into build/grpc");
    }
}

[CollectionDefinition("loopback")]
public sealed class LoopbackCollection : ICollectionFixture<LoopbackServer>
{
}

[Collection("loopback")]
public sealed class LoopbackTests(LoopbackServer server)
{
    private static ByteString Bytes(int length)
    {
        var bytes = new byte[length];
        RandomNumberGenerator.Fill(bytes);
        return ByteString.CopyFrom(bytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(8063)]
    [InlineData(8064)]
    [InlineData(8065)]
    [InlineData(65536)]
    [InlineData(1 << 20)]
    [InlineData(4 << 20)]
    public async Task UnaryEchoesAcrossSizes(int size)
    {
        var data = Bytes(size);
        var reply = await server.Client.UnaryAsync(new Payload { Data = data });
        Assert.Equal(data, reply.Data);
    }

    [Fact]
    public async Task BidiEchoesInOrder()
    {
        using var call = server.Client.Bidi();
        var sent = Enumerable.Range(0, 1000).Select(i => Bytes(i % 300)).ToList();
        var reader = Task.Run(async () =>
        {
            var received = new List<ByteString>();
            await foreach (var payload in call.ResponseStream.ReadAllAsync()) received.Add(payload.Data);
            return received;
        });
        foreach (var data in sent) await call.RequestStream.WriteAsync(new Payload { Data = data });
        await call.RequestStream.CompleteAsync();
        Assert.Equal(sent, await reader);
    }

    [Fact]
    public async Task ConcurrentCallsShareTheConnection()
    {
        var calls = Enumerable.Range(0, 64).Select(async i =>
        {
            for (var j = 0; j < 20; ++j)
            {
                var data = Bytes(64 + i);
                var reply = await server.Client.UnaryAsync(new Payload { Data = data });
                Assert.Equal(data, reply.Data);
            }
        });
        await Task.WhenAll(calls);
    }

    [Fact]
    public async Task SlowReaderReceivesEveryByte()
    {
        // 16 MiB against a 1 MiB input threshold: the client's stream holds
        // receive credit while the reader sleeps, then resumes.
        var chunk = Bytes(256 * 1024);
        using var call = server.Client.Source(new Payload { Data = chunk, Count = 64 });
        long index = 0;
        await foreach (var payload in call.ResponseStream.ReadAllAsync())
        {
            Assert.Equal(index++, payload.Count);
            Assert.Equal(chunk, payload.Data);
            if (index % 8 == 0) await Task.Delay(20);
        }
        Assert.Equal(64, index);
    }

    [Fact]
    public async Task ClientStreamingDeliversEveryByte()
    {
        using var call = server.Client.Sink();
        var chunk = Bytes(512 * 1024);
        for (var i = 0; i < 32; ++i) await call.RequestStream.WriteAsync(new Payload { Data = chunk });
        await call.RequestStream.CompleteAsync();
        Assert.Equal(32L * chunk.Length, (await call).Count);
    }

    [Fact]
    public async Task DeadlineThenReuse()
    {
        var error = await Assert.ThrowsAsync<RpcException>(() => server.Client.SleepAsync(
            new Payload { DelayMs = 5000 }, deadline: DateTime.UtcNow.AddMilliseconds(100)).ResponseAsync);
        Assert.Equal(StatusCode.DeadlineExceeded, error.StatusCode);
        var data = Bytes(100);
        Assert.Equal(data, (await server.Client.UnaryAsync(new Payload { Data = data })).Data);
    }

    [Fact]
    public async Task CancellationThenReuse()
    {
        using var cts = new CancellationTokenSource(100);
        var error = await Assert.ThrowsAsync<RpcException>(() => server.Client.SleepAsync(
            new Payload { DelayMs = 5000 }, cancellationToken: cts.Token).ResponseAsync);
        Assert.Equal(StatusCode.Cancelled, error.StatusCode);
        var data = Bytes(100);
        Assert.Equal(data, (await server.Client.UnaryAsync(new Payload { Data = data })).Data);
    }

    [Fact]
    public async Task ManyChannelsOpenAndClose()
    {
        for (var i = 0; i < 20; ++i)
        {
            using var channel = GrpcChannel.ForAddress("http://echo.test:50051",
                new GrpcChannelOptions { HttpHandler = DpumeshHttpHandler.Create() });
            var data = Bytes(1000);
            var reply = await new Echo.EchoClient(channel).UnaryAsync(new Payload { Data = data });
            Assert.Equal(data, reply.Data);
        }
    }
}

// Its own collection: collections run one at a time, and this one stops and
// restarts the process's listener.
[Collection("lifecycle")]
public sealed class ServerLifecycleTests
{
    [Fact]
    public async Task StoppedServerFailsCallsThenRestartServes()
    {
        var app = await LoopbackServer.StartAsync();
        using (var channel = GrpcChannel.ForAddress("http://echo.test:50051",
            new GrpcChannelOptions { HttpHandler = DpumeshHttpHandler.Create() }))
        {
            var client = new Echo.EchoClient(channel);
            Assert.Equal(1, (await client.UnaryAsync(new Payload { Count = 1 })).Count);
            await app.StopAsync();
            await app.DisposeAsync();
            var error = await Assert.ThrowsAsync<RpcException>(() => client
                .UnaryAsync(new Payload(), deadline: DateTime.UtcNow.AddSeconds(5)).ResponseAsync);
            Assert.Contains(error.StatusCode, new[] { StatusCode.Unavailable, StatusCode.DeadlineExceeded });
        }

        await using var restarted = await LoopbackServer.StartAsync();
        using var fresh = GrpcChannel.ForAddress("http://echo.test:50051",
            new GrpcChannelOptions { HttpHandler = DpumeshHttpHandler.Create() });
        var reply = await new Echo.EchoClient(fresh).UnaryAsync(new Payload { Count = 7 });
        Assert.Equal(7, reply.Count);
    }
}

public sealed class DisabledTests
{
    [Fact]
    public void UseDpumeshWithoutEnableChangesNothing()
    {
        var previous = Environment.GetEnvironmentVariable("DPUMESH_ENABLE");
        Environment.SetEnvironmentVariable("DPUMESH_ENABLE", null);
        try
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.UseDpumesh();
            Assert.DoesNotContain(builder.Services,
                d => d.ServiceType == typeof(IConnectionListenerFactory)
                    && d.ImplementationType == typeof(DpumeshTransportFactory));
        }
        finally
        {
            Environment.SetEnvironmentVariable("DPUMESH_ENABLE", previous);
        }
    }
}
