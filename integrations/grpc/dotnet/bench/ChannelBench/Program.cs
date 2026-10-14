// channel-bench for .NET: the peer of integrations/grpc/go/cmd/channel-bench.
// It speaks the same wire (/dmesh.ChannelBench/Echo, a 64-byte raw request
// echoed unchanged, bytes 0-7 the little-endian sequence number and 8-15 the
// worker ID), takes the same flags and prints the same JSON lines. The server
// is ASP.NET Core gRPC on Kestrel, the client Grpc.Net.Client; both run over
// DPUMesh or, with -tcp host:port, kernel TCP. -start-file is not supported.
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Dpumesh.Grpc;
using Grpc.AspNetCore.Server.Model;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection.Extensions;

var raw = Marshallers.Create<byte[]>(bytes => bytes, bytes => bytes);
var method = new Method<byte[], byte[]>(MethodType.Unary, "dmesh.ChannelBench", "Echo", raw, raw);

Dictionary<string, string> flags;
try
{
    flags = Flags.Parse(args);
}
catch (ArgumentException e)
{
    Console.Error.WriteLine(e.Message);
    return 2;
}
var tcp = flags.GetValueOrDefault("tcp", "");
var mode = flags.GetValueOrDefault("mode", "client");
if (mode == "server") return await RunServer(tcp, method);
if (mode != "client")
{
    Console.Error.WriteLine($"unknown mode {mode}");
    return 2;
}
var config = new Config(
    int.Parse(flags.GetValueOrDefault("connections", "1"), CultureInfo.InvariantCulture),
    int.Parse(flags.GetValueOrDefault("concurrency", "64"), CultureInfo.InvariantCulture),
    Flags.Duration(flags.GetValueOrDefault("warmup", "3s")),
    Flags.Duration(flags.GetValueOrDefault("duration", "10s")),
    Flags.Duration(flags.GetValueOrDefault("rpc-timeout", "5s")),
    tcp);
var ip = Environment.GetEnvironmentVariable("DPUMESH_SERVICE_IP");
var port = Environment.GetEnvironmentVariable("DPUMESH_SERVICE_PORT");
if ((tcp == "" && (string.IsNullOrEmpty(ip) || string.IsNullOrEmpty(port))) || config.Connections < 1 ||
    config.Connections > 4 || config.Concurrency < config.Connections || config.Duration <= TimeSpan.Zero)
{
    Console.Error.WriteLine("set DPUMESH_SERVICE_IP/PORT for the client, connections 1..4, " +
                            "concurrency >= connections, and valid durations");
    return 2;
}
return await Client.Run(config, tcp != "" ? $"http://{tcp}" : $"http://{ip}:{port}", method);

static async Task<int> RunServer(string tcp, Method<byte[], byte[]> method)
{
    var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [] });
    builder.Logging.ClearProviders();
    builder.Services.AddGrpc();
    builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IServiceMethodProvider<EchoService>>(
        new EchoMethodProvider(method)));
    if (tcp == "") builder.Services.AddDpumeshTransport();
    builder.WebHost.ConfigureKestrel(options =>
    {
        if (tcp == "") options.ListenDpumesh();
        else options.Listen(IPEndPoint.Parse(tcp), listen => listen.Protocols = HttpProtocols.Http2);
    });
    var app = builder.Build();
    app.MapGrpcService<EchoService>();
    await app.StartAsync();
    var service = tcp == "" ? Environment.GetEnvironmentVariable("DPUMESH_SERVICE") : tcp;
    Console.Error.WriteLine($"CHANNEL_BENCH_SERVER_READY service={service}");
    // SIGTERM and SIGINT stop the host gracefully; the DPUMesh runtime closes
    // on process exit.
    await app.WaitForShutdownAsync();
    Console.Error.WriteLine("CHANNEL_BENCH_SERVER_CLOSED");
    return 0;
}

record Config(int Connections, int Concurrency, TimeSpan Warmup, TimeSpan Duration, TimeSpan RpcTimeout, string Tcp);

sealed class EchoService;

sealed class EchoMethodProvider(Method<byte[], byte[]> method) : IServiceMethodProvider<EchoService>
{
    public void OnServiceMethodDiscovery(ServiceMethodProviderContext<EchoService> context) =>
        context.AddUnaryMethod(method, new List<object>(), (_, request, _) => Task.FromResult(request));
}

static class Flags
{
    // Accepts -name value, -name=value and the -- forms, as Go's flag package does.
    public static Dictionary<string, string> Parse(string[] args)
    {
        var flags = new Dictionary<string, string>();
        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith('-')) throw new ArgumentException($"unexpected argument {args[i]}");
            var body = args[i].TrimStart('-');
            var eq = body.IndexOf('=');
            if (eq >= 0) flags[body[..eq]] = body[(eq + 1)..];
            else if (i + 1 < args.Length) flags[body] = args[++i];
            else throw new ArgumentException($"flag -{body} needs a value");
        }
        return flags;
    }

    // Go time.ParseDuration for one number and one unit of ns, us, ms, s, m or h.
    public static TimeSpan Duration(string text)
    {
        var unit = text.TrimStart("0123456789.".ToCharArray());
        var value = double.Parse(text[..^unit.Length], CultureInfo.InvariantCulture);
        return unit switch
        {
            "ns" => TimeSpan.FromTicks((long)(value / 100)),
            "us" => TimeSpan.FromMicroseconds(value),
            "ms" => TimeSpan.FromMilliseconds(value),
            "s" => TimeSpan.FromSeconds(value),
            "m" => TimeSpan.FromMinutes(value),
            "h" => TimeSpan.FromHours(value),
            _ => throw new ArgumentException($"invalid duration {text}"),
        };
    }
}

static class Client
{
    const int PayloadBytes = 64;

    sealed class Worker
    {
        public readonly List<long> Latencies = new(16384);
        public long Completed;
        public long Errors;
        public string? Error;
    }

    static long Now => Stopwatch.GetTimestamp();

    static DateTime Wall(long timestamp) =>
        DateTime.UtcNow + Stopwatch.GetElapsedTime(Now, timestamp);

    static string Rfc3339(DateTime time) => time.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);

    static void Marker(string name, long at)
    {
        Console.Out.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["event"] = name,
            ["timestamp"] = Rfc3339(Wall(at)),
            ["emitted_at"] = Rfc3339(DateTime.UtcNow),
        }));
        Console.Out.Flush();
    }

    static async ValueTask<Stream> TcpConnect(SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(context.DnsEndPoint, cancellationToken).ConfigureAwait(false);
            return new NetworkStream(socket, ownsSocket: true);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    public static async Task<int> Run(Config c, string target, Method<byte[], byte[]> method)
    {
        var perConn = Enumerable.Range(0, c.Connections)
            .Select(i => c.Concurrency / c.Connections + (i < c.Concurrency % c.Connections ? 1 : 0)).ToArray();
        var dials = new long[c.Connections];
        var channels = new List<GrpcChannel>();
        var errors = new List<string>();
        long rpcErrors = 0;
        var workers = new List<Worker>();
        long start = 0, end = 0;
        double cpuPct = 0, cpuSeconds = 0;

        for (var i = 0; i < c.Connections && errors.Count == 0; i++)
        {
            // One handler per channel: each channel holds its own HTTP/2 connection.
            var handler = c.Tcp == "" ? DpumeshHttpHandler.Create() : new SocketsHttpHandler();
            Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>> connect =
                handler.ConnectCallback ?? TcpConnect;
            var index = i;
            handler.ConnectCallback = (context, cancellationToken) =>
            {
                Interlocked.Increment(ref dials[index]);
                return connect(context, cancellationToken);
            };
            var channel = GrpcChannel.ForAddress(target, new GrpcChannelOptions { HttpHandler = handler });
            channels.Add(channel);
            var payload = Enumerable.Repeat((byte)(i + 1), PayloadBytes).ToArray();
            try
            {
                var reply = await channel.CreateCallInvoker().AsyncUnaryCall(method, null,
                    new CallOptions(deadline: DateTime.UtcNow + c.RpcTimeout), payload);
                if (!reply.AsSpan().SequenceEqual(payload)) throw new InvalidDataException("payload mismatch");
            }
            catch (Exception e)
            {
                rpcErrors++;
                errors.Add($"connection {i + 1} preflight: {e.Message}");
            }
        }

        if (errors.Count == 0)
        {
            Console.Error.WriteLine($"PREFLIGHT_OK connections={c.Connections} concurrency=[{string.Join(' ', perConn)}] payload={PayloadBytes}B");
            using var cancel = new CancellationTokenSource();
            var begin = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tasks = new List<Task>();
            var id = 0;
            for (var conn = 0; conn < c.Connections; conn++)
            {
                var invoker = channels[conn].CreateCallInvoker();
                for (var j = 0; j < perConn[conn]; j++, id++)
                {
                    var w = new Worker();
                    workers.Add(w);
                    var workerId = id;
                    var connIndex = conn;
                    tasks.Add(Task.Run(async () =>
                    {
                        var payload = Enumerable.Repeat((byte)(workerId + 1), PayloadBytes).ToArray();
                        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(8), (ulong)workerId);
                        await begin.Task.ConfigureAwait(false);
                        for (ulong sequence = 1; !cancel.IsCancellationRequested && Now < end; sequence++)
                        {
                            BinaryPrimitives.WriteUInt64LittleEndian(payload, sequence);
                            var started = Now;
                            try
                            {
                                var reply = await invoker.AsyncUnaryCall(method, null,
                                    new CallOptions(deadline: DateTime.UtcNow + c.RpcTimeout,
                                                    cancellationToken: cancel.Token), payload).ConfigureAwait(false);
                                if (!reply.AsSpan().SequenceEqual(payload))
                                    throw new InvalidDataException($"payload mismatch: {PayloadBytes} bytes sent, {reply.Length} received");
                                if (Volatile.Read(ref dials[connIndex]) != 1)
                                    throw new InvalidOperationException($"unexpected reconnect: {dials[connIndex]} dial attempts");
                            }
                            catch (Exception e)
                            {
                                w.Errors++;
                                w.Error = $"worker {workerId} request {sequence}: {e.Message}";
                                cancel.Cancel();
                                return;
                            }
                            var completed = Now;
                            w.Completed++;
                            if (completed >= start && completed < end) w.Latencies.Add(completed - started);
                        }
                    }));
                }
            }
            var loadStart = Now;
            start = loadStart + (long)(c.Warmup.TotalSeconds * Stopwatch.Frequency);
            end = start + (long)(c.Duration.TotalSeconds * Stopwatch.Frequency);
            begin.SetResult();
            await Task.Delay(Stopwatch.GetElapsedTime(Now, start));
            var process = Process.GetCurrentProcess();
            var cpuStart = process.TotalProcessorTime;
            var cpuStartAt = Now;
            Marker("measure_start", start);
            await Task.Delay(Stopwatch.GetElapsedTime(Now, end));
            process.Refresh();
            cpuSeconds = Stopwatch.GetElapsedTime(cpuStartAt).TotalSeconds;
            cpuPct = 100 * (process.TotalProcessorTime - cpuStart).TotalSeconds / cpuSeconds;
            Marker("measure_end", end);
            await Task.WhenAll(tasks);
        }

        var latencies = workers.SelectMany(w => w.Latencies).ToList();
        latencies.Sort();
        foreach (var w in workers)
        {
            rpcErrors += w.Errors;
            if (w.Error != null) errors.Add(w.Error);
        }
        foreach (var channel in channels) channel.Dispose();
        foreach (var d in dials.Where(d => d != 1)) errors.Add($"expected one dial per connection, got {d}");
        var elapsed = (double)(end - start) / Stopwatch.Frequency;
        if (errors.Count == 0 && latencies.Count == 0) errors.Add("no successful RPC completions in measurement window");
        double Us(long ticks) => ticks * 1e6 / Stopwatch.Frequency;
        double Pct(double p) => latencies.Count == 0 ? 0 : Us(latencies[(int)Math.Ceiling(p * latencies.Count) - 1]);
        var result = new Dictionary<string, object>
        {
            ["event"] = "result",
            ["ok"] = errors.Count == 0,
            ["connections"] = c.Connections,
            ["concurrency"] = c.Concurrency,
            ["concurrency_per_conn"] = perConn,
            ["payload_bytes"] = PayloadBytes,
            ["warmup_seconds"] = c.Warmup.TotalSeconds,
            ["duration_seconds"] = c.Duration.TotalSeconds,
            ["elapsed_secs"] = elapsed,
            ["client_process_cpu_pct"] = cpuPct,
            ["client_cpu_sample_seconds"] = cpuSeconds,
            ["measurement_start"] = Rfc3339(Wall(start)),
            ["measurement_end"] = Rfc3339(Wall(end)),
            ["completed"] = latencies.Count,
            ["total_completed_including_warmup_and_drain"] = workers.Sum(w => w.Completed),
            ["rpc_errors"] = rpcErrors,
            ["native_dials"] = dials,
            ["reconnects"] = dials.Sum(d => Math.Max(0, d - 1)),
            ["qps"] = elapsed > 0 ? latencies.Count / elapsed : 0,
            ["latency_mean_us"] = latencies.Count == 0 ? 0 : latencies.Average(l => Us(l)),
            ["latency_p50_us"] = Pct(0.5),
            ["latency_p99_us"] = Pct(0.99),
        };
        if (errors.Count > 0) result["error"] = string.Join("\n", errors);
        Console.Out.WriteLine(JsonSerializer.Serialize(result));
        Console.Out.Flush();
        if (errors.Count > 0) Console.Error.WriteLine(string.Join("\n", errors));
        return errors.Count == 0 ? 0 : 1;
    }
}
