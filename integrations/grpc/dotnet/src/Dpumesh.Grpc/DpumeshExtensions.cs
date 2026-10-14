using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Dpumesh.Grpc;

/// <summary>Puts an ASP.NET Core gRPC server on DPUMesh.</summary>
public static class DpumeshExtensions
{
    /// <summary>
    /// With DPUMESH_ENABLE=1, Kestrel serves this process's DPUMESH_SERVICE
    /// over DPUMesh with HTTP/2 in place of its configured addresses.
    /// Otherwise the builder is unchanged, so one build runs with or without
    /// the mesh.
    /// </summary>
    public static IWebHostBuilder UseDpumesh(this IWebHostBuilder builder)
    {
        if (!DpumeshRuntime.Enabled) return builder;
        return builder
            .ConfigureServices(services => services.AddDpumeshTransport())
            .ConfigureKestrel(options => options.ListenDpumesh());
    }

    /// <summary>Registers the DPUMesh transport next to Kestrel's own.</summary>
    public static IServiceCollection AddDpumeshTransport(this IServiceCollection services)
    {
        services.AddSingleton<IConnectionListenerFactory, DpumeshTransportFactory>();
        return services;
    }

    /// <summary>Serves DPUMESH_SERVICE over DPUMesh with HTTP/2.</summary>
    public static void ListenDpumesh(this KestrelServerOptions options) =>
        options.Listen(new DpumeshEndPoint(), listen => listen.Protocols = HttpProtocols.Http2);
}
