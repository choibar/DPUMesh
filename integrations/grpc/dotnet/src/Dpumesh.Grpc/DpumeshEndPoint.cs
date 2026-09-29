using System.Net;

namespace Dpumesh.Grpc;

/// <summary>
/// The Kestrel endpoint for this process's DPUMESH_SERVICE target: streams the
/// DPU routes to the service arrive on it.
/// </summary>
public sealed class DpumeshEndPoint : EndPoint
{
    /// <summary>The service target, from DPUMESH_SERVICE.</summary>
    public string Service { get; } = Environment.GetEnvironmentVariable("DPUMESH_SERVICE") ?? "";

    /// <inheritdoc />
    public override string ToString() => $"dpumesh://{Service}";
}
