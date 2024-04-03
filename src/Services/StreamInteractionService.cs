using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Services.Base;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services;

public class StreamInteractionService : IStreamInteractionService
{
    private const int STREAM_STOP_PORT = 13000;
    private readonly IKubeCluster kubernetesService;

    public StreamInteractionService(IKubeCluster kubernetesService)
    {
        this.kubernetesService = kubernetesService;
    }

    /// <inheritdoc/>
    public Task SetupTermination(Action<Task> onStreamTermination)
    {
        var server = new TcpListener(IPAddress.Parse("0.0.0.0"), STREAM_STOP_PORT);
        server.Start();
        return server.AcceptSocketAsync().ContinueWith(onStreamTermination);
    }

    /// <inheritdoc/>
    public async Task SendStopRequest(string streamerIp)
    {
        var endpoint = new IPEndPoint(IPAddress.Parse(streamerIp), STREAM_STOP_PORT);
        using var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync(endpoint);
    }

    /// <inheritdoc/>
    public Task ReportSchemaMismatch(string streamId)
    {
        var nameSpace = this.kubernetesService.GetCurrentNamespace();
        return this.kubernetesService.AnnotateJob(streamId,
            nameSpace,
            Annotations.STATE_ANNOTATION_KEY,
            Annotations.SCHEMA_MISMATCH_STATE_ANNOTATION_VALUE);
    }
}
