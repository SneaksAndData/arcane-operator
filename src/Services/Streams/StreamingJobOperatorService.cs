using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Services.Base;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Streams;

public class StreamingJobOperatorService : IStreamingJobOperatorService
{
    private readonly StreamingJobOperatorServiceConfiguration configuration;
    private readonly IKubeCluster kubernetesService;
    private readonly ILogger<StreamingJobOperatorService> logger;

    public StreamingJobOperatorService(
        ILogger<StreamingJobOperatorService> logger,
        IOptions<StreamingJobOperatorServiceConfiguration> configuration,
        IKubeCluster kubernetesService)
    {
        this.logger = logger;
        this.configuration = configuration.Value;
        this.kubernetesService = kubernetesService;
    }


    public string StreamJobNamespace => this.configuration.Namespace;

    public Task<Option<V1Job>> GetStreamingJob(string streamId)
    {
        return this.kubernetesService.GetJob(streamId, this.StreamJobNamespace)
            .TryMap(job => job.AsOption(), exception =>
            {
                this.logger.LogWarning(exception, "Streaming job {streamId} not found", streamId);
                return Option<V1Job>.None;
            });
    }

}
