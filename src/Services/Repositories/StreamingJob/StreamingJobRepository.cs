using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Resources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using Arcane.Operator.Services.Models;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Repositories.StreamingJob;

public class StreamingJobRepository : IStreamingJobCollection
{
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobRepository> logger;

    public StreamingJobRepository(IKubeCluster kubeCluster, ILogger<StreamingJobRepository> logger)
    {
        this.kubeCluster = kubeCluster;
        this.logger = logger;
    }

    public Source<ResourceEvent<V1Job>, NotUsed> GetEvents(string nameSpace, int maxBufferCapacity) =>
        this.kubeCluster
            .StreamJobEvents(nameSpace, maxBufferCapacity, OverflowStrategy.Fail)
            .Select(tuple => new ResourceEvent<V1Job>(tuple.Item1, tuple.Item2));

    public Task<Option<V1Job>> Get(string nameSpace, string name) =>
        this.kubeCluster.GetJob(name, nameSpace)
            .TryMap(job => job.AsOption(), exception =>
            {
                this.logger.LogWarning(exception, "The job resource {jobName} not found", name);
                return Option<V1Job>.None;
            });
}
