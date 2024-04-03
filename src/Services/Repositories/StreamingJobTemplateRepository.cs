using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.JobTemplates.V1Beta1;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Repositories;

public class StreamingJobTemplateRepository : IStreamingJobTemplateRepository
{
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobTemplateRepository> logger;
    private readonly IStreamClassRepository streamClassRepository;

    public StreamingJobTemplateRepository(IKubeCluster kubeCluster,
        IStreamClassRepository streamClassRepository,
        ILogger<StreamingJobTemplateRepository> logger)
    {
        this.kubeCluster = kubeCluster;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
    }

    public Task<Option<V1Beta1StreamingJobTemplate>> GetStreamingJobTemplate(string kind, string jobNamespace,
        string templateName)
    {
        var crdConf = this.streamClassRepository.Get(jobNamespace, kind);
        if (crdConf is { ApiGroup: null, Version: null, Plural: null })
        {
            this.logger.LogError("Failed to get configuration for kind {kind} and mapped type {typeName}",
                kind,
                crdConf.GetType().Name);
            return Task.FromResult(Option<V1Beta1StreamingJobTemplate>.None);
        }

        return this.kubeCluster
            .GetCustomResource<V1Beta1StreamingJobTemplate>(
                crdConf.ApiGroup,
                crdConf.Version,
                crdConf.Plural,
                jobNamespace,
                templateName)
            .Map(resource => resource.AsOption());
    }
}
