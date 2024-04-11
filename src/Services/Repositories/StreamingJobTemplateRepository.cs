using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.JobTemplates.V1Beta1;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Repositories;

public class StreamingJobTemplateRepository : IStreamingJobTemplateRepository
{
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobTemplateRepository> logger;
    private readonly IOptions<CustomResourceConfiguration> configuration;

    public StreamingJobTemplateRepository(IKubeCluster kubeCluster,
        IOptions<CustomResourceConfiguration> configuration,
        ILogger<StreamingJobTemplateRepository> logger)
    {
        this.kubeCluster = kubeCluster;
        this.logger = logger;
        this.configuration = configuration;
    }

    public Task<Option<V1Beta1StreamingJobTemplate>> GetStreamingJobTemplate(string kind, string jobNamespace,
        string templateName)
    {
        var jobTemplateResourceConfiguration = this.configuration.Value;
        if (jobTemplateResourceConfiguration is { ApiGroup: null, Version: null, Plural: null })
        {
            this.logger.LogError("Failed to get job template configuration for kind {kind}", kind);
            return Task.FromResult(Option<V1Beta1StreamingJobTemplate>.None);
        }

        return this.kubeCluster
            .GetCustomResource<V1Beta1StreamingJobTemplate>(
                jobTemplateResourceConfiguration.ApiGroup,
                jobTemplateResourceConfiguration.Version,
                jobTemplateResourceConfiguration.Plural,
                jobNamespace,
                templateName)
            .Map(resource => resource.AsOption());
    }
}
