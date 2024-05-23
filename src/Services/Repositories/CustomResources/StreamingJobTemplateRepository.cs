using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.JobTemplates.V1Beta1;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Repositories.CustomResources;

public class StreamingJobTemplateRepository : IStreamingJobTemplateRepository
{
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobTemplateRepository> logger;
    private readonly StreamingJobTemplateRepositoryConfiguration configuration;

    public StreamingJobTemplateRepository(IKubeCluster kubeCluster,
        IOptions<StreamingJobTemplateRepositoryConfiguration> configuration,
        ILogger<StreamingJobTemplateRepository> logger)
    {
        this.kubeCluster = kubeCluster;
        this.logger = logger;
        this.configuration = configuration.Value;
    }

    public Task<Option<IStreamingJobTemplate>> GetStreamingJobTemplate(string kind, string jobNamespace,
        string templateName)
    {
        var jobTemplateResourceConfiguration = this.configuration.ResourceConfiguration;
        if (jobTemplateResourceConfiguration is { ApiGroup: null, Version: null, Plural: null })
        {
            this.logger.LogError("Failed to get job template configuration for kind {kind}", kind);
            return Task.FromResult(Option<IStreamingJobTemplate>.None);
        }

        return this.kubeCluster
            .GetCustomResource<V1Beta1StreamingJobTemplate>(
                jobTemplateResourceConfiguration.ApiGroup,
                jobTemplateResourceConfiguration.Version,
                jobTemplateResourceConfiguration.Plural,
                jobNamespace,
                templateName)
            .Map(resource => resource.AsOption<IStreamingJobTemplate>());
    }
}
