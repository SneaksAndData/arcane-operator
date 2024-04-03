using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Services.Base;
using Arcane.Operator.StreamDefinitions;
using Arcane.Operator.StreamDefinitions.Base;
using Arcane.Operator.StreamStatuses.StreamStatus.V1Beta1;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Repositories;

public class StreamDefinitionRepository : IStreamDefinitionRepository
{
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamDefinitionRepository> logger;
    private readonly IStreamClassRepository streamClassRepository;

    public StreamDefinitionRepository(ILogger<StreamDefinitionRepository> logger,
        IStreamClassRepository streamClassRepository,
        IKubeCluster kubeCluster)
    {
        this.kubeCluster = kubeCluster;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
    }

    public Task<Option<IStreamDefinition>> GetStreamDefinition(string nameSpace, string kind, string streamId)
    {
        var crdConf = this.streamClassRepository.Get(nameSpace, kind);
        if (crdConf is { ApiGroup: null, Version: null, Plural: null })
        {
            this.logger.LogError("Failed to get configuration for kind {kind}", kind);
            return Task.FromResult(Option<IStreamDefinition>.None);
        }

        return this.kubeCluster
            .GetCustomResource(
                crdConf.ApiGroup,
                crdConf.Version,
                crdConf.Plural,
                nameSpace,
                streamId,
                element => (IStreamDefinition)element.Deserialize<StreamDefinition>())
            .Map(resource => resource.AsOption());
    }

    public Task<Option<IStreamDefinition>> SetStreamStatus(string nameSpace, string kind, string streamId,
        V1Beta1StreamStatus streamStatus)
    {
        this.logger.LogInformation(
            "Status and phase of stream with kind {kind} and id {streamId} changed to {statuses}, {phase}",
            kind,
            streamId,
            string.Join(", ", streamStatus.Conditions.Select(sc => sc.Type)),
            streamStatus.Phase);
        var crdConf = this.streamClassRepository.Get(nameSpace, kind);
        if (crdConf is { ApiGroup: null, Version: null, Plural: null })
        {
            this.logger.LogError("Failed to get configuration for kind {kind}", kind);
            return Task.FromResult(Option<IStreamDefinition>.None);
        }

        return this.kubeCluster.UpdateCustomResourceStatus(
                crdConf.ApiGroup,
                crdConf.Version,
                crdConf.Plural,
                nameSpace,
                streamId,
                streamStatus,
                element => (IStreamDefinition)element.Deserialize<StreamDefinition>())
            .Map(resource => resource.AsOption());
    }

    public Task<Option<IStreamDefinition>> RemoveReloadingAnnotation(string nameSpace, string kind, string streamId)
    {
        var crdConf = this.streamClassRepository.Get(nameSpace, kind);
        if (crdConf is { ApiGroup: null, Version: null, Plural: null })
        {
            this.logger.LogError("Failed to get configuration for kind {kind}", kind);
            return Task.FromResult(Option<IStreamDefinition>.None);
        }

        return this.kubeCluster
            .RemoveObjectAnnotation(crdConf.ToNamespacedCrd(),
                Annotations.STATE_ANNOTATION_KEY,
                streamId,
                nameSpace)
            .Map(result => (IStreamDefinition)((JsonElement)result).Deserialize<StreamDefinition>())
            .Map(result => result.AsOption());
    }

    public Task<Option<IStreamDefinition>> SetCrashLoopAnnotation(string nameSpace, string kind, string streamId)
    {
        var crdConf = this.streamClassRepository.Get(nameSpace, kind);
        if (crdConf is { ApiGroup: null, Version: null, Plural: null })
        {
            this.logger.LogError("Failed to get configuration for kind {kind}", kind);
            return Task.FromResult(Option<IStreamDefinition>.None);
        }

        return this.kubeCluster
            .AnnotateObject(crdConf.ToNamespacedCrd(),
                Annotations.STATE_ANNOTATION_KEY,
                Annotations.CRASH_LOOP_STATE_ANNOTATION_VALUE,
                streamId,
                nameSpace)
            .Map(result => ((IStreamDefinition)((JsonElement)result).Deserialize<StreamDefinition>()).AsOption());
    }
}
