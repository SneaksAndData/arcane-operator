using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;
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

    public Task<(Option<IStreamClass>, Option<IStreamDefinition>)> GetStreamDefinition(string nameSpace, string kind, string streamId) =>
        this.streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
            {
                if (crdConf is { HasValue: false })
                {
                    this.logger.LogError("Failed to get configuration for kind {kind}", kind);
                    return Task.FromResult((Option<IStreamClass>.None, Option<IStreamDefinition>.None));
                }

                return this.kubeCluster
                    .GetCustomResource(
                        crdConf.Value.ApiGroupRef,
                        crdConf.Value.VersionRef,
                        crdConf.Value.PluralNameRef,
                        nameSpace,
                        streamId,
                        element => (crdConf, element.AsOptionalStreamDefinition()));
            }
        );

    public Task<Option<IStreamDefinition>> SetStreamStatus(string nameSpace, string kind, string streamId,
        V1Beta1StreamStatus streamStatus) =>
        this.streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                this.logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.FromResult(Option<IStreamDefinition>.None);
            }

            this.logger.LogInformation(
                "Status and phase of stream with kind {kind} and id {streamId} changed to {statuses}, {phase}",
                kind,
                streamId,
                string.Join(", ", streamStatus.Conditions.Select(sc => sc.Type)),
                streamStatus.Phase);

            return this.kubeCluster.UpdateCustomResourceStatus(
                crdConf.Value.ApiGroupRef,
                crdConf.Value.VersionRef,
                crdConf.Value.PluralNameRef,
                nameSpace,
                streamId,
                streamStatus,
                element => element.AsOptionalStreamDefinition());
        });

    public Task<Option<IStreamDefinition>> RemoveReloadingAnnotation(string nameSpace, string kind, string streamId) =>
        this.streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                this.logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.FromResult(Option<IStreamDefinition>.None);
            }

            return this.kubeCluster
                .RemoveObjectAnnotation(crdConf.Value.ToNamespacedCrd(),
                    Annotations.STATE_ANNOTATION_KEY,
                    streamId,
                    nameSpace)
                .Map(result => ((JsonElement)result).AsOptionalStreamDefinition());
        });

    public Task<Option<IStreamDefinition>> SetCrashLoopAnnotation(string nameSpace, string kind, string streamId) =>
        this.streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                this.logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.FromResult(Option<IStreamDefinition>.None);
            }

            return this.kubeCluster
                .AnnotateObject(crdConf.Value.ToNamespacedCrd(),
                    Annotations.STATE_ANNOTATION_KEY,
                    Annotations.CRASH_LOOP_STATE_ANNOTATION_VALUE,
                    streamId,
                    nameSpace)
                .Map(result => ((JsonElement)result).AsOptionalStreamDefinition());
        });

    /// <inheritdoc cref="IReactiveResourceCollection{TResourceType}.GetEvents"/>
    public Source<ResourceEvent<IStreamDefinition>, NotUsed> GetEvents(CustomResourceApiRequest request, int maxBufferCapacity) =>
        this.kubeCluster.StreamCustomResourceEvents<StreamDefinition>(
                request.Namespace,
                request.ApiGroup,
                request.ApiVersion,
                request.PluralName,
                maxBufferCapacity,
                OverflowStrategy.Fail)
            .Select(tuple => new ResourceEvent<IStreamDefinition>(tuple.Item1, tuple.Item2));
}
