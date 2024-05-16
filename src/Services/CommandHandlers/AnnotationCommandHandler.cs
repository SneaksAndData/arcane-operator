using System.Threading.Tasks;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

public class AnnotationCommandHandler :
   ICommandHandler<SetAnnotationCommand<IStreamDefinition>>,
   ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>,
   ICommandHandler<SetAnnotationCommand<V1Job>>
{
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<AnnotationCommandHandler> logger;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;

    public AnnotationCommandHandler(
        IStreamClassRepository streamClassRepository,
        IStreamDefinitionRepository streamDefinitionRepository,
        IKubeCluster kubeCluster,
        ILogger<AnnotationCommandHandler> logger)
    {
        this.streamClassRepository = streamClassRepository;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.kubeCluster = kubeCluster;
        this.logger = logger;
    }
    public Task Handle(SetAnnotationCommand<IStreamDefinition> command)
    {
        var ((nameSpace, kind, name), annotationKey, annotationValue) = command;
        return this.streamClassRepository.Get(nameSpace, kind).Map(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                this.logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.CompletedTask;
            }

            return this.kubeCluster
                .AnnotateObject(crdConf.Value.ToNamespacedCrd(),
                    annotationKey,
                    annotationValue,
                    name,
                    nameSpace);
        });
    }

    public Task Handle(SetAnnotationCommand<V1Job> command)
    {
        var ((nameSpace, _, name), annotationKey, annotationValue) = command;
        return this.kubeCluster.AnnotateJob(name, nameSpace, annotationKey, annotationValue);
    }

    public Task Handle(RemoveAnnotationCommand<IStreamDefinition> command)
    {
        var ((nameSpace, kind, name), _) = command;
        return this.streamDefinitionRepository.RemoveReloadingAnnotation(nameSpace, kind, name);
    }
}
