using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
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

    public AnnotationCommandHandler(
        IStreamClassRepository streamClassRepository,
        IKubeCluster kubeCluster,
        ILogger<AnnotationCommandHandler> logger)
    {
        this.streamClassRepository = streamClassRepository;
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

            return this.kubeCluster.AnnotateObject(crdConf.Value.ToNamespacedCrd(),
                annotationKey,
                annotationValue,
                name,
                nameSpace);
        });
    }

    public Task Handle(SetAnnotationCommand<V1Job> command)
    {
        var ((nameSpace, name), annotationKey, annotationValue) = command;
        return this.kubeCluster.AnnotateJob(name, nameSpace, annotationKey, annotationValue);
    }

    public Task Handle(RemoveAnnotationCommand<IStreamDefinition> command)
    {
        var ((nameSpace, kind, name), annotationKey) = command;
        return this.streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                this.logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.FromResult(Option<object>.None);
            }

            var crd = crdConf.Value.ToNamespacedCrd();
            return this.kubeCluster
                .RemoveObjectAnnotation(crd, annotationKey, name, nameSpace)
                .TryMap(result => result.AsOption(), exception =>
                {
                    this.logger.LogError(exception,
                        "Failed to remove annotation {annotationKey} from {nameSpace}/{name}",
                        annotationKey,
                        nameSpace,
                        name);
                    return default;
                });
        });
    }
}
