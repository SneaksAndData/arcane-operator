using System.Threading.Tasks;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

public class SetAnnotationCommandHandler: ICommandHandler<SetAnnotationCommand>
{
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<SetAnnotationCommandHandler> logger;

    public SetAnnotationCommandHandler(
        IStreamClassRepository streamClassRepository,
        IKubeCluster kubeCluster,
        ILogger<SetAnnotationCommandHandler> logger)
    {
        this.streamClassRepository = streamClassRepository;
        this.kubeCluster = kubeCluster;
        this.logger = logger;
    }
    public Task Handle(SetAnnotationCommand command)
    {
        var ( (nameSpace, kind, streamId), annotationKey, annotationValue) = command;
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
                    streamId,
                    nameSpace);
        });
    }
}
