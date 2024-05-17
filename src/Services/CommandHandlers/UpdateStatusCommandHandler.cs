using System.Linq;
using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

public class UpdateStatusCommandHandler : ICommandHandler<UpdateStatusCommand>
{
    private readonly ILogger<UpdateStatusCommandHandler> logger;
    private readonly IKubeCluster kubeCluster;
    private readonly IStreamClassRepository streamClassRepository;

    public UpdateStatusCommandHandler(
        IKubeCluster kubeCluster,
        IStreamClassRepository streamClassRepository,
        ILogger<UpdateStatusCommandHandler> logger)
    {
        this.logger = logger;
        this.kubeCluster = kubeCluster;
        this.streamClassRepository = streamClassRepository;
    }

    /// <inheritdoc cref="ICommandHandler{T}.Handle" />
    public Task Handle(UpdateStatusCommand command)
    {
        var ((nameSpace, kind, streamId), streamStatus, phase) = command;
        return this.streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
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
                string.Join(", ", streamStatus.Select(sc => sc.Type)),
                phase);

            return this.kubeCluster.UpdateCustomResourceStatus(
                crdConf.Value.ApiGroupRef,
                crdConf.Value.VersionRef,
                crdConf.Value.PluralNameRef,
                nameSpace,
                streamId,
                streamStatus,
                element => element.AsOptionalStreamDefinition());
        });
    }
}
