using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

public class UpdateStatusCommandHandler : ICommandHandler<UpdateStatusCommand>,
    ICommandHandler<SetStreamClassStatusCommand>
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
        var ((nameSpace, kind, streamId), conditions, phase) = command;
        return this.streamClassRepository.Get(nameSpace, kind).FlatMap(crdConf =>
        {
            if (crdConf is { HasValue: false })
            {
                this.logger.LogError("Failed to get configuration for kind {kind}", kind);
                return Task.FromResult(Option<IStreamDefinition>.None);
            }

            var status = new V1Alpha1StreamStatus { Phase = phase.ToString(), Conditions = conditions };

            this.logger.LogInformation(
                "Status and phase of stream with kind {kind} and id {streamId} changed to {statuses}, {phase}",
                kind,
                streamId,
                string.Join(", ", conditions.Select(sc => sc.Type)),
                phase);

            return this.kubeCluster.UpdateCustomResourceStatus(
                crdConf.Value.ApiGroupRef,
                crdConf.Value.VersionRef,
                crdConf.Value.PluralNameRef,
                nameSpace,
                streamId,
                status,
                element => element.AsOptionalStreamDefinition());
        });
    }

    public Task Handle(SetStreamClassStatusCommand command)
    {
        var status = new V1Alpha1StreamStatus
        {
            Phase = command.phase.ToString(),
            Conditions = command.conditions.ToArray()
        };

        return this.kubeCluster.UpdateCustomResourceStatus(
                command.request.ApiGroup,
                command.request.ApiVersion,
                command.request.PluralName,
                command.request.Namespace,
                command.resourceName,
                status,
                element => element.AsOptionalStreamClass())
            .TryMap(success => this.OnSuccess(success, command.phase),
                exception => this.OnFailure(exception, command.request));
    }

    private Option<IStreamClass> OnSuccess(Option<IStreamClass> maybeStreamClass, StreamClassPhase phase)
    {
        if (maybeStreamClass is { HasValue: false })
        {
            this.logger.LogError("Failed to get stream definition");
        }

        this.logger.LogInformation("The phase of the stream class {namespace}/{name} changed to {status}",
            maybeStreamClass.Value.Metadata.Namespace(),
            maybeStreamClass.Value.Metadata.Name,
            phase);

        return maybeStreamClass;
    }

    private Option<IStreamClass> OnFailure(Exception exception, CustomResourceApiRequest request)
    {
        this.logger.LogError(exception, "Failed to update stream class status for {@request}", request);
        return Option<IStreamClass>.None;
    }
}
