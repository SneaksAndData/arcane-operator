using System.Threading.Tasks;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using Microsoft.Extensions.Logging;
using Serilog;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

public class UpdateStatusCommandHandler : ICommandHandler<UpdateStatusCommand>
{
    private readonly IStreamDefinitionRepository streamDefinitionRepository;
    private readonly ILogger<UpdateStatusCommandHandler> logger;

    public UpdateStatusCommandHandler(IStreamDefinitionRepository streamDefinitionRepository, ILogger<UpdateStatusCommandHandler> logger)
    {
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.logger = logger;
    }

    /// <inheritdoc cref="ICommandHandler{T}.Handle" />
    public Task Handle(UpdateStatusCommand command)
    {
        var ((nameSpace, kind, streamId), streamStatus, phase) = command;
        var status = new V1Beta1StreamStatus { Conditions = streamStatus, Phase = phase.ToString() };
        return this.streamDefinitionRepository.SetStreamStatus(nameSpace, kind, streamId,status)
            .TryMap(success => success, exception =>
            {
                this.logger.LogError(exception, "Failed to update status for {kind} {streamId}", kind, streamId);
                return default;
            });
    }
}
