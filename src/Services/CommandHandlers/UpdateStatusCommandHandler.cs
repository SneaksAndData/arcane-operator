using System.Threading.Tasks;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;

namespace Arcane.Operator.Services.CommandHandlers;

public class UpdateStatusCommandHandler : ICommandHandler<UpdateStatusCommand>
{
    private readonly IStreamDefinitionRepository streamDefinitionRepository;

    public UpdateStatusCommandHandler(IStreamDefinitionRepository streamDefinitionRepository)
    {
        this.streamDefinitionRepository = streamDefinitionRepository;
    }

    /// <inheritdoc cref="ICommandHandler{T}.Handle" />
    public Task Handle(UpdateStatusCommand command)
    {
        var ((nameSpace, kind, streamId), streamStatus, phase) = command;
        return this.streamDefinitionRepository.SetStreamStatus(nameSpace, kind, streamId,
            new V1Beta1StreamStatus
            {
                Conditions = streamStatus,
                Phase = phase.ToString()
            });
    }
}
