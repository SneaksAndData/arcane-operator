using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;

namespace Arcane.Operator.Services.CommandHandlers;

/// <summary>
/// Command handler for streaming job commands
/// </summary>
public interface IStreamingJobCommandHandler : ICommandHandler<StreamingJobCommand>,
    ICommandHandler<RequestJobRestartCommand>,
    ICommandHandler<RequestJobReloadCommand>
{

}
