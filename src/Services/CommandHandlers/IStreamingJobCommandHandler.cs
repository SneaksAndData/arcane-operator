using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using k8s.Models;

namespace Arcane.Operator.Services.CommandHandlers;

/// <summary>
/// Command handler for streaming job commands
/// </summary>
public interface IStreamingJobCommandHandler : ICommandHandler<StreamingJobCommand>,
    ICommandHandler<RequestJobReloadCommand>,
    ICommandHandler<SetAnnotationCommand<V1Job>>
{

}
