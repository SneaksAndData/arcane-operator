using Arcane.Operator.Services.Commands;
using k8s.Models;

namespace Arcane.Operator.Services.Base;

/// <summary>
/// Command handler for streaming job commands
/// </summary>
public interface IStreamingJobCommandHandler : ICommandHandler<StreamingJobCommand>,
    ICommandHandler<SetAnnotationCommand<V1Job>>
{

}
