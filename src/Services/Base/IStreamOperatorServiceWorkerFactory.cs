using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Operator;

namespace Arcane.Operator.Services.Base;

/// <summary>
/// Abstract factory for creating <see cref="StreamOperatorServiceWorker"/> instances from configuration settings,
/// extracted from the given <see cref="IStreamClass"/> data Model.
/// </summary>
public interface IStreamOperatorServiceWorkerFactory
{
    /// Creates a new instance of <see cref="StreamOperatorServiceWorker"/> for the given <see cref="IStreamClass"/>.
    StreamOperatorServiceWorker Create(IStreamClass streamClass);
}
