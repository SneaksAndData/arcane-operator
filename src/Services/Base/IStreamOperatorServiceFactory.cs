using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Operator;

namespace Arcane.Operator.Services.Base;

/// <summary>
/// Abstract factory for creating <see cref="BackgroundStreamOperatorService"/> instances from configuration settings,
/// extracted from the given <see cref="IStreamClass"/> data Model.
/// </summary>
public interface IStreamOperatorServiceFactory
{
    /// Creates a new instance of <see cref="BackgroundStreamOperatorService"/> for the given <see cref="IStreamClass"/>.
    BackgroundStreamOperatorService Create(IStreamClass streamClass);
}
