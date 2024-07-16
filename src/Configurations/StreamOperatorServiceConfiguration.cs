using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Operators;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="StreamOperatorService"/>
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamOperatorServiceConfiguration
{
    /// <summary>
    /// Max buffer capacity for StreamDefinitions events stream
    /// </summary>
    public int MaxBufferCapacity { get; init; }
}
