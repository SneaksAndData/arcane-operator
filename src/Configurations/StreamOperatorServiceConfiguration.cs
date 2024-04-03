using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Operator;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="StreamOperatorService{TStreamType}"/>
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamOperatorServiceConfiguration
{
    /// <summary>
    /// Max buffer capacity for StreamDefinitions events stream
    /// </summary>
    public int MaxBufferCapacity { get; init; }

    /// <summary>
    /// Parallelism for StreamDefinitions events stream
    /// </summary>
    public int Parallelism { get; init; }
}
