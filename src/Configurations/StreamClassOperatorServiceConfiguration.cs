using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Operators;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="StreamClassOperatorService"/>
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamClassOperatorServiceConfiguration
{
    /// <summary>
    /// Max buffer capacity for StreamClasses events stream
    /// </summary>
    public int MaxBufferCapacity { get; init; }

    /// <summary>
    /// Api group of the StreamClass CRD
    /// </summary>
    public string ApiGroup { get; init; }

    /// <summary>
    /// Version of the StreamClass CRD
    /// </summary>
    public string Version { get; init; }

    /// <summary>
    /// Plural name of the StreamClass CRD
    /// </summary>
    public string Plural { get; init; }

    /// <summary>
    /// The namespace where the StreamClass CRDs located
    /// </summary>
    public string NameSpace { get; set; }
}
