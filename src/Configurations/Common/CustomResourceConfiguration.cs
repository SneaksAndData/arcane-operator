using System.Diagnostics.CodeAnalysis;

namespace Arcane.Operator.Configurations.Common;

/// <summary>
/// Configuration of various custom resources listeners
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class CustomResourceConfiguration
{
    /// <summary>
    /// Api group of the StreamDefinition CRD
    /// </summary>
    public string ApiGroup { get; init; }

    /// <summary>
    /// Version of the CRD
    /// </summary>
    public string Version { get; init; }

    /// <summary>
    /// Plural of the CRD
    /// </summary>
    public string Plural { get; init; }
}
