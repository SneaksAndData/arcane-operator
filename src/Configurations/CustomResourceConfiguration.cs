using System.Diagnostics.CodeAnalysis;
using Snd.Sdk.Kubernetes;

namespace Arcane.Operator.Configurations;

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

    /// <summary>
    /// Convert configuration to NamespacedCrd object for consuming in the Proteus library
    /// </summary>
    /// <returns><see cref="NamespacedCrd"/> object</returns>
    public NamespacedCrd ToNamespacedCrd()
    {
        return new NamespacedCrd
        {
            Group = this.ApiGroup,
            Plural = this.Plural,
            Version = this.Version
        };
    }
}
