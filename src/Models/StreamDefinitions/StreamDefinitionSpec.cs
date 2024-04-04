using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using k8s.Models;

namespace Arcane.Operator.Models.StreamDefinitions;

/// <summary>
/// Configuration for Sql Server Single Table Stream.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamDefinitionSpec
{
    /// <summary>
    /// Sql Server connection string.
    /// </summary>
    [JsonPropertyName("secretRef")]
    public V1SecretEnvSource SecretRef { get; init; }

    /// <summary>
    /// Job template reference
    /// </summary>
    [JsonPropertyName("jobTemplateRef")]
    public V1TypedLocalObjectReference JobTemplateRef { get; init; }

    /// <summary>
    /// Job template reference
    /// </summary>
    [JsonPropertyName("reloadingJobTemplateRef")]
    public V1TypedLocalObjectReference ReloadingJobTemplateRef { get; init; }

    /// <summary>
    /// Table schema.
    /// </summary>
    [JsonPropertyName("streamSettings")]
    public string StreamSettings { get; init; }

    /// <summary>
    /// Convert secret references from this spec to V1EnvFromSource objects.
    /// </summary>
    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources()
    {
        return new[] { new V1EnvFromSource(secretRef: new V1SecretEnvSource(this.SecretRef.Name)) };
    }

    /// <summary>
    /// Convert configuration to environment variables.
    /// </summary>
    /// <returns></returns>
    public Dictionary<string, string> ToEnvironment()
    {
        return new Dictionary<string, string>
        {
            { nameof(this.StreamSettings).GetEnvironmentVariableName(), this.StreamSettings }
        };
    }
}
