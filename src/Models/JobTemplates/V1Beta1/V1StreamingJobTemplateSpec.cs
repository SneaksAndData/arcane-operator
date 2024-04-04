using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using k8s.Models;

namespace Arcane.Operator.Models.JobTemplates.V1Beta1;

/// <summary>
/// Configuration for streaming job template.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class V1Beta1StreamingJobTemplateSpec
{
    /// <summary>
    /// Job template reference
    /// </summary>
    [JsonPropertyName("template")]
    public V1Job Template { get; init; }

    /// <summary>
    /// Job template reference
    /// </summary>
    [JsonPropertyName("metadata")]
    public V1ObjectMeta Metadata { get; init; }
}
