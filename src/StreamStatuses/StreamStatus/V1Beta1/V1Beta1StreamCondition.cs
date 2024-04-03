using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace Arcane.Operator.StreamStatuses.StreamStatus.V1Beta1;

/// <summary>
/// Represents the status of a stream for Kubernetes CRD
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class V1Beta1StreamCondition
{
    /// <summary>
    /// Latest observed state of the stream
    /// </summary>
    [JsonPropertyName("status")]
    public string Status { get; init; }

    /// <summary>
    /// Latest observed state of the stream
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; init; }

    /// <summary>
    /// Latest observed state of the stream
    /// </summary>
    [JsonPropertyName("message")]
    public string Message { get; set; }
}
