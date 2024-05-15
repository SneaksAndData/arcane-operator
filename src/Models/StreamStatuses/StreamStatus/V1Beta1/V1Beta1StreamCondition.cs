using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using Arcane.Operator.Models.Common;

namespace Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;

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
    
    public static V1Beta1StreamCondition[] ErrorCondition =>
        new[]
        {
            new V1Beta1StreamCondition
            {
                Type = ResourceStatus.ERROR.ToString(),
                Status = "True"
            }
        };
    public static V1Beta1StreamCondition[] WarningCondition =>
        new[]
        {
            new V1Beta1StreamCondition
            {
                Type = ResourceStatus.WARNING.ToString(),
                Status = "True"
            }
        };
    public static V1Beta1StreamCondition[] ReadyCondition =>
        new[]
        {
            new V1Beta1StreamCondition
            {
                Type = ResourceStatus.READY.ToString(),
                Status = "True"
            }
        };
}
