using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace Arcane.Operator.Models.Resources.Status.V1Alpha1;

/// <summary>
/// Represents stream status badge for Lens app.
/// </summary>
public enum ResourceStatus
{
    /// <summary>
    /// The stream is in a ready state.
    /// </summary>
    READY,

    /// <summary>
    /// The stream is in an error state.
    /// </summary>
    ERROR,

    /// <summary>
    /// The stream is in a warning state.
    /// </summary>
    WARNING
}

/// <summary>
/// Represents the status of a stream for Kubernetes CRD
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class V1Alpha1StreamCondition
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

    public static V1Alpha1StreamCondition[] ErrorCondition =>
        new[]
        {
            new V1Alpha1StreamCondition
            {
                Type = ResourceStatus.ERROR.ToString(),
                Status = "True"
            }
        };
    public static V1Alpha1StreamCondition[] WarningCondition =>
        new[]
        {
            new V1Alpha1StreamCondition
            {
                Type = ResourceStatus.WARNING.ToString(),
                Status = "True"
            }
        };
    public static V1Alpha1StreamCondition[] ReadyCondition =>
        new[]
        {
            new V1Alpha1StreamCondition
            {
                Type = ResourceStatus.READY.ToString(),
                Status = "True"
            }
        };

    public static V1Alpha1StreamCondition[] CustomErrorCondition(string message) =>
        new[]
        {
            new V1Alpha1StreamCondition
            {
                Type = ResourceStatus.ERROR.ToString(),
                Status = "True",
                Message = message
            }
        };
}
