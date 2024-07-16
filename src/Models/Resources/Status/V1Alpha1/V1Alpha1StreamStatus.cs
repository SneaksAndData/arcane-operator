using System.Text.Json.Serialization;

namespace Arcane.Operator.Models.Resources.Status.V1Alpha1;

public record V1Alpha1StreamStatus
{
    /// <summary>
    /// List of conditions of the stream
    /// </summary>
    [JsonPropertyName("conditions")]
    public V1Alpha1StreamCondition[] Conditions { get; init; }

    /// <summary>
    /// List of conditions of the stream
    /// </summary>
    [JsonPropertyName("phase")]
    public string Phase { get; init; }
}
