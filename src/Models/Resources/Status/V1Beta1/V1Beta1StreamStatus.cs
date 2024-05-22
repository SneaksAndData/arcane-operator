using System.Text.Json.Serialization;

namespace Arcane.Operator.Models.Resources.Status.V1Beta1;

public class V1Beta1StreamStatus
{
    /// <summary>
    /// List of conditions of the stream
    /// </summary>
    [JsonPropertyName("conditions")]
    public V1Beta1StreamCondition[] Conditions { get; init; }

    /// <summary>
    /// List of conditions of the stream
    /// </summary>
    [JsonPropertyName("phase")]
    public string Phase { get; init; }
}
