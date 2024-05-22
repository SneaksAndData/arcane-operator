using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Arcane.Operator.Models.Resources.StreamClass.V1Beta1;

public class V1Beta1StreamClassSpec
{
    /// <summary>
    /// Api group of the stream class
    /// </summary>
    [JsonPropertyName("streamClassResourceKind")]
    public string StreamClassResourceKind { get; set; }

    /// <summary>
    /// Api group of the stream class
    /// </summary>
    [JsonPropertyName("apiGroupRef")]
    public string ApiGroupRef { get; set; }

    /// <summary>
    /// Stream class object version
    /// </summary>
    [JsonPropertyName("apiVersion")]
    public string ApiVersion { get; set; }

    /// <summary>
    /// Stream class object plural name
    /// </summary>
    [JsonPropertyName("pluralName")]
    public string PluralName { get; set; }

    /// <summary>
    /// Stream class object kind
    /// </summary>
    [JsonPropertyName("kindRef")]
    public string KindRef { get; set; }

    /// <summary>
    /// Stream class buffer object max capacity.
    /// This value is dependent on the expected number of streams that will be created for this class.
    /// </summary>
    [JsonPropertyName("maxBufferCapacity")]
    public int MaxBufferCapacity { get; set; }

    /// <summary>
    /// Stream class buffer object max capacity.
    /// This value is dependent on the expected number of streams that will be created for this class.
    /// </summary>
    [JsonPropertyName("secretRefs")]
    public List<string> SecretRefs { get; set; }
}
