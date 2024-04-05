﻿using System.Text.Json.Serialization;

namespace Arcane.Operator.Models.StreamClass;

public class V1Beta1StreamClassSpec
{
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
    /// Stream class buffer object max capacity.
    /// This value is dependent on the expected number of streams that will be created for this class.
    /// </summary>
    [JsonPropertyName("maxBufferCapacity")]
    public string MaxBufferCapacity { get; set; }
}
