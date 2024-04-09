using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.StreamClass.Base;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Models.StreamClass;

/// <summary>
/// Object representing a StreamClass
/// The Arcane streaming plugin should create a Kubernetes object of this type to register a new stream class.
/// Arcane.Operator watches for StreamClass objects and when the stream class is created, it starts listening for
/// new streams claims of that class.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class V1Beta1StreamClass: IStreamClass
{
    /// <inheritdoc cref="IMetadata{T}.Metadata"/>
    [JsonPropertyName("metadata")]
    public V1ObjectMeta Metadata { get; set; }
    
    /// <inheritdoc cref="IKubernetesObject.ApiVersion"/>
    [JsonPropertyName("apiVersion")]
    public string ApiVersion { get; set; }

    /// <inheritdoc cref="IKubernetesObject.Kind"/>
    [JsonPropertyName("kind")]
    public string Kind { get; set; }
    
    /// <summary>
    /// The StreamClass configuration
    /// </summary>
    [JsonPropertyName("spec")]
    public V1Beta1StreamClassSpec Spec { get; set; }

    /// <inheritdoc cref="IKubernetesObject.Kind"/>
    public string ToStreamClassId()
    {
        return $"{this.Metadata.Namespace()}/{this.Metadata.Name}";
    }

    public StreamOperatorServiceConfiguration ToStreamOperatorServiceConfiguration()
    {
        return new StreamOperatorServiceConfiguration
        {
            MaxBufferCapacity = this.Spec.MaxBufferCapacity,
        };
    }
}
