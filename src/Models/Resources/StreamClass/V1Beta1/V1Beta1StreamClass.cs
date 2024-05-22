using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using k8s;
using k8s.Models;
using Snd.Sdk.Kubernetes;

namespace Arcane.Operator.Models.Resources.StreamClass.V1Beta1;

/// <summary>
/// Object representing a StreamClass
/// The Arcane streaming plugin should create a Kubernetes object of this type to register a new stream class.
/// Arcane.Operator watches for StreamClass objects and when the stream class is created, it starts listening for
/// new streams claims of that class.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class V1Beta1StreamClass : IStreamClass
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
        return $"{this.Namespace()}.{this.Name()}";
    }

    /// <inheritdoc cref="IStreamClass.ApiGroupRef"/>
    public string ApiGroupRef => this.Spec.ApiGroupRef;

    /// <inheritdoc cref="IStreamClass.VersionRef"/>
    public string VersionRef => this.Spec.ApiVersion;

    /// <inheritdoc cref="IStreamClass.PluralNameRef"/>
    public string PluralNameRef => this.Spec.PluralName;

    /// <inheritdoc cref="IStreamClass.KindRef"/>
    public string KindRef => this.Spec.KindRef;

    /// <inheritdoc cref="IStreamClass.MaxBufferCapacity"/>
    public int MaxBufferCapacity => this.Spec.MaxBufferCapacity;

    /// <inheritdoc cref="IStreamClass.IsSecretRef"/>
    public bool IsSecretRef(string propertyName)
    {
        return this.Spec?.SecretRefs?.Contains(propertyName) ?? false;
    }

    /// <inheritdoc cref="IStreamClass.ToNamespacedCrd"/>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public NamespacedCrd ToNamespacedCrd() => new()
    {
        Group = this.ApiGroupRef,
        Plural = this.PluralNameRef,
        Version = this.VersionRef
    };
}
