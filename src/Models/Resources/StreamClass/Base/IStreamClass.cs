using System;
using Akka.Streams;
using k8s;
using k8s.Models;
using Snd.Sdk.Kubernetes;

namespace Arcane.Operator.Models.Resources.StreamClass.Base;

/// <summary>
/// Base interface for StreamClass objects
/// </summary>
public interface IStreamClass : IKubernetesObject<V1ObjectMeta>
{
    /// <summary>
    /// Return Unique ID for the StreamClass object
    /// </summary>
    /// <returns></returns>
    string ToStreamClassId();

    /// <summary>
    /// Reference to the API group of the StreamDefinition CRD
    /// </summary>
    string ApiGroupRef { get; }

    /// <summary>
    /// Reference to the API version of the StreamDefinition CRD
    /// </summary>
    string VersionRef { get; }

    /// <summary>
    /// Reference to the plural name of the StreamDefinition CRD
    /// </summary>
    string PluralNameRef { get; }

    /// <summary>
    /// Reference to the kind name of the StreamDefinition CRD
    /// </summary>
    string KindRef { get; }

    /// <summary>
    /// Max buffer capacity for StreamDefinitions events stream
    /// </summary>
    int MaxBufferCapacity { get; }

    /// <summary>
    /// Convert configuration to NamespacedCrd object for consuming in the Proteus library
    /// </summary>
    /// <returns><see cref="NamespacedCrd"/>NamespacedCrd object</returns>
    NamespacedCrd ToNamespacedCrd();

    /// <summary>
    /// Returns true if the property should be mapped to environment variable with secret reference.
    /// </summary>
    /// <param name="propertyName">Name of the property to test</param>
    /// <returns></returns>
    bool IsSecretRef(string propertyName);
    
    /// <summary>
    /// Reads the restart settings for source that emits StreamDefinitionEvents for this StreamClass.
    /// For now, it is a default value.
    /// </summary>
    /// <returns>Akka restart settings instance.</returns>
    public RestartSettings RestartSettings => DefaultRestartSettings;

    /// <summary>
    /// Default restart settings for the StreamClass.
    /// Minimum backoff is 1 second.
    /// Maximum backoff is 30 seconds.
    /// Random factor is 0.2 adds 20% "noise" to vary the intervals slightly.
    /// Maximum number of restarts is 20 restarts within 10 minutes.
    /// </summary>
    private static RestartSettings DefaultRestartSettings =>
        RestartSettings.Create(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 0.2)
        .WithMaxRestarts(20, TimeSpan.FromMinutes(10));
}
