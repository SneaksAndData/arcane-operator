﻿using k8s;
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
}
