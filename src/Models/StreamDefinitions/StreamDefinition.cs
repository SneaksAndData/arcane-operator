﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using k8s.Models;

namespace Arcane.Operator.Models.StreamDefinitions;

[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamDefinition : IStreamDefinition
{
    /// <summary>
    /// Stream configuration
    /// </summary>
    [JsonPropertyName("spec")]
    public JsonElement Spec { get; set; }

    /// <summary>
    /// Api version
    /// </summary>
    [JsonPropertyName("apiVersion")]
    public string ApiVersion { get; set; }

    /// <summary>
    /// Object kind (should always be "SqlServerStream")
    /// </summary>
    [JsonPropertyName("kind")]
    public string Kind { get; set; }

    /// <summary>
    /// Object metadata see <see cref="V1ObjectMeta"/>
    /// </summary>
    [JsonPropertyName("metadata")]
    public V1ObjectMeta Metadata { get; set; }

    /// <inheritdoc cref="IStreamDefinition"/>
    [JsonIgnore]
    public bool Suspended
        =>
            this.Metadata?.Annotations != null
            && this.Metadata.Annotations.TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
            && value == Annotations.SUSPENDED_STATE_ANNOTATION_VALUE;

    /// <inheritdoc cref="IStreamDefinition"/>
    [JsonIgnore]
    public bool ReloadRequested
        =>
            this.Metadata?.Annotations != null
            && this.Metadata.Annotations.TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
            && value == Annotations.RELOADING_STATE_ANNOTATION_VALUE;

    /// <inheritdoc cref="IStreamDefinition"/>
    [JsonIgnore]
    public V1TypedLocalObjectReference JobTemplateRef =>
        this.Spec.GetProperty("jobTemplateRef").Deserialize<V1TypedLocalObjectReference>();

    /// <inheritdoc cref="IStreamDefinition"/>
    [JsonIgnore]
    public V1TypedLocalObjectReference ReloadingJobTemplateRef => 
        this.Spec.GetProperty("reloadingJobTemplateRef").Deserialize<V1TypedLocalObjectReference>();

    /// <inheritdoc cref="IStreamDefinition"/>
    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources(IStreamClass streamDefinition)
    {
        foreach (var property in this.Spec.EnumerateObject())
        {
            if (streamDefinition.IsSecretField(property.Name))
            {
                yield return new V1EnvFromSource(secretRef: new V1SecretEnvSource(property.Value.GetString()));
            }
        }
    }

    /// <summary>
    /// Encode Stream runner configuration to dictionary that can be passed as environment variables
    /// </summary>
    /// <param name="fullLoad"></param>
    /// <returns>Dictionary of strings</returns>
    public Dictionary<string, string> ToEnvironment(bool fullLoad, IStreamClass streamClass)
    {
        return this.SelfToEnvironment(fullLoad)
            .Concat(this.SpecToEnvironment(streamClass))
            .ToDictionary(x => x.Key, x => x.Value);
    }

    private IEnumerable<KeyValuePair<string,string>> SpecToEnvironment(IStreamClass streamClass)
    {
        var newObj = this.Spec.Clone().Deserialize<Dictionary<string, object>>();
        foreach (var property in this.Spec.EnumerateObject().Where(property => streamClass.IsSecretField(property.Name)))
        {
            newObj.Remove(property.Name);
        }
        return new KeyValuePair<string, string>[]
        {
            new("SPEC".GetEnvironmentVariableName(), JsonSerializer.Serialize(newObj))
        };
    }

    public string GetConfigurationChecksum()
    {
        var base64Hash = Convert.ToBase64String(this.GetSpecHash());
        return base64Hash[..7].ToLowerInvariant();
    }

    /// <summary>
    /// Stream identifier
    /// </summary>
    [JsonIgnore]
    public string StreamId => this.Metadata.Name;

    private byte[] GetSpecHash()
    {
        return SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this.Spec)));
    }

    private Dictionary<string, string> SelfToEnvironment(bool fullLoad)
    {
        return new Dictionary<string, string>
        {
            { "STREAM_ID".GetEnvironmentVariableName(), this.StreamId },
            { "STREAM_KIND".GetEnvironmentVariableName(), this.Kind },
            { "FULL_LOAD".GetEnvironmentVariableName(), fullLoad.ToString().ToLowerInvariant() }
        };
    }
}
