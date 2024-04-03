using System.Collections.Generic;
using Arcane.Models.StreamingJobLifecycle;
using k8s;
using k8s.Models;
using Newtonsoft.Json;

namespace Arcane.Operator.StreamDefinitions.Base;

public interface IStreamDefinition : IKubernetesObject<V1ObjectMeta>
{
    /// <summary>
    /// Stream identifier
    /// </summary>
    [JsonIgnore]
    public string StreamId { get; }

    /// <summary>
    /// True if the stream is suspended
    /// </summary>
    public bool Suspended { get; }

    /// <summary>
    /// True if the stream is a crash loop detected
    /// </summary>
    [JsonIgnore]
    public bool CrashLoopDetected
        =>
            this.Metadata?.Annotations != null
            && this.Metadata.Annotations.TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
            && value == Annotations.CRASH_LOOP_STATE_ANNOTATION_VALUE;

    /// <summary>
    /// True if a data backfill (full reload) is requested
    /// </summary>
    public bool ReloadRequested { get; }

    /// <summary>
    /// Streaming job template reference
    /// </summary>
    public V1TypedLocalObjectReference JobTemplateRef { get; }

    /// <summary>
    /// Streaming job template reference for full load job mode
    /// </summary>
    public V1TypedLocalObjectReference ReloadingJobTemplateRef { get; }

    /// <summary>
    /// Convert stream configuration to Kubernetes environment references
    /// </summary>
    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources();

    /// <summary>
    /// Convert stream configuration to environment variables
    /// </summary>
    /// <param name="fullLoad"></param>
    public Dictionary<string, string> ToEnvironment(bool fullLoad);

    /// <summary>
    /// Returns checksum of the stream configuration
    /// </summary>
    public string GetConfigurationChecksum();
}
