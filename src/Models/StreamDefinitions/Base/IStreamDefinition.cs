using System.Collections.Generic;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Models.StreamClass.Base;
using k8s;
using k8s.Models;
using Newtonsoft.Json;

namespace Arcane.Operator.Models.StreamDefinitions.Base;

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
    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources(IStreamClass streamClass);

    /// <summary>
    /// Convert stream configuration to environment variables.
    /// </summary>
    /// <param name="fullLoad">True if stream should run in backfill mode.</param>
    /// <param name="streamClass">Stream class object containing stream metadata.</param>
    public Dictionary<string, string> ToEnvironment(bool fullLoad, IStreamClass streamClass);

    /// <summary>
    /// Returns checksum of the stream configuration
    /// </summary>
    public string GetConfigurationChecksum();
}
