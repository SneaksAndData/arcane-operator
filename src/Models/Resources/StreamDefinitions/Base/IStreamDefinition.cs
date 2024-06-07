using System.Collections.Generic;
using Arcane.Operator.Contracts;
using Arcane.Operator.Models.Resources.StreamClass.Base;
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
    /// Convert stream configuration to Kubernetes environment references
    /// </summary>
    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources(IStreamClass streamClass);

    /// <summary>
    /// Convert stream configuration to environment variables.
    /// </summary>
    /// <param name="isBackfilling">True if stream should run in backfill mode.</param>
    /// <param name="streamClass">Stream class object containing stream metadata.</param>
    public Dictionary<string, string> ToEnvironment(bool isBackfilling, IStreamClass streamClass);

    /// <summary>
    /// Returns checksum of the stream configuration
    /// </summary>
    public string GetConfigurationChecksum();

    /// <summary>
    /// Returns the job template for the stream
    /// </summary>
    /// <param name="isBackfilling">True if stream is should run in backfill mode, false otherwise</param>
    /// <returns>Reference to the JobTemplate custom Kubernetes object</returns>
    public V1TypedLocalObjectReference GetJobTemplate(bool isBackfilling);

    /// <summary>
    /// Deconstruct the stream definition into its parts for pattern matching
    /// </summary>
    /// <param name="nameSpace">Namespace where stream definition is located</param>
    /// <param name="kind">Stream definition object kind</param>
    /// <param name="streamId">Stream ID of the stream definition</param>
    public void Deconstruct(out string nameSpace, out string kind, out string streamId);
}
