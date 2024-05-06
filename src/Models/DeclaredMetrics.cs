using System.Collections.Generic;
using System.Linq;
using Arcane.Operator.Extensions;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Models;

public static class DeclaredMetrics
{
   public static string PhaseTransitions(string entity) => $"{entity}.phase_transitions";
   public static string Conditions(string entity) => $"{entity}.conditions";
   public static string TrafficMetric(this IKubernetesObject<V1ObjectMeta> obj, WatchEventType eventType)
    => $"{obj.Kind.ToLowerInvariant()}.{eventType.ToString().ToLowerInvariant()}";
   
    public static SortedDictionary<string, string> GetMetricsTags(this IKubernetesObject<V1ObjectMeta> job) => new()
    {
        { "namespace", job.Namespace() },
        { "kind", job.Kind },
        { "name", job.Name() }
    };
    
    public static SortedDictionary<string, string> GetMetricsTags(this V1Job job) => new()
    {
        { "namespace", job.Namespace() },
        { "kind", job.GetStreamKind() },
        { "streamId", job.GetStreamId() }
    };
    
    public static SortedDictionary<string, string> GetMetricsTags(this StreamOperatorResponse s) => new()
    {
        { "namespace", s.Namespace },
        { "kind", s.Kind },
        { "streamId", s.Id },
        { "condition", s.Conditions.Single().Type.ToLowerInvariant() }
    };

    public static SortedDictionary<string, string> GetMetricsTags(this StreamClassOperatorResponse s) => new()
    {
        { "namespace", s.StreamClass.Namespace().ToLowerInvariant() },
        { "kind", s.StreamClass.Kind.ToLowerInvariant() },
        { "condition", s.Conditions.Single().Type.ToLowerInvariant() }
    };
}
