using System.Collections.Generic;
using System.Linq;
using Arcane.Operator.Extensions;
using IdentityModel;
using k8s;
using k8s.Models;
using Snd.Sdk.Helpers;

namespace Arcane.Operator.Models;

public static class DeclaredMetrics
{
    public static string PhaseTransitions(string entity) => $"{entity}.phase_transitions";
    public static string Conditions(string entity) => $"{entity}.conditions";
    public static string TrafficMetric(this WatchEventType eventType)
     => $"objects.{eventType.ToString().ToLowerInvariant()}";

    public static string ErrorMetric = "errors";

    public static SortedDictionary<string, string> GetMetricsTags(this IKubernetesObject<V1ObjectMeta> job) => new()
    {
        { "namespace", job.Namespace() },
        { "kind", job.Kind },
        { "name", job.Name() },
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
        { "phase", s.Phase.ToString().ToLowerInvariant() }
    };

    public static SortedDictionary<string, string> GetMetricsTags(this StreamClassOperatorResponse s) => new()
    {
        { "namespace", s.StreamClass?.Namespace().ToLowerInvariant() },
        { "kind", CodeExtensions.CamelCaseToSnakeCase(s.StreamClass?.KindRef ?? "unknown") },
        { "phase", s.Phase.ToString().ToLowerInvariant() }
    };
}
