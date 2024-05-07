using System.Collections.Generic;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using k8s;
using k8s.Models;
using Snd.Sdk.Helpers;

namespace Arcane.Operator.Services.Metrics;

public static class DeclaredMetrics
{
    public static string TrafficMetricName(this WatchEventType eventType) => $"objects.{eventType.ToString().ToLowerInvariant()}";

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

    public static SortedDictionary<string, string> GetMetricsTags(this StreamClassOperatorResponse s) => new()
    {
        { "namespace", s.StreamClass?.Namespace().ToLowerInvariant() },
        { "kind", CodeExtensions.CamelCaseToSnakeCase(s.StreamClass?.KindRef ?? "unknown") },
        { "phase", s.Phase.ToString().ToLowerInvariant() }
    };
}
