﻿using System.Collections.Generic;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.Commands;
using k8s;
using k8s.Models;
using Snd.Sdk.Helpers;

namespace Arcane.Operator.Services.Metrics;

public static class DeclaredMetrics
{
    /// <summary>
    /// Prefix for metrics published by the Arcane Operator
    /// </summary>
    private const string TAG_PREFIX = "arcane.sneaksanddata.com";
    public static string TrafficMetricName(this WatchEventType eventType) => $"objects.{eventType.ToString().ToLowerInvariant()}";

    public static SortedDictionary<string, string> GetMetricsTags(this IKubernetesObject<V1ObjectMeta> job) => new()
    {
        { $"{TAG_PREFIX}/namespace", job.Namespace() },
        { $"{TAG_PREFIX}/kind", job.Kind },
        { $"{TAG_PREFIX}/name", job.Name() },
    };

    public static SortedDictionary<string, string> GetMetricsTags(this V1Job job) => new()
    {
        { $"{TAG_PREFIX}/namespace", job.Namespace() },
        { $"{TAG_PREFIX}/kind", job.GetStreamKind() },
        { $"{TAG_PREFIX}/stream_id", job.GetStreamId() }
    };

    public static SortedDictionary<string, string> GetMetricsTags(this SetStreamClassStatusCommand s) => new()
    {
        { $"{TAG_PREFIX}/namespace", s.streamClass?.Namespace().ToLowerInvariant() },
        { $"{TAG_PREFIX}/kind_ref", CodeExtensions.CamelCaseToSnakeCase(s.streamClass?.KindRef ?? "unknown") },
        { $"{TAG_PREFIX}/kind", CodeExtensions.CamelCaseToSnakeCase(s.streamClass?.Kind ?? "unknown") },
        { $"{TAG_PREFIX}/phase", s.phase.ToString().ToLowerInvariant() }
    };
}
