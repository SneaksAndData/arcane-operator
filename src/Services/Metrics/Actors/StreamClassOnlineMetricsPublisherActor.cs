using System;
using System.Collections.Generic;
using SnD.Sdk.Metrics.Actors;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics.Actors;

/// <summary>
/// The actor that emits a single value metric for each stream class that is online.
/// </summary>
public class StreamClassOnlineMetricsPublisherActor : MetricsPublisherActor
{
    public StreamClassOnlineMetricsPublisherActor(TimeSpan initialDelay, TimeSpan emitInterval, MetricsService metricsService)
        : base(initialDelay, emitInterval, metricsService)
    {
    }

    /// <inheritdoc cref="MetricsPublisherActor.EmitMetric"/>
    protected override void EmitMetric(MetricsService metricsService, string name, int value, SortedDictionary<string, string> tags)
    {
        metricsService.Count(name, value, tags);
    }
}
