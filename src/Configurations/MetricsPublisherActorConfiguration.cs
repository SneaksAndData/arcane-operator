using System;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Metrics.Actors;

namespace Arcane.Operator.Configurations;

/// <summary>
/// The configuration for the <see cref="MetricsPublisherActor"/>
/// </summary>
public class MetricsPublisherActorConfiguration
{
    /// <summary>
    /// Interval to publish metrics
    /// </summary>
    public TimeSpan UpdateInterval { get; set; }


    /// <summary>
    /// Initial delay for the first metrics publication
    /// </summary>
    public TimeSpan InitialDelay { get; set; }
};
