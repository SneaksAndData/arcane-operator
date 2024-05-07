using Arcane.Operator.Services.Metrics;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="MetricsReporter"/> class.
/// </summary>
public class MetricsReporterConfiguration
{
    public StreamClassStatusActorConfiguration StreamClassStatusActorConfiguration { get; set; }
};
