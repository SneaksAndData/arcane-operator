using Arcane.Operator.Services.Metrics;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="MetricsReporter"/> class.
/// </summary>
/// <param name="PeriodicMetricsUpdateConfiguration">Stream class periodic status reporting settings.</param>
public record MetricsReporterConfiguration(StreamClassStatusActorConfiguration PeriodicMetricsUpdateConfiguration);
