using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Metrics;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="MetricsReporter"/> class.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class CrashLoopMetricsReporterConfiguration
{
    public MetricsPublisherActorConfiguration MetricsPublisherActorConfiguration { get; set; }
};
