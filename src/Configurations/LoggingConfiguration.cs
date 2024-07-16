using System.Collections.Generic;
using Arcane.Operator.Services.Metrics;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="MetricsReporter"/> class.
/// </summary>
public class LoggingConfiguration
{
    /// <summary>
    /// Custom static properties to enrich the logger with.
    /// </summary>
    public Dictionary<string, string> CustomProperties { get; init; }
};
