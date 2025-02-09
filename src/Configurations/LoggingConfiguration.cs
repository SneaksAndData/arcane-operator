using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Metrics;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="MetricsReporter"/> class.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class LoggingConfiguration
{
    /// <summary>
    /// Custom static properties to enrich the logger with.
    /// </summary>
    public Dictionary<string, string> CustomProperties { get; init; }

    /// <summary>
    /// Minimum level overrides for specific loggers.
    /// <example>
    /// {
    ///   "Microsoft": "Warning",
    ///   "System": "Warning"
    /// }
    /// </example>
    /// </summary>
    public Dictionary<string, string> MinimumLevelOverrides { get; init; }
};
