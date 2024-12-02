using System.Collections.Generic;
using Arcane.Operator.Services.Metrics.Actors;

namespace Arcane.Operator.Services.Base.Metrics;

/// <summary>
/// Wrapper for the <see cref="CrashLoopMetricsPublisherActor"/>
/// </summary>
public interface ICrashLoopReporterService
{
    /// <summary>
    /// Adds the crash loop event to the metrics reporter
    /// </summary>
    /// <param name="streamId">The id of the affected stream.</param>
    /// <param name="metricTags">Metric tags</param>
    void AddCrashLoopEvent(string streamId, SortedDictionary<string, string> metricTags);

    /// <summary>
    /// Removes the crash loop event from the metrics reporter
    /// </summary>
    /// <param name="streamId">The id of the affected stream.</param>
    void RemoveCrashLoopEvent(string streamId);
}
