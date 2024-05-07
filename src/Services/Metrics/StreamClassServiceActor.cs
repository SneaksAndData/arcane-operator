using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Event;
using Arcane.Operator.Configurations;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics;

/// <summary>
/// Add stream class metrics message. Once received, the metrics will be added to the
/// metrics collection in the <see cref="StreamClassServiceActor"/> actor.
/// </summary>
/// <param name="StreamKindRef">Name of the stream kind referenced by the stream class</param>
/// <param name="MetricName">Name of the metric to report</param>
/// <param name="MetricTags">Tags of the metric to report</param>
public record AddStreamClassMetricsMessage(string StreamKindRef, string MetricName,
    SortedDictionary<string, string> MetricTags);


/// <summary>
/// Remove stream class metrics message. Once received, the metrics will be removed from the 
/// metrics collection in the <see cref="StreamClassServiceActor"/> actor.
/// </summary>
/// <param name="StreamKindRef">Name of the stream kind referenced by the stream class</param>
public record RemoveStreamClassMetricsMessage(string StreamKindRef);


/// <summary>
/// Emit metrics message. Once received, the metrics will be emitted to the metrics service.
/// This message is emitted periodically by the <see cref="StreamClassServiceActor"/> actor.
/// </summary>
public record EmitMetricsMessage;

/// <summary>
/// A metric collection element for a stream class.
/// </summary>
public class StreamClassMetric
{
    /// <summary>
    /// Name of the metric to report.
    /// </summary>
    public string MetricName { get; init; }


    /// <summary>
    /// Tags of the metric to report.
    /// </summary>
    public SortedDictionary<string, string> MetricTags { get; init; }

    /// <summary>
    /// Metric Value
    /// </summary>
    public int MetricValue { get; set; } = 1;
}


/// <summary>
/// Stream class service actor. This actor is responsible for collecting metrics for stream classes
/// that should be emitted periodically.
/// </summary>
public class StreamClassServiceActor : ReceiveActor, IWithTimers
{
    public ITimerScheduler Timers { get; set; }
    private readonly Dictionary<string, StreamClassMetric> streamClassMetrics = new();
    private readonly ILoggingAdapter Log = Context.GetLogger();
    private readonly StreamClassStatusActorConfiguration configuration;

    public StreamClassServiceActor(StreamClassStatusActorConfiguration configuration, MetricsService metricsService)
    {
        this.configuration = configuration;
        this.Receive<AddStreamClassMetricsMessage>(s =>
        {
            this.Log.Debug("Adding stream class metrics for {streamKindRef}", s.StreamKindRef);
            this.streamClassMetrics[s.StreamKindRef] = new StreamClassMetric
            {
                MetricTags = s.MetricTags,
                MetricName = s.MetricName,
            };
        });

        this.Receive<RemoveStreamClassMetricsMessage>(s =>
        {
            if (!this.streamClassMetrics.Remove(s.StreamKindRef))
            {
                this.Log.Warning("Stream class {streamKindRef} not found in metrics collection", s.StreamKindRef);
            }
        });

        this.Receive<EmitMetricsMessage>(_ =>
        {
            this.Log.Debug("Start emitting stream class metrics");
            foreach (var (_, metric) in this.streamClassMetrics)
            {
                metricsService.Count(metric.MetricName, metric.MetricValue, metric.MetricTags);
            }
        });
    }

    protected override void PreStart()
    {
        this.Timers.StartPeriodicTimer(nameof(EmitMetricsMessage),
            new EmitMetricsMessage(),
            this.configuration.InitialDelay,
            this.configuration.UpdateInterval);
    }
}
