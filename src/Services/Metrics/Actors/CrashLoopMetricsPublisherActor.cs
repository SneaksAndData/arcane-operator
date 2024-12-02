using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Event;
using Arcane.Operator.Configurations;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics.Actors;

/// <summary>
/// Add stream class metrics message. Once received, the metrics will be added to the
/// metrics collection in the actor.
/// </summary>
/// <param name="MetricName">Name of the stream kind referenced by the stream class</param>
/// <param name="MetricTags">Name of the metric to report</param>
/// <param name="MetricTags">Tags of the metric to report</param>
public record AddSCrashLoopMetricsMessage(string StreamId, string MetricName,
    SortedDictionary<string, string> MetricTags);


/// <summary>
/// Remove stream class metrics message. Once received, the metrics will be removed from the 
/// metrics collection in the actor.
/// </summary>
/// <param name="StreamId">Name of the stream kind referenced by the stream class</param>
public record RemoveCrashLoopMetricsMessage(string StreamId);


/// <summary>
/// Emit metrics message. Once received, the metrics will be emitted to the metrics service.
/// This message is emitted periodically by the <see cref="CrashLoopMetricsPublisherActor"/> actor.
/// </summary>
public record EmitCrashLoopMetricsMessage;

/// <summary>
/// A metric collection element for a stream.
/// </summary>
public record CrashLoopMetric(string MetricName, SortedDictionary<string, string> MetricTags, int MetricValue);


/// <summary>
/// This actor is responsible for collecting metrics representing the number of streams in the crash loop stage.
/// </summary>
public class CrashLoopMetricsPublisherActor : ReceiveActor, IWithTimers
{
    public ITimerScheduler Timers { get; set; }
    private readonly Dictionary<string, CrashLoopMetric> streamClassMetrics = new();
    private readonly ILoggingAdapter Log = Context.GetLogger();
    private readonly MetricsPublisherActorConfiguration configuration;

    public CrashLoopMetricsPublisherActor(MetricsPublisherActorConfiguration configuration, MetricsService metricsService)
    {
        this.configuration = configuration;
        this.Receive<AddSCrashLoopMetricsMessage>(s =>
        {
            if (s.MetricTags == null || s.MetricName == null || s.StreamId == null)
            {
                this.Log.Warning("Skip malformed {messageName} for {key} with value: {@message}",
                    nameof(AddSCrashLoopMetricsMessage),
                    s.StreamId,
                    s);
                return;
            }
            this.Log.Debug("Adding crash loop metric for {streamId}", s.StreamId);
            this.streamClassMetrics[s.StreamId] = new CrashLoopMetric(s.MetricName, s.MetricTags, 1);
        });

        this.Receive<RemoveCrashLoopMetricsMessage>(s =>
        {
            if (!this.streamClassMetrics.Remove(s.StreamId))
            {
                this.Log.Warning("Stream class {streamId} not found in metrics collection", s.StreamId);
            }
        });

        this.Receive<EmitCrashLoopMetricsMessage>(_ =>
        {
            this.Log.Debug("Start emitting crash loop metrics");
            foreach (var (_, metric) in this.streamClassMetrics)
            {
                try
                {
                    metricsService.Count(metric.MetricName, metric.MetricValue, metric.MetricTags);
                }
                catch (Exception exception)
                {
                    this.Log.Error(exception, "Failed to publish metrics for {key}", metric.MetricName);
                }
            }
        });
    }

    protected override void PreStart()
    {
        this.Timers.StartPeriodicTimer(nameof(EmitCrashLoopMetricsMessage),
            new EmitCrashLoopMetricsMessage(),
            this.configuration.InitialDelay,
            this.configuration.UpdateInterval);
    }
}
