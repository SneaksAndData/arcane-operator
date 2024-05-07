using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Event;
using Akka.Routing;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics;

public record AddStreamClassMetricsMessage(string StreamKindRef, string MetricName,
    SortedDictionary<string, string> Metrics);
public record RemoveStreamClassMetricsMessage(string StreamKindRef);
public record EmitMetricsMessage;
public record StreamClassMetric(SortedDictionary<string, string> metricTags, string metricName);

public class StreamClassStatusService : ReceiveActor, IWithTimers
{
    public ITimerScheduler Timers { get; set; }
    private readonly Dictionary<string, StreamClassMetric> streamClassMetrics = new();
    private readonly ILoggingAdapter Log = Context.GetLogger();
    private readonly StreamClassStatusServiceConfiguration configuration;

    public StreamClassStatusService(StreamClassStatusServiceConfiguration configuration, MetricsService metricsService)
    {
        this.configuration = configuration;
        this.Receive<AddStreamClassMetricsMessage>(s =>
        {
            this.Log.Debug("Adding stream class metrics for {streamKindRef}", s.StreamKindRef);
            this.streamClassMetrics[s.StreamKindRef] = new StreamClassMetric(s.Metrics, s.MetricName);
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
                metricsService.Count(metric.metricName, 1, metric.metricTags);
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

public class StreamClassStatusServiceConfiguration
{
    public TimeSpan UpdateInterval { get; set; }
    public TimeSpan InitialDelay { get; set; }
}
