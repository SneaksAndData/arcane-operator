using Akka.Actor;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Metrics.Actors;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Options;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics;

/// <summary>
/// The IMetricsReporter implementation.
/// </summary>
public class MetricsReporter : IMetricsReporter
{
    private readonly MetricsService metricsService;
    private readonly IActorRef statusActor;

    public MetricsReporter(MetricsService metricsService, ActorSystem actorSystem,
        IOptions<MetricsReporterConfiguration> metricsReporterConfiguration)
    {
        this.metricsService = metricsService;
        this.statusActor = actorSystem.ActorOf(Props.Create(() => new MetricsPublisherActor(
                metricsReporterConfiguration.Value.StreamClassStatusActorConfiguration,
                metricsService)),
            nameof(MetricsPublisherActor));
    }

    /// <inheritdoc cref="IMetricsReporter.ReportStatusMetrics"/>
    public StreamClassOperatorResponse ReportStatusMetrics(StreamClassOperatorResponse streamClass)
    {
        if (streamClass.Phase.IsFinal())
        {
            this.statusActor.Tell(new RemoveStreamClassMetricsMessage(streamClass.StreamClass.KindRef));
        }
        else
        {
            var msg = new AddStreamClassMetricsMessage(streamClass.StreamClass.KindRef, "stream_class", streamClass.GetMetricsTags());
            this.statusActor.Tell(msg);
        }
        return streamClass;
    }

    /// <inheritdoc cref="IMetricsReporter.ReportTrafficMetrics"/>
    public (WatchEventType, V1Job) ReportTrafficMetrics((WatchEventType, V1Job) jobEvent)
    {
        this.metricsService.Count(jobEvent.Item1.TrafficMetricName(), 1, jobEvent.Item2.GetMetricsTags());
        return jobEvent;
    }

    /// <inheritdoc cref="IMetricsReporter.ReportTrafficMetrics"/>
    public ResourceEvent<TResource> ReportTrafficMetrics<TResource>(ResourceEvent<TResource> ev) where TResource : IKubernetesObject<V1ObjectMeta>
    {
        this.metricsService.Count(ev.EventType.TrafficMetricName(), 1, ev.kubernetesObject.GetMetricsTags());
        return ev;
    }
}
