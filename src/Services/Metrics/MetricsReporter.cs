using Akka.Actor;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Metrics.Actors;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Options;
using SnD.Sdk.Metrics.Actors;
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
        this.statusActor = actorSystem.StartMetricsPublisher(() =>
            new StreamClassOnlineMetricsPublisherActor(
                metricsReporterConfiguration.Value.MetricsPublisherActorConfiguration.InitialDelay,
                metricsReporterConfiguration.Value.MetricsPublisherActorConfiguration.UpdateInterval,
                this.metricsService));
    }

    /// <inheritdoc cref="IMetricsReporter.ReportStatusMetrics"/>
    public SetStreamClassStatusCommand ReportStatusMetrics(SetStreamClassStatusCommand command)
    {
        if (command.phase.IsFinal())
        {
            this.statusActor.Tell(new RemoveMetricMessage(command.streamClass.KindRef));
        }
        else
        {
            var msg = new AddMetricMessage(command.streamClass.KindRef, "stream_class", command.GetMetricsTags());
            this.statusActor.Tell(msg);
        }
        return command;
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
