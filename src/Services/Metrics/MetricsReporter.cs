using System;
using Akka.Actor;
using Arcane.Operator.Models;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics;

public class MetricsReporter : IMetricsReporter
{
    private readonly MetricsService metricsService;
    private readonly ActorSystem actorSystem;
    private readonly IActorRef statusActor;

    public MetricsReporter(MetricsService metricsService, ActorSystem actorSystem)
    {
        this.metricsService = metricsService;
        this.actorSystem = actorSystem;
        var config = new StreamClassStatusServiceConfiguration
        {
            InitialDelay = TimeSpan.FromSeconds(30),
            UpdateInterval = TimeSpan.FromSeconds(10)
        };
        this.statusActor = actorSystem.ActorOf(Props.Create(() => new StreamClassStatusService(config, metricsService)), nameof(StreamClassStatusService));
    }

    public StreamClassOperatorResponse ReportStatusMetrics(StreamClassOperatorResponse arg)
    {
        if (arg.Phase.IsFinal())
        {
            this.statusActor.Tell(new RemoveStreamClassMetricsMessage(arg.StreamClass.KindRef));
        }
        else
        {
            var msg = new AddStreamClassMetricsMessage(arg.StreamClass.KindRef, "stream_class", arg.GetMetricsTags());
            this.statusActor.Tell(msg);
        }
        return arg;
    }

    public StreamOperatorResponse ReportStatusMetrics(StreamOperatorResponse arg)
    {
        this.metricsService.Count(DeclaredMetrics.PhaseTransitions("streams"), 1, arg.GetMetricsTags());
        return arg;
    }

    public (WatchEventType, V1Job) ReportTrafficMetrics((WatchEventType, V1Job) jobEvent)
    {
        this.metricsService.Gauge(jobEvent.Item1.TrafficMetric(), 1, jobEvent.Item2.GetMetricsTags());
        return jobEvent;
    }

    public ResourceEvent<TResource> ReportTrafficMetrics<TResource>(ResourceEvent<TResource> ev) where TResource : IKubernetesObject<V1ObjectMeta>
    {
        this.metricsService.Count(ev.EventType.TrafficMetric(), 1, ev.kubernetesObject.GetMetricsTags());
        return ev;
    }

    public ResourceEvent<TResource> ReportErrorMetrics<TResource>(ResourceEvent<TResource> ev) where TResource : IKubernetesObject<V1ObjectMeta>
    {
        this.metricsService.Count(DeclaredMetrics.ErrorMetric, 1, ev.kubernetesObject.GetMetricsTags());
        return ev;
    }
}
