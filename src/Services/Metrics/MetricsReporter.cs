using Arcane.Operator.Models;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics;

public class MetricsReporter : IMetricsReporter
{
    private readonly MetricsService metricsService;

    public MetricsReporter(MetricsService metricsService)
    {
        this.metricsService = metricsService;
    }
    
    public StreamClassOperatorResponse ReportStatusMetrics(StreamClassOperatorResponse arg)
    {
        this.metricsService.Count( DeclaredMetrics.PhaseTransitions("stream_classes"), 1, arg.GetMetricsTags());
        return arg;
    }
    
    public StreamOperatorResponse ReportStatusMetrics(StreamOperatorResponse arg)
    {
        this.metricsService.Count( DeclaredMetrics.PhaseTransitions("streams"), 1, arg.GetMetricsTags());
        return arg;
    }
    
    public (WatchEventType, V1Job) ReportTrafficMetrics((WatchEventType, V1Job) jobEvent)
    {
        this.metricsService.Gauge(jobEvent.Item2.TrafficMetric(jobEvent.Item1), 1, jobEvent.Item2.GetMetricsTags() );
        return jobEvent;
    }
    
    public ResourceEvent<TResource> ReportTrafficMetrics<TResource>(ResourceEvent<TResource> ev) where TResource: IKubernetesObject<V1ObjectMeta>
    {
        this.metricsService.Count(ev.kubernetesObject.TrafficMetric(ev.EventType), 1, ev.kubernetesObject.GetMetricsTags());
        return ev;
    }
}
