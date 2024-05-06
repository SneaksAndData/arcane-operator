using Arcane.Operator.Models;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Services.Metrics;

public interface IMetricsReporter
{
    StreamClassOperatorResponse ReportStatusMetrics(StreamClassOperatorResponse arg);
    StreamOperatorResponse ReportStatusMetrics(StreamOperatorResponse arg);
    (WatchEventType, V1Job) ReportTrafficMetrics((WatchEventType, V1Job) jobEvent);
    ResourceEvent<TResource> ReportTrafficMetrics<TResource>(ResourceEvent<TResource> ev) where TResource: IKubernetesObject<V1ObjectMeta>;
}
