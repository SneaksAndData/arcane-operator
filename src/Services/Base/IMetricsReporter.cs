using Arcane.Operator.Models;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Services.Base;

/// <summary>
/// Interface for reporting metrics.
/// </summary>
public interface IMetricsReporter
{
    /// <summary>
    /// Report status metrics for a StreamClass object
    /// </summary>
    /// <param name="streamClass">StreamClassOperatorResponse object with StreamClass status information</param>
    /// <returns>The same object for processing in the next stages of operator state machine.</returns>
    StreamClassOperatorResponse ReportStatusMetrics(StreamClassOperatorResponse streamClass);

    /// <summary>
    /// Reports Count metric for a V1Job object
    /// </summary>
    /// <param name="jobEvent">Job event type and job object</param>
    /// <returns>The same object for processing in the next stages of operator state machine.</returns>
    (WatchEventType, V1Job) ReportTrafficMetrics((WatchEventType, V1Job) jobEvent);

    /// <summary>
    /// Reports Count metric for a Kubernetes custom resource object
    /// </summary>
    /// <param name="ev">Object event metadata</param>
    /// <typeparam name="TResource">Type of custom resource, must be a Kubernetes object</typeparam>
    /// <returns></returns>
    ResourceEvent<TResource> ReportTrafficMetrics<TResource>(ResourceEvent<TResource> ev) where TResource : IKubernetesObject<V1ObjectMeta>;
}
