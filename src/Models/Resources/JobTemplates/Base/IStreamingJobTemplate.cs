using k8s;
using k8s.Models;

namespace Arcane.Operator.Models.Resources.JobTemplates.Base;

/// <summary>
/// Common abstraction for streaming job templates.
/// </summary>
public interface IStreamingJobTemplate : IKubernetesObject<V1ObjectMeta>
{
    /// <summary>
    /// Returns the job object stored in the job template.
    /// </summary>
    /// <returns>Kubernetes job object</returns>
    public V1Job GetJob();
}
