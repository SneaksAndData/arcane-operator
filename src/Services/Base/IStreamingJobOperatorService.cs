using System.Threading.Tasks;
using Akka.Util;
using k8s.Models;

namespace Arcane.Operator.Services.Base;

public interface IStreamingJobOperatorService
{
    /// <summary>
    /// Namespace for kubernetes jobs
    /// </summary>
    public string StreamJobNamespace { get; }

    /// <summary>
    /// Retrieves a streaming job with name equal to streamId from the cluster. If not found, returns None.
    /// </summary>
    /// <param name="streamId">Stream identifier that should be started.</param>
    /// <returns></returns>
    Task<Option<V1Job>> GetStreamingJob(string streamId);
}
