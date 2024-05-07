using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using k8s.Models;

namespace Arcane.Operator.Services.Base;

public interface IStreamingJobOperatorService
{
    /// <summary>
    /// Namespace for kubernetes jobs
    /// </summary>
    public string StreamJobNamespace { get; }

    /// <summary>
    /// Starts a new stream using an existing stream definition in Kubernetes database.
    /// </summary>
    /// <param name="streamDefinition">Stream definition</param>
    /// <param name="isBackfilling">Whether to perform a full reload for this stream.</param>
    /// <param name="streamClass"></param>
    /// <returns>StreamInfo if stream was created or None if an error occured</returns>
    Task<Option<StreamOperatorResponse>> StartRegisteredStream(IStreamDefinition streamDefinition, bool isBackfilling,
        IStreamClass streamClass);

    /// <summary>
    /// Retrieves a streaming job with name equal to streamId from the cluster. If not found, returns None.
    /// </summary>
    /// <param name="streamId">Stream identifier that should be started.</param>
    /// <returns></returns>
    Task<Option<V1Job>> GetStreamingJob(string streamId);

    /// <summary>
    /// Marks streaming job for restart
    /// </summary>
    /// <param name="streamId">Stream identifier that should be terminated.</param>
    /// <returns></returns>
    Task<Option<StreamOperatorResponse>> RequestStreamingJobRestart(string streamId);

    /// <summary>
    /// Marks streaming job for stop
    /// </summary>
    /// <param name="streamId">Stream identifier that should be terminated.</param>
    /// <returns></returns>
    Task<Option<StreamOperatorResponse>> RequestStreamingJobReload(string streamId);

    /// <summary>
    /// Delete the streaming job
    /// </summary>
    /// <param name="kind">Stream definition kind</param>
    /// <param name="streamId">Stream identifier that should be terminated.</param>
    /// <returns></returns>
    Task<Option<StreamOperatorResponse>> DeleteJob(string kind, string streamId);
}
