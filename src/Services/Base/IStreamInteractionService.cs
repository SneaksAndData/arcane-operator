using System;
using System.Threading.Tasks;

namespace Arcane.Operator.Services.Base;

public interface IStreamInteractionService
{
    /// <summary>
    /// Set stream termination callback
    /// </summary>
    /// <param name="onStreamTermination">The action to be invoked on stream termination</param>
    /// <returns>Task that completes when termination event occured</returns>
    Task SetupTermination(Action<Task> onStreamTermination);

    /// <summary>
    /// Send termination request to a stream pod
    /// </summary>
    /// <param name="streamerIp">Ip address of a pod that running stream</param>
    Task SendStopRequest(string streamerIp);

    /// <summary>
    /// Report that schema mismatch occured to the maintainer service
    /// </summary>
    /// <param name="streamId">Id of the current stream</param>
    Task ReportSchemaMismatch(string streamId);
}
