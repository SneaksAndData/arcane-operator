using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;

namespace Arcane.Operator.Services.Base;

public interface IStreamDefinitionRepository : IReactiveResourceCollection<IStreamDefinition>
{
    /// <summary>
    /// Return the definition object fot the given stream id
    /// </summary>
    /// <param name="nameSpace">Stream definition namespace</param>
    /// <param name="kind">Stream definition kind to update</param>
    /// <param name="streamId">Stream identifier</param>
    /// <returns>IStreamDefinition or None, it it does not exit</returns>
    public Task<Option<IStreamDefinition>> GetStreamDefinition(string nameSpace, string kind,
        string streamId);

    /// <summary>
    /// Update stream condition for the given stream id
    /// </summary>
    /// <param name="nameSpace">Stream definition namespace</param>
    /// <param name="kind">Stream definition namespace</param>
    /// <param name="streamId">Stream identifier</param>
    /// <param name="streamStatus">Stream condition</param>
    /// <returns>Updated IStreamDefinition version or None, it it does not exit</returns>
    public Task<Option<IStreamDefinition>> SetStreamStatus(string nameSpace, string kind, string streamId,
        V1Beta1StreamStatus streamStatus);

    /// <summary>
    /// Remove reloading annotation for the given stream id
    /// </summary>
    /// <param name="nameSpace">Stream definition namespace</param>
    /// <param name="kind">Stream definition kind to update</param>
    /// <param name="streamId">Stream identifier</param>
    /// <returns>IStreamDefinition or None, it it does not exit</returns>
    public Task<Option<IStreamDefinition>> RemoveReloadingAnnotation(string nameSpace, string kind, string streamId);
}
