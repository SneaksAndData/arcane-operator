using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base.Repositories.CustomResources;

namespace Arcane.Operator.Services.Base;

/// <summary>
/// The stream class storage abstraction
/// </summary>
public interface IStreamClassRepository : IReactiveResourceCollection<IStreamClass>
{
    /// <summary>
    /// Reads a stream class from the repository by it's namespace and kind of the stream definition of this class
    /// </summary>
    /// <param name="nameSpace">Namespace where stream class was created</param>
    /// <param name="streamDefinitionKind">Kind of the stream definition object</param>
    /// <returns>Optional stream class result</returns>
    Task<Option<IStreamClass>> Get(string nameSpace, string streamDefinitionKind);

    /// <summary>
    /// Insert or update a stream class in the repository.
    /// </summary>
    /// <param name="streamClass">The StreamClass object to insert or update.</param>
    /// <param name="phase">The stream class phase</param>
    /// <param name="conditions">The stream class conditions</param>
    /// <param name="pluralName">The stream class plural name</param>
    /// <returns></returns>
    Task InsertOrUpdate(IStreamClass streamClass, StreamClassPhase phase,
        IEnumerable<V1Beta1StreamCondition> conditions, string pluralName);
}
