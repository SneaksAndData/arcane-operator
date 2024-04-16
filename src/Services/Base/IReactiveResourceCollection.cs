using Akka;
using Akka.Streams.Dsl;
using Arcane.Operator.Services.Models;

namespace Arcane.Operator.Services.Base;

public interface IReactiveResourceCollection<TResourceType>
{
    /// <summary>
    /// Subscribe to a stream class updates
    /// </summary>
    /// <param name="request">An object that contains required information for a Kubernetes API call</param>
    /// <param name="maxBufferCapacity">Maximum capacity of the buffer in the source</param>
    /// <returns>An Akka source that emits a Kubernetes entity updates</returns>
    Source<UpdateEvent<TResourceType>, NotUsed> GetUpdates(CustomResourceApiRequest request, int maxBufferCapacity);
}
