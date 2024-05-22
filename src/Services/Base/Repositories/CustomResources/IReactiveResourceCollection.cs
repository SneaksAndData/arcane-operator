using Akka;
using Akka.Streams.Dsl;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Services.Base.Repositories.CustomResources;

public interface IReactiveResourceCollection<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    /// <summary>
    /// Subscribe to a stream class updates
    /// </summary>
    /// <param name="request">An object that contains required information for a Kubernetes API call</param>
    /// <param name="maxBufferCapacity">Maximum capacity of the buffer in the source</param>
    /// <returns>An Akka source that emits a Kubernetes entity updates</returns>
    Source<ResourceEvent<TResourceType>, NotUsed> GetEvents(CustomResourceApiRequest request, int maxBufferCapacity);
}
