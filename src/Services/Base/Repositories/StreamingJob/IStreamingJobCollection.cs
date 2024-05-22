using System.Threading.Tasks;
using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Services.Models;
using k8s.Models;

namespace Arcane.Operator.Services.Base.Repositories.StreamingJob;

public interface IStreamingJobCollection
{
    /// <summary>
    /// Subscribe to a stream class updates
    /// </summary>
    /// <param name="nameSpace">The namespace to watch for</param>
    /// <param name="maxBufferCapacity">Maximum capacity of the buffer in the source</param>
    /// <returns>An Akka source that emits a Kubernetes entity updates</returns>
    Source<ResourceEvent<V1Job>, NotUsed> GetEvents(string nameSpace, int maxBufferCapacity);

    /// <summary>
    /// Subscribe to a stream class updates
    /// </summary>
    /// <param name="nameSpace">An object that contains required information for a Kubernetes API call</param>
    /// <param name="name"></param>
    /// <returns>An Akka source that emits a Kubernetes entity updates</returns>
    Task<Option<V1Job>> Get(string nameSpace, string name);
}
