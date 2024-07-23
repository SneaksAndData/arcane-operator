using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Services.Base.EventFilters;

/// <summary>
/// Common interface for filtering Kubernetes custom resource events.
/// </summary>
/// <typeparam name="TResourceType">Resource type parameter</typeparam>
public interface IEventFilter<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    /// <summary>
    /// Creates a flow that filters out Kubernetes custom resource events based on the implementation.
    /// </summary>
    /// <returns></returns>
    public Flow<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>, NotUsed> Filter();
}
