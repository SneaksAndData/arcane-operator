using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Services.Base.EventFilters;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.Helpers;

/// <summary>
/// Does not do anything, used for testing.
/// </summary>
/// <typeparam name="TResourceType">Resource type</typeparam>
public class EmptyEventFilter<TResourceType> : IEventFilter<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    /// <summary>
    /// Does not do anything, used for testing.
    /// </summary>
    /// <returns>Every event passed to it.</returns>
    public Flow<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>, NotUsed> Filter()
    {
        return Flow.FromFunction<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>>(e => e);
    }
}
