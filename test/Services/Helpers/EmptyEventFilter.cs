using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Services.Base;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.Helpers;

public class EmptyEventFilter<TResourceType> : IEventFilter<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    public Flow<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>, NotUsed> Filter()
    {
        return Flow.FromFunction<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>>(e => e);
    }
}
