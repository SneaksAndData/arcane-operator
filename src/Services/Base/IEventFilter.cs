using Akka;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Services.Base;

public interface IEventFilter<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    public Flow<ResourceEvent<TResourceType>, Option<ResourceEvent<TResourceType>>, NotUsed> Filter();
}