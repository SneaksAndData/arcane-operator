using System.Collections.Generic;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.StreamClass;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Microsoft.Extensions.Caching.Memory;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services.Repositories.CustomResources;

public class StreamClassRepository : IStreamClassRepository
{
    private readonly IMemoryCache memoryCache;
    private readonly IKubeCluster kubeCluster;

    public StreamClassRepository(IMemoryCache memoryCache, IKubeCluster kubeCluster)
    {
        this.memoryCache = memoryCache;
        this.kubeCluster = kubeCluster;
    }

    public Task<Option<IStreamClass>> Get(string nameSpace, string streamDefinitionKind) =>
        this.memoryCache.Get<V1Beta1StreamClass>(streamDefinitionKind) switch
        {
            null => Task.FromResult(Option<IStreamClass>.None),
            var streamClass => Task.FromResult(Option<IStreamClass>.Create(streamClass))
        };

    public Task InsertOrUpdate(IStreamClass streamClass, StreamClassPhase phase, IEnumerable<V1Alpha1StreamCondition> conditions, string pluralName)
    {
        this.memoryCache.Set(streamClass.KindRef, streamClass);
        return Task.CompletedTask;
    }

    /// <inheritdoc cref="IReactiveResourceCollection{TResourceType}.GetEvents"/>>
    public Source<ResourceEvent<IStreamClass>, NotUsed> GetEvents(CustomResourceApiRequest request, int maxBufferCapacity) =>
        this.kubeCluster.StreamCustomResourceEvents<V1Beta1StreamClass>(
                request.Namespace,
                request.ApiGroup,
                request.ApiVersion,
                request.PluralName,
                maxBufferCapacity,
                OverflowStrategy.Fail)
            .Select((tuple) => new ResourceEvent<IStreamClass>(tuple.Item1, tuple.Item2));
}

