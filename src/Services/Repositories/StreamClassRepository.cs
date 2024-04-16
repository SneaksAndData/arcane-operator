using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Caching.Memory;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services.Repositories;

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

    public Task InsertOrUpdate(IStreamClass streamClass, StreamClassPhase phase, IEnumerable<V1Beta1StreamCondition> conditions, string pluralName)
    {
        this.memoryCache.Set(streamClass.KindRef, streamClass);

        var status = new V1Beta1StreamStatus()
        {
            Phase = phase.ToString(),
            Conditions = conditions.ToArray()
        };
        return this.kubeCluster.UpdateCustomResourceStatus(
            streamClass.ApiGroup(),
            streamClass.ApiGroupVersion(),
            pluralName,
            streamClass.Namespace(),
            streamClass.Name(),
            status,
            element => element.AsOptionalStreamClass());
    }

    /// <inheritdoc cref="IStreamClassRepository.GetUpdates"/>>
    public Source<UpdateEvent<IStreamClass>, NotUsed> GetUpdates(CustomResourceApiRequest request, int maxBufferCapacity) =>
        this.kubeCluster.StreamCustomResourceEvents<V1Beta1StreamClass>(
                request.Namespace,
                request.ApiGroup,
                request.ApiVersion,
                request.PluralName,
                maxBufferCapacity,
                OverflowStrategy.Fail)
            .Select((tuple) => new UpdateEvent<IStreamClass>(tuple.Item1, tuple.Item2));
}

