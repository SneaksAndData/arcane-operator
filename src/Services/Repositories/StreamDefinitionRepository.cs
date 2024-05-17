using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services.Repositories;

public class StreamDefinitionRepository : IReactiveResourceCollection<IStreamDefinition>, IResourceCollection<IStreamDefinition>
{
    private readonly IKubeCluster kubeCluster;

    public StreamDefinitionRepository(IKubeCluster kubeCluster)
    {
        this.kubeCluster = kubeCluster;
    }

    /// <inheritdoc cref="IReactiveResourceCollection{TResourceType}.GetEvents"/>
    public Source<ResourceEvent<IStreamDefinition>, NotUsed> GetEvents(CustomResourceApiRequest request,
        int maxBufferCapacity) =>
        this.kubeCluster.StreamCustomResourceEvents<StreamDefinition>(
                request.Namespace,
                request.ApiGroup,
                request.ApiVersion,
                request.PluralName,
                maxBufferCapacity,
                OverflowStrategy.Fail)
            .Select(tuple => new ResourceEvent<IStreamDefinition>(tuple.Item1, tuple.Item2));

    /// <inheritdoc cref="IResourceCollection{TResourceType}.Get"/>
    public Task<Option<IStreamDefinition>> Get(string name, CustomResourceApiRequest request)
    {
        return this.kubeCluster.GetCustomResource(request.ApiGroup,
            request.ApiVersion,
            request.PluralName,
            request.Namespace,
            name,
    element => element.AsOptionalStreamDefinition());
    }
}
