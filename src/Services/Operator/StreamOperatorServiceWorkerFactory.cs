using Akka.Streams;
using Arcane.Operator.Configurations;
using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamOperatorServiceWorkerFactory"/>
public class StreamOperatorServiceWorkerFactory : IStreamOperatorServiceWorkerFactory
{
    private readonly ILoggerFactory loggerFactory;
    private readonly IMaterializer materializer;
    private readonly IKubeCluster kubeCluster;
    private readonly IStreamingJobOperatorService jobOperatorService;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;

    public StreamOperatorServiceWorkerFactory(ILoggerFactory loggerFactory, IMaterializer materializer,
        IKubeCluster kubeCluster, IStreamingJobOperatorService jobOperatorService,
        IStreamDefinitionRepository streamDefinitionRepository)
    {
        this.loggerFactory = loggerFactory;
        this.materializer = materializer;
        this.kubeCluster = kubeCluster;
        this.jobOperatorService = jobOperatorService;
        this.streamDefinitionRepository = streamDefinitionRepository;
    }

    /// <inheritdoc cref="IStreamOperatorServiceWorkerFactory.Create"/>
    public StreamOperatorServiceWorker Create(IStreamClass streamClass)
    {
        var streamOperatorService = new StreamOperatorService<StreamDefinition>(
            this.kubeCluster,
            streamClass,
            this.jobOperatorService,
            this.streamDefinitionRepository,
            this.loggerFactory.CreateLogger<StreamOperatorService<StreamDefinition>>()
        );
        return new StreamOperatorServiceWorker(
            this.loggerFactory.CreateLogger<StreamOperatorServiceWorker>(),
            streamOperatorService,
            this.materializer
        );
    }
}
