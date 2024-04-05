using Akka.Streams;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamOperatorServiceWorkerFactory"/>
public class StreamOperatorServiceWorkerFactory: IStreamOperatorServiceWorkerFactory
{
    private readonly ILoggerFactory loggerFactory;
    private readonly IMaterializer materializer;
    private readonly IKubeCluster kubeCluster;
    private readonly IStreamingJobOperatorService jobOperatorService;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;
    private readonly IOptionsSnapshot<CustomResourceConfiguration> customResourceConfigurationsOptionsSnapshot;

    public StreamOperatorServiceWorkerFactory(ILoggerFactory loggerFactory,IMaterializer materializer,
        IKubeCluster kubeCluster, IStreamingJobOperatorService jobOperatorService, IStreamDefinitionRepository streamDefinitionRepository,
        IOptionsSnapshot<CustomResourceConfiguration> customResourceConfigurationsOptionsSnapshot)
    {
        this.loggerFactory = loggerFactory;
        this.materializer = materializer;
        this.kubeCluster = kubeCluster;
        this.jobOperatorService = jobOperatorService;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.customResourceConfigurationsOptionsSnapshot = customResourceConfigurationsOptionsSnapshot;
    }
    
    /// <inheritdoc cref="IStreamOperatorServiceWorkerFactory.Create"/>
    public StreamOperatorServiceWorker Create(IStreamClass streamClass)
    {
        var streamOperatorService = new StreamOperatorService<StreamDefinition>(
            this.kubeCluster,
            Options.Create(streamClass.ToStreamOperatorServiceConfiguration()),
            this.customResourceConfigurationsOptionsSnapshot,
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
