using Akka.Streams;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamOperatorServiceFactory"/>
public class StreamOperatorServiceFactory: IStreamOperatorServiceFactory
{
    private readonly ILoggerFactory loggerFactory;
    private readonly IMaterializer materializer;
    private readonly IKubeCluster kubeCluster;
    private readonly IStreamingJobOperatorService jobOperatorService;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;
    private readonly IOptionsSnapshot<CustomResourceConfiguration> customResourceConfigurationsOptionsSnapshot;

    public StreamOperatorServiceFactory(ILoggerFactory loggerFactory,IMaterializer materializer,
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
    
    /// <inheritdoc cref="IStreamOperatorServiceFactory.Create"/>
    public BackgroundStreamOperatorService Create(IStreamClass streamClass)
    {
        var streamOperatorService = new StreamOperatorService<StreamDefinition>(
            this.kubeCluster,
            Options.Create(streamClass.ToStreamOperatorServiceConfiguration()),
            this.customResourceConfigurationsOptionsSnapshot,
            this.jobOperatorService, 
            this.streamDefinitionRepository,
            this.loggerFactory.CreateLogger<StreamOperatorService<StreamDefinition>>()
        );
        return new BackgroundStreamOperatorService(
            this.loggerFactory.CreateLogger<BackgroundStreamOperatorService>(),
            streamOperatorService,
            this.materializer
        );
    }
}
