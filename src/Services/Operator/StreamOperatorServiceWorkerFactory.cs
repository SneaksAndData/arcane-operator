using Akka.Streams;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Commands;
using k8s.Models;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamOperatorServiceWorkerFactory"/>
public class StreamOperatorServiceWorkerFactory : IStreamOperatorServiceWorkerFactory
{
    private readonly ILoggerFactory loggerFactory;
    private readonly IMaterializer materializer;
    private readonly IStreamingJobOperatorService jobOperatorService;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;
    private readonly IMetricsReporter metricsService;
    private readonly ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler;
    private readonly IStreamingJobCommandHandler streamingJobCommandHandler;
    private readonly ICommandHandler<SetAnnotationCommand<V1Job>> setAnnotationCommandHandler;
    private readonly ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>> removeAnnotationCommandHandler;

    public StreamOperatorServiceWorkerFactory(ILoggerFactory loggerFactory,
        IMaterializer materializer,
        IMetricsReporter metricsService,
        IStreamingJobOperatorService jobOperatorService,
        IStreamDefinitionRepository streamDefinitionRepository,
        ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
        ICommandHandler<SetAnnotationCommand<V1Job>> setAnnotationCommandHandler,
        ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>> removeAnnotationCommandHandler,
        IStreamingJobCommandHandler streamingJobCommandHandler)
    {
        this.loggerFactory = loggerFactory;
        this.materializer = materializer;
        this.jobOperatorService = jobOperatorService;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.metricsService = metricsService;
        this.updateStatusCommandHandler = updateStatusCommandHandler;
        this.streamingJobCommandHandler = streamingJobCommandHandler;
        this.setAnnotationCommandHandler = setAnnotationCommandHandler;
        this.removeAnnotationCommandHandler = removeAnnotationCommandHandler;
    }

    /// <inheritdoc cref="IStreamOperatorServiceWorkerFactory.Create"/>
    public StreamOperatorServiceWorker Create(IStreamClass streamClass)
    {
        var streamOperatorService = new StreamOperatorService(
            streamClass,
            this.jobOperatorService,
            this.streamDefinitionRepository,
            this.metricsService,
            this.updateStatusCommandHandler,
            this.setAnnotationCommandHandler,
            this.removeAnnotationCommandHandler,
            this.streamingJobCommandHandler,
            this.loggerFactory.CreateLogger<StreamOperatorService>()
        );
        return new StreamOperatorServiceWorker(
            this.loggerFactory.CreateLogger<StreamOperatorServiceWorker>(),
            streamOperatorService,
            this.materializer
        );
    }
}
