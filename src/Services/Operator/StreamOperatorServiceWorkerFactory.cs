﻿using Akka.Streams;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using Arcane.Operator.Services.Metrics;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Metrics.Base;

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
    private readonly ICommandHandler<StreamingJobCommand> streamingJobCommandHandler;
    private readonly ICommandHandler<SetAnnotationCommand> setAnnotationCommandHandler;

    public StreamOperatorServiceWorkerFactory(ILoggerFactory loggerFactory,
        IMaterializer materializer,
        IMetricsReporter metricsService,
        IStreamingJobOperatorService jobOperatorService,
        IStreamDefinitionRepository streamDefinitionRepository,
        ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
        ICommandHandler<SetAnnotationCommand> setAnnotationCommandHandler,
        ICommandHandler<StreamingJobCommand> streamingJobCommandHandler)
    {
        this.loggerFactory = loggerFactory;
        this.materializer = materializer;
        this.jobOperatorService = jobOperatorService;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.metricsService = metricsService;
        this.updateStatusCommandHandler = updateStatusCommandHandler;
        this.streamingJobCommandHandler = streamingJobCommandHandler;
        this.setAnnotationCommandHandler = setAnnotationCommandHandler;
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
