using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Operator;

public class StreamOperatorService : IStreamOperatorService
{
    private const int parallelism = 1;

    private readonly ILogger<StreamOperatorService> logger;
    private readonly IStreamingJobOperatorService operatorService;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;
    private readonly IStreamClass streamClass;
    private readonly IMetricsReporter metricsReporter;
    private readonly ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler;
    private readonly ICommandHandler<SetAnnotationCommand> setAnnotationCommandHandler;
    private readonly ICommandHandler<StreamingJobCommand> streamingJobCommandHandler;

    public StreamOperatorService(IStreamClass streamClass,
        IStreamingJobOperatorService operatorService,
        IStreamDefinitionRepository streamDefinitionRepository,
        IMetricsReporter metricsReporter,
        ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
        ICommandHandler<SetAnnotationCommand> setAnnotationCommandHandler,
        ICommandHandler<StreamingJobCommand> streamingJobCommandHandler,
        ILogger<StreamOperatorService> logger)
    {
        this.streamClass = streamClass;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.operatorService = operatorService;
        this.logger = logger;
        this.metricsReporter = metricsReporter;
        this.updateStatusCommandHandler = updateStatusCommandHandler;
        this.streamingJobCommandHandler = streamingJobCommandHandler;
        this.setAnnotationCommandHandler = setAnnotationCommandHandler;
    }

    public IRunnableGraph<Task> GetStreamDefinitionEventsGraph(CancellationToken cancellationToken)
    {
        var request = new CustomResourceApiRequest(
            this.operatorService.StreamJobNamespace,
            this.streamClass.ApiGroupRef,
            this.streamClass.VersionRef,
            this.streamClass.PluralNameRef
        );
        this.logger.LogInformation("Start listening to event stream for {@streamClass}", request);

        var restartSettings = RestartSettings.Create(
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMinutes(3),
            0.2);

        var eventsSource = this.streamDefinitionRepository.GetEvents(request, this.streamClass.MaxBufferCapacity)
            .RecoverWithRetries(exception =>
            {
                if (exception is HttpOperationException { Response.StatusCode: System.Net.HttpStatusCode.NotFound })
                {
                    this.logger.LogWarning("The resource definition {@streamClass} not found", request);
                }

                throw exception;
            }, 1);

        return RestartSource.OnFailuresWithBackoff(() => eventsSource, restartSettings)
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamDefinition>>(true))
            .Select(this.metricsReporter.ReportTrafficMetrics)
            .SelectAsync(parallelism, ev => this.operatorService.GetStreamingJob(ev.kubernetesObject.StreamId).Map(job => (ev, job)))
            .Select(this.OnEvent)
            .SelectMany(e => e)
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .ToMaterialized(Sink.ForEachAsync<KubernetesCommand>(parallelism, this.HandleCommand), Keep.Right);
    }

    private List<KubernetesCommand> OnEvent((ResourceEvent<IStreamDefinition>, Option<V1Job>) resourceEvent)
    {
        return resourceEvent switch
        {
            ((WatchEventType.Added, var sd), var maybeJob) => this.OnAdded(sd, maybeJob).AsList(),
            ((WatchEventType.Modified, var sd), var maybeJob) => this.OnModified(sd, maybeJob),
            _ => new List<KubernetesCommand>()
        };
    }

    private Directive HandleError(Exception exception)
    {
        this.logger.LogError(exception, "Failed to handle stream definition event");
        return exception switch
        {
            BufferOverflowException => Directive.Stop,
            _ => Directive.Resume
        };
    }

    private KubernetesCommand OnAdded(IStreamDefinition streamDefinition, Option<V1Job> maybeJob)
    {
        this.logger.LogInformation("Added a stream definition with id {streamId}", streamDefinition.StreamId);
        return maybeJob switch
        {
            { HasValue: true, Value: var job } when job.IsReloading() => new Reloading(streamDefinition),
            { HasValue: true, Value: var job } when !job.IsReloading() => new Running(streamDefinition),
            { HasValue: true, Value: var job } when streamDefinition.Suspended => new StopJob(job.GetStreamId(),job.GetStreamKind()),
            { HasValue: false } when streamDefinition.Suspended => new Suspended(streamDefinition),
            { HasValue: false } when !streamDefinition.Suspended => new StartJob(streamDefinition, true),
            _ => throw new ArgumentOutOfRangeException(nameof(maybeJob), maybeJob, null)
        };
    }

    private List<KubernetesCommand> OnModified(IStreamDefinition streamDefinition, Option<V1Job> maybeJob)
    {
        this.logger.LogInformation("Modified a stream definition with id {streamId}", streamDefinition.StreamId);
        return maybeJob switch
        {
            { HasValue: false } when streamDefinition.CrashLoopDetected => new SetCrashLoopStatusCommand(streamDefinition).AsList(),
            { HasValue: false } when streamDefinition.Suspended => new Suspended(streamDefinition).AsList(),
            { HasValue: false } when streamDefinition.ReloadRequested => new List<KubernetesCommand>
            {
                new RemoveReloadRequestedAnnotation(streamDefinition),
                new StartJob(streamDefinition, true),
                new Reloading(streamDefinition)
            },
            { HasValue: false } => new StartJob(streamDefinition, false).AsList(),
            
            { HasValue: true, Value: var job } when streamDefinition.CrashLoopDetected => new StopJob(job.GetStreamId(), job.GetStreamKind()).AsList(),
            { HasValue: true, Value: var job } when streamDefinition.Suspended => new StopJob(job.GetStreamId(), job.GetStreamKind()).AsList(),
            { HasValue: true, Value: var job } when job.ConfigurationMatches(streamDefinition) => new List<KubernetesCommand>(),
            { HasValue: true, Value: var job } when !job.ConfigurationMatches(streamDefinition) => new
                List<KubernetesCommand>
                {
                    new StopJob(job.GetStreamId(), job.GetStreamKind()),
                    new StartJob(streamDefinition, false)
                },
            { HasValue: true, Value: var job } when streamDefinition.ReloadRequested => new
                List<KubernetesCommand>
                {
                    new RemoveReloadRequestedAnnotation(streamDefinition),
                    new StartJob(streamDefinition, true),
                    new StopJob(job.GetStreamId(), job.GetStreamKind()),
                    new Reloading(streamDefinition)
                },
            _ => new List<KubernetesCommand>()
        };
    }
    
    private Task HandleCommand(KubernetesCommand response) => response switch
    {
        UpdateStatusCommand sdc => this.updateStatusCommandHandler.Handle(sdc),
        StreamingJobCommand sjc => this.streamingJobCommandHandler.Handle(sjc),
        SetAnnotationCommand sac => this.setAnnotationCommandHandler.Handle(sac),
        _ => throw new ArgumentOutOfRangeException(nameof(response), response, null)
    };
}
