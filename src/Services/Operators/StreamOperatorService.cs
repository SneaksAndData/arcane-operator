using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.EventFilters;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Operators;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Operators;

public class StreamOperatorService : IStreamOperatorService, IDisposable
{
    private const int parallelism = 1;
    private const int bufferSize = 1000;

    private readonly ILogger<StreamOperatorService> logger;
    private readonly IMetricsReporter metricsReporter;
    private readonly ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler;
    private readonly ICommandHandler<SetAnnotationCommand<V1Job>> setAnnotationCommandHandler;
    private readonly ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>> removeAnnotationCommandHandler;
    private readonly ICommandHandler<StreamingJobCommand> streamingJobCommandHandler;
    private readonly IMaterializer materializer;
    private readonly CancellationTokenSource cancellationTokenSource;
    private readonly IStreamingJobCollection streamingJobCollection;
    private readonly Dictionary<string, UniqueKillSwitch> killSwitches = new();
    private readonly IReactiveResourceCollection<IStreamDefinition> streamDefinitionSource;
    private readonly IEventFilter<IStreamDefinition> eventFilter;

    public StreamOperatorService(
        IMetricsReporter metricsReporter,
        ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
        ICommandHandler<SetAnnotationCommand<V1Job>> setAnnotationCommandHandler,
        ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>> removeAnnotationCommandHandler,
        ICommandHandler<StreamingJobCommand> streamingJobCommandHandler,
        ILogger<StreamOperatorService> logger,
        IMaterializer materializer,
        IReactiveResourceCollection<IStreamDefinition> streamDefinitionSource,
        IEventFilter<IStreamDefinition> eventFilter,
        IStreamingJobCollection streamingJobCollection)
    {
        this.logger = logger;
        this.metricsReporter = metricsReporter;
        this.updateStatusCommandHandler = updateStatusCommandHandler;
        this.streamingJobCommandHandler = streamingJobCommandHandler;
        this.setAnnotationCommandHandler = setAnnotationCommandHandler;
        this.removeAnnotationCommandHandler = removeAnnotationCommandHandler;
        this.materializer = materializer;
        this.cancellationTokenSource = new CancellationTokenSource();
        this.streamDefinitionSource = streamDefinitionSource;
        this.streamingJobCollection = streamingJobCollection;
        this.eventFilter = eventFilter;
    }

    public virtual void Dispose()
    {
        this.cancellationTokenSource?.Cancel();
    }

    public void Attach(IStreamClass streamClass)
    {
        var request = new CustomResourceApiRequest(
            streamClass.Namespace(),
            streamClass.ApiGroupRef,
            streamClass.VersionRef,
            streamClass.PluralNameRef
        );

        var eventsSource = this.streamDefinitionSource.GetEvents(request, streamClass.MaxBufferCapacity)
            .RecoverWithRetries(exception =>
            {
                if (exception is HttpOperationException { Response.StatusCode: System.Net.HttpStatusCode.NotFound })
                {
                    this.logger.LogWarning("The resource definition {@streamClass} not found", request);
                }

                throw exception;
            }, 1)
            .ViaMaterialized(KillSwitches.Single<ResourceEvent<IStreamDefinition>>(), Keep.Right);

        var ks = eventsSource.ToMaterialized(this.Sink.Value, Keep.Left).Run(this.materializer);
        this.killSwitches[streamClass.ToStreamClassId()] = ks;
    }

    public void Detach(IStreamClass streamClass)
    {
        if (this.killSwitches.TryGetValue(streamClass.ToStreamClassId(), out var ks))
        {
            ks.Shutdown();
            this.killSwitches.Remove(streamClass.ToStreamClassId());
        }
    }

    private Lazy<Sink<ResourceEvent<IStreamDefinition>, NotUsed>> Sink =>
        new(() => this.BuildSink(this.cancellationTokenSource.Token).Run(this.materializer));

    private IRunnableGraph<Sink<ResourceEvent<IStreamDefinition>, NotUsed>> BuildSink(CancellationToken cancellationToken)
    {
        return MergeHub.Source<ResourceEvent<IStreamDefinition>>(perProducerBufferSize: bufferSize)
            .Via(this.eventFilter.Filter())
            .CollectOption()
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamDefinition>>(true))
            .Select(this.metricsReporter.ReportTrafficMetrics)
            .SelectAsync(parallelism,
                ev => this.streamingJobCollection.Get(ev.kubernetesObject.Namespace(),
                        ev.kubernetesObject.StreamId)
                    .Map(job => (ev, job)))
            .Select(this.OnEvent)
            .SelectMany(e => e)
            .To(Akka.Streams.Dsl.Sink.ForEachAsync<KubernetesCommand>(parallelism, this.HandleCommand))
            .WithAttributes(new Attributes(new ActorAttributes.SupervisionStrategy(this.LogAndResumeDecider)));
    }

    private Decider LogAndResumeDecider => cause =>
    {
        this.logger.LogWarning(cause, "Queue element dropped due to exception in processing code.");
        return Directive.Resume;
    };

    private List<KubernetesCommand> OnEvent((ResourceEvent<IStreamDefinition>, Option<V1Job>) resourceEvent)
    {
        return resourceEvent switch
        {
            ((WatchEventType.Added, var sd), var maybeJob) => this.OnAdded(sd, maybeJob).AsList(),
            ((WatchEventType.Modified, var sd), var maybeJob) => this.OnModified(sd, maybeJob),
            _ => new List<KubernetesCommand>()
        };
    }

    private KubernetesCommand OnAdded(IStreamDefinition streamDefinition, Option<V1Job> maybeJob)
    {
        this.logger.LogInformation("Added a stream definition with id {streamId}", streamDefinition.StreamId);
        return maybeJob switch
        {
            { HasValue: true, Value: var job } when job.IsReloading() => new Reloading(streamDefinition),
            { HasValue: true, Value: var job } when !job.IsReloading() => new Running(streamDefinition),
            { HasValue: true, Value: var job } when streamDefinition.Suspended => new StopJob(job.Name(), job.Namespace()),
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
                new StartJob(streamDefinition, true),
            },
            { HasValue: false } => new StartJob(streamDefinition, false).AsList(),

            { HasValue: true, Value: var job } when streamDefinition.CrashLoopDetected => new
                List<KubernetesCommand>
                {
                    new SetCrashLoopStatusCommand(streamDefinition)
                },
            { HasValue: true, Value: var job } when streamDefinition.Suspended => new
                List<KubernetesCommand>
                {
                    new StopJob(job.Name(), job.Namespace()),
                },
            { HasValue: true, Value: var job } when !job.ConfigurationMatches(streamDefinition) => new
                List<KubernetesCommand>
                {
                    new RequestJobRestartCommand(job),
                },
            { HasValue: true, Value: var job } when streamDefinition.ReloadRequested => new
                List<KubernetesCommand>
                {
                    new RequestJobReloadCommand(job),
                },
            { HasValue: true, Value: var job } when job.ConfigurationMatches(streamDefinition) =>
                new List<KubernetesCommand>(),
            _ => new List<KubernetesCommand>()
        };
    }

    private Task HandleCommand(KubernetesCommand response) => response switch
    {
        UpdateStatusCommand sdc => this.updateStatusCommandHandler.Handle(sdc),
        StreamingJobCommand sjc => this.streamingJobCommandHandler.Handle(sjc),
        RequestJobRestartCommand rrc => this.setAnnotationCommandHandler.Handle(rrc),
        RequestJobReloadCommand rrc => this.setAnnotationCommandHandler.Handle(rrc),
        SetAnnotationCommand<V1Job> sac => this.setAnnotationCommandHandler.Handle(sac),
        _ => throw new ArgumentOutOfRangeException(nameof(response), response, null)
    };
}
