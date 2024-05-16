using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Kubernetes;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Maintenance;

public class StreamingJobMaintenanceService : IStreamingJobMaintenanceService
{
    private const int parallelism = 1;
    private readonly StreamingJobMaintenanceServiceConfiguration configuration;
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobMaintenanceService> logger;
    private readonly IStreamingJobOperatorService operatorService;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;
    private readonly IMetricsReporter metricsReporter;
    private readonly ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler;
    private readonly ICommandHandler<SetAnnotationCommand<IStreamDefinition>> setAnnotationCommandHandler;
    private readonly ICommandHandler<StreamingJobCommand> streamingJobCommandHandler;

    public StreamingJobMaintenanceService(
        ILogger<StreamingJobMaintenanceService> logger,
        IOptions<StreamingJobMaintenanceServiceConfiguration> options,
        IKubeCluster kubeCluster,
        IMetricsReporter metricsReporter,
        IStreamDefinitionRepository streamDefinitionRepository,
        ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
        ICommandHandler<SetAnnotationCommand<IStreamDefinition>> setAnnotationCommandHandler,
        ICommandHandler<StreamingJobCommand> streamingJobCommandHandler,
        IStreamingJobOperatorService operatorService)
    {
        this.configuration = options.Value;
        this.kubeCluster = kubeCluster;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.operatorService = operatorService;
        this.logger = logger;
        this.metricsReporter = metricsReporter;
        this.updateStatusCommandHandler = updateStatusCommandHandler;
        this.streamingJobCommandHandler = streamingJobCommandHandler;
        this.setAnnotationCommandHandler = setAnnotationCommandHandler;
    }


    public IRunnableGraph<Task> GetJobEventsGraph(CancellationToken cancellationToken)
    {
        return this.kubeCluster
            .StreamJobEvents(this.operatorService.StreamJobNamespace, this.configuration.MaxBufferCapacity, OverflowStrategy.Fail)
            .Via(cancellationToken.AsFlow<(WatchEventType, V1Job)>(true))
            .Select(this.metricsReporter.ReportTrafficMetrics)
            .SelectAsync(parallelism, this.OnJobEvent)
            .SelectMany(e => e)
            .CollectOption()
            .ToMaterialized(Sink.ForEachAsync<KubernetesCommand>(parallelism, this.HandleCommand), Keep.Right);
    }

    private Task<List<Option<KubernetesCommand>>> OnJobEvent((WatchEventType, V1Job) valueTuple)
    {
        return valueTuple switch
        {
            (WatchEventType.Deleted, var job) => this.OnJobDelete(job),
            (WatchEventType.Modified, var job) => Task.FromResult(new List<Option<KubernetesCommand>> { this.OnJobModified(job) }),
            _ => Task.FromResult(new List<Option<KubernetesCommand>>())
        };
    }

    private Option<KubernetesCommand> OnJobModified(V1Job job)
    {
        var streamId = job.GetStreamId();
        if (job.IsStopping())
        {
            this.logger.LogInformation("Streaming job for stream with id {streamId} is already stopping",
                streamId);
            return Option<KubernetesCommand>.None;
        }

        if (job.IsReloadRequested() || job.IsRestartRequested())
        {
            return new StopJob(job.GetStreamKind(), streamId);
        }

        return Option<KubernetesCommand>.None;
    }

    private Task<List<Option<KubernetesCommand>>> OnJobDelete(V1Job job)
    {
        return this.streamDefinitionRepository
            .GetStreamDefinition(job.Namespace(), job.GetStreamKind(), job.GetStreamId())
            .Map(maybeSd => maybeSd switch
            {
                { HasValue: true, Value: var sd } when job.IsFailed() => new List<Option<KubernetesCommand>>
                {
                    new SetCrashLoopStatusCommand(sd),
                    new SetCrashLoopStatusAnnotationCommand(sd)
                },
                { HasValue: true, Value: var sd } when sd.Suspended => new List<Option<KubernetesCommand>>
                {
                    new Suspended(sd)
                },
                { HasValue: true, Value: var sd } when sd.CrashLoopDetected => new List<Option<KubernetesCommand>>
                {
                    new SetCrashLoopStatusCommand(sd)
                },
                { HasValue: true, Value: var sd } when !sd.Suspended => new List<Option<KubernetesCommand>>
                {
                    new StartJob(sd, job.IsReloadRequested() || job.IsSchemaMismatch())
                },
                { HasValue: false } => new List<Option<KubernetesCommand>>(),
                _ => throw new ArgumentOutOfRangeException(nameof(maybeSd), maybeSd, null)
            });
    }

    private Task HandleCommand(KubernetesCommand response) => response switch
    {
        UpdateStatusCommand sdc => this.updateStatusCommandHandler.Handle(sdc),
        StreamingJobCommand sjc => this.streamingJobCommandHandler.Handle(sjc),
        SetAnnotationCommand<IStreamDefinition> sac => this.setAnnotationCommandHandler.Handle(sac),
        _ => throw new ArgumentOutOfRangeException(nameof(response), response, null)
    };
}
