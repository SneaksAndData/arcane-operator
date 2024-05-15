using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
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
    private readonly ICommandHandler<UpdateStatusCommand> streamDefinitionCommandHandler;
    private readonly ICommandHandler<StreamingJobCommand> streamingJobCommandHandler;

    public StreamingJobMaintenanceService(
        ILogger<StreamingJobMaintenanceService> logger,
        IOptions<StreamingJobMaintenanceServiceConfiguration> options,
        IKubeCluster kubeCluster,
        IMetricsReporter metricsReporter,
        IStreamDefinitionRepository streamDefinitionRepository,
        ICommandHandler<UpdateStatusCommand> streamDefinitionCommandHandler,
        ICommandHandler<StreamingJobCommand> streamingJobCommandHandler,
        IStreamingJobOperatorService operatorService)
    {
        this.configuration = options.Value;
        this.kubeCluster = kubeCluster;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.operatorService = operatorService;
        this.logger = logger;
        this.metricsReporter = metricsReporter;
        this.streamDefinitionCommandHandler = streamDefinitionCommandHandler;
        this.streamingJobCommandHandler = streamingJobCommandHandler;
    }


    public IRunnableGraph<Task> GetJobEventsGraph(CancellationToken cancellationToken)
    {
        return this.kubeCluster
            .StreamJobEvents(this.operatorService.StreamJobNamespace, this.configuration.MaxBufferCapacity, OverflowStrategy.Fail)
            .Via(cancellationToken.AsFlow<(WatchEventType, V1Job)>(true))
            .Select(this.metricsReporter.ReportTrafficMetrics)
            .SelectAsync(parallelism, this.OnJobEvent)
            .CollectOption()
            .ToMaterialized(Sink.ForEachAsync<KubernetesCommand>(parallelism, this.HandleCommand), Keep.Right);
    }

    private Task<Option<KubernetesCommand>> OnJobEvent((WatchEventType, V1Job) valueTuple)
    {
        return valueTuple switch
        {
            (WatchEventType.Deleted, var job) => this.OnJobDelete(job),
            (WatchEventType.Modified, var job) => Task.FromResult(this.OnJobModified(job)),
            _ => Task.FromResult(Option<KubernetesCommand>.None)
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

    private Task<Option<KubernetesCommand>> OnJobDelete(V1Job job)
    {
        var isBackfilling = job.IsReloadRequested() || job.IsSchemaMismatch();
        return this.streamDefinitionRepository
            .GetStreamDefinition(job.Namespace(), job.GetStreamKind(), job.GetStreamId())
            .Map(maybeSd => maybeSd switch
            {
                ({ HasValue: true }, { HasValue: true, Value: var sd }) when job.IsFailed() => new CrashLoopDetected(sd),
                (_, { HasValue: true, Value: var sd }) when sd.Suspended => new Suspended(sd),
                (_, { HasValue: true, Value: var sd }) when sd.CrashLoopDetected => new CrashLoopDetected(sd),
                ({ HasValue: true, Value: var sc }, { HasValue: true, Value: var sd }) when !sd.Suspended => new StartJob(sd, isBackfilling),
                (_, { HasValue: false }) => Option<KubernetesCommand>.None,
                _ => throw new ArgumentOutOfRangeException(nameof(maybeSd), maybeSd, null)
            });
    }

    private Task HandleCommand(KubernetesCommand response) => response switch
    {
        UpdateStatusCommand sdc => this.streamDefinitionCommandHandler.Handle(sdc),
        StreamingJobCommand sjc => this.streamingJobCommandHandler.Handle(sjc),
        _ => throw new ArgumentOutOfRangeException(nameof(response), response, null)
    };
}
