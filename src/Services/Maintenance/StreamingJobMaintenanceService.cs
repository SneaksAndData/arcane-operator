using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
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

    public StreamingJobMaintenanceService(
        ILogger<StreamingJobMaintenanceService> logger,
        IOptions<StreamingJobMaintenanceServiceConfiguration> options,
        IKubeCluster kubeCluster,
        IStreamDefinitionRepository streamDefinitionRepository,
        IStreamingJobOperatorService operatorService)
    {
        this.configuration = options.Value;
        this.kubeCluster = kubeCluster;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.operatorService = operatorService;
        this.logger = logger;
    }


    public IRunnableGraph<Task> GetJobEventsGraph(CancellationToken cancellationToken)
    {
        return this.kubeCluster
            .StreamJobEvents(this.operatorService.StreamJobNamespace, this.configuration.MaxBufferCapacity,
                OverflowStrategy.Fail)
            .Via(cancellationToken.AsFlow<(WatchEventType, V1Job)>(true))
            .SelectAsync(parallelism, this.OnJobEvent)
            .CollectOption()
            .SelectAsync(parallelism, this.HandleStreamOperatorResponse)
            .ToMaterialized(Sink.Ignore<Option<IStreamDefinition>>(), Keep.Right);
    }

    private Task<Option<StreamOperatorResponse>> OnJobEvent((WatchEventType, V1Job) valueTuple)
    {
        return valueTuple switch
        {
            (WatchEventType.Deleted, var job) => this.OnJobDelete(job),
            (WatchEventType.Added, var job) => this.OnJobAdded(job),
            (WatchEventType.Modified, var job) => this.OnJobModified(job),
            _ => Task.FromResult(Option<StreamOperatorResponse>.None)
        };
    }

    private Task<Option<StreamOperatorResponse>> OnJobModified(V1Job job)
    {
        var streamId = job.GetStreamId();
        if (job.IsStopping())
        {
            this.logger.LogInformation("Streaming job for stream with id {streamId} is already stopping",
                streamId);
            return Task.FromResult(Option<StreamOperatorResponse>.None);
        }

        if (job.IsReloadRequested() || job.IsRestartRequested())
        {
            return this.operatorService.DeleteJob(job.GetStreamKind(), streamId);
        }

        return Task.FromResult(Option<StreamOperatorResponse>.None);
    }

    private Task<Option<StreamOperatorResponse>> OnJobAdded(V1Job job)
    {
        var streamId = job.GetStreamId();
        if (job.IsReloading())
        {
            return Task.FromResult(StreamOperatorResponse.Reloading(job.Namespace(), job.GetStreamKind(), streamId)
                .AsOption());
        }

        if (job.IsRunning())
        {
            return Task.FromResult(StreamOperatorResponse.Running(job.Namespace(), job.GetStreamKind(), streamId)
                .AsOption());
        }

        this.logger.LogError(
            "{handler} handler triggered for the streaming job {streamId}, but the job is not in a running state",
            nameof(this.OnJobAdded), streamId);
        return Task.FromResult(Option<StreamOperatorResponse>.None);
    }

    private Task<Option<StreamOperatorResponse>> OnJobDelete(V1Job job)
    {
        var isBackfilling = job.IsReloadRequested() || job.IsSchemaMismatch();
        return this.streamDefinitionRepository
            .GetStreamDefinition(job.Namespace(), job.GetStreamKind(), job.GetStreamId())
            .Map(maybeSd => maybeSd switch
            {
                ({ HasValue: true }, { HasValue: true, Value: var sd }) when job.IsFailed()
                    => this.streamDefinitionRepository
                        .SetCrashLoopAnnotation(sd.Namespace(), sd.Kind, sd.StreamId)
                        .Map(maybeUpdatedSd => maybeUpdatedSd.HasValue
                            ? StreamOperatorResponse.CrashLoopDetected(maybeUpdatedSd.Value.Namespace(),
                                    maybeUpdatedSd.Value.Kind,
                                    maybeUpdatedSd.Value.StreamId)
                                .AsOption()
                            : Option<StreamOperatorResponse>.None),
                (_, { HasValue: true, Value: var sd }) when sd.Suspended
                    => Task.FromResult(
                        StreamOperatorResponse.Suspended(sd.Namespace(), sd.Kind, sd.StreamId).AsOption()),
                (_, { HasValue: true, Value: var sd }) when sd.CrashLoopDetected
                    => Task.FromResult(StreamOperatorResponse.CrashLoopDetected(sd.Namespace(), sd.Kind, sd.StreamId)
                        .AsOption()),
                ({ HasValue: true, Value: var sc }, { HasValue: true, Value: var sd }) when !sd.Suspended
                    => this.operatorService.StartRegisteredStream(sd, isBackfilling, sc),
                (_, { HasValue: false })
                    => Task.FromResult(Option<StreamOperatorResponse>.None),
                _ => throw new ArgumentOutOfRangeException(nameof(maybeSd), maybeSd, null)
            }).Flatten();
    }

    private Task<Option<IStreamDefinition>> HandleStreamOperatorResponse(StreamOperatorResponse response)
    {
        return this.streamDefinitionRepository.SetStreamStatus(response.Namespace,
            response.Kind,
            response.Id,
            response.ToStatus());
    }
}
