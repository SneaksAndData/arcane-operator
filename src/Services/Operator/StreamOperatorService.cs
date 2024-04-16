using System;
using System.Threading;
using System.Threading.Tasks;
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
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Operator;

public class StreamOperatorService<TStreamType> : IStreamOperatorService<TStreamType>
    where TStreamType : IStreamDefinition
{
    private const int parallelism = 1;

    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamOperatorService<TStreamType>> logger;
    private readonly IStreamingJobOperatorService operatorService;
    private readonly IStreamDefinitionRepository streamDefinitionRepository;
    private readonly IStreamClass streamClass;

    public StreamOperatorService(IKubeCluster kubeCluster,
        IStreamClass streamClass,
        IStreamingJobOperatorService operatorService,
        IStreamDefinitionRepository streamDefinitionRepository,
        ILogger<StreamOperatorService<TStreamType>> logger)
    {
        this.kubeCluster = kubeCluster;
        this.streamClass = streamClass;
        this.streamDefinitionRepository = streamDefinitionRepository;
        this.operatorService = operatorService;
        this.logger = logger;
    }

    public IRunnableGraph<Task> GetStreamDefinitionEventsGraph(CancellationToken cancellationToken)
    {
        var request = new CustomResourceApiRequest(
            this.operatorService.StreamJobNamespace,
            this.streamClass.ApiGroupRef,
            this.streamClass.VersionRef,
            this.streamClass.PluralNameRef
        );
        return this.streamDefinitionRepository.GetEvents(request, this.streamClass.MaxBufferCapacity)
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamDefinition>>(true))
            .SelectAsync(parallelism, this.OnEvent)
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .CollectOption()
            .SelectAsync(parallelism,
                response => this.streamDefinitionRepository.SetStreamStatus(response.Namespace,
                    response.Kind,
                    response.Id,
                    response.ToStatus()))
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .ToMaterialized(Sink.Ignore<Option<IStreamDefinition>>(), Keep.Right);
    }

    private Task<Option<StreamOperatorResponse>> OnEvent(ResourceEvent<IStreamDefinition> resourceEvent)
    {
        return resourceEvent switch
        {
            (WatchEventType.Added, var streamDefinition) => this.OnAdded(streamDefinition),
            (WatchEventType.Modified, var streamDefinition) => this.OnModified(streamDefinition),
            _ => Task.FromResult(Option<StreamOperatorResponse>.None)
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

    private Task<Option<StreamOperatorResponse>> OnModified(IStreamDefinition streamDefinition)
    {
        this.logger.LogInformation("Modified a stream definition with id {streamId}", streamDefinition.StreamId);
        return this.operatorService.GetStreamingJob(streamDefinition.StreamId)
            .Map(maybeJob =>
            {
                return maybeJob switch
                {
                    { HasValue: false } when streamDefinition.CrashLoopDetected
                        => Task.FromResult(StreamOperatorResponse.CrashLoopDetected(streamDefinition.Namespace(),
                                streamDefinition.Kind,
                                streamDefinition.StreamId)
                            .AsOption()),
                    { HasValue: true } when streamDefinition.CrashLoopDetected
                        => this.operatorService.DeleteJob(streamDefinition.Kind, streamDefinition.StreamId),
                    { HasValue: false } when streamDefinition.ReloadRequested
                        => this.streamDefinitionRepository
                            .RemoveReloadingAnnotation(streamDefinition.Namespace(), streamDefinition.Kind,
                                streamDefinition.StreamId)
                            .Map(sd => sd.HasValue
                                ? this.operatorService.StartRegisteredStream(sd.Value, true, this.streamClass)
                                : Task.FromResult(Option<StreamOperatorResponse>.None))
                            .Flatten(),
                    { HasValue: true } when streamDefinition.ReloadRequested
                        => this.streamDefinitionRepository
                            .RemoveReloadingAnnotation(streamDefinition.Namespace(), streamDefinition.Kind,
                                streamDefinition.StreamId)
                            .Map(sd => sd.HasValue
                                ? this.operatorService.RequestStreamingJobReload(streamDefinition.StreamId)
                                : Task.FromResult(Option<StreamOperatorResponse>.None))
                            .Flatten(),
                    { HasValue: true } when streamDefinition.Suspended
                        => this.operatorService.DeleteJob(streamDefinition.Kind, streamDefinition.StreamId),
                    { HasValue: false } when streamDefinition.Suspended
                        => Task.FromResult(StreamOperatorResponse.Suspended(streamDefinition.Namespace(),
                                streamDefinition.Kind,
                                streamDefinition.StreamId)
                            .AsOption()),
                    { Value: var job } when job.GetConfigurationChecksum() ==
                                            streamDefinition.GetConfigurationChecksum()
                        => Task.FromResult(Option<StreamOperatorResponse>.None),
                    { Value: var job } when !string.IsNullOrEmpty(job.GetConfigurationChecksum()) &&
                                            job.GetConfigurationChecksum() !=
                                            streamDefinition.GetConfigurationChecksum()
                        => this.operatorService.RequestStreamingJobRestart(streamDefinition.StreamId),
                    { HasValue: false }
                        => this.operatorService.StartRegisteredStream(streamDefinition, false, this.streamClass),
                    _ => Task.FromResult(Option<StreamOperatorResponse>.None)
                };
            }).Flatten();
    }

    private Task<Option<StreamOperatorResponse>> OnAdded(IStreamDefinition streamDefinition)
    {
        this.logger.LogInformation("Added a stream definition with id {streamId}", streamDefinition.StreamId);
        return streamDefinition.Suspended
            ? Task.FromResult(StreamOperatorResponse.Suspended(
                streamDefinition.Namespace(),
                streamDefinition.Kind,
                streamDefinition.StreamId).AsOption())
            : this.operatorService.GetStreamingJob(streamDefinition.StreamId)
                .Map(maybeJob => maybeJob switch
                {
                    { HasValue: true, Value: var job } when job.IsReloading()
                        => Task.FromResult(StreamOperatorResponse.Reloading(
                                streamDefinition.Metadata.Namespace(),
                                streamDefinition.Kind,
                                streamDefinition.StreamId)
                            .AsOption()),
                    { HasValue: true, Value: var job } when !job.IsReloading()
                        => Task.FromResult(StreamOperatorResponse.Running(
                                streamDefinition.Metadata.Namespace(),
                                streamDefinition.Kind,
                                streamDefinition.StreamId)
                            .AsOption()),
                    { HasValue: false } => this.operatorService.StartRegisteredStream(streamDefinition, true, this.streamClass)
                }).Flatten();
    }
}
