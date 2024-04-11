using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamClassOperatorService"/>
public class StreamClassOperatorService : IStreamClassOperatorService
{
    private const int parallelism = 1;
    
    private readonly StreamClassOperatorServiceConfiguration configuration;
    private readonly IKubeCluster kubeCluster;

    private readonly Dictionary<string, StreamOperatorServiceWorker> streams = new();
    private readonly ILogger<StreamClassOperatorService> logger;
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IStreamOperatorServiceWorkerFactory streamOperatorServiceWorkerFactory;

    public StreamClassOperatorService(IKubeCluster kubeCluster,
        IOptions<StreamClassOperatorServiceConfiguration> streamOperatorServiceOptions,
        IStreamOperatorServiceWorkerFactory streamOperatorServiceWorkerFactory,
        IStreamClassRepository streamClassRepository,
        ILogger<StreamClassOperatorService> logger)
    {
        this.kubeCluster = kubeCluster;
        this.configuration = streamOperatorServiceOptions.Value;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
        this.streamOperatorServiceWorkerFactory = streamOperatorServiceWorkerFactory;
    }

    /// <inheritdoc cref="IStreamClassOperatorService.GetStreamClassEventsGraph"/>
    public IRunnableGraph<Task> GetStreamClassEventsGraph(CancellationToken cancellationToken)
    {
        var synchronizationSource = this.GetStreamingJobSynchronizationGraph();
        var actualStateEventSource = this.kubeCluster.StreamCustomResourceEvents<V1Beta1StreamClass>(
            this.configuration.NameSpace,
            this.configuration.ApiGroup,
            this.configuration.Version,
            this.configuration.Plural,
            this.configuration.MaxBufferCapacity,
            OverflowStrategy.Fail);

        var sink = Sink.ForEachAsync<StreamClassOperatorResponse>(parallelism, response =>
        {
            this.logger.LogInformation("The phase of the stream class {namespace}/{name} changed to {status}",
                response.StreamClass.Metadata.Namespace(),
                response.StreamClass.Metadata.Name,
                response.Phase);
            return this.streamClassRepository.InsertOrUpdate(response.StreamClass, response.Phase, response.Conditions, this.configuration.Plural);
        });
        
        return synchronizationSource
            .Concat(actualStateEventSource)
            .Via(cancellationToken.AsFlow<(WatchEventType, V1Beta1StreamClass)>(true))
            .Select(this.OnEvent)
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .CollectOption()
            .ToMaterialized(sink, Keep.Right);
    }

    private Option<StreamClassOperatorResponse> OnEvent((WatchEventType, V1Beta1StreamClass) arg)
    {
        return arg switch
        {
            (WatchEventType.Added, var streamClass) => this.OnAdded(streamClass),
            (WatchEventType.Modified, var streamClass) => this.OnModified(streamClass),
            (WatchEventType.Deleted, var streamClass) => this.OnDeleted(streamClass),
            _ => Option<StreamClassOperatorResponse>.None
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

    private Option<StreamClassOperatorResponse> OnAdded(IStreamClass streamClass) => this.StartStreamWorker(streamClass);

    private Option<StreamClassOperatorResponse> OnDeleted(IStreamClass streamClass) => this.StopStreamWorker(streamClass);
    
    private Option<StreamClassOperatorResponse> OnModified(IStreamClass streamClass)
    {
        this.StopStreamWorker(streamClass);
        return this.StartStreamWorker(streamClass);
    }
    
    private Option<StreamClassOperatorResponse> StartStreamWorker(IStreamClass streamClass)
    {
        if (!this.streams.ContainsKey(streamClass.ToStreamClassId()))
        {
            var listener = this.streamOperatorServiceWorkerFactory.Create(streamClass);
            this.streams[streamClass.ToStreamClassId()] = listener;
            this.streams[streamClass.ToStreamClassId()].Start(streamClass.ToStreamClassId());
        }
        return StreamClassOperatorResponse.Ready(streamClass);
    }

    private Option<StreamClassOperatorResponse> StopStreamWorker(IStreamClass streamClass)
    {
        if (this.streams.ContainsKey(streamClass.ToStreamClassId()))
        {
            this.streams[streamClass.ToStreamClassId()].Stop();
        }
        return StreamClassOperatorResponse.Stopped(streamClass);
    }
    
    
    private Source<(WatchEventType, V1Beta1StreamClass), NotUsed> GetStreamingJobSynchronizationGraph()
    {
        var listTask = this.kubeCluster.ListCustomResources<V1Beta1StreamClass>(
            this.configuration.ApiGroup,
            this.configuration.Version,
            this.configuration.Plural,
            this.configuration.NameSpace);

        return Source
            .FromTask(listTask)
            .SelectMany(l => l.Select(d => (WatchEventType.Modified, d)));
    }
}
