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
    private readonly StreamClassOperatorServiceConfiguration configuration;
    private readonly IKubeCluster kubeCluster;

    private readonly Dictionary<string, BackgroundStreamOperatorService> streams = new();
    private readonly ILogger<StreamClassOperatorService> logger;
    private readonly IStreamingClassRepository streamingClassRepository;
    private readonly IStreamOperatorServiceFactory streamOperatorServiceFactory;

    public StreamClassOperatorService(IKubeCluster kubeCluster,
        IOptions<StreamClassOperatorServiceConfiguration> streamOperatorServiceOptions,
        IStreamingClassRepository streamingClassRepository,
        IStreamOperatorServiceFactory streamOperatorServiceFactory,
        ILogger<StreamClassOperatorService> logger)
    {
        this.kubeCluster = kubeCluster;
        this.configuration = streamOperatorServiceOptions.Value;
        this.logger = logger;
        this.streamingClassRepository = streamingClassRepository;
        this.streamOperatorServiceFactory = streamOperatorServiceFactory;
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

        var sink = Sink.ForEachAsync<StreamClassOperatorResponse>(this.configuration.Parallelism, this.streamingClassRepository.SetStreamingClassStatus);
        
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
            (WatchEventType.Added, var streamClass) => this.OnStartListener(streamClass),
            (WatchEventType.Modified, var streamClass) => this.OnRestartListener(streamClass),
            (WatchEventType.Deleted, var streamClass) => this.OnStopListener(streamClass),
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

    private Option<StreamClassOperatorResponse> OnStartListener(IStreamClass streamClass)
    {
        try
        {
            if (this.streams.ContainsKey(streamClass.ToStreamClassId()))
            {
                return StreamClassOperatorResponse.Ready(streamClass.Namespace(), streamClass.Kind, streamClass.Name());
            }

            var listener = this.streamOperatorServiceFactory.Create(streamClass);
            this.streams[streamClass.ToStreamClassId()] = listener;
            this.streams[streamClass.ToStreamClassId()].Start(streamClass.ToStreamClassId());
            return StreamClassOperatorResponse.Ready(streamClass.Namespace(), streamClass.Kind, streamClass.Name());
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Failed to initialize stream listener");
            return StreamClassOperatorResponse.Failed(streamClass.Namespace(), streamClass.Kind, streamClass.Name());
        }
    }

    private Option<StreamClassOperatorResponse> OnStopListener(IStreamClass streamClass)
    {
        if (!this.streams.ContainsKey(streamClass.ToStreamClassId()))
        {
            return Option<StreamClassOperatorResponse>.None;
        }

        this.streams[streamClass.ToStreamClassId()].Stop();
        return StreamClassOperatorResponse.Stopped(streamClass.Namespace(), streamClass.Kind, streamClass.Name());

    }
    
    private Option<StreamClassOperatorResponse> OnRestartListener(IStreamClass streamClass)
    {
        this.OnStopListener(streamClass);
        return this.OnStartListener(streamClass);
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
