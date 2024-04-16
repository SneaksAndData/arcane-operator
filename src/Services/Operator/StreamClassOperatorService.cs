using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.ActorProviders;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamClassOperatorService"/>
public class StreamClassOperatorService : IStreamClassOperatorService
{
    private const int parallelism = 1;

    private readonly StreamClassOperatorServiceConfiguration configuration;

    private readonly Dictionary<string, StreamOperatorServiceWorker> streams = new();
    private readonly ILogger<StreamClassOperatorService> logger;
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IStreamOperatorServiceWorkerFactory streamOperatorServiceWorkerFactory;

    public StreamClassOperatorService(IOptions<StreamClassOperatorServiceConfiguration> streamOperatorServiceOptions,
        IStreamOperatorServiceWorkerFactory streamOperatorServiceWorkerFactory,
        IStreamClassRepository streamClassRepository,
        ILogger<StreamClassOperatorService> logger)
    {
        this.configuration = streamOperatorServiceOptions.Value;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
        this.streamOperatorServiceWorkerFactory = streamOperatorServiceWorkerFactory;
    }

    /// <inheritdoc cref="IStreamClassOperatorService.GetStreamClassEventsGraph"/>
    public IRunnableGraph<Task> GetStreamClassEventsGraph(CancellationToken cancellationToken)
    {
        var sink = Sink.ForEachAsync<StreamClassOperatorResponse>(parallelism, response =>
        {
            this.logger.LogInformation("The phase of the stream class {namespace}/{name} changed to {status}",
                response.StreamClass.Metadata.Namespace(),
                response.StreamClass.Metadata.Name,
                response.Phase);
            return this.streamClassRepository.InsertOrUpdate(response.StreamClass, response.Phase, response.Conditions, this.configuration.Plural);
        });

        var request = new CustomResourceApiRequest(
            this.configuration.NameSpace,
            this.configuration.ApiGroup,
            this.configuration.Version,
            this.configuration.Plural
        );

        return this.streamClassRepository.GetEvents(request, this.configuration.MaxBufferCapacity)
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamClass>>(true))
            .Select(this.OnEvent)
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .CollectOption()
            .ToMaterialized(sink, Keep.Right);
    }

    private Option<StreamClassOperatorResponse> OnEvent(ResourceEvent<IStreamClass> resourceEvent)
    {
        return resourceEvent switch
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
}
