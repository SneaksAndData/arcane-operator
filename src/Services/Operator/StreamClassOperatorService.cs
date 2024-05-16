using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Actors;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Tasks;
using Directive = Akka.Streams.Supervision.Directive;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamClassOperatorService"/>
public class StreamClassOperatorService : IStreamClassOperatorService
{
    private const int parallelism = 1;

    private readonly StreamClassOperatorServiceConfiguration configuration;

    private readonly Dictionary<string, StreamOperatorServiceWorker> streams = new();
    private readonly ILogger<StreamClassOperatorService> logger;
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IMetricsReporter metricsService;
    private readonly ActorSystem actorSystem;
    private readonly IServiceProvider sp;

    public StreamClassOperatorService(IOptions<StreamClassOperatorServiceConfiguration> streamOperatorServiceOptions,
        IStreamClassRepository streamClassRepository,
        IMetricsReporter metricsService,
        ILogger<StreamClassOperatorService> logger,
        IServiceProvider sp,
        ActorSystem actorSystem)
    {
        this.configuration = streamOperatorServiceOptions.Value;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
        this.sp = sp;
        this.metricsService = metricsService;
        this.actorSystem = actorSystem;
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
            .Select(this.metricsService.ReportTrafficMetrics)
            .Select(OnEvent)
            .CollectOption()
            .SelectAsync(1, msg => this.GetOrCreate(msg.streamClass).FlatMap(actor => actor.Ask<StreamClassOperatorResponse>(msg)))
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .Select(this.metricsService.ReportStatusMetrics)
            .ToMaterialized(sink, Keep.Right);
    }

    private static Option<StreamClassManagementMessage> OnEvent(ResourceEvent<IStreamClass> resourceEvent)
    {
        return resourceEvent switch
        {
            (WatchEventType.Added, var streamClass) => new StartListeningMessage(streamClass),
            (WatchEventType.Deleted, var streamClass) => new StopListeningMessage(streamClass),
            _ => Option<StreamClassManagementMessage>.None
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

    private Task<IActorRef> GetOrCreate(IStreamClass streamClass) => this.actorSystem
        .ActorSelection(streamClass.ToStreamClassId()).ResolveOne(TimeSpan.FromSeconds(10))
        .TryMap(success => success, exception => exception switch
        {
            ActorNotFoundException => this.actorSystem.ActorOf(Props.Create(() => new StreamClassActor(this.sp.GetRequiredService<IStreamOperatorService>())), streamClass.ToStreamClassId()),
            _ => throw exception
        });
}
