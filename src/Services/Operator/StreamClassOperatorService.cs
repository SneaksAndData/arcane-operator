using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
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
using Directive = Akka.Streams.Supervision.Directive;

namespace Arcane.Operator.Services.Operator;

/// <inheritdoc cref="IStreamClassOperatorService"/>
public class StreamClassOperatorService : IStreamClassOperatorService
{
    private const int parallelism = 1;

    private readonly StreamClassOperatorServiceConfiguration configuration;

    private readonly ILogger<StreamClassOperatorService> logger;
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IMetricsReporter metricsService;
    private readonly IStreamOperatorService streamOperatorService;

    public StreamClassOperatorService(IOptions<StreamClassOperatorServiceConfiguration> streamOperatorServiceOptions,
        IStreamClassRepository streamClassRepository,
        IMetricsReporter metricsService,
        ILogger<StreamClassOperatorService> logger,
        IStreamOperatorService streamOperatorService)
    {
        this.configuration = streamOperatorServiceOptions.Value;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
        this.metricsService = metricsService;
        this.streamOperatorService = streamOperatorService;
    }

    /// <inheritdoc cref="IStreamClassOperatorService.GetStreamClassEventsGraph"/>
    public IRunnableGraph<Task> GetStreamClassEventsGraph(CancellationToken cancellationToken)
    {
        var request = new CustomResourceApiRequest(
            this.configuration.NameSpace,
            this.configuration.ApiGroup,
            this.configuration.Version,
            this.configuration.Plural
        );

        var sink = Sink.ForEachAsync<StreamClassOperatorResponse>(parallelism, response =>
        {
            this.logger.LogInformation("The phase of the stream class {namespace}/{name} changed to {status}",
                response.StreamClass.Metadata.Namespace(),
                response.StreamClass.Metadata.Name,
                response.Phase);
            return this.streamClassRepository.InsertOrUpdate(response.StreamClass, response.Phase, response.Conditions, this.configuration.Plural);
        });

        return this.streamClassRepository.GetEvents(request, this.configuration.MaxBufferCapacity)
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamClass>>(true))
            .Select(this.OnEvent)
            .CollectOption()
            .Select(this.metricsService.ReportStatusMetrics)
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .ToMaterialized(sink, Keep.Right);
    }

    private Option<StreamClassOperatorResponse> OnEvent(ResourceEvent<IStreamClass> resourceEvent)
    {
        return resourceEvent switch
        {
            (WatchEventType.Added, var streamClass) => this.Attach(streamClass),
            (WatchEventType.Deleted, var streamClass) => this.Detach(streamClass),
            _ => Option<StreamClassOperatorResponse>.None
        };
    }

    private StreamClassOperatorResponse Attach(IStreamClass streamClass)
    {
        this.streamOperatorService.Attach(streamClass);
        return StreamClassOperatorResponse.Ready(streamClass);
    }

    private StreamClassOperatorResponse Detach(IStreamClass streamClass)
    {
        this.streamOperatorService.Detach(streamClass);
        return StreamClassOperatorResponse.Stopped(streamClass);
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
}
