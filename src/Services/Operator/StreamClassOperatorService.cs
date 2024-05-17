using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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

        var source = this.streamClassRepository.GetEvents(request, this.configuration.MaxBufferCapacity)
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamClass>>(true))
            .Select(this.metricsService.ReportTrafficMetrics);
        var sink = Sink.ForEach<ResourceEvent<IStreamClass>>(re => this.streamOperatorService.Attach(re.kubernetesObject));

        return source.ToMaterialized(sink, Keep.Right);
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
