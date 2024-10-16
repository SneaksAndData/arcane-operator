﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Supervision;
using Akka.Util;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Operators;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.ActorProviders;

namespace Arcane.Operator.Services.Operators;

/// <inheritdoc cref="IStreamClassOperatorService"/>
public class StreamClassOperatorService : IStreamClassOperatorService
{
    private const int PARALLELISM = 1;

    private readonly StreamClassOperatorServiceConfiguration configuration;

    private readonly ILogger<StreamClassOperatorService> logger;
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IMetricsReporter metricsService;
    private readonly IStreamOperatorService streamOperatorService;
    private readonly CustomResourceApiRequest request;
    private readonly ICommandHandler<SetStreamClassStatusCommand> streamClassStatusCommandHandler;

    public StreamClassOperatorService(IOptions<StreamClassOperatorServiceConfiguration> streamOperatorServiceOptions,
        IStreamClassRepository streamClassRepository,
        IMetricsReporter metricsService,
        ILogger<StreamClassOperatorService> logger,
        ICommandHandler<SetStreamClassStatusCommand> streamClassStatusCommandHandler,
        IStreamOperatorService streamOperatorService)
    {
        this.configuration = streamOperatorServiceOptions.Value;
        this.logger = logger;
        this.streamClassRepository = streamClassRepository;
        this.metricsService = metricsService;
        this.streamOperatorService = streamOperatorService;
        this.streamClassStatusCommandHandler = streamClassStatusCommandHandler;
        this.request = new CustomResourceApiRequest(
            this.configuration.NameSpace,
            this.configuration.ApiGroup,
            this.configuration.Version,
            this.configuration.Plural
        );

    }

    /// <inheritdoc cref="IStreamClassOperatorService.GetStreamClassEventsGraph"/>
    public IRunnableGraph<Task> GetStreamClassEventsGraph(CancellationToken cancellationToken)
    {
        var sink = Sink.ForEachAsync<SetStreamClassStatusCommand>(PARALLELISM, command =>
        {
            this.streamClassStatusCommandHandler.Handle(command);
            return this.streamClassRepository.InsertOrUpdate(command.streamClass, command.phase, command.conditions, command.request.PluralName);
        });

        return this.streamClassRepository.GetEvents(this.request, this.configuration.MaxBufferCapacity)
            .Via(cancellationToken.AsFlow<ResourceEvent<IStreamClass>>(true))
            .Select(this.OnEvent)
            .CollectOption()
            .Select(streamClass => this.metricsService.ReportStatusMetrics(streamClass))
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(this.HandleError))
            .ToMaterialized(sink, Keep.Right);
    }

    private Option<SetStreamClassStatusCommand> OnEvent(ResourceEvent<IStreamClass> resourceEvent)
    {
        return resourceEvent switch
        {
            (WatchEventType.Added, var streamClass) => this.Attach(streamClass),
            (WatchEventType.Deleted, var streamClass) => this.Detach(streamClass),
            _ => Option<SetStreamClassStatusCommand>.None
        };
    }

    private SetStreamClassReady Attach(IStreamClass streamClass)
    {
        this.streamOperatorService.Attach(streamClass);
        return new SetStreamClassReady(streamClass.Name(), this.request, streamClass);
    }

    private Option<SetStreamClassStatusCommand> Detach(IStreamClass streamClass)
    {
        this.streamOperatorService.Detach(streamClass);
        return Option<SetStreamClassStatusCommand>.None;
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
