﻿using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.Operator;

/// <summary>
/// Background service that listens for stream definition events and executes the stream operator service.
/// </summary>
public class BackgroundStreamOperatorService
{
    private readonly ILogger<BackgroundStreamOperatorService> logger;
    private readonly IMaterializer materializer;
    private readonly IStreamOperatorService<StreamDefinition> streamOperatorService;
    private readonly CancellationTokenSource cts;
    private Task streamExecutionTask;
    private string streamClassId;

    public BackgroundStreamOperatorService(
        ILogger<BackgroundStreamOperatorService> logger,
        IStreamOperatorService<StreamDefinition> streamOperatorService,
        IMaterializer materializer, CancellationTokenSource cts)
    {
        this.logger = logger;
        this.streamOperatorService = streamOperatorService;
        this.materializer = materializer;
        this.cts = cts;
    }

    /// <summary>
    /// Start new Stream Operator service.
    /// </summary>
    /// <param name="toStreamClassId"></param>
    public void Start(string toStreamClassId)
    {
        this.streamExecutionTask = this.Execute(this.cts.Token);
        this.streamClassId = toStreamClassId;
    }
    
    /// <summary>
    /// Stop the Stream Operator service and wait for it to finish.
    /// </summary>
    public void Stop()
    {
        this.logger.LogInformation("Stopping {service}", this.streamClassId);
        this.cts.Cancel();
        this.streamExecutionTask.GetAwaiter().GetResult();
    }
    
    private async Task Execute(CancellationToken stoppingToken)
    {
        this.logger.LogInformation("Activated {service}", this.streamClassId);
        while (!stoppingToken.IsCancellationRequested)
        {
            this.logger.LogInformation("Start listening for {streamClassId} events", this.streamClassId);
            var task = this.streamOperatorService
                .GetStreamDefinitionEventsGraph(stoppingToken)
                .Run(this.materializer);
            await task;
        }
    }

}
