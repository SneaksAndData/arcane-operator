using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.Operator;

/// <summary>
/// Background service that listens for stream definition events and executes the stream operator service.
/// </summary>
public class StreamOperatorServiceWorker
{
    private readonly ILogger<StreamOperatorServiceWorker> logger;
    private readonly IMaterializer materializer;
    private readonly IStreamOperatorService streamOperatorService;
    private readonly CancellationTokenSource cts;
    private Task streamExecutionTask;
    private string streamClassId;

    public StreamOperatorServiceWorker(
        ILogger<StreamOperatorServiceWorker> logger,
        IStreamOperatorService streamOperatorService,
        IMaterializer materializer)
    {
        this.logger = logger;
        this.streamOperatorService = streamOperatorService;
        this.materializer = materializer;
        this.cts = new CancellationTokenSource();
    }

    /// <summary>
    /// Start new Stream Operator service.
    /// </summary>
    /// <param name="toStreamClassId"></param>
    public void Start(string toStreamClassId)
    {
        this.streamClassId = toStreamClassId;
        this.streamExecutionTask = this.Execute(this.cts.Token);
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
