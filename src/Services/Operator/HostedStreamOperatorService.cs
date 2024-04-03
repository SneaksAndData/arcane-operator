using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.Operator;

public class HostedStreamOperatorService<TStreamType> : BackgroundService
{
    private readonly ILogger<HostedStreamOperatorService<TStreamType>> logger;
    private readonly IMaterializer materializer;
    private readonly IStreamOperatorService<TStreamType> streamOperatorService;

    public HostedStreamOperatorService(
        ILogger<HostedStreamOperatorService<TStreamType>> logger,
        IStreamOperatorService<TStreamType> streamOperatorService,
        IMaterializer materializer)
    {
        this.logger = logger;
        this.streamOperatorService = streamOperatorService;
        this.materializer = materializer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        this.logger.LogInformation("Activated {service}", nameof(HostedStreamOperatorService<TStreamType>));
        while (!stoppingToken.IsCancellationRequested)
        {
            this.logger.LogInformation("Start listening for {crdName} events", typeof(TStreamType).Name);
            await this.streamOperatorService
                .GetStreamDefinitionEventsGraph(stoppingToken)
                .Run(this.materializer);
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        this.logger.LogInformation("Stopping {service}", nameof(HostedStreamOperatorService<TStreamType>));
        return base.StopAsync(cancellationToken);
    }
}
