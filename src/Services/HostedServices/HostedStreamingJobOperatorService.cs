using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.HostedServices;

public class HostedStreamingJobOperatorService : BackgroundService
{
    private readonly ILogger<HostedStreamingJobOperatorService> logger;
    private readonly IMaterializer materializer;
    private readonly IStreamingJobOperatorService streamingJobOperatorService;

    public HostedStreamingJobOperatorService(
        ILogger<HostedStreamingJobOperatorService> logger,
        IStreamingJobOperatorService streamingJobOperatorService,
        IMaterializer materializer)
    {
        this.logger = logger;
        this.streamingJobOperatorService = streamingJobOperatorService;
        this.materializer = materializer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        this.logger.LogInformation("Activated {service}", nameof(HostedStreamingJobOperatorService));
        while (!stoppingToken.IsCancellationRequested)
        {
            this.logger.LogInformation("Activated JobEventGraph");
            await this.streamingJobOperatorService
                .GetJobEventsGraph(stoppingToken)
                .Run(this.materializer);
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        this.logger.LogInformation("Stopping {service}", nameof(HostedStreamingJobOperatorService));
        return base.StopAsync(cancellationToken);
    }
}
