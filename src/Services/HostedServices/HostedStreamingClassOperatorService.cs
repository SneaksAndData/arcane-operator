using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.HostedServices;

[ExcludeFromCodeCoverage(Justification = "Trivial")]
public class HostedStreamingClassOperatorService : BackgroundService
{
    private readonly ILogger<HostedStreamingClassOperatorService> logger;
    private readonly IMaterializer materializer;
    private readonly IStreamClassOperatorService streamClassOperatorService;

    public HostedStreamingClassOperatorService(
        ILogger<HostedStreamingClassOperatorService> logger,
        IStreamClassOperatorService streamClassOperatorService,
        IMaterializer materializer)
    {
        this.logger = logger;
        this.streamClassOperatorService = streamClassOperatorService;
        this.materializer = materializer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        this.logger.LogInformation("Activated {service}", nameof(HostedStreamingClassOperatorService));
        while (!stoppingToken.IsCancellationRequested)
        {
            this.logger.LogInformation("Started listening for stream class events");
            await this.streamClassOperatorService
                .GetStreamClassEventsGraph(stoppingToken)
                .Run(this.materializer);
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        this.logger.LogInformation("Stopping {service}", nameof(HostedStreamingClassOperatorService));
        return base.StopAsync(cancellationToken);
    }
}
