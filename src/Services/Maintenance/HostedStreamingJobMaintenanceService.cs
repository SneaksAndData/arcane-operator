using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Arcane.Operator.Services.Base;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Arcane.Operator.Services.Maintenance;

public class HostedStreamingJobMaintenanceService : BackgroundService
{
    private readonly ILogger<HostedStreamingJobMaintenanceService> logger;
    private readonly IMaterializer materializer;
    private readonly IStreamingJobMaintenanceService streamingJobMaintenanceService;

    public HostedStreamingJobMaintenanceService(
        ILogger<HostedStreamingJobMaintenanceService> logger,
        IStreamingJobMaintenanceService streamingJobMaintenanceService,
        IMaterializer materializer)
    {
        this.logger = logger;
        this.streamingJobMaintenanceService = streamingJobMaintenanceService;
        this.materializer = materializer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        this.logger.LogInformation("Activated {service}", nameof(HostedStreamingJobMaintenanceService));
        while (!stoppingToken.IsCancellationRequested)
        {
            this.logger.LogInformation("Activated JobEventGraph");
            await this.streamingJobMaintenanceService
                .GetJobEventsGraph(stoppingToken)
                .Run(this.materializer);
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        this.logger.LogInformation("Stopping {service}", nameof(HostedStreamingJobMaintenanceService));
        return base.StopAsync(cancellationToken);
    }
}
