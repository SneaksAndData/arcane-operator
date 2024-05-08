using System;
using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Maintenance;
using Arcane.Operator.Services.Operator;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Snd.Sdk.Logs.Providers;
using Snd.Sdk.Logs.Providers.Configurations;

namespace Arcane.Operator;

[ExcludeFromCodeCoverage(Justification = "Service entrypoint")]
public class Program
{
    public static int Main(string[] args)
    {
        Log.Logger = DefaultLoggingProvider.CreateBootstrapLogger(AppDomain.CurrentDomain.FriendlyName);
        try
        {
            Log.Information("Starting web host");
            CreateHostBuilder(args).Build().Run();
            return 0;
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Host terminated unexpectedly");
            return 1;
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }

    public static IHostBuilder CreateHostBuilder(string[] args)
    {
        return Host.CreateDefaultBuilder(args)
            .AddSerilogLogger(AppDomain.CurrentDomain.FriendlyName, loggerConfiguration => loggerConfiguration.Default().AddDatadog())
            .ConfigureWebHostDefaults(webBuilder => { webBuilder.UseStartup<Startup>(); })
            .ConfigureServices(services =>
            {
                services.AddHostedService<HostedStreamingJobMaintenanceService>();
                services.AddHostedService<HostedStreamingClassOperatorService>();
            });
    }
}
