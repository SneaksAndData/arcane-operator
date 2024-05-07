using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using Akka.Actor;
using Arcane.Operator.Configurations;
using Arcane.Operator.Services;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Maintenance;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Services.Repositories;
using Arcane.Operator.Services.Streams;
using Azure.Data.Tables;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Kubernetes.Providers;
using Snd.Sdk.Metrics.Configurations;
using Snd.Sdk.Metrics.Providers;
using Snd.Sdk.Storage.Providers;
using Snd.Sdk.Storage.Providers.Configurations;

namespace Arcane.Operator;

[ExcludeFromCodeCoverage]
public class Startup
{
    public const string ApplicationName = $"{nameof(Arcane)}.{nameof(Operator)}";

    public Startup(IConfiguration configuration)
    {
        this.Configuration = configuration;
    }

    public IConfiguration Configuration { get; }

    public void ConfigureServices(IServiceCollection services)
    {
        // service config injections

        services.AddLocalActorSystem();

        services.AddAzureBlob(AzureStorageConfiguration.CreateDefault());
        services.AddAzureTable<TableEntity>(AzureStorageConfiguration.CreateDefault());
        services.AddDatadogMetrics(DatadogConfiguration.UnixDomainSocket(ApplicationName));
        services.AddSingleton<IMetricsReporter, MetricsReporter>();

        var config = Configuration.GetSection(nameof(StreamingJobMaintenanceServiceConfiguration));
        services.Configure<StreamingJobMaintenanceServiceConfiguration>(config);

        services.Configure<StreamingJobOperatorServiceConfiguration>(
                Configuration.GetSection(nameof(StreamingJobOperatorServiceConfiguration)));

        services.Configure<MetricsReporterConfiguration>(
                Configuration.GetSection(nameof(MetricsReporterConfiguration)));
        
        services.Configure<StreamClassOperatorServiceConfiguration>(
                Configuration.GetSection(nameof(StreamClassOperatorServiceConfiguration)));

        services.Configure<StreamingJobTemplateRepositoryConfiguration>(
                Configuration.GetSection(nameof(StreamingJobTemplateRepositoryConfiguration)));

        services.AddSingleton<IStreamingJobOperatorService, StreamingJobOperatorService>();
        services.AddSingleton<IStreamingJobMaintenanceService, StreamingJobMaintenanceService>();
        services.AddSingleton<IStreamDefinitionRepository, StreamDefinitionRepository>();
        services.AddSingleton<IStreamingJobTemplateRepository, StreamingJobTemplateRepository>();
        services.AddSingleton<IStreamClassRepository, StreamClassRepository>();
        services.AddSingleton<IStreamClassOperatorService, StreamClassOperatorService>();
        services.AddSingleton<IStreamOperatorServiceWorkerFactory, StreamOperatorServiceWorkerFactory>();
        services.AddMemoryCache();
        services.AddKubernetes();

        services.AddHealthChecks();

        services.AddControllers().AddJsonOptions(options =>
            options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter()));
    }


    public void Configure(IApplicationBuilder app, IWebHostEnvironment env,
        IHostApplicationLifetime hostApplicationLifetime)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseRouting();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();
            endpoints.MapHealthChecks("/health");
        });
    }
}
