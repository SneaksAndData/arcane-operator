using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Services.Repositories.CustomResources;
using Arcane.Operator.Services.Repositories.StreamingJob;
using Azure.Data.Tables;
using k8s.Models;
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
        services.AddDatadogMetrics(DatadogConfiguration.UnixDomainSocket(AppDomain.CurrentDomain.FriendlyName));
        services.AddSingleton<IMetricsReporter, MetricsReporter>();

        this.AddServiceConfigurations(services);

        services.AddSingleton<IStreamingJobOperatorService, StreamingJobOperatorService>();

        services.AddSingleton<StreamDefinitionRepository>();
        services.AddSingleton<IResourceCollection<IStreamDefinition>>(sp => sp.GetRequiredService<StreamDefinitionRepository>());
        services.AddSingleton<IReactiveResourceCollection<IStreamDefinition>>(sp => sp.GetRequiredService<StreamDefinitionRepository>());

        services.AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>();
        services.AddSingleton<ICommandHandler<SetStreamClassStatusCommand>, UpdateStatusCommandHandler>();
        services.AddSingleton<ICommandHandler<SetAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>();
        services.AddSingleton<ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>();
        services.AddSingleton<ICommandHandler<SetAnnotationCommand<V1Job>>, AnnotationCommandHandler>();
        services.AddSingleton<IStreamingJobCommandHandler, StreamingJobCommandHandler>();
        services.AddSingleton<IStreamingJobCollection, StreamingJobRepository>();
        services.AddSingleton<IStreamOperatorService, StreamOperatorService>();

        services.AddSingleton<IStreamingJobTemplateRepository, StreamingJobTemplateRepository>();
        services.AddSingleton<IStreamClassRepository, StreamClassRepository>();
        services.AddSingleton<IStreamClassOperatorService, StreamClassOperatorService>();
        services.AddMemoryCache();
        services.AddKubernetes();

        services.AddHealthChecks();

        services.AddControllers().AddJsonOptions(options =>
            options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter()));
    }

    private IServiceCollection AddServiceConfigurations(IServiceCollection services)
    {
        return services
            .Configure<StreamingJobOperatorServiceConfiguration>(this.Configuration.GetSection(nameof(StreamingJobOperatorServiceConfiguration)))
            .Configure<MetricsReporterConfiguration>( this.Configuration.GetSection(nameof(MetricsReporterConfiguration)))
            .Configure<StreamClassOperatorServiceConfiguration>(this.Configuration.GetSection(nameof(StreamClassOperatorServiceConfiguration)))
            .Configure<StreamingJobTemplateRepositoryConfiguration>(this.Configuration.GetSection(nameof(StreamingJobTemplateRepositoryConfiguration)));
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
