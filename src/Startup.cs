using System;
using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Operators;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Filters;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Operators;
using Arcane.Operator.Services.Repositories.CustomResources;
using Arcane.Operator.Services.Repositories.StreamingJob;
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

        // Register the configuration
        services.Configure<StreamingJobOperatorServiceConfiguration>(this.Configuration.GetSection(nameof(StreamingJobOperatorServiceConfiguration)));
        services.Configure<MetricsReporterConfiguration>(
            this.Configuration.GetSection(nameof(MetricsReporterConfiguration)));
        services.Configure<StreamClassOperatorServiceConfiguration>(
            this.Configuration.GetSection(nameof(StreamClassOperatorServiceConfiguration)));
        services.Configure<StreamingJobTemplateRepositoryConfiguration>(this.Configuration.GetSection(nameof(StreamingJobTemplateRepositoryConfiguration)));


        // Register the custom resource repositories
        services.AddSingleton<StreamDefinitionRepository>();
        services.AddSingleton<IResourceCollection<IStreamDefinition>>(sp => sp.GetRequiredService<StreamDefinitionRepository>());
        services.AddSingleton<IReactiveResourceCollection<IStreamDefinition>>(sp => sp.GetRequiredService<StreamDefinitionRepository>());
        services.AddSingleton<IStreamingJobCollection, StreamingJobRepository>();
        services.AddSingleton<IStreamingJobTemplateRepository, StreamingJobTemplateRepository>();
        services.AddSingleton<IStreamClassRepository, StreamClassRepository>();

        services.AddSingleton<IEventFilter<IStreamDefinition>, DuplicateFilter<IStreamDefinition>>();

        // Register the command handlers
        services.AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>();
        services.AddSingleton<ICommandHandler<SetStreamClassStatusCommand>, UpdateStatusCommandHandler>();
        services.AddSingleton<ICommandHandler<SetAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>();
        services.AddSingleton<ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>();
        services.AddSingleton<ICommandHandler<SetAnnotationCommand<V1Job>>, AnnotationCommandHandler>();
        services.AddSingleton<ICommandHandler<StreamingJobCommand>, StreamingJobCommandHandler>();

        // Register the operator services
        services.AddSingleton<IStreamOperatorService, StreamOperatorService>();
        services.AddSingleton<IStreamingJobOperatorService, StreamingJobOperatorService>();
        services.AddSingleton<IStreamClassOperatorService, StreamClassOperatorService>();

        // Register the metrics providers
        services.AddDatadogMetrics(DatadogConfiguration.UnixDomainSocket(AppDomain.CurrentDomain.FriendlyName));
        services.AddSingleton<IMetricsReporter, MetricsReporter>();

        // Register additional services
        services.AddLocalActorSystem();
        services.AddMemoryCache();
        services.AddKubernetes();
        services.AddHealthChecks();
        services.AddControllers();
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env,
        IHostApplicationLifetime hostApplicationLifetime)
    {
        app.UseRouting();
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();
            endpoints.MapHealthChecks("/health");
        });
    }
}
