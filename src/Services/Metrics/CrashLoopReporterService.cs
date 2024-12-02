using System.Collections.Generic;
using Akka.Actor;
using Arcane.Operator.Configurations;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Metrics.Actors;
using Microsoft.Extensions.Options;
using Snd.Sdk.Metrics.Base;

namespace Arcane.Operator.Services.Metrics;

public class CrashLoopReporterService : ICrashLoopReporterService
{
    private readonly IActorRef statusActor;
    private const string METRIC_NAME = "crash_loop";

    public CrashLoopReporterService(MetricsService metricsService, ActorSystem actorSystem,
        IOptions<CrashLoopMetricsReporterConfiguration> metricsReporterConfiguration)
    {
        this.statusActor = actorSystem.ActorOf(Props.Create(() => new CrashLoopMetricsPublisherActor(
                metricsReporterConfiguration.Value.MetricsPublisherActorConfiguration,
                metricsService)),
            nameof(CrashLoopMetricsPublisherActor));
    }

    public void AddCrashLoopEvent(string streamId, SortedDictionary<string, string> metricTags)
    {
        this.statusActor.Tell(new AddSCrashLoopMetricsMessage(streamId, METRIC_NAME, metricTags));
    }
    
    public void RemoveCrashLoopEvent(string streamId)
    {
        this.statusActor.Tell(new RemoveCrashLoopMetricsMessage(streamId));
    }
}
