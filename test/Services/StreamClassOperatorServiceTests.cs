using System;
using System.Collections.Generic;
using System.Drawing;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Operator.Configurations;
using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Models.StreamClass;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Models;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Services.Repositories;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Moq;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Metrics.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamClassOperatorServiceTests : IClassFixture<LoggerFixture>, IClassFixture<AkkaFixture>
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamClassOperatorServiceTests));
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IStreamingJobOperatorService> streamingJobOperatorServiceMock = new();
    private readonly Mock<IStreamDefinitionRepository> streamDefinitionRepositoryMock = new();

    public StreamClassOperatorServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        this.materializer = this.actorSystem.Materializer();
    }

    [Fact]
    public async Task TestStreamAdded()
    {
        // Arrange
        this.kubeClusterMock
            .Setup(m => m.StreamCustomResourceEvents<V1Beta1StreamClass>(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<OverflowStrategy>(),
                It.IsAny<TimeSpan?>()))
            .Returns(Source.Single<(WatchEventType, V1Beta1StreamClass)>((WatchEventType.Added,
                (V1Beta1StreamClass)StreamClass)));

        this.streamDefinitionRepositoryMock
            .Setup(m => m.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(
                new List<ResourceEvent<IStreamDefinition>>
                {
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition())
                }));

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(this.materializer);
        await Task.Delay(5000);

        // Assert
        this.streamingJobOperatorServiceMock.Verify(service => service.StartRegisteredStream(It.IsAny<StreamDefinition>(), It.IsAny<bool>(), It.IsAny<IStreamClass>()));
    }

    [Fact]
    public async Task TestStreamDeleted()
    {
        // Arrange
        this.kubeClusterMock
            .Setup(m => m.StreamCustomResourceEvents<V1Beta1StreamClass>(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<OverflowStrategy>(),
                It.IsAny<TimeSpan?>()))
            .Returns(Source.Single<(WatchEventType, V1Beta1StreamClass)>((WatchEventType.Deleted,
                (V1Beta1StreamClass)StreamClass)));

        this.streamDefinitionRepositoryMock
            .Setup(m => m.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(
                new List<ResourceEvent<IStreamDefinition>>
                {
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition())
                }));

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(this.materializer);
        await Task.Delay(5000);

        // Assert
        this.streamingJobOperatorServiceMock.Verify(
                service => service.StartRegisteredStream(It.IsAny<StreamDefinition>(), It.IsAny<bool>(), It.IsAny<IStreamClass>()),
                Times.Never
            );
    }

    private ServiceProvider CreateServiceProvider()
    {
        var optionsMock = new Mock<IOptionsSnapshot<CustomResourceConfiguration>>();
        optionsMock
            .Setup(m => m.Get(It.IsAny<string>()))
            .Returns(new CustomResourceConfiguration());
        var metricsReporterConfiguration = Options.Create(new MetricsReporterConfiguration
        {
            MetricsPublisherActorConfiguration = new MetricsPublisherActorConfiguration
            {
                InitialDelay = TimeSpan.FromSeconds(30),
                UpdateInterval = TimeSpan.FromSeconds(10)
            }
        });
        return new ServiceCollection()
            .AddSingleton<IMaterializer>(this.actorSystem.Materializer())
            .AddSingleton(this.actorSystem)
            .AddSingleton(this.kubeClusterMock.Object)
            .AddSingleton(this.streamingJobOperatorServiceMock.Object)
            .AddSingleton(this.streamDefinitionRepositoryMock.Object)
            .AddSingleton<IStreamClassRepository, StreamClassRepository>()
            .AddMemoryCache()
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamOperatorService>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamClassOperatorService>())
            .AddSingleton(loggerFixture.Factory)
            .AddSingleton(optionsMock.Object)
            .AddSingleton(metricsReporterConfiguration)
            .AddSingleton(Options.Create(new StreamClassOperatorServiceConfiguration
            {
                MaxBufferCapacity = 100
            }))
            .AddSingleton<IStreamClassOperatorService, StreamClassOperatorService>()
            .AddSingleton<IStreamOperatorServiceWorkerFactory, StreamOperatorServiceWorkerFactory>()
            .BuildServiceProvider();
    }
}
