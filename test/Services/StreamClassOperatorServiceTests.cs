using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Operator.Configurations;
using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Models.StreamClass;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestPlatform.PlatformAbstractions.Interfaces;
using Moq;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamClassOperatorServiceTests : IClassFixture<ServiceFixture>, IClassFixture<LoggerFixture>,
    IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly LoggerFixture loggerFixture;
    private readonly ServiceFixture serviceFixture;
    private readonly Mock<IStreamingJobOperatorService> streamingJobOperatorServiceMock;
    private readonly Mock<IStreamClassRepository> streamClassStateRepository;

    public StreamClassOperatorServiceTests(ServiceFixture serviceFixture, LoggerFixture loggerFixture,
        AkkaFixture akkaFixture)
    {
        this.serviceFixture = serviceFixture;
        this.loggerFixture = loggerFixture;
        this.akkaFixture = akkaFixture;
        this.streamingJobOperatorServiceMock = new Mock<IStreamingJobOperatorService>();
        this.streamClassStateRepository = new Mock<IStreamClassRepository>();
    }

    [Fact]
    public async Task TestStreamAdded()
    {
        // Arrange
        this.serviceFixture
            .MockKubeCluster
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

        this.serviceFixture
            .MockKubeCluster
            .Setup(m => m.StreamCustomResourceEvents<StreamDefinition>(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<OverflowStrategy>(),
                It.IsAny<TimeSpan?>()))
            .Returns(Source.From(
                new List<(WatchEventType, StreamDefinition)>
                {
                    (WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    (WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    (WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition())
                }));

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);
        await Task.Delay(5000);

        // Assert
        this.streamingJobOperatorServiceMock.Verify(service => service.StartRegisteredStream(It.IsAny<StreamDefinition>(), It.IsAny<bool>(), It.IsAny<IStreamClass>()));
    }

    [Fact]
    public async Task TestStreamDeleted()
    {
        // Arrange
        this.serviceFixture.MockKubeCluster.Reset();
        this.serviceFixture
            .MockKubeCluster
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

        this.serviceFixture
            .MockKubeCluster
            .Setup(m => m.StreamCustomResourceEvents<StreamDefinition>(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<OverflowStrategy>(),
                It.IsAny<TimeSpan?>()))
            .Returns(Source.From(
                new List<(WatchEventType, StreamDefinition)>
                {
                    (WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    (WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    (WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition())
                }));

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);
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
        return new ServiceCollection()
            .AddSingleton(this.akkaFixture.Materializer)
            .AddSingleton(this.serviceFixture.MockKubeCluster.Object)
            .AddSingleton(this.streamingJobOperatorServiceMock.Object)
            .AddSingleton(this.serviceFixture.MockStreamDefinitionRepository.Object)
            .AddSingleton(this.streamClassStateRepository.Object)
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamOperatorService<StreamDefinition>>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamClassOperatorService>())
            .AddSingleton(this.loggerFixture.Factory)
            .AddSingleton(optionsMock.Object)
            .AddSingleton(Options.Create(new StreamClassOperatorServiceConfiguration
            {
                MaxBufferCapacity = 100
            }))
            .AddSingleton<IStreamClassOperatorService, StreamClassOperatorService>()
            .AddSingleton<IStreamOperatorServiceWorkerFactory, StreamOperatorServiceWorkerFactory>()
            .BuildServiceProvider();
    }
}
