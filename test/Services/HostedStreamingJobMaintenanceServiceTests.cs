using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Maintenance;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Services.Streams;
using Arcane.Operator.Tests.Fixtures;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Arcane.Operator.Tests.Services;

public class HostedStreamingJobMaintenanceServiceTests : IClassFixture<ServiceFixture>, IClassFixture<LoggerFixture>,
    IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly LoggerFixture loggerFixture;
    private readonly ServiceFixture serviceFixture;

    public HostedStreamingJobMaintenanceServiceTests(ServiceFixture serviceFixture, LoggerFixture loggerFixture,
        AkkaFixture akkaFixture)
    {
        this.serviceFixture = serviceFixture;
        this.loggerFixture = loggerFixture;
        this.akkaFixture = akkaFixture;
    }

    [Fact]
    public async Task TestHostedServiceRestart()
    {
        // Arrange
        var mockSource = Source.From(new List<(WatchEventType, StreamDefinition)>
            {
                (WatchEventType.Added,
                    new StreamDefinition()
                        { Metadata = new V1ObjectMeta { Name = nameof(this.TestHostedServiceRestart) } })
            }
        );
        this.serviceFixture
            .MockKubeCluster
            .Setup(cluster
                => cluster.StreamCustomResourceEvents<StreamDefinition>(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<int>(),
                    It.IsAny<OverflowStrategy>(), It.IsAny<TimeSpan?>()))
            .Returns(mockSource);

        this.serviceFixture.MockKubeCluster.Setup(s
                => s.ListCustomResources(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<Func<JsonElement, List<StreamDefinition>>>(),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<StreamDefinition>());

        using var service = this.CreateService();

        // Act
        await service.StartAsync(CancellationToken.None);

        await Task.Delay(2000);

        // Assert
        this.serviceFixture
            .MockKubeCluster
            .Verify(cluster
                    => cluster.StreamCustomResourceEvents<StreamDefinition>(
                        It.IsAny<string>(),
                        It.IsAny<string>(),
                        It.IsAny<string>(),
                        It.IsAny<string>(),
                        It.IsAny<int>(),
                        It.IsAny<OverflowStrategy>(), It.IsAny<TimeSpan?>()),
                Times.AtLeast(2));
    }

    private HostedStreamOperatorService<StreamDefinition> CreateService()
    {
        var optionsMock = new Mock<IOptionsSnapshot<CustomResourceConfiguration>>();
        optionsMock
            .Setup(m => m.Get(It.IsAny<string>()))
            .Returns(new CustomResourceConfiguration());
        return new ServiceCollection()
            .AddSingleton(Options.Create(new StreamOperatorServiceConfiguration
            {
                MaxBufferCapacity = 100
            }))
            .AddSingleton(optionsMock.Object)
            .AddSingleton(this.serviceFixture.MockKubeCluster.Object)
            .AddSingleton(this.serviceFixture.MockStreamInteractionService.Object)
            .AddSingleton(this.serviceFixture.MockStreamDefinitionRepository.Object)
            .AddSingleton(this.serviceFixture.MockStreamingJobOperatorService.Object)
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamOperatorService<StreamDefinition>>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<HostedStreamOperatorService<StreamDefinition>>())
            .AddSingleton<IStreamingJobMaintenanceService, StreamingJobMaintenanceService>()
            .AddSingleton(Options.Create(new StreamingJobOperatorServiceConfiguration()))
            .AddSingleton<IStreamOperatorService<StreamDefinition>, StreamOperatorService<StreamDefinition>>()
            .AddSingleton<HostedStreamOperatorService<StreamDefinition>>()
            .AddSingleton(this.akkaFixture.Materializer)
            .BuildServiceProvider()
            .GetRequiredService<HostedStreamOperatorService<StreamDefinition>>();
    }
}
