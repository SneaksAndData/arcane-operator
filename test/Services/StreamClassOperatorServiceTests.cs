using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Models;
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.StreamClass.V1Beta1;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Services.Repositories.CustomResources;
using Arcane.Operator.Services.Repositories.StreamingJob;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Moq;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Metrics.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamingJobTemplateTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamClassOperatorServiceTests : IClassFixture<LoggerFixture>, IClassFixture<AkkaFixture>
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamClassOperatorServiceTests));
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IStreamingJobCollection> streamingJobCollectionMock = new();
    private readonly Mock<IReactiveResourceCollection<IStreamDefinition>> streamDefinitionSourceMock = new();
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly Mock<IStreamingJobTemplateRepository> streamingJobTemplateRepositoryMock = new();
    private readonly TaskCompletionSource tcs = new();
    private readonly CancellationTokenSource cts = new();

    public StreamClassOperatorServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        this.materializer = this.actorSystem.Materializer();
        this.streamingJobTemplateRepositoryMock
            .Setup(s => s.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamingJobTemplate.AsOption<IStreamingJobTemplate>());
        this.cts.CancelAfter(TimeSpan.FromSeconds(15));
        this.cts.Token.Register(() => this.tcs.TrySetResult());
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

        this.kubeClusterMock.Setup(service => service.SendJob(
                It.IsAny<V1Job>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => this.tcs.TrySetResult());

        this.streamDefinitionSourceMock
            .Setup(m => m.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(
                new List<ResourceEvent<IStreamDefinition>>
                {
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition())
                }));

        this.streamClassRepositoryMock
            .Setup(m => m.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());

        var task = this.tcs.Task;

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(this.materializer);
        await task;

        // Assert
        this.kubeClusterMock.Verify(
            service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()));
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

        this.kubeClusterMock.Setup(service => service.SendJob(
                It.IsAny<V1Job>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => this.tcs.TrySetResult());

        this.streamDefinitionSourceMock
            .Setup(m => m.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.From(
                new List<ResourceEvent<IStreamDefinition>>
                {
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition()),
                    new(WatchEventType.Added, StreamDefinitionTestCases.NamedStreamDefinition())
                }));
        var task = this.tcs.Task;

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamClassOperatorService>()
            .GetStreamClassEventsGraph(CancellationToken.None)
            .Run(this.materializer);
        await task;

        // Assert
        this.kubeClusterMock.Verify(
                service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never
            );
    }

    private ServiceProvider CreateServiceProvider()
    {
        return new ServiceCollection()
            .AddSingleton<IMaterializer>(this.actorSystem.Materializer())
            .AddSingleton(this.actorSystem)
            .AddSingleton(this.kubeClusterMock.Object)
            .AddSingleton(this.streamingJobCollectionMock.Object)
            .AddSingleton(this.streamDefinitionSourceMock.Object)
            .AddSingleton(this.streamingJobTemplateRepositoryMock.Object)
            .AddSingleton<IStreamClassRepository, StreamClassRepository>()
            .AddMemoryCache()
            .AddSingleton<IStreamOperatorService, StreamOperatorService>()
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetStreamClassStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<SetAnnotationCommand<V1Job>>, AnnotationCommandHandler>()
            .AddSingleton<IStreamingJobCommandHandler, StreamingJobCommandHandler>()
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamClassOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<AnnotationCommandHandler>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobCommandHandler>())
            .AddSingleton(Options.Create(new StreamClassOperatorServiceConfiguration
            {
                MaxBufferCapacity = 100
            }))
            .AddSingleton<IStreamClassOperatorService, StreamClassOperatorService>()
            .BuildServiceProvider();
    }
}
