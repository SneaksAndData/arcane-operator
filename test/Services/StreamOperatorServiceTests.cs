using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Models.StreamClass;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Commands;
using Arcane.Operator.Services.Maintenance;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Models;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Services.Repositories;
using Arcane.Operator.Services.Streams;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Metrics.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.JobTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamDefinitionTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamOperatorServiceTests : IClassFixture<LoggerFixture>, IDisposable
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamClassOperatorServiceTests));
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IStreamingJobOperatorService> streamingJobOperatorServiceMock = new();
    private readonly Mock<IStreamDefinitionRepository> streamDefinitionRepositoryMock = new();
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly TaskCompletionSource tcs = new();
    private readonly CancellationTokenSource cts = new();

    public StreamOperatorServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        this.materializer = this.actorSystem.Materializer();
        this.streamClassRepositoryMock
            .Setup(c => c.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        this.cts.CancelAfter(TimeSpan.FromSeconds(5));
        this.cts.Token.Register(this.tcs.SetResult);
    }

    public static IEnumerable<object[]> GenerateSynchronizationTestCases()
    {
        yield return new object[] { WatchEventType.Added, StreamDefinitionTestCases.StreamDefinition, true, false, false, false };
        yield return new object[] { WatchEventType.Modified, StreamDefinitionTestCases.StreamDefinition, false, true, true, false };
        yield return new object[] { WatchEventType.Modified, StreamDefinitionTestCases.StreamDefinition, false, true, false, false };

        yield return new object[] { WatchEventType.Added, SuspendedStreamDefinition, false, false, false, false };
        yield return new object[] { WatchEventType.Deleted, SuspendedStreamDefinition, false, false, false, false };
        yield return new object[] { WatchEventType.Modified, SuspendedStreamDefinition, false, false, true, true };
        yield return new object[] { WatchEventType.Modified, SuspendedStreamDefinition, false, false, false, false };
    }

    [Theory]
    [MemberData(nameof(GenerateSynchronizationTestCases))]
    public async Task TestHandleAddedStreamEvent(WatchEventType eventType,
        StreamDefinition streamDefinition,
        bool expectBackfill,
        bool expectRestart,
        bool streamingJobExists,
        bool expectTermination)
    {
        // Arrange
        this.SetupEventMock(eventType, streamDefinition);
        streamingJobOperatorServiceMock
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(streamingJobExists ? JobWithChecksum("checksum").AsOption() : Option<V1Job>.None);

        var task = this.tcs.Task;
        streamingJobOperatorServiceMock
            .Setup(service => service.StartRegisteredStream(
                It.IsAny<StreamDefinition>(), It.IsAny<bool>(), It.IsAny<IStreamClass>()))
            .Callback(() => this.tcs.SetResult());

        streamingJobOperatorServiceMock
            .Setup(service => service.RequestStreamingJobRestart(It.IsAny<string>()))
            .Callback(() => this.tcs.SetResult());

        streamingJobOperatorServiceMock
            .Setup(service => service.DeleteJob(It.IsAny<string>(), It.IsAny<string>()))
            .Callback(() => this.tcs.SetResult());

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert

        streamingJobOperatorServiceMock.Verify(service
                => service.StartRegisteredStream(
                    It.IsAny<StreamDefinition>(), true, It.IsAny<IStreamClass>()),
            Times.Exactly(expectBackfill ? 1 : 0));

        streamingJobOperatorServiceMock.Verify(service
                => service.RequestStreamingJobRestart(streamDefinition.StreamId),
            Times.Exactly(expectRestart && streamingJobExists ? 1 : 0));

        streamingJobOperatorServiceMock.Verify(service
                => service.StartRegisteredStream(
                    It.IsAny<StreamDefinition>(), false, It.IsAny<IStreamClass>()),
            Times.Exactly(expectRestart && !streamingJobExists ? 1 : 0));

        streamingJobOperatorServiceMock.Verify(service
                => service.DeleteJob(It.IsAny<string>(), It.IsAny<string>()),
            Times.Exactly(expectTermination ? 1 : 0));
    }

    public static IEnumerable<object[]> GenerateModifiedTestCases()
    {
        yield return new object[] { StreamDefinitionTestCases.StreamDefinition, true, true };
        yield return new object[] { StreamDefinitionTestCases.StreamDefinition, false, false };
    }

    [Theory]
    [MemberData(nameof(GenerateModifiedTestCases))]
    public async Task TestJobModificationEvent(StreamDefinition streamDefinition,
        bool jobChecksumChanged, bool expectRestart)
    {
        // Arrange
        this.SetupEventMock(WatchEventType.Modified, streamDefinition);
        var mockJob = JobWithChecksum(jobChecksumChanged ? "checksum" : streamDefinition.GetConfigurationChecksum());
        streamingJobOperatorServiceMock
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(mockJob.AsOption());

        var task = this.tcs.Task;
        streamingJobOperatorServiceMock
            .Setup(service => service.RequestStreamingJobRestart(It.IsAny<string>()))
            .Callback(this.tcs.SetResult);

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert

        streamingJobOperatorServiceMock.Verify(service
            => service.RequestStreamingJobRestart(It.IsAny<string>()), Times.Exactly(expectRestart ? 1 : 0));
    }


    public static IEnumerable<object[]> GenerateReloadTestCases()
    {
        yield return new object[] { ReloadRequestedStreamDefinition, true, true };
        yield return new object[] { ReloadRequestedStreamDefinition, false, false };
    }

    [Theory]
    [MemberData(nameof(GenerateReloadTestCases))]
    public async Task TestStreamReload(StreamDefinition streamDefinition, bool jobExists, bool expectReload)
    {
        // Arrange
        this.streamDefinitionRepositoryMock
            .Setup(sdr
                => sdr.RemoveReloadingAnnotation(streamDefinition.Namespace(), streamDefinition.Kind,
                    streamDefinition.StreamId))
            .ReturnsAsync(((IStreamDefinition)streamDefinition).AsOption());
        this.SetupEventMock(WatchEventType.Modified, streamDefinition);
        var mockJob = JobWithChecksum(streamDefinition.GetConfigurationChecksum());
        streamingJobOperatorServiceMock
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(jobExists ? mockJob.AsOption() : Option<V1Job>.None);

        var task = this.tcs.Task;
        streamingJobOperatorServiceMock
            .Setup(service => service.RequestStreamingJobReload(It.IsAny<string>()))
            .Callback(this.tcs.SetResult);
        streamingJobOperatorServiceMock
            .Setup(service => service.StartRegisteredStream(It.IsAny<IStreamDefinition>(), It.IsAny<bool>(), It.IsAny<IStreamClass>()))
            .Callback(this.tcs.SetResult);

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        streamingJobOperatorServiceMock.Verify(service
            => service.RequestStreamingJobReload(It.IsAny<string>()), Times.Exactly(expectReload ? 1 : 0));

        streamingJobOperatorServiceMock.Verify(service
                => service.StartRegisteredStream(It.IsAny<IStreamDefinition>(), true, It.IsAny<IStreamClass>()),
            Times.Exactly(expectReload ? 0 : 1));

        this.streamDefinitionRepositoryMock.Verify(service
            => service.RemoveReloadingAnnotation(streamDefinition.Namespace(), streamDefinition.Kind,
                streamDefinition.StreamId));
    }

    public static IEnumerable<object[]> GenerateAddTestCases()
    {
        yield return new object[] { ReloadRequestedStreamDefinition, true, true, false };
        yield return new object[] { ReloadRequestedStreamDefinition, false, false, true };
        yield return new object[] { ReloadRequestedStreamDefinition, true, false, false };

        yield return new object[] { StreamDefinitionTestCases.StreamDefinition, true, true, false };
        yield return new object[] { StreamDefinitionTestCases.StreamDefinition, false, false, true };
        yield return new object[] { StreamDefinitionTestCases.StreamDefinition, true, false, false };

        yield return new object[] { SuspendedStreamDefinition, true, true, false };
        yield return new object[] { SuspendedStreamDefinition, false, false, false };
        yield return new object[] { SuspendedStreamDefinition, true, false, false };
    }

    [Theory]
    [MemberData(nameof(GenerateAddTestCases))]
    public async Task TestStreamAdded(StreamDefinition streamDefinition, bool jobExists, bool jobIsReloading,
        bool expectStart)
    {
        // Arrange
        this.streamDefinitionRepositoryMock
            .Setup(sdr
                => sdr.RemoveReloadingAnnotation(streamDefinition.Namespace(), streamDefinition.Kind,
                    streamDefinition.StreamId))
            .ReturnsAsync(((IStreamDefinition)streamDefinition).AsOption());
        this.SetupEventMock(WatchEventType.Added, streamDefinition);
        streamingJobOperatorServiceMock
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(jobExists ? jobIsReloading ? ReloadingJob : RunningJob : Option<V1Job>.None);

        var task = this.tcs.Task;
        streamingJobOperatorServiceMock
            .Setup(service => service.StartRegisteredStream(It.IsAny<IStreamDefinition>(), It.IsAny<bool>(), It.IsAny<IStreamClass>()))
            .Callback(this.tcs.SetResult);

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        streamingJobOperatorServiceMock.Verify(service
            => service.RequestStreamingJobReload(It.IsAny<string>()), Times.Exactly(0));

        streamingJobOperatorServiceMock.Verify(service
                => service.StartRegisteredStream(It.IsAny<IStreamDefinition>(), true, It.IsAny<IStreamClass>()),
            Times.Exactly(expectStart ? 1 : 0));
    }

    public static IEnumerable<object[]> GenerateRecoverableTestCases()
    {
        yield return new object[] { FailedStreamDefinition(new JsonException()) };
    }

    [Theory]
    [MemberData(nameof(GenerateRecoverableTestCases))]
    public async Task HandleBrokenStreamDefinition(FailedStreamDefinition streamDefinition)
    {
        // Arrange
        this.streamDefinitionRepositoryMock.Setup(
                cluster => cluster.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.Single(new ResourceEvent<IStreamDefinition>(WatchEventType.Added, streamDefinition)));

        streamingJobOperatorServiceMock
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        var task = this.tcs.Task;

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        streamingJobOperatorServiceMock.Verify(service
            => service.GetStreamingJob(It.IsAny<string>()), Times.Never);
    }

    public static IEnumerable<object[]> GenerateFatalTestCases()
    {
        yield return new object[] { FailedStreamDefinition(new BufferOverflowException("test")) };
    }

    [Theory]
    [MemberData(nameof(GenerateFatalTestCases))]
    public async Task HandleFatalException(FailedStreamDefinition streamDefinition)
    {
        // Arrange
        this.streamDefinitionRepositoryMock.Setup(s =>
                s.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.Single(new ResourceEvent<IStreamDefinition>(WatchEventType.Added, streamDefinition)));

        var task = this.tcs.Task;
        streamingJobOperatorServiceMock
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert that code above didn't throw
        Assert.True(task.IsCompleted);
    }

    [Fact]
    public async Task HandleBrokenStreamRepository()
    {
        // Arrange
        this.streamDefinitionRepositoryMock.Setup(s
                => s.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(
                Source.Single(new ResourceEvent<IStreamDefinition>(WatchEventType.Added,
                    StreamDefinitionTestCases.StreamDefinition)));

        streamingJobOperatorServiceMock
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        this.streamDefinitionRepositoryMock
            .Setup(service
                => service.SetStreamStatus(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<V1Beta1StreamStatus>()))
            .ThrowsAsync(new Exception());

        var task = this.tcs.Task;

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert that code above didn't throw
        Assert.True(task.IsCompleted);
    }


    private void SetupEventMock(WatchEventType eventType, IStreamDefinition streamDefinition)
    {
        this.streamDefinitionRepositoryMock
            .Setup(service => service.GetEvents(It.IsAny<CustomResourceApiRequest>(), It.IsAny<int>()))
            .Returns(Source.Single(new ResourceEvent<IStreamDefinition>(eventType, streamDefinition)));
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
            .AddSingleton(this.materializer)
            .AddSingleton(this.actorSystem)
            .AddSingleton(this.kubeClusterMock.Object)
            .AddSingleton<IMaterializer>(this.actorSystem.Materializer())
            .AddSingleton(streamingJobOperatorServiceMock.Object)
            .AddSingleton(this.streamDefinitionRepositoryMock.Object)
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetAnnotationCommand<V1Job>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>()
            .AddSingleton<IStreamingJobCommandHandler, StreamingJobCommandHandler>()
            .AddSingleton(this.streamClassRepositoryMock.Object)
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton(metricsReporterConfiguration)
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamDefinitionRepository>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobMaintenanceService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<AnnotationCommandHandler>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton(streamingJobOperatorServiceMock.Object)
            .AddSingleton(optionsMock.Object)
            .AddSingleton<IStreamOperatorService, StreamOperatorService>()
            .BuildServiceProvider();
    }

    public void Dispose()
    {
        this.actorSystem?.Dispose();
        this.materializer?.Dispose();
        this.cts?.Dispose();
    }
}
