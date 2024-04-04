using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Operator;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.JobTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamDefinitionTestCases;
using FailedStreamDefinition = Arcane.Operator.Tests.Services.TestCases.FailedStreamDefinition;

namespace Arcane.Operator.Tests.Services;

public class StreamOperatorServiceTests : IClassFixture<ServiceFixture>, IClassFixture<LoggerFixture>,
    IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly LoggerFixture loggerFixture;
    private readonly ServiceFixture serviceFixture;

    public StreamOperatorServiceTests(ServiceFixture serviceFixture, LoggerFixture loggerFixture,
        AkkaFixture akkaFixture)
    {
        this.serviceFixture = serviceFixture;
        this.loggerFixture = loggerFixture;
        this.akkaFixture = akkaFixture;
    }

    public static IEnumerable<object[]> GenerateSynchronizationTestCases()
    {
        yield return new object[]
            { WatchEventType.Added, StreamDefinitionTestCases.StreamDefinition, true, false, false, false };
        yield return new object[]
            { WatchEventType.Modified, StreamDefinitionTestCases.StreamDefinition, false, true, true, false };
        yield return new object[]
            { WatchEventType.Modified, StreamDefinitionTestCases.StreamDefinition, false, true, false, false };

        yield return new object[] { WatchEventType.Added, SuspendedStreamDefinition, false, false, false, false };
        yield return new object[] { WatchEventType.Deleted, SuspendedStreamDefinition, false, false, false, false };
        yield return new object[] { WatchEventType.Modified, SuspendedStreamDefinition, false, false, true, true };
        yield return new object[] { WatchEventType.Modified, SuspendedStreamDefinition, false, false, false, false };
    }

    [Theory]
    [MemberData(nameof(GenerateSynchronizationTestCases))]
    public async Task TestHandleAddedStreamEvent(WatchEventType eventType,
        StreamDefinition streamDefinition,
        bool expectFullLoad,
        bool expectRestart,
        bool streamingJobExists,
        bool expectTermination)
    {
        // Arrange
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.SetupEventMock(eventType, streamDefinition);
        this.serviceFixture.MockStreamingJobOperatorService
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(streamingJobExists ? JobWithChecksum("checksum").AsOption() : Option<V1Job>.None);

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamOperatorService<StreamDefinition>>()
            .GetStreamDefinitionEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);

        // Assert

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
                => service.StartRegisteredStream(
                    It.IsAny<StreamDefinition>(), true),
            Times.Exactly(expectFullLoad ? 1 : 0));

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
                => service.RequestStreamingJobRestart(streamDefinition.StreamId),
            Times.Exactly(expectRestart && streamingJobExists ? 1 : 0));

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
                => service.StartRegisteredStream(
                    It.IsAny<StreamDefinition>(), false),
            Times.Exactly(expectRestart && !streamingJobExists ? 1 : 0));

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
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
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.SetupEventMock(WatchEventType.Modified, streamDefinition);
        var mockJob = JobWithChecksum(jobChecksumChanged ? "checksum" : streamDefinition.GetConfigurationChecksum());
        this.serviceFixture.MockStreamingJobOperatorService
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(mockJob.AsOption());

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamOperatorService<StreamDefinition>>()
            .GetStreamDefinitionEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);

        // Assert

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
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
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.serviceFixture.MockStreamDefinitionRepository.Invocations.Clear();
        this.serviceFixture.MockStreamDefinitionRepository
            .Setup(sdr
                => sdr.RemoveReloadingAnnotation(streamDefinition.Namespace(), streamDefinition.Kind,
                    streamDefinition.StreamId))
            .ReturnsAsync(((IStreamDefinition)streamDefinition).AsOption());
        this.SetupEventMock(WatchEventType.Modified, streamDefinition);
        var mockJob = JobWithChecksum(streamDefinition.GetConfigurationChecksum());
        this.serviceFixture.MockStreamingJobOperatorService
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(jobExists ? mockJob.AsOption() : Option<V1Job>.None);

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamOperatorService<StreamDefinition>>()
            .GetStreamDefinitionEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);

        // Assert
        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
            => service.RequestStreamingJobReload(It.IsAny<string>()), Times.Exactly(expectReload ? 1 : 0));

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
            => service.StartRegisteredStream(It.IsAny<IStreamDefinition>(), true), Times.Exactly(expectReload ? 0 : 1));

        this.serviceFixture.MockStreamDefinitionRepository.Verify(service
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
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.serviceFixture.MockStreamDefinitionRepository.Invocations.Clear();
        this.serviceFixture.MockStreamDefinitionRepository
            .Setup(sdr
                => sdr.RemoveReloadingAnnotation(streamDefinition.Namespace(), streamDefinition.Kind,
                    streamDefinition.StreamId))
            .ReturnsAsync(((IStreamDefinition)streamDefinition).AsOption());
        this.SetupEventMock(WatchEventType.Added, streamDefinition);
        this.serviceFixture.MockStreamingJobOperatorService
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(jobExists ? jobIsReloading ? ReloadingJob : RunningJob : Option<V1Job>.None);

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamOperatorService<StreamDefinition>>()
            .GetStreamDefinitionEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);

        // Assert
        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
            => service.RequestStreamingJobReload(It.IsAny<string>()), Times.Exactly(0));

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
            => service.StartRegisteredStream(It.IsAny<IStreamDefinition>(), true), Times.Exactly(expectStart ? 1 : 0));
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
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.serviceFixture
            .MockKubeCluster.Setup(cluster =>
                cluster.StreamCustomResourceEvents<FailedStreamDefinition>(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<int>(),
                    It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(Source.Single((WatchEventType.Added, streamDefinition)));

        this.serviceFixture.MockStreamingJobOperatorService
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamOperatorService<FailedStreamDefinition>>()
            .GetStreamDefinitionEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);

        // Assert

        this.serviceFixture.MockStreamingJobOperatorService.Verify(service
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
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.serviceFixture
            .MockKubeCluster.Setup(cluster =>
                cluster.StreamCustomResourceEvents<FailedStreamDefinition>(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<int>(),
                    It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(Source.Single((WatchEventType.Added, streamDefinition)));

        this.serviceFixture.MockStreamingJobOperatorService
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        // Act
        var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var sp = this.CreateServiceProvider();
            await sp.GetRequiredService<IStreamOperatorService<FailedStreamDefinition>>()
                .GetStreamDefinitionEventsGraph(CancellationToken.None)
                .Run(this.akkaFixture.Materializer);
        });
        // Assert

        Assert.Equal("test", exception.Message);
    }

    [Fact]
    public async Task HandleBrokenStreamRepository()
    {
        // Arrange
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.serviceFixture.MockStreamDefinitionRepository.Invocations.Clear();
        this.serviceFixture
            .MockKubeCluster.Setup(cluster =>
                cluster.StreamCustomResourceEvents<StreamDefinition>(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<int>(),
                    It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(
                Source.Single((WatchEventType.Added, (StreamDefinition)StreamDefinitionTestCases.StreamDefinition)));

        this.serviceFixture.MockStreamingJobOperatorService
            .Setup(service => service.GetStreamingJob(It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        this.serviceFixture.MockStreamDefinitionRepository
            .Setup(service
                => service.SetStreamStatus(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<V1Beta1StreamStatus>()))
            .ThrowsAsync(new Exception());

        // Act
        var sp = this.CreateServiceProvider();
        await sp.GetRequiredService<IStreamOperatorService<StreamDefinition>>()
            .GetStreamDefinitionEventsGraph(CancellationToken.None)
            .Run(this.akkaFixture.Materializer);

        // Assert that code above didn't throw
        Assert.True(true);
    }


    private void SetupEventMock(WatchEventType eventType, StreamDefinition streamDefinition)
    {
        this.serviceFixture
            .MockKubeCluster.Setup(cluster =>
                cluster.StreamCustomResourceEvents<StreamDefinition>(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<int>(),
                    It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(Source.Single<(WatchEventType, StreamDefinition)>((eventType, streamDefinition)));
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
            .AddSingleton(this.serviceFixture.MockStreamingJobOperatorService.Object)
            .AddSingleton(this.serviceFixture.MockStreamDefinitionRepository.Object)
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamOperatorService<StreamDefinition>>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamOperatorService<FailedStreamDefinition>>())
            .AddSingleton(this.serviceFixture.MockStreamingJobOperatorService.Object)
            .AddSingleton(optionsMock.Object)
            .AddSingleton(Options.Create(new StreamOperatorServiceConfiguration
            {
                Parallelism = 1, MaxBufferCapacity = 100
            }))
            .AddSingleton<IStreamOperatorService<StreamDefinition>, StreamOperatorService<StreamDefinition>>()
            .AddSingleton<IStreamOperatorService<FailedStreamDefinition>, StreamOperatorService<FailedStreamDefinition>>()
            .BuildServiceProvider();
    }
}
