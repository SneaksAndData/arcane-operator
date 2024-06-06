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
using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Operators;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Operators;
using Arcane.Operator.Services.Repositories.CustomResources;
using Arcane.Operator.Services.Repositories.StreamingJob;
using Arcane.Operator.StreamingJobLifecycle;
using Arcane.Operator.Tests.Extensions;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Snd.Sdk.Kubernetes;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Metrics.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.JobTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamDefinitionTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamingJobTemplateTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamOperatorServiceTests : IClassFixture<LoggerFixture>, IDisposable
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamOperatorServiceTests));
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IStreamingJobCollection> streamingJobOperatorServiceMock = new();
    private readonly Mock<IReactiveResourceCollection<IStreamDefinition>> streamDefinitionRepositoryMock = new();
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly TaskCompletionSource tcs = new();
    private readonly CancellationTokenSource cts = new();
    private readonly Mock<IStreamingJobTemplateRepository> streamingJobTemplateRepositoryMock = new();

    public StreamOperatorServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        this.materializer = this.actorSystem.Materializer();
        this.streamClassRepositoryMock
            .Setup(c => c.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        this.cts.CancelAfter(TimeSpan.FromSeconds(5));
        this.cts.Token.Register(this.tcs.SetResult);
        this.streamingJobTemplateRepositoryMock
            .Setup(s => s.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamingJobTemplate.AsOption<IStreamingJobTemplate>());
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
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(streamingJobExists ? JobWithChecksum("checksum").AsOption() : Option<V1Job>.None);

        var task = this.tcs.Task;
        this.kubeClusterMock
             .Setup(service => service.SendJob(
                 It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
             .Callback(() => this.tcs.SetResult());

        this.kubeClusterMock.Setup(c => c.AnnotateJob(It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>()))
            .Callback(() => this.tcs.SetResult());

        this.kubeClusterMock
             .Setup(service => service.DeleteJob(
                 It.IsAny<string>(),
                 It.IsAny<string>(),
                 It.IsAny<CancellationToken>(),
                 It.IsAny<PropagationPolicy>()))
             .Callback(() => this.tcs.SetResult());

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert

        this.kubeClusterMock.Verify(service
                => service.SendJob(
                    It.Is<V1Job>(job => job.IsBackfilling()),
                    It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Exactly(expectBackfill ? 1 : 0));

        this.kubeClusterMock
            .Verify(c => c.AnnotateJob(It.IsAny<string>(),
            It.IsAny<string>(),
            It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
            It.Is<string>(a => a == Annotations.RESTARTING_STATE_ANNOTATION_VALUE)),
            Times.Exactly(expectRestart && streamingJobExists ? 1 : 0));

        this.kubeClusterMock.Verify(service
                => service.SendJob(
                    It.Is<V1Job>(job => !job.IsBackfilling()),
                    It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Exactly(expectRestart && !streamingJobExists ? 1 : 0));

        this.kubeClusterMock.Verify(service
                => service.DeleteJob(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>(),
                    It.IsAny<PropagationPolicy>()),
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
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(mockJob.AsOption());

        var task = this.tcs.Task;
        this.kubeClusterMock.Setup(c => c.AnnotateJob(It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>()))
            .Callback(() => this.tcs.SetResult());

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        this.kubeClusterMock
            .Verify(c => c.AnnotateJob(It.IsAny<string>(),
            It.IsAny<string>(),
            It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
            It.Is<string>(a => a == Annotations.RESTARTING_STATE_ANNOTATION_VALUE)),
            Times.Exactly(expectRestart ? 1 : 0));
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
        this.SetupEventMock(WatchEventType.Modified, streamDefinition);
        var mockJob = JobWithChecksum(streamDefinition.GetConfigurationChecksum());
        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(jobExists ? mockJob.AsOption() : Option<V1Job>.None);

        var task = this.tcs.Task;
        this.kubeClusterMock.Setup(c => c.AnnotateJob(It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>()))
            .Callback(() => this.tcs.SetResult());
        this.kubeClusterMock
             .Setup(service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
             .Callback(this.tcs.SetResult);

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        this.kubeClusterMock
            .Verify(c => c.AnnotateJob(It.IsAny<string>(),
            It.IsAny<string>(),
            It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
            It.Is<string>(a => a == Annotations.RELOADING_STATE_ANNOTATION_VALUE)),
            Times.Exactly(expectReload ? 1 : 0));

        this.kubeClusterMock.Verify(service
                => service.SendJob(It.Is<V1Job>(job => job.IsBackfilling()),
                    It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Exactly(expectReload ? 0 : 1));

        var crd = StreamClass.ToNamespacedCrd();
        this.kubeClusterMock.Verify(service => service.RemoveObjectAnnotation(
            It.Is<NamespacedCrd>(namespacedCrd => namespacedCrd.Group == crd.Group &&
                                                  namespacedCrd.Plural == crd.Plural &&
                                                  namespacedCrd.Version == crd.Version),
            Annotations.STATE_ANNOTATION_KEY,
            streamDefinition.StreamId,
            streamDefinition.Namespace()));
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
        this.SetupEventMock(WatchEventType.Added, streamDefinition);
        streamingJobOperatorServiceMock
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(jobExists ? jobIsReloading ? ReloadingJob : RunningJob : Option<V1Job>.None);

        var task = this.tcs.Task;
        this.kubeClusterMock
            .Setup(service => service.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback(this.tcs.SetResult);

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        this.kubeClusterMock
            .Verify(c => c.AnnotateJob(It.IsAny<string>(),
            It.IsAny<string>(),
            It.Is<string>(a => a == Annotations.STATE_ANNOTATION_KEY),
            It.Is<string>(a => a == Annotations.RELOADING_STATE_ANNOTATION_VALUE)),
            Times.Exactly(0));

        this.kubeClusterMock.Verify(service
                => service.SendJob(It.Is<V1Job>(job => job.IsBackfilling()), It.IsAny<string>(), It.IsAny<CancellationToken>()),
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
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        var task = this.tcs.Task;

        // Act
        var sp = this.CreateServiceProvider();
        sp.GetRequiredService<IStreamOperatorService>().Attach(StreamClass);
        await task;

        // Assert
        streamingJobOperatorServiceMock.Verify(service
            => service.Get(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
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
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
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
            .Setup(service => service.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(FailedJob.AsOption());

        this.kubeClusterMock
            .Setup(service
                => service.UpdateCustomResourceStatus(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Func<JsonElement, It.IsAnyType>>()
                    ))
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
            .AddSingleton(this.streamingJobTemplateRepositoryMock.Object)
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetAnnotationCommand<V1Job>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<StreamingJobCommand>, StreamingJobCommandHandler>()
            .AddSingleton(this.streamClassRepositoryMock.Object)
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton(metricsReporterConfiguration)
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamDefinitionRepository>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobRepository>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<AnnotationCommandHandler>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobCommandHandler>())
            .AddSingleton(this.streamingJobOperatorServiceMock.Object)
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
