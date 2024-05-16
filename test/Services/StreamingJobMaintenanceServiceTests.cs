using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.Commands;
using Arcane.Operator.Services.Maintenance;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Streams;
using Arcane.Operator.Tests.Fixtures;
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

public class StreamingJobMaintenanceServiceTests : IClassFixture<LoggerFixture>
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamingJobMaintenanceServiceTests));
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IStreamingJobOperatorService> streamingJobOperatorServiceMock = new();
    private readonly Mock<IStreamDefinitionRepository> streamDefinitionRepositoryMock = new();
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();

    public StreamingJobMaintenanceServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        this.materializer = this.actorSystem.Materializer();

        this.streamClassRepositoryMock
            .Setup(m => m.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
    }

    [Theory]
    [MemberData(nameof(GenerateCompletedJobTestCases))]
    public async Task HandleCompletedJob(V1Job job, bool definitionExists, bool isBackfilling, bool expectRestart)
    {
        // Arrange
        var mockSource = Source.From(new List<(WatchEventType, V1Job)>
        {
            (WatchEventType.Deleted, job)
        });
        this.kubeClusterMock.Setup(cluster =>
                cluster.StreamJobEvents(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(mockSource);

        this.streamDefinitionRepositoryMock
            .Setup(s => s.GetStreamDefinition(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(() =>
                definitionExists ? (Mock.Of<IStreamClass>().AsOption(), Mock.Of<IStreamDefinition>().AsOption()) : (Option<IStreamClass>.None, Option<IStreamDefinition>.None));
        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.materializer);

        // Assert
        this.streamingJobOperatorServiceMock
            .Verify(s => s.StartRegisteredStream(It.IsAny<IStreamDefinition>(), isBackfilling, It.IsAny<IStreamClass>()), Times.Exactly(definitionExists && expectRestart ? 1 : 0));
    }

    [Theory]
    [MemberData(nameof(GenerateModifiedJobCases))]
    public async Task HandleIntermediateJobModifiedEvent(V1Job job, bool expectToStopJob)
    {
        // Arrange
        var mockSource = Source.From(new List<(WatchEventType, V1Job)>
        {
            (WatchEventType.Modified, job)
        });
        this.kubeClusterMock.Setup(cluster =>
                cluster.StreamJobEvents(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(mockSource);

        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.materializer);

        this.streamingJobOperatorServiceMock
            .Verify(s => s.DeleteJob(It.IsAny<string>(), It.IsAny<string>()),
                Times.Exactly(expectToStopJob ? 1 : 0)
            );
    }

    [Theory]
    [MemberData(nameof(GenerateDeletedJobTestCases))]
    public async Task HandleDeletedJob(V1Job job, IStreamClass streamClass, IStreamDefinition streamDefinition, bool expectToRestart,
        bool expectBackfill)
    {
        // Arrange
        var mockSource = Source.From(new List<(WatchEventType, V1Job)>
        {
            (WatchEventType.Deleted, job)
        });
        this.kubeClusterMock
                    .Setup(cluster =>
                        cluster.StreamJobEvents(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<OverflowStrategy>(),
                            It.IsAny<TimeSpan?>()))
                    .Returns(mockSource);

        this.streamDefinitionRepositoryMock
                    .Setup(service =>
                        service.GetStreamDefinition(job.Namespace(), job.GetStreamKind(), job.GetStreamId()))
                    .ReturnsAsync((streamClass.AsOption(), streamDefinition.AsOption()));


        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.materializer);

        this.streamingJobOperatorServiceMock.Verify(s =>
                s.StartRegisteredStream(streamDefinition, expectBackfill, It.IsAny<IStreamClass>()),
            Times.Exactly(expectToRestart ? 1 : 0)
        );
    }

    public static IEnumerable<object[]> GenerateDeletedJobTestCases()
    {
        yield return new object[] { CompletedJob, StreamClass, StreamDefinition, true, false };
        yield return new object[] { ReloadRequestedJob, StreamClass, StreamDefinition, true, true };
        yield return new object[] { SchemaMismatchJob, StreamClass, StreamDefinition, true, true };

        yield return new object[] { CompletedJob, StreamClass, SuspendedStreamDefinition, false, false };
        yield return new object[] { ReloadRequestedJob, StreamClass, SuspendedStreamDefinition, false, true };
        yield return new object[] { SchemaMismatchJob, StreamClass, SuspendedStreamDefinition, false, true };
    }

    public static IEnumerable<object[]> GenerateCompletedJobTestCases()
    {
        yield return new object[] { FailedJob, true, false, false };
        yield return new object[] { FailedJob, false, false, false };

        yield return new object[] { CompletedJob, true, false, true };
        yield return new object[] { CompletedJob, false, false, true };

        yield return new object[] { RunningJob, true, false, true };
        yield return new object[] { RunningJob, false, false, true };

        yield return new object[] { SchemaMismatchJob, true, true, true };
        yield return new object[] { SchemaMismatchJob, false, true, true };

        yield return new object[] { ReloadRequestedJob, true, true, true };
        yield return new object[] { ReloadRequestedJob, false, true, true };
    }

    public static IEnumerable<object[]> GenerateModifiedJobCases()
    {
        yield return new object[] { TerminatingJob, false };
        yield return new object[] { ReloadRequestedJob, true };
        yield return new object[] { CompletedJob, false };
        yield return new object[] { FailedJob, false };
        yield return new object[] { RunningJob, false };
        yield return new object[] { SchemaMismatchJob, false };
    }


    private StreamingJobMaintenanceService CreateService()
    {
        var metricsReporterConfiguration = Options.Create(new MetricsReporterConfiguration
        {
            MetricsPublisherActorConfiguration = new MetricsPublisherActorConfiguration
            {
                InitialDelay = TimeSpan.FromSeconds(30),
                UpdateInterval = TimeSpan.FromSeconds(10)
            }
        });
        return new ServiceCollection()
            .AddSingleton(this.kubeClusterMock.Object)
            .AddSingleton(this.streamDefinitionRepositoryMock.Object)
            .AddSingleton(this.streamClassRepositoryMock.Object)
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>()
            .AddSingleton<IStreamingJobCommandHandler, StreamingJobCommandHandler>()
            .AddSingleton(this.streamingJobOperatorServiceMock.Object)
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobMaintenanceService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<AnnotationCommandHandler>())
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton<StreamingJobMaintenanceService>()
            .AddSingleton(Options.Create(new StreamingJobMaintenanceServiceConfiguration
            {
                MaxBufferCapacity = 1000
            }))
            .AddSingleton(metricsReporterConfiguration)
            .AddSingleton(Options.Create(new StreamingJobOperatorServiceConfiguration()))
            .AddSingleton<HostedStreamingJobMaintenanceService>()
            .AddSingleton(this.materializer)
            .AddSingleton(this.actorSystem)
            .BuildServiceProvider()
            .GetRequiredService<StreamingJobMaintenanceService>();
    }
}
