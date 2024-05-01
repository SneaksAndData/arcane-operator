using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Maintenance;
using Arcane.Operator.Services.Streams;
using Arcane.Operator.Tests.Fixtures;
using k8s;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.JobTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamDefinitionTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamingJobMaintenanceServiceTests : IClassFixture<ServiceFixture>, IClassFixture<LoggerFixture>,
    IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly LoggerFixture loggerFixture;
    private readonly ServiceFixture serviceFixture;

    public StreamingJobMaintenanceServiceTests(ServiceFixture serviceFixture, LoggerFixture loggerFixture,
        AkkaFixture akkaFixture)
    {
        this.serviceFixture = serviceFixture;
        this.loggerFixture = loggerFixture;
        this.akkaFixture = akkaFixture;
    }

    [Theory]
    [MemberData(nameof(GenerateCompletedJobTestCases))]
    public async Task HandleCompletedJob(V1Job job, bool definitionExists, bool fullLoad, bool expectRestart)
    {
        // Arrange
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        this.serviceFixture.MockStreamDefinitionRepository.Invocations.Clear();
        var mockSource = Source.From(new List<(WatchEventType, V1Job)>
        {
            (WatchEventType.Deleted, job)
        });
        this.serviceFixture
            .MockKubeCluster.Setup(cluster =>
                cluster.StreamJobEvents(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(mockSource);

        this.serviceFixture
            .MockStreamDefinitionRepository
            .Setup(s => s.GetStreamDefinition(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(() =>
                definitionExists ? (Mock.Of<IStreamClass>().AsOption(), Mock.Of<IStreamDefinition>().AsOption()) : (Option<IStreamClass>.None, Option<IStreamDefinition>.None));
        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.akkaFixture.Materializer);

        // Assert
        this.serviceFixture.MockStreamingJobOperatorService
            .Verify(s => s.StartRegisteredStream(It.IsAny<IStreamDefinition>(), fullLoad, It.IsAny<IStreamClass>()),
                Times.Exactly(definitionExists && expectRestart ? 1 : 0));
    }

    [Theory]
    [MemberData(nameof(GenerateModifiedJobCases))]
    public async Task HandleIntermediateJobModifiedEvent(V1Job job, bool expectToStopJob)
    {
        // Arrange
        this.serviceFixture.MockStreamingJobOperatorService.Invocations.Clear();
        var mockSource = Source.From(new List<(WatchEventType, V1Job)>
        {
            (WatchEventType.Modified, job)
        });
        this.serviceFixture
            .MockKubeCluster.Setup(cluster =>
                cluster.StreamJobEvents(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(mockSource);

        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.akkaFixture.Materializer);

        this.serviceFixture
            .MockStreamingJobOperatorService
            .Verify(s => s.DeleteJob(It.IsAny<string>(), It.IsAny<string>()),
                Times.Exactly(expectToStopJob ? 1 : 0)
            );
    }

    [Theory]
    [MemberData(nameof(GenerateAddedJobTestCases))]
    public async Task HandleAddedJob(string expectedPhase, V1Job job, bool expectToChangeState)
    {
        // Arrange
        this.serviceFixture.MockStreamDefinitionRepository.Invocations.Clear();
        var mockSource = Source.From(new List<(WatchEventType, V1Job)>
        {
            (WatchEventType.Added, job)
        });
        this.serviceFixture
            .MockKubeCluster.Setup(cluster =>
                cluster.StreamJobEvents(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(mockSource);

        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.akkaFixture.Materializer);

        this.serviceFixture
            .MockStreamDefinitionRepository
            .Verify(s => s.SetStreamStatus(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.Is<V1Beta1StreamStatus>(cs => cs.Phase == expectedPhase)),
                Times.Exactly(expectToChangeState ? 1 : 0));
    }

    [Theory]
    [MemberData(nameof(GenerateDeletedJobTestCases))]
    public async Task HandleDeletedJob(V1Job job, IStreamClass streamClass, IStreamDefinition streamDefinition, bool expectToRestart,
        bool expectFullLoad)
    {
        // Arrange
        this.serviceFixture.MockStreamDefinitionRepository.Invocations.Clear();
        var mockSource = Source.From(new List<(WatchEventType, V1Job)>
        {
            (WatchEventType.Deleted, job)
        });
        this.serviceFixture
            .MockKubeCluster
            .Setup(cluster =>
                cluster.StreamJobEvents(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<OverflowStrategy>(),
                    It.IsAny<TimeSpan?>()))
            .Returns(mockSource);

        this.serviceFixture
            .MockStreamDefinitionRepository
            .Setup(service =>
                service.GetStreamDefinition(job.Namespace(), job.GetStreamKind(), job.GetStreamId()))
            .ReturnsAsync((streamClass.AsOption(), streamDefinition.AsOption()));


        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.akkaFixture.Materializer);

        this.serviceFixture.MockStreamingJobOperatorService.Verify(s =>
                s.StartRegisteredStream(streamDefinition, expectFullLoad, It.IsAny<IStreamClass>()),
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

    public static IEnumerable<object[]> GenerateAddedJobTestCases()
    {
        yield return new object[] { StreamPhase.RUNNING.ToString(), RunningJob, true };
        yield return new object[] { StreamPhase.RELOADING.ToString(), ReloadingJob, true };
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
        return new ServiceCollection()
            .AddSingleton(this.serviceFixture.MockKubeCluster.Object)
            .AddSingleton(this.serviceFixture.MockStreamDefinitionRepository.Object)
            .AddSingleton(this.serviceFixture.MockStreamingJobOperatorService.Object)
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobMaintenanceService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobOperatorService>())
            .AddSingleton<StreamingJobMaintenanceService>()
            .AddSingleton(Options.Create(new StreamingJobMaintenanceServiceConfiguration
            {
                MaxBufferCapacity = 1000
            }))
            .AddSingleton(Options.Create(new StreamingJobOperatorServiceConfiguration()))
            .AddSingleton<HostedStreamingJobMaintenanceService>()
            .AddSingleton(this.akkaFixture.Materializer)
            .BuildServiceProvider()
            .GetRequiredService<StreamingJobMaintenanceService>();
    }
}
