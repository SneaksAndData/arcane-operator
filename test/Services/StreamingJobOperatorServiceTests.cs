﻿using System;
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
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.EventFilters;
using Arcane.Operator.Services.Base.Metrics;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.Base.Repositories.StreamingJob;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Services.HostedServices;
using Arcane.Operator.Services.Metrics;
using Arcane.Operator.Services.Operators;
using Arcane.Operator.Services.Repositories.StreamingJob;
using Arcane.Operator.Tests.Extensions;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.Helpers;
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
using static Arcane.Operator.Tests.Services.TestCases.StreamingJobTemplateTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamingJobOperatorServiceTests : IClassFixture<LoggerFixture>
{
    // Akka service and test helpers
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(StreamingJobOperatorServiceTests));
    private readonly LoggerFixture loggerFixture;
    private readonly ActorMaterializer materializer;

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IResourceCollection<IStreamDefinition>> streamDefinitionRepositoryMock = new();
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly Mock<IStreamingJobTemplateRepository> streamingJobTemplateRepositoryMock = new();

    public StreamingJobOperatorServiceTests(LoggerFixture loggerFixture)
    {
        this.loggerFixture = loggerFixture;
        this.materializer = this.actorSystem.Materializer();

        this.streamClassRepositoryMock
            .Setup(m => m.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());

        this.streamingJobTemplateRepositoryMock
            .Setup(s => s.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamingJobTemplate.AsOption<IStreamingJobTemplate>());
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
            .Setup(service => service.Get(job.Name(), job.ToOwnerApiRequest()))
            .ReturnsAsync(() => definitionExists ? StreamDefinition.AsOption() : Option<IStreamDefinition>.None);
        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.materializer);

        // Assert
        this.kubeClusterMock.Verify(s =>
            s.SendJob(It.Is<V1Job>(j => isBackfilling ? j.IsBackfilling() : !j.IsBackfilling()), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Exactly(definitionExists && expectRestart ? 1 : 0)
        );
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

        this.kubeClusterMock
            .Verify(s => s.DeleteJob(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>(),
                    It.IsAny<PropagationPolicy>()),
                Times.Exactly(expectToStopJob ? 1 : 0)
            );
    }

    [Theory]
    [MemberData(nameof(GenerateDeletedJobTestCases))]
    public async Task HandleDeletedJob(V1Job job, IStreamDefinition streamDefinition, bool expectToRestart, bool expectBackfill)
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
            .Setup(service => service.Get(job.Name(), job.ToOwnerApiRequest()))
            .ReturnsAsync(streamDefinition.AsOption());


        var service = this.CreateService();

        // Act
        await service.GetJobEventsGraph(CancellationToken.None).Run(this.materializer);

        this.kubeClusterMock.Verify(s =>
            s.SendJob(It.Is<V1Job>(j => expectBackfill ? j.IsBackfilling() : !j.IsBackfilling()), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Exactly(expectToRestart ? 1 : 0)
        );
    }

    public static IEnumerable<object[]> GenerateDeletedJobTestCases()
    {
        yield return new object[] { CompletedJob, StreamDefinition, true, false };
        yield return new object[] { ReloadRequestedJob, StreamDefinition, true, true };
        yield return new object[] { SchemaMismatchJob, StreamDefinition, true, true };

        yield return new object[] { CompletedJob, SuspendedStreamDefinition, false, false };
        yield return new object[] { ReloadRequestedJob, SuspendedStreamDefinition, false, true };
        yield return new object[] { SchemaMismatchJob, SuspendedStreamDefinition, false, true };
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


    private StreamingJobOperatorService CreateService()
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
            .AddSingleton(this.streamingJobTemplateRepositoryMock.Object)
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<SetAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>()
            .AddSingleton<ICommandHandler<StreamingJobCommand>, StreamingJobCommandHandler>()
            .AddSingleton<ICommandHandler<RemoveAnnotationCommand<IStreamDefinition>>, AnnotationCommandHandler>()
            .AddSingleton<IStreamingJobCollection, StreamingJobRepository>()
            .AddSingleton<IEventFilter<IStreamDefinition>, EmptyEventFilter<IStreamDefinition>>()
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobOperatorService>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobRepository>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<AnnotationCommandHandler>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton(this.loggerFixture.Factory.CreateLogger<StreamingJobCommandHandler>())
            .AddSingleton<IMetricsReporter, MetricsReporter>()
            .AddSingleton(Mock.Of<MetricsService>())
            .AddSingleton<StreamingJobOperatorService>()
            .AddSingleton(Options.Create(new StreamingJobOperatorServiceConfiguration
            {
                MaxBufferCapacity = 1000
            }))
            .AddSingleton(metricsReporterConfiguration)
            .AddSingleton<HostedStreamingJobOperatorService>()
            .AddSingleton(this.materializer)
            .AddSingleton(this.actorSystem)
            .BuildServiceProvider()
            .GetRequiredService<StreamingJobOperatorService>();
    }
}
