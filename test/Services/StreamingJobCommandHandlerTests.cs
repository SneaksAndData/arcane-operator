using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Util.Extensions;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using Arcane.Operator.Services.CommandHandlers;
using Arcane.Operator.Tests.Extensions;
using Arcane.Operator.Tests.Fixtures;
using Arcane.Operator.Tests.Services.TestCases;
using k8s.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Snd.Sdk.Kubernetes.Base;
using Xunit;
using static Arcane.Operator.Tests.Services.TestCases.StreamDefinitionTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamClassTestCases;
using static Arcane.Operator.Tests.Services.TestCases.StreamingJobTemplateTestCases;

namespace Arcane.Operator.Tests.Services;

public class StreamingJobCommandHandlerTests(LoggerFixture loggerFixture) : IClassFixture<LoggerFixture>,
    IClassFixture<AkkaFixture>
{
    // Akka service and test helpers

    // Mocks
    private readonly Mock<IKubeCluster> kubeClusterMock = new();
    private readonly Mock<IStreamClassRepository> streamClassRepositoryMock = new();
    private readonly Mock<IStreamingJobTemplateRepository> streamingJobTemplateRepositoryMock = new();

    [Fact]
    public async Task HandleStreamStopCommand()
    {
        // Arrange
        var command = new StopJob("job-name", "job-namespace");
        var service = this.CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();

        // Act
        await service.Handle(command);

        // Assert
        this.kubeClusterMock.Verify(k => k.DeleteJob(command.name,
            command.nameSpace,
            It.IsAny<CancellationToken>(),
            It.IsAny<PropagationPolicy>()));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandleStreamStartCommand(bool isBackfilling)
    {
        // Arrange
        var command = new StartJob(StreamDefinition, isBackfilling);
        var service = this.CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();
        this.streamClassRepositoryMock
            .Setup(scr => scr.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        this.streamingJobTemplateRepositoryMock
            .Setup(sjtr => sjtr.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(((IStreamingJobTemplate)StreamingJobTemplate).AsOption());
        var expectedState = isBackfilling ? StreamPhase.RELOADING.ToString() : StreamPhase.RUNNING.ToString();

        // Act
        await service.Handle(command);

        // Assert
        this.kubeClusterMock.Verify(k =>
            k.SendJob(It.Is<V1Job>(job => job.IsBackfilling() == isBackfilling), It.IsAny<string>(), It.IsAny<CancellationToken>()));
        this.kubeClusterMock.Verify(k => k.UpdateCustomResourceStatus(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                StreamDefinition.Namespace(),
                StreamDefinition.Name(),
                It.Is<V1Alpha1StreamStatus>(s => s.Phase == expectedState),
                It.IsAny<Func<JsonElement, It.IsAnyType>>()),
            Times.Once);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandleFailedStreamTemplate(bool isBackfilling)
    {
        // Arrange
        var command = new StartJob(StreamDefinition, isBackfilling);
        var service = this.CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();
        this.streamClassRepositoryMock
            .Setup(scr => scr.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());
        this.streamingJobTemplateRepositoryMock
            .Setup(sjtr => sjtr.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(((IStreamingJobTemplate)new FailedStreamingJobTemplate(new Exception())).AsOption());

        // Act
        await service.Handle(command);

        // Assert
        this.kubeClusterMock.Verify(k => k.UpdateCustomResourceStatus(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                StreamDefinition.Namespace(),
                StreamDefinition.Name(),
                It.Is<V1Alpha1StreamStatus>(s => s.Phase == StreamPhase.FAILED.ToString()),
                It.IsAny<Func<JsonElement, It.IsAnyType>>()),
            Times.Once);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandleFailedSendJob(bool isBackfilling)
    {
        // Arrange
        var command = new StartJob(StreamDefinition, isBackfilling);
        var service = this.CreateServiceProvider().GetRequiredService<ICommandHandler<StreamingJobCommand>>();

        this.streamClassRepositoryMock
            .Setup(scr => scr.Get(It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(StreamClass.AsOption());

        this.streamingJobTemplateRepositoryMock
            .Setup(sjtr => sjtr.GetStreamingJobTemplate(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
            .ReturnsAsync(((IStreamingJobTemplate)new FailedStreamingJobTemplate(new Exception())).AsOption());

        this.kubeClusterMock
            .Setup(k => k.SendJob(It.IsAny<V1Job>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Throws<Exception>();

        // Act
        await service.Handle(command);

        // Assert
        this.kubeClusterMock.Verify(k => k.UpdateCustomResourceStatus(It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                StreamDefinition.Namespace(),
                StreamDefinition.Name(),
                It.Is<V1Alpha1StreamStatus>(s => s.Phase == StreamPhase.FAILED.ToString()),
                It.IsAny<Func<JsonElement, It.IsAnyType>>()),
            Times.Once);
    }

    private ServiceProvider CreateServiceProvider()
    {
        return new ServiceCollection()
            .AddSingleton(this.kubeClusterMock.Object)
            .AddSingleton(this.streamClassRepositoryMock.Object)
            .AddSingleton(this.streamingJobTemplateRepositoryMock.Object)
            .AddSingleton(loggerFixture.Factory.CreateLogger<StreamingJobCommandHandler>())
            .AddSingleton(loggerFixture.Factory.CreateLogger<UpdateStatusCommandHandler>())
            .AddSingleton<ICommandHandler<UpdateStatusCommand>, UpdateStatusCommandHandler>()
            .AddSingleton<ICommandHandler<StreamingJobCommand>, StreamingJobCommandHandler>()
            .BuildServiceProvider();
    }
}
