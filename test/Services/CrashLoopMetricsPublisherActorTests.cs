using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit.Xunit2;
using Arcane.Operator.Configurations;
using Arcane.Operator.Services.Metrics.Actors;
using Moq;
using Snd.Sdk.Metrics.Base;
using Xunit;

namespace Arcane.Operator.Tests.Services;

public class CrashLoopMetricsPublisherActorTests : TestKit
{
    // Akka service and test helpers
    private readonly TaskCompletionSource tcs = new();
    private readonly CancellationTokenSource cts = new();


    // Mocks
    private readonly Mock<MetricsService> metricsServiceMock = new();

    public CrashLoopMetricsPublisherActorTests()
    {
        this.cts.CancelAfter(TimeSpan.FromSeconds(5));
        this.cts.Token.Register(this.tcs.SetResult);
    }

    [Fact]
    public async Task TestMetricsAdded()
    {
        // Arrange
        this.metricsServiceMock.Setup(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>())).Callback(() => this.tcs.TrySetResult());
        var subject = this.CreateSubject(null);

        // Act
        subject.Tell(new AddSCrashLoopMetricsMessage("test", "test", new()));
        subject.Tell(new AddSCrashLoopMetricsMessage("test2", "test", new()));
        subject.Tell(new EmitCrashLoopMetricsMessage());
        await this.tcs.Task;

        // Assert
        this.metricsServiceMock.Verify(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>()), Times.Exactly(2));
    }

    [Fact]
    public async Task TestBrokenMetricsService()
    {
        // Arrange
        this.metricsServiceMock.Setup(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>())).Callback(() => this.tcs.TrySetResult());
        this.metricsServiceMock.Setup(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>())).Throws(new Exception("Test exception"));

        var subject = this.CreateSubject(null);

        // Act
        subject.Tell(new AddSCrashLoopMetricsMessage("test", "test", new()));
        subject.Tell(new EmitCrashLoopMetricsMessage());
        subject.Tell(new EmitCrashLoopMetricsMessage());
        await this.tcs.Task;

        // Assert
        this.metricsServiceMock.Verify(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>()), Times.AtLeast(2));
    }

    [Fact]
    public async Task TestRemoveMetricsMessage()
    {
        // Arrange
        var subject = this.CreateSubject(this.tcs);

        // Act
        subject.Tell(new AddSCrashLoopMetricsMessage("test", "test", new()));
        subject.Tell(new EmitCrashLoopMetricsMessage());
        subject.Tell(new RemoveCrashLoopMetricsMessage("test"));
        subject.Tell(new EmitCrashLoopMetricsMessage());
        subject.Tell(PoisonPill.Instance);
        await this.tcs.Task;

        // Assert
        this.metricsServiceMock.Verify(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>()), Times.Exactly(1));
    }

    [Fact]
    public async Task TestRemoveNonExisingMetric()
    {
        // Arrange
        var subject = this.CreateSubject(this.tcs);

        // Act
        subject.Tell(new AddSCrashLoopMetricsMessage("test", "test", new()));
        subject.Tell(new EmitCrashLoopMetricsMessage());
        subject.Tell(new RemoveCrashLoopMetricsMessage("not-exists"));
        subject.Tell(new EmitCrashLoopMetricsMessage());
        subject.Tell(PoisonPill.Instance);
        await this.tcs.Task;

        // Assert
        this.metricsServiceMock.Verify(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>()), Times.Exactly(2));
    }

    [Fact]
    public async Task TestBrokenMessage()
    {
        // Arrange
        var subject = this.CreateSubject(this.tcs);

        // Act
        subject.Tell(new AddSCrashLoopMetricsMessage("test", "test", new()));
        subject.Tell(new AddSCrashLoopMetricsMessage(null, null, null));
        subject.Tell(new EmitCrashLoopMetricsMessage());
        subject.Tell(PoisonPill.Instance);
        await this.tcs.Task;

        // Assert
        this.metricsServiceMock.Verify(ms => ms.Count(It.IsAny<string>(),
            It.IsAny<int>(),
            It.IsAny<SortedDictionary<string, string>>()), Times.Exactly(1));
    }

    private IActorRef CreateSubject(TaskCompletionSource taskCompletionSource)
    {
        var config = new MetricsPublisherActorConfiguration
        {
            InitialDelay = TimeSpan.Zero,
            UpdateInterval = TimeSpan.FromSeconds(10),
        };
        return this.Sys.ActorOf(Props.Create(() =>
            new TestMetricsPublisherActor(config, this.metricsServiceMock.Object, taskCompletionSource)));
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        this.cts?.Dispose();
    }

    private class TestMetricsPublisherActor(
        MetricsPublisherActorConfiguration configuration,
        MetricsService metricsService,
        TaskCompletionSource tcs)
        : CrashLoopMetricsPublisherActor(configuration, metricsService)
    {
        protected override void PostStop()
        {
            tcs?.TrySetResult();
        }
    }
}
