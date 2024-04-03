using Arcane.Operator.Services.Base;
using Moq;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Operator.Tests.Fixtures;

public class ServiceFixture
{
    public Mock<IKubeCluster> MockKubeCluster { get; } = new();

    public Mock<IStreamInteractionService> MockStreamInteractionService { get; } = new();

    public Mock<IStreamingJobOperatorService> MockStreamingJobOperatorService { get; } = new();

    public Mock<IStreamDefinitionRepository> MockStreamDefinitionRepository { get; } = new();
}
