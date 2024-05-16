using System;
using System.Threading.Tasks;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using k8s.Models;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

/// <inheritdoc cref="ICommandHandler{T}" />
public class StreamingJobCommandHandler : ICommandHandler<StreamingJobCommand>
{
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IStreamingJobOperatorService streamingJobOperatorService;

    public StreamingJobCommandHandler(
        IStreamClassRepository streamClassRepository,
        IStreamingJobOperatorService streamingJobOperatorService)
    {
        this.streamClassRepository = streamClassRepository;
        this.streamingJobOperatorService = streamingJobOperatorService;
    }

    /// <inheritdoc cref="ICommandHandler{T}.Handle" />
    public Task Handle(StreamingJobCommand command) => command switch
    {
        StartJob startJob => this.streamClassRepository
            .Get(startJob.streamDefinition.Namespace(), startJob.streamDefinition.Kind)
            .Map(maybeSc => maybeSc switch
            {
                { HasValue: true, Value: var sc } => this.streamingJobOperatorService.StartRegisteredStream(startJob.streamDefinition, startJob.IsBackfilling, sc),
                { HasValue: false } => throw new InvalidOperationException($"Stream class not found for {startJob.streamDefinition.Kind}"),
            }),
        StopJob stopJob => this.streamingJobOperatorService.DeleteJob(stopJob.streamKind, stopJob.streamId),
        _ => throw new ArgumentOutOfRangeException(nameof(command), command, null)
    };
}
