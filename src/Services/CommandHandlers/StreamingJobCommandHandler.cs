using System;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Extensions;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

/// <inheritdoc cref="ICommandHandler{T}" />
public class StreamingJobCommandHandler : IStreamingJobCommandHandler
{
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IStreamingJobOperatorService streamingJobOperatorService;
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobCommandHandler> logger;

    public StreamingJobCommandHandler(
        IStreamClassRepository streamClassRepository,
        IStreamingJobOperatorService streamingJobOperatorService,
        IKubeCluster kubeCluster, ILogger<StreamingJobCommandHandler> logger)
    {
        this.streamClassRepository = streamClassRepository;
        this.streamingJobOperatorService = streamingJobOperatorService;
        this.kubeCluster = kubeCluster;
        this.logger = logger;
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
        StopJob stopJob => this.kubeCluster.DeleteJob(stopJob.name, stopJob.nameSpace),
        _ => throw new ArgumentOutOfRangeException(nameof(command), command, null)
    };

    public Task Handle(SetAnnotationCommand<V1Job> command)
    {
        return this.kubeCluster.AnnotateJob(command.affectedResource.Name(), command.affectedResource.Namespace(),
                command.annotationKey, command.annotationValue)
            .TryMap(job => job.AsOption(),
                exception =>
                {
                    this.logger.LogError(exception, "Failed to annotate {streamId} with {annotationKey}:{annotationValue}", 
                        command.affectedResource, command.annotationKey, command.annotationValue);
                    return Option<V1Job>.None;
                });
    }
}
