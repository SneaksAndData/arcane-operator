using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Commands;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

/// <inheritdoc cref="ICommandHandler{T}" />
public class StreamingJobCommandHandler : IStreamingJobCommandHandler
{
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobCommandHandler> logger;
    private readonly IStreamingJobTemplateRepository streamingJobTemplateRepository;

    public StreamingJobCommandHandler(
        IStreamClassRepository streamClassRepository,
        IKubeCluster kubeCluster,
        ILogger<StreamingJobCommandHandler> logger,
        IStreamingJobTemplateRepository streamingJobTemplateRepository)
    {
        this.streamClassRepository = streamClassRepository;
        this.kubeCluster = kubeCluster;
        this.logger = logger;
        this.streamingJobTemplateRepository = streamingJobTemplateRepository;
    }

    /// <inheritdoc cref="ICommandHandler{T}.Handle" />
    public Task Handle(StreamingJobCommand command) => command switch
    {
        StartJob startJob => this.streamClassRepository
            .Get(startJob.streamDefinition.Namespace(), startJob.streamDefinition.Kind)
            .Map(maybeSc => maybeSc switch
            {
                { HasValue: true, Value: var sc } => this.StartJob(startJob.streamDefinition, startJob.IsBackfilling, sc),
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

    private Task StartJob(IStreamDefinition streamDefinition, bool isBackfilling, IStreamClass streamClass)
    {
        var template = streamDefinition.GetJobTemplate(isBackfilling);
        return this.streamingJobTemplateRepository
            .GetStreamingJobTemplate(template.Kind, streamDefinition.Namespace(), template.Name)
            .Map(jobTemplate =>
            {
                if (!jobTemplate.HasValue)
                {
                    return Task.FromResult(StreamOperatorResponse.OperationFailed(streamDefinition.Metadata.Namespace(),
                            streamDefinition.Kind,
                            streamDefinition.StreamId,
                            $"Failed to find job template with kind {template.Kind} and name {template.Name}")
                        .AsOption());
                }

                var job = jobTemplate
                    .Value
                    .GetJob()
                    .WithStreamingJobLabels(streamDefinition.StreamId, isBackfilling, streamDefinition.Kind)
                    .WithStreamingJobAnnotations(streamDefinition.GetConfigurationChecksum())
                    .WithMetadataAnnotations(streamClass)
                    .WithCustomEnvironment(streamDefinition.ToV1EnvFromSources(streamClass))
                    .WithCustomEnvironment(streamDefinition.ToEnvironment(isBackfilling, streamClass))
                    .WithOwnerReference(streamDefinition)
                    .WithName(streamDefinition.StreamId);
                this.logger.LogInformation("Starting a new stream job with an id {streamId}",
                    streamDefinition.StreamId);
                return this.kubeCluster
                    .SendJob(job, streamDefinition.Metadata.Namespace(), CancellationToken.None)
                    .TryMap(
                        _ => isBackfilling
                            ? StreamOperatorResponse.Reloading(streamDefinition.Metadata.Namespace(),
                                streamDefinition.Kind,
                                streamDefinition.StreamId)
                            : StreamOperatorResponse.Running(streamDefinition.Metadata.Namespace(),
                                streamDefinition.Kind,
                                streamDefinition.StreamId),
                        exception =>
                        {
                            this.logger.LogError(exception, "Failed to send job");
                            return Option<StreamOperatorResponse>.None;
                        });
            })
            .Flatten();
    }
}
