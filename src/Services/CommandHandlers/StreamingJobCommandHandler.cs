﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.Services.Base.Repositories.CustomResources;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Kubernetes;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.CommandHandlers;

/// <inheritdoc cref="ICommandHandler{T}" />
public class StreamingJobCommandHandler : ICommandHandler<StreamingJobCommand>
{
    private readonly IStreamClassRepository streamClassRepository;
    private readonly IKubeCluster kubeCluster;
    private readonly ILogger<StreamingJobCommandHandler> logger;
    private readonly IStreamingJobTemplateRepository streamingJobTemplateRepository;
    private readonly ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler;

    public StreamingJobCommandHandler(
        IStreamClassRepository streamClassRepository,
        IKubeCluster kubeCluster,
        ILogger<StreamingJobCommandHandler> logger,
        ICommandHandler<UpdateStatusCommand> updateStatusCommandHandler,
        IStreamingJobTemplateRepository streamingJobTemplateRepository)
    {
        this.streamClassRepository = streamClassRepository;
        this.kubeCluster = kubeCluster;
        this.logger = logger;
        this.streamingJobTemplateRepository = streamingJobTemplateRepository;
        this.updateStatusCommandHandler = updateStatusCommandHandler;
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

    private Task StartJob(IStreamDefinition streamDefinition, bool isBackfilling, IStreamClass streamClass)
    {
        var template = streamDefinition.GetJobTemplate(isBackfilling);
        return this.streamingJobTemplateRepository
            .GetStreamingJobTemplate(template.Kind, streamDefinition.Namespace(), template.Name)
            .Map(jobTemplate =>
            {
                if (!jobTemplate.HasValue)
                {
                    var message = $"Failed to find job template with kind {template.Kind} and name {template.Name}";
                    var command = new SetInternalErrorStatus(streamDefinition,
                        V1Alpha1StreamCondition.CustomErrorCondition(message));
                    return this.updateStatusCommandHandler.Handle(command);
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
                    .TryMap(result => result.AsOption(),
                        exception =>
                        {
                            this.logger.LogError(exception, "Failed to send job");
                            return Option<V1JobStatus>.None;
                        });
            });
    }
}
