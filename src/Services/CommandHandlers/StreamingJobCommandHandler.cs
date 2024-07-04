using System;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Util;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models.Commands;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
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
            .FlatMap(t => this.TryStartJobFromTemplate(t, streamDefinition, streamClass, isBackfilling, template))
            .FlatMap(async command =>
            {
                await this.updateStatusCommandHandler.Handle(command);
                return NotUsed.Instance;
            });
    }

    private Task<UpdateStatusCommand> TryStartJobFromTemplate(Option<IStreamingJobTemplate> jobTemplate,
        IStreamDefinition streamDefinition,
        IStreamClass streamClass,
        bool isBackfilling,
        V1TypedLocalObjectReference reference)
    {
        if (!jobTemplate.HasValue)
        {
            var message = $"Failed to find job template with kind {reference.Kind} and name {reference.Name}";
            var condition = V1Alpha1StreamCondition.CustomErrorCondition(message);
            var command1 = new SetInternalErrorStatus(streamDefinition, condition);
            return Task.FromResult<UpdateStatusCommand>(command1);
        }


        try
        {
            var job = this.BuildJob(jobTemplate, streamDefinition, streamClass, isBackfilling);
            this.logger.LogInformation("Starting a new stream job with an id {streamId}", streamDefinition.StreamId);
            return this.kubeCluster
                .SendJob(job, streamDefinition.Metadata.Namespace(), CancellationToken.None)
                .TryMap(_ => new Running(streamDefinition), ex => this.HandleError(ex, streamDefinition));
        }
        catch (Exception ex)
        {
            var condition = V1Alpha1StreamCondition.CustomErrorCondition($"Failed to build job: {ex.Message}");
            return Task.FromResult<UpdateStatusCommand>(new SetInternalErrorStatus(streamDefinition, condition));
        }
    }

    private UpdateStatusCommand HandleError(Exception exception, IStreamDefinition streamDefinition)
    {
        this.logger.LogError(exception, "Failed to send job");
        var condition = V1Alpha1StreamCondition.CustomErrorCondition($"Failed to start job: {exception.Message}");
        return new SetInternalErrorStatus(streamDefinition, condition);
    }

    private V1Job BuildJob(Option<IStreamingJobTemplate> jobTemplate, IStreamDefinition streamDefinition,
        IStreamClass streamClass, bool isBackfilling) =>
        jobTemplate
            .Value
            .GetJob()
            .WithStreamingJobLabels(streamDefinition.StreamId, isBackfilling, streamDefinition.Kind)
            .WithStreamingJobAnnotations(streamDefinition.GetConfigurationChecksum())
            .WithMetadataAnnotations(streamClass)
            .WithCustomEnvironment(streamDefinition.ToV1EnvFromSources(streamClass))
            .WithCustomEnvironment(streamDefinition.ToEnvironment(isBackfilling, streamClass))
            .WithOwnerReference(streamDefinition)
            .WithName(streamDefinition.StreamId);
}
