using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using k8s.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Snd.Sdk.Kubernetes;
using Snd.Sdk.Kubernetes.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Operator.Services.Streams;

public class StreamingJobOperatorService : IStreamingJobOperatorService
{
    private readonly StreamingJobOperatorServiceConfiguration configuration;
    private readonly IKubeCluster kubernetesService;
    private readonly ILogger<StreamingJobOperatorService> logger;
    private readonly IStreamingJobTemplateRepository streamingJobTemplateRepository;
    private readonly IStreamInteractionService streamInteractionService;

    public StreamingJobOperatorService(
        ILogger<StreamingJobOperatorService> logger,
        IOptions<StreamingJobOperatorServiceConfiguration> configuration,
        IKubeCluster kubernetesService,
        IStreamInteractionService streamInteractionService,
        IStreamingJobTemplateRepository streamingJobTemplateRepository)
    {
        this.logger = logger;
        this.configuration = configuration.Value;
        this.kubernetesService = kubernetesService;
        this.streamInteractionService = streamInteractionService;
        this.streamingJobTemplateRepository = streamingJobTemplateRepository;
    }


    public string StreamJobNamespace => this.configuration.Namespace;

    public Task<Option<V1Job>> GetStreamingJob(string streamId)
    {
        return this.kubernetesService.GetJob(streamId, this.StreamJobNamespace)
            .TryMap(job => job.AsOption(), exception =>
            {
                this.logger.LogWarning(exception, "Streaming job {streamId} not found", streamId);
                return Option<V1Job>.None;
            });
    }

    public Task<Option<StreamOperatorResponse>> StartRegisteredStream(IStreamDefinition streamDefinition, bool fullLoad)
    {
        var templateRefKind = fullLoad
            ? streamDefinition.ReloadingJobTemplateRef.Kind
            : streamDefinition.JobTemplateRef.Kind;
        var templateRefName = fullLoad
            ? streamDefinition.ReloadingJobTemplateRef.Name
            : streamDefinition.JobTemplateRef.Name;
        return this.streamingJobTemplateRepository
            .GetStreamingJobTemplate(templateRefKind, streamDefinition.Namespace(), templateRefName)
            .Map(jobTemplate =>
            {
                if (!jobTemplate.HasValue)
                {
                    return Task.FromResult(StreamOperatorResponse.OperationFailed(streamDefinition.Metadata.Namespace(),
                            streamDefinition.Kind,
                            streamDefinition.StreamId,
                            $"Failed to find job template with kind {templateRefKind} and name {templateRefName}")
                        .AsOption());
                }

                var job = jobTemplate
                    .Value
                    .GetJob()
                    .WithStreamingJobLabels(streamDefinition.StreamId, fullLoad, streamDefinition.Kind)
                    .WithStreamingJobAnnotations(streamDefinition.GetConfigurationChecksum())
                    .WithCustomEnvironment(streamDefinition.ToV1EnvFromSources())
                    .WithCustomEnvironment(streamDefinition.ToEnvironment(fullLoad))
                    .WithOwnerReference(streamDefinition)
                    .WithName(streamDefinition.StreamId);
                this.logger.LogInformation("Starting a new stream job with an id {streamId}",
                    streamDefinition.StreamId);
                return this.kubernetesService
                    .SendJob(job, streamDefinition.Metadata.Namespace(), CancellationToken.None)
                    .TryMap(
                        _ => fullLoad
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

    public Task<Option<StreamOperatorResponse>> RequestStreamingJobRestart(string streamId)
    {
        return this.SetStreamingJobAnnotation(streamId, Annotations.RESTARTING_STATE_ANNOTATION_VALUE)
            .Map(maybeSi
                => maybeSi.Select(job
                    => StreamOperatorResponse.Restarting(this.StreamJobNamespace, job.GetStreamKind(), streamId)));
    }

    public Task<Option<StreamOperatorResponse>> RequestStreamingJobTermination(string streamId)
    {
        return this.SetStreamingJobAnnotation(streamId, Annotations.TERMINATE_REQUESTED_STATE_ANNOTATION_VALUE)
            .Map(maybeSi
                => maybeSi.Select(job
                    => StreamOperatorResponse.Terminating(this.StreamJobNamespace, job.GetStreamKind(), streamId)));
    }

    public Task<Option<StreamOperatorResponse>> RequestStreamingJobReload(string streamId)
    {
        return this.SetStreamingJobAnnotation(streamId, Annotations.RELOADING_STATE_ANNOTATION_VALUE)
            .Map(maybeSi
                => maybeSi.Select(job
                    => StreamOperatorResponse.Terminating(this.StreamJobNamespace, job.GetStreamKind(), streamId)));
    }

    public Task<Option<StreamOperatorResponse>> DeleteJob(string kind, string streamId)
    {
        return this.kubernetesService.DeleteJob(streamId, this.StreamJobNamespace)
            .Map(_ => StreamOperatorResponse.Suspended(this.StreamJobNamespace, kind, streamId).AsOption());
    }

    public Task<Option<StreamOperatorResponse>> FindAndStopStreamingJob(string kind, string streamId)
    {
        var labels = new Dictionary<string, string> { { V1JobExtensions.STREAM_ID_LABEL, streamId } };
        return this.kubernetesService
            .GetPods(labels, this.configuration.Namespace)
            .Map(pods => this.StopActivePod(pods, kind, streamId))
            .Flatten();
    }

    private async Task<Option<StreamOperatorResponse>> StopActivePod(IEnumerable<V1Pod> pods, string kind,
        string streamId)
    {
        var activePods = pods.Where(p =>
            p.Status != null
            && p.Status.Phase.Equals("running", StringComparison.OrdinalIgnoreCase)
            && !string.IsNullOrEmpty(p.Status.PodIP)).ToList();
        if (activePods is { Count: 1 })
        {
            try
            {
                await this.streamInteractionService
                    .SendStopRequest(activePods.Single().Status.PodIP);
                return StreamOperatorResponse.Stopped(this.StreamJobNamespace, kind, streamId);
            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Failed to send stop request to pod with stream id: {streamId}", streamId);
                return StreamOperatorResponse.OperationFailed(this.StreamJobNamespace,
                    kind,
                    streamId,
                    "Stopping the stream failed");
            }
        }

        if (activePods is { Count: < 1 })
        {
            this.logger.LogWarning("Cannot find active pod for stream id {streamId}", streamId);
            return StreamOperatorResponse.OperationFailed(this.StreamJobNamespace,
                kind,
                streamId,
                "Stopping the stream failed: no running pods found");
        }

        this.logger.LogError("Found multiple active pods for stream id {streamId}: {count}", streamId,
            activePods.Count);
        return StreamOperatorResponse.OperationFailed(this.StreamJobNamespace,
            kind,
            streamId,
            "Stopping the stream failed: multiple running pods found");
    }

    private Task<Option<V1Job>> SetStreamingJobAnnotation(string streamId, string annotationValue)
    {
        return this.kubernetesService.AnnotateJob(streamId, this.configuration.Namespace,
                Annotations.STATE_ANNOTATION_KEY, annotationValue)
            .TryMap(job => job.AsOption(),
                exception =>
                {
                    this.logger.LogError(exception, "Failed request {streamId} termination", streamId);
                    return Option<V1Job>.None;
                });
    }
}
