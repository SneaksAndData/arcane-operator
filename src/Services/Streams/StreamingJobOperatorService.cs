using System.Threading;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Configurations;
using Arcane.Operator.Extensions;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
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

    public StreamingJobOperatorService(
        ILogger<StreamingJobOperatorService> logger,
        IOptions<StreamingJobOperatorServiceConfiguration> configuration,
        IKubeCluster kubernetesService,
        IStreamingJobTemplateRepository streamingJobTemplateRepository)
    {
        this.logger = logger;
        this.configuration = configuration.Value;
        this.kubernetesService = kubernetesService;
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

    public Task<Option<StreamOperatorResponse>> StartRegisteredStream(IStreamDefinition streamDefinition, bool fullLoad,
        IStreamClass streamClass)
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
                    .WithCustomEnvironment(streamDefinition.ToV1EnvFromSources(streamClass))
                    .WithCustomEnvironment(streamDefinition.ToEnvironment(fullLoad, streamClass))
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
