using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using k8s.Models;

namespace Arcane.Operator.Services.Commands;

/// <summary>
/// Base class for streaming job commands
/// </summary>
public abstract record StreamingJobCommand : KubernetesCommand;

/// <summary>
/// Start a streaming job
/// </summary>
/// <param name="streamDefinition">Definition to use to generate steaming job</param>
/// <param name="IsBackfilling">True if job should be started in backfill mode</param>
public record StartJob(IStreamDefinition streamDefinition, bool IsBackfilling) : StreamingJobCommand;

/// <summary>
/// Stop a streaming job
/// </summary>
/// <param name="streamKind">Stream kind</param>
/// <param name="streamId">Id of the stream</param>
public record StopJob(string streamKind, string streamId) : StreamingJobCommand;

/// <summary>
/// Request a streaming job to restart
/// </summary>
/// <param name="affectedResource">Job object</param>
public record RequestJobRestartCommand(V1Job affectedResource) : SetAnnotationCommand<V1Job>(affectedResource,
    Annotations.STATE_ANNOTATION_KEY,
    Annotations.RESTARTING_STATE_ANNOTATION_VALUE);

/// <summary>
/// Request a streaming job to restart in backfill mode
/// </summary>
/// <param name="affectedResource">Job object</param>
public record RequestJobReloadCommand(V1Job affectedResource) : SetAnnotationCommand<V1Job>(affectedResource,
    Annotations.STATE_ANNOTATION_KEY,
    Annotations.RELOADING_STATE_ANNOTATION_VALUE);
