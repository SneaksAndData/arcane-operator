using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;
using Arcane.Operator.StreamingJobLifecycle;

namespace Arcane.Operator.Models.Commands;

/// <summary>
/// Possible stream states.
/// </summary>
public enum StreamPhase
{
    /// <summary>
    /// A running stream.
    /// </summary>
    RUNNING,

    /// <summary>
    /// A stream that is in a data backfill process.
    /// </summary>
    RELOADING,

    /// <summary>
    /// A stream that had been suspended.
    /// </summary>
    SUSPENDED,

    /// <summary>
    /// A stream that has failed and cannot be automatically recovered.
    /// </summary>
    FAILED
}

/// <summary>
/// Abstract class for stream definition commands
/// </summary>
public abstract record StreamDefinitionCommand : KubernetesCommand;

/// <summary>
/// Update the stream definition status
/// </summary>
/// <param name="affectedResource"></param>
/// <param name="conditions"></param>
/// <param name="phase"></param>
public abstract record UpdateStatusCommand(IStreamDefinition affectedResource,
    V1Alpha1StreamCondition[] conditions,
    StreamPhase phase) : StreamDefinitionCommand;

/// <summary>
/// Abstract class for setting error status
/// </summary>
/// <param name="affectedResource"></param>
public abstract record SetErrorStatus(IStreamDefinition affectedResource) : UpdateStatusCommand(affectedResource,
    V1Alpha1StreamCondition.ErrorCondition,
    StreamPhase.FAILED);

/// <summary>
/// Abstract class for setting error status
/// </summary>
/// <param name="affectedResource"></param>
public abstract record SetCustomErrorStatus(IStreamDefinition affectedResource, V1Alpha1StreamCondition[] conditions) : UpdateStatusCommand(affectedResource,
    conditions,
    StreamPhase.FAILED);

/// <summary>
/// Abstract class for setting error status
/// </summary>
/// <param name="affectedResource"></param>
public abstract record SetWarningStatus(IStreamDefinition affectedResource, StreamPhase phase) : UpdateStatusCommand(affectedResource,
    V1Alpha1StreamCondition.WarningCondition, phase);

/// <summary>
/// Abstract class for setting error status
/// </summary>
/// <param name="affectedResource"></param>
public abstract record SetReadyStatus(IStreamDefinition affectedResource, StreamPhase phase) : UpdateStatusCommand(affectedResource,
    V1Alpha1StreamCondition.ReadyCondition, phase);

/// <summary>
/// Sets the stream definition status to CrashLoop
/// </summary>
/// <param name="affectedResource"></param>
public record SetCrashLoopStatusCommand(IStreamDefinition affectedResource) : SetErrorStatus(affectedResource);

/// <summary>
/// Used to set error statuses in case of internal stream operator errors.
/// </summary>
/// <param name="affectedResource"></param>
public record SetInternalErrorStatus(IStreamDefinition affectedResource, V1Alpha1StreamCondition[] conditions) : SetCustomErrorStatus(affectedResource, conditions);

/// <summary>
/// Sets the stream definition status to Suspended
/// </summary>
/// <param name="affectedResource"></param>
public record Suspended(IStreamDefinition affectedResource) : SetWarningStatus(affectedResource, StreamPhase.SUSPENDED);

/// <summary>
/// Sets the stream definition status to Reloading
/// </summary>
/// <param name="affectedResource"></param>
public record Reloading(IStreamDefinition affectedResource) : SetReadyStatus(affectedResource, StreamPhase.RELOADING);

/// <summary>
/// Sets the stream definition status to Running
/// </summary>
/// <param name="affectedResource"></param>
public record Running(IStreamDefinition affectedResource) : SetReadyStatus(affectedResource, StreamPhase.RUNNING);

/// <summary>
/// Sets the stream definition annotation to indicate that the stream is in a crash loop
/// </summary>
/// <param name="affectedResource">The resource to update</param>
public record SetCrashLoopStatusAnnotationCommand(IStreamDefinition affectedResource) :
    SetAnnotationCommand<IStreamDefinition>(
    affectedResource,
    Annotations.STATE_ANNOTATION_KEY,
    Annotations.CRASH_LOOP_STATE_ANNOTATION_VALUE);

/// <summary>
/// Removes the stream definition annotation to indicate that the stream is should restart in backfill mode
/// </summary>
/// <param name="affectedResource">The resource to update</param>
public record RemoveReloadRequestedAnnotation(IStreamDefinition affectedResource) :
    RemoveAnnotationCommand<IStreamDefinition>(
    affectedResource,
    Annotations.STATE_ANNOTATION_KEY);
