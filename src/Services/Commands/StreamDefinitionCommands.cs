﻿using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;

namespace Arcane.Operator.Services.Commands;

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
    V1Beta1StreamCondition[] conditions,
    StreamPhase phase) : StreamDefinitionCommand;

/// <summary>
/// Abstract class for setting error status
/// </summary>
/// <param name="affectedResource"></param>
public abstract record SetErrorStatus(IStreamDefinition affectedResource) : UpdateStatusCommand(affectedResource,
    V1Beta1StreamCondition.ErrorCondition,
    StreamPhase.FAILED);

/// <summary>
/// Abstract class for setting error status
/// </summary>
/// <param name="affectedResource"></param>
public abstract record SetWarningStatus(IStreamDefinition affectedResource, StreamPhase phase) : UpdateStatusCommand(affectedResource,
    V1Beta1StreamCondition.WarningCondition, phase);

/// <summary>
/// Abstract class for setting error status
/// </summary>
/// <param name="affectedResource"></param>
public abstract record SetReadyStatus(IStreamDefinition affectedResource, StreamPhase phase) : UpdateStatusCommand(affectedResource,
    V1Beta1StreamCondition.ReadyCondition, phase);

/// <summary>
/// Sets the stream definition status to CrashLoop
/// </summary>
/// <param name="affectedResource"></param>
public record SetCrashLoopStatusCommand(IStreamDefinition affectedResource) : SetErrorStatus(affectedResource);

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
public record Running(IStreamDefinition affectedResource) : SetReadyStatus(affectedResource, StreamPhase.RELOADING);

/// <summary>
/// Abstract class for setting annotation on a stream definition Kubernetes object
/// </summary>
/// <param name="affectedResource">The resource to update</param>
/// <param name="annotationKey">Annotation key</param>
/// <param name="annotationValue">Annotation value</param>
public abstract record SetAnnotationCommand(IStreamDefinition affectedResource, string annotationKey, string annotationValue) : StreamDefinitionCommand;

/// <summary>
/// Abstract class for setting annotation on a stream definition Kubernetes object
/// </summary>
/// <param name="affectedResource">The resource to update</param>
/// <param name="annotationKey">Annotation key</param>
public abstract record RemoveAnnotationCommand(IStreamDefinition affectedResource, string annotationKey) : StreamDefinitionCommand;

/// <summary>
/// Sets the stream definition annotation to indicate that the stream is in a crash loop
/// </summary>
/// <param name="affectedResource">The resource to update</param>
public record SetCrashLoopStatusAnnotationCommand(IStreamDefinition affectedResource) : SetAnnotationCommand(
    affectedResource,
    Annotations.STATE_ANNOTATION_KEY,
    Annotations.CRASH_LOOP_STATE_ANNOTATION_VALUE);

/// <summary>
/// Sets the stream definition annotation to indicate that the stream is in a crash loop
/// </summary>
/// <param name="affectedResource">The resource to update</param>
public record RemoveReloadRequestedAnnotation(IStreamDefinition affectedResource) : RemoveAnnotationCommand(
    affectedResource,
    Annotations.STATE_ANNOTATION_KEY);
