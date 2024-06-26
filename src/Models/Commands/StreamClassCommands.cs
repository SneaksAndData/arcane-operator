using Arcane.Operator.Models.Api;
using Arcane.Operator.Models.Base;
using Arcane.Operator.Models.Resources;
using Arcane.Operator.Models.Resources.Status.V1Alpha1;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Base.CommandHandlers;

namespace Arcane.Operator.Models.Commands;

/// <summary>
/// Possible stream class lifecycle phases.
/// </summary>
public enum StreamClassPhase
{
    /// <summary>
    /// A ready streaming class is ready to be used and new Streams of this class can be created.
    /// </summary>
    READY,

    /// <summary>
    /// An error occured in stream class controller and new Streams of this class can not be created.
    /// </summary>
    FAILED,

    /// <summary>
    /// The stream class is stopped and new Streams of this class can not be created.
    /// </summary>
    STOPPED
}

/// <summary>
/// Base class for stream class status update commands
/// </summary>
/// <param name="resourceName">Affected resource name</param>
/// <param name="request">Resource metadata required for the Kubernetes Custom Resource APIs</param>
/// <param name="conditions">Resource conditions</param>
/// <param name="phase">Resource phase</param>
/// <param name="streamClass">Affected resource</param>
public abstract record SetStreamClassStatusCommand(string resourceName,
    CustomResourceApiRequest request, V1Alpha1StreamCondition[] conditions,
    StreamClassPhase phase, IStreamClass streamClass) : SetResourceStatusCommand<V1Alpha1StreamCondition, StreamClassPhase>(request, conditions, phase);

/// <summary>
/// Update the stream class status command to Ready
/// </summary>
/// <param name="resourceName">Affected resource name</param>
/// <param name="request">Resource metadata required for the Kubernetes Custom Resource APIs</param>
/// <param name="streamClass">Affected resource</param>
public record SetStreamClassReady(string resourceName, CustomResourceApiRequest request, IStreamClass streamClass)
    : SetStreamClassStatusCommand(resourceName, request, V1Alpha1StreamCondition.ReadyCondition, StreamClassPhase.READY, streamClass);

/// <summary>
/// Update the stream class status command to Stopped
/// </summary>
/// <param name="resourceName">Affected resource name</param>
/// <param name="request">Resource metadata required for the Kubernetes Custom Resource APIs</param>
/// <param name="streamClass">Affected resource</param>
public record SetStreamClassStopped(string resourceName, CustomResourceApiRequest request, IStreamClass streamClass)
    : SetStreamClassStatusCommand(resourceName, request, V1Alpha1StreamCondition.WarningCondition, StreamClassPhase.STOPPED, streamClass);

