using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;

namespace Arcane.Operator.Services.Commands;

/// <summary>
/// Base class for stream class status update commands
/// </summary>
/// <param name="resourceName">Affected resource name</param>
/// <param name="request">Resource metadata required for the Kubernetes Custom Resource APIs</param>
/// <param name="conditions">Resource conditions</param>
/// <param name="phase">Resource phase</param>
/// <param name="streamClass">Affected resource</param>
public abstract record SetStreamClassStatusCommand(string resourceName,
    CustomResourceApiRequest request, V1Beta1StreamCondition[] conditions,
    StreamClassPhase phase, IStreamClass streamClass) : SetResourceStatusCommand<V1Beta1StreamCondition, StreamClassPhase>(request, conditions, phase);

/// <summary>
/// Update the stream class status command to Ready
/// </summary>
/// <param name="resourceName">Affected resource name</param>
/// <param name="request">Resource metadata required for the Kubernetes Custom Resource APIs</param>
/// <param name="streamClass">Affected resource</param>
public record SetStreamClassReady(string resourceName, CustomResourceApiRequest request, IStreamClass streamClass)
    : SetStreamClassStatusCommand(resourceName, request, V1Beta1StreamCondition.ReadyCondition, StreamClassPhase.READY, streamClass);

/// <summary>
/// Update the stream class status command to Stopped
/// </summary>
/// <param name="resourceName">Affected resource name</param>
/// <param name="request">Resource metadata required for the Kubernetes Custom Resource APIs</param>
/// <param name="streamClass">Affected resource</param>
public record SetStreamClassStopped(string resourceName, CustomResourceApiRequest request, IStreamClass streamClass)
    : SetStreamClassStatusCommand(resourceName, request, V1Beta1StreamCondition.WarningCondition, StreamClassPhase.STOPPED, streamClass);
