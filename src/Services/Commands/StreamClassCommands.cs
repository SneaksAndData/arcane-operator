using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Models;

namespace Arcane.Operator.Services.Commands;

/// <summary>
/// Update the stream definition status
/// </summary>
/// <param name="request"></param>
/// <param name="conditions"></param>
/// <param name="phase"></param>
public abstract record SetStreamClassStatusCommand(string resourceName,
    CustomResourceApiRequest request, V1Beta1StreamCondition[] conditions,
    StreamClassPhase phase, IStreamClass streamClass) : SetResourceStatusCommand<V1Beta1StreamCondition, StreamClassPhase>(request, conditions, phase);

/// <summary>
/// Update the stream definition status
/// </summary>
/// <param name="request"></param>
public record SetStreamClassReady(string resourceName, CustomResourceApiRequest request, IStreamClass streamClass)
    : SetStreamClassStatusCommand(resourceName, request, V1Beta1StreamCondition.ReadyCondition, StreamClassPhase.READY, streamClass);

/// <summary>
/// Update the stream definition status
/// </summary>
/// <param name="request"></param>
public record SetStreamClassStopped(string resourceName, CustomResourceApiRequest request, IStreamClass streamClass)
    : SetStreamClassStatusCommand(resourceName, request, V1Beta1StreamCondition.WarningCondition, StreamClassPhase.STOPPED, streamClass);
