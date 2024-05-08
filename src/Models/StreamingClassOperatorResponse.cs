using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Models.Common;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamStatuses.StreamStatus.V1Beta1;

namespace Arcane.Operator.Models;

/// <summary>
/// Possible stream states.
/// </summary>
public enum StreamClassPhase
{
    /// <summary>
    /// An initial state of the StreamClass object.
    /// </summary>
    INITIALIZING,

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
/// The StreamClassPhase extension methods.
/// </summary>
public static class StreamClassPhaseExtensions
{
    /// <summary>
    /// Returns True if the lifecycle phase is final.
    /// </summary>
    /// <param name="phase">Phase</param>
    /// <returns></returns>
    public static bool IsFinal(this StreamClassPhase phase)
    {
        return phase is StreamClassPhase.FAILED or StreamClassPhase.STOPPED;
    }
}

/// <summary>
/// Contains response from stream operator that can be used by other services inside the application
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public record StreamClassOperatorResponse
{
    /// <summary>
    /// Affected StreamClass object
    /// </summary>
    public IStreamClass StreamClass { get; set; }

    /// <summary>
    /// Latest observed state of the StreamClass object
    /// </summary>
    public IEnumerable<V1Beta1StreamCondition> Conditions { get; private init; }

    /// <summary>
    /// StreamClass livecycle phase
    /// </summary>
    public StreamClassPhase Phase { get; private set; }


    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to RUNNING 
    /// </summary>
    /// <param name="affectedObject">Affected stream class model</param>
    public static StreamClassOperatorResponse Ready(IStreamClass affectedObject)
    {
        return new StreamClassOperatorResponse
        {
            StreamClass = affectedObject,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = ResourceStatus.READY.ToString(), Status = "True" }
            },
            Phase = StreamClassPhase.READY
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to RUNNING 
    /// </summary>
    /// <param name="affectedObject">Affected stream class model</param>
    public static StreamClassOperatorResponse Failed(IStreamClass affectedObject)
    {
        return new StreamClassOperatorResponse
        {
            StreamClass = affectedObject,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = ResourceStatus.READY.ToString(), Status = "True" }
            },
            Phase = StreamClassPhase.FAILED
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to RUNNING 
    /// </summary>
    /// <param name="affectedObject">Affected stream class model</param>
    public static StreamClassOperatorResponse Stopped(IStreamClass affectedObject)
    {
        return new StreamClassOperatorResponse
        {
            StreamClass = affectedObject,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = ResourceStatus.READY.ToString(), Status = "True" }
            },
            Phase = StreamClassPhase.STOPPED
        };
    }
}
