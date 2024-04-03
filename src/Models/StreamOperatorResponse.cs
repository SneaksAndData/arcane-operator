using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Arcane.Operator.StreamStatuses.StreamStatus.V1Beta1;

namespace Arcane.Operator.Models;

/// <summary>
/// Represents stream status badge for Lens app.
/// </summary>
public enum StreamStatusType
{
    /// <summary>
    /// The stream is in a ready state.
    /// </summary>
    READY,

    /// <summary>
    /// The stream is in an error state.
    /// </summary>
    ERROR,

    /// <summary>
    /// The stream is in a warning state.
    /// </summary>
    WARNING
}

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
    /// A stopped stream.
    /// </summary>
    STOPPED,

    /// <summary>
    /// A stream that is shutting down.
    /// </summary>
    TERMINATING,

    /// <summary>
    /// A restarting stream.
    /// </summary>
    RESTARTING,

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
/// Contains response from stream operator that can be used by other services inside the application
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamOperatorResponse
{
    /// <summary>
    /// Affected stream identifier
    /// </summary>
    public string Id { get; private init; }

    /// <summary>
    /// Affected stream kind
    /// </summary>
    public string Kind { get; set; }

    /// <summary>
    /// Affected stream namespace
    /// </summary>
    public string Namespace { get; set; }

    /// <summary>
    /// Latest observed state of the stream
    /// </summary>
    public IEnumerable<V1Beta1StreamCondition> Conditions { get; private init; }

    /// <summary>
    /// Stream livecycle phase
    /// </summary>
    public StreamPhase Phase { get; private set; }


    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to RESTARTING
    /// </summary>
    /// <param name="streamId">Affected stream identifier</param>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    public static StreamOperatorResponse Restarting(string nameSpace, string kind, string streamId)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Namespace = nameSpace,
            Kind = kind,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = StreamStatusType.WARNING.ToString(), Status = "True" }
            },
            Phase = StreamPhase.RESTARTING
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to RUNNING 
    /// </summary>
    /// <param name="streamId">Affected stream identifier</param>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    public static StreamOperatorResponse Running(string nameSpace, string kind, string streamId)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Kind = kind,
            Namespace = nameSpace,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = StreamStatusType.READY.ToString(), Status = "True" }
            },
            Phase = StreamPhase.RUNNING
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to RELOADING 
    /// </summary>
    /// <param name="streamId">Affected stream identifier</param>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    public static StreamOperatorResponse Reloading(string nameSpace, string kind, string streamId)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Kind = kind,
            Namespace = nameSpace,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = StreamStatusType.READY.ToString(), Status = "True" }
            },
            Phase = StreamPhase.RELOADING
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to TERMINATING
    /// </summary>
    /// <param name="streamId">Affected stream identifier</param>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    public static StreamOperatorResponse Terminating(string nameSpace, string kind, string streamId)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Namespace = nameSpace,
            Kind = kind,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = StreamStatusType.WARNING.ToString(), Status = "True" }
            },
            Phase = StreamPhase.TERMINATING
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to TERMINATING 
    /// </summary>
    /// <param name="streamId">Affected stream identifier</param>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    public static StreamOperatorResponse Stopped(string nameSpace, string kind, string streamId)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Kind = kind,
            Namespace = nameSpace,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = StreamStatusType.WARNING.ToString(), Status = "True" }
            },
            Phase = StreamPhase.STOPPED
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to FAILED
    /// with specified message
    /// </summary>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    /// <param name="streamId">Affected stream identifier</param>
    /// <param name="message">Error message</param>
    public static StreamOperatorResponse OperationFailed(string nameSpace, string kind, string streamId, string message)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Kind = kind,
            Namespace = nameSpace,
            Conditions = new[]
            {
                new V1Beta1StreamCondition
                    { Type = StreamStatusType.ERROR.ToString(), Status = "True", Message = message }
            },
            Phase = StreamPhase.FAILED
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to STOPPED
    /// with specified message
    /// </summary>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    /// <param name="streamId">Affected stream identifier</param>
    public static StreamOperatorResponse Suspended(string nameSpace, string kind, string streamId)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Kind = kind,
            Namespace = nameSpace,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = StreamStatusType.WARNING.ToString(), Status = "True" }
            },
            Phase = StreamPhase.SUSPENDED
        };
    }

    /// <summary>
    /// Creates a StreamOperatorResponse object for stream with specified identifier, setting it state to FAILED
    /// with message "Crash loop detected"
    /// </summary>
    /// <param name="nameSpace">Affected stream namespace</param>
    /// <param name="kind">Affected stream kind</param>
    /// <param name="streamId">Affected stream identifier</param>
    public static StreamOperatorResponse CrashLoopDetected(string nameSpace, string kind, string streamId)
    {
        return new StreamOperatorResponse
        {
            Id = streamId,
            Kind = kind,
            Namespace = nameSpace,
            Conditions = new[]
            {
                new V1Beta1StreamCondition { Type = StreamStatusType.ERROR.ToString(), Status = "True" }
            },
            Phase = StreamPhase.FAILED
        };
    }

    public V1Beta1StreamStatus ToStatus()
    {
        return new V1Beta1StreamStatus
        {
            Conditions = this.Conditions.ToArray(),
            Phase = this.Phase.ToString()
        };
    }
}
