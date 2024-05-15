using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Base;

namespace Arcane.Operator.Services.Commands;

/// <summary>
/// Base class for streaming job commands
/// </summary>
public abstract record StreamingJobCommand: KubernetesCommand;

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

