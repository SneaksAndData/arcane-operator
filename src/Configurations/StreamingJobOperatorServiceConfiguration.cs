using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Streams;
using k8s.Models;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="StreamingJobOperatorService"/>
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamingJobOperatorServiceConfiguration
{
    /// <summary>
    /// Template for the job to be created.
    /// </summary>
    public V1Job JobTemplate { get; set; }

    /// <summary>
    /// Namespace where the job will be created
    /// </summary>
    public string Namespace { get; set; }
}
