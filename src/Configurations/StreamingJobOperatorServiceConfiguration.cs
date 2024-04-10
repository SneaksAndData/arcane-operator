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
    /// Namespace where the job will be created
    /// </summary>
    public string Namespace { get; set; }
}
