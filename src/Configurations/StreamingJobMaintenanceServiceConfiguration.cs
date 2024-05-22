using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Services.Maintenance;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for the <see cref="StreamingJobMaintenanceService"/>
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamingJobMaintenanceServiceConfiguration
{
    /// <summary>
    /// Max buffer capacity for job events stream
    /// </summary>
    public int MaxBufferCapacity { get; init; }
    
    /// <summary>
    /// Namespace where the job will be created
    /// </summary>
    public string Namespace { get; set; }
}
