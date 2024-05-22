using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Services.Repositories;
using Arcane.Operator.Services.Repositories.CustomResources;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for <see cref="StreamingJobTemplateRepository"/>
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamingJobTemplateRepositoryConfiguration
{
    public CustomResourceConfiguration ResourceConfiguration { get; set; }
}
