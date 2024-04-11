using Arcane.Operator.Configurations.Common;
using Arcane.Operator.Services.Repositories;

namespace Arcane.Operator.Configurations;

/// <summary>
/// Configuration for <see cref="StreamingJobTemplateRepository"/>
/// </summary>
public class StreamingJobTemplateRepositoryConfiguration
{
    public CustomResourceConfiguration ResourceConfiguration { get; set; }
}
