using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Arcane.Operator.Configurations;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace Arcane.Operator.Extensions;

[ExcludeFromCodeCoverage(Justification = "Startup configuration extension methods")]
internal static class LoggerConfigurationExtensions
{
    public static LoggerConfiguration EnrichFromConfiguration(this LoggerConfiguration loggerConfiguration, IConfiguration configuration)
    {
        var customProperties = configuration.GetSection(nameof(LoggingConfiguration)).Get<LoggingConfiguration>();
        foreach (var (key, value) in customProperties?.CustomProperties ?? new Dictionary<string, string>())
        {
            loggerConfiguration.Enrich.WithProperty(key, value);
        }
        return loggerConfiguration;
    }
}
