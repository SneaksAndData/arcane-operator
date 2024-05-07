using System;
using Arcane.Operator.Services.Metrics;

namespace Arcane.Operator.Configurations;

/// <summary>
/// The configuration for the <see cref="StreamClassServiceActor"/>
/// </summary>
public class StreamClassStatusActorConfiguration
{
    public TimeSpan UpdateInterval { get; set; }
    public TimeSpan InitialDelay { get; set; }
};
