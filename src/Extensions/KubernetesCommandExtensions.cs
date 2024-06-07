using System.Collections.Generic;
using Arcane.Operator.Models.Base;

namespace Arcane.Operator.Extensions;

public static class KubernetesCommandExtensions
{
    /// <summary>
    /// Handle the command asynchronously
    /// </summary>
    /// <param name="command">Command instance</param>
    /// <returns>Type of the command</returns>
    public static List<KubernetesCommand> AsList(this KubernetesCommand command) => new() { command };
}
