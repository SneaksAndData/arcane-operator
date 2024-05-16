using System.Threading.Tasks;

namespace Arcane.Operator.Services.Base;

/// <summary>
/// Abstract base class for Kubernetes commands
/// </summary>
public abstract record KubernetesCommand;

/// <summary>
/// Base interface for Kubernetes command handlers
/// </summary>
/// <typeparam name="T">Typeof the command to handle</typeparam>
public interface ICommandHandler<in T> where T : KubernetesCommand
{
    /// <summary>
    /// Handle the command asynchronously
    /// </summary>
    /// <param name="command">Command instance</param>
    /// <returns>Type of the command</returns>
    public Task Handle(T command);
}
