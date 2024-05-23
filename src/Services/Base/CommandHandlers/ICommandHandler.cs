using System.Threading.Tasks;
using Arcane.Operator.Models.Base;

namespace Arcane.Operator.Services.Base.CommandHandlers;
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
