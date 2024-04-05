using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Arcane.Operator.Services.Base;

public interface IStreamClassOperatorService
{
    /// <summary>
    /// Return graph that watches StreamClass events
    /// </summary>
    public IRunnableGraph<Task> GetStreamClassEventsGraph(CancellationToken cancellationToken);
}
