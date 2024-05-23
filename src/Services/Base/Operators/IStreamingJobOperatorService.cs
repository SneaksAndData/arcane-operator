using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;

namespace Arcane.Operator.Services.Base;

public interface IStreamingJobOperatorService
{
    /// <summary>
    /// Return graph that watches for job events and updates stream state accordingly
    /// </summary>
    public IRunnableGraph<Task> GetJobEventsGraph(CancellationToken cancellationToken);
}
