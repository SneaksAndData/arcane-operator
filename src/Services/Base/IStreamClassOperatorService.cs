using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Models;

namespace Arcane.Operator.Services.Base;

public interface IStreamClassOperatorService
{
    /// <summary>
    /// Return graph that watches StreamClass events
    /// </summary>
    public IRunnableGraph<Task<Done>> GetStreamClassEventsGraph(CancellationToken cancellationToken);
}
