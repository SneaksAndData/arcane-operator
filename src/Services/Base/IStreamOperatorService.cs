using System;
using System.Threading;
using System.Threading.Tasks;
using Akka;
using Akka.Streams.Dsl;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.Services.Models;

namespace Arcane.Operator.Services.Base;

public interface IStreamOperatorService
{
    Lazy<Sink<ResourceEvent<IStreamDefinition>, NotUsed>> CommonSink { get; }
    void Attach(IStreamClass streamClass);
}
