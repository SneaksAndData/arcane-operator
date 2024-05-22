using Akka;
using Akka.Streams.Dsl;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Models;

namespace Arcane.Operator.Services.Base;

public interface IStreamDefinitionMetadataRepository
{
    public CustomResourceApiRequest GetStreamDefinitionMetadataRequest(string nameSpace, string streamDefinitionKind);
    
    public Flow<IStreamClass, IStreamClass, NotUsed> Insert();
}
