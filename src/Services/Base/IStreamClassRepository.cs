using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Models.StreamClass.Base;

namespace Arcane.Operator.Services.Base;

public interface IStreamClassRepository
{
    Task<Option<IStreamClass>> Get(string nameSpace, string streamDefinitionKind);
}
