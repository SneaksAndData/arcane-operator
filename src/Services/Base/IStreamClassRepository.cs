using Arcane.Operator.Configurations;

namespace Arcane.Operator.Services.Base;

public interface IStreamClassRepository
{
    CustomResourceConfiguration Get(string nameSpace, string kind);
}
