using System.Threading.Tasks;
using Arcane.Operator.Models;

namespace Arcane.Operator.Services.Base;

public interface IStreamingClassRepository
{
    Task SetStreamingClassStatus(StreamClassOperatorResponse response);
}
