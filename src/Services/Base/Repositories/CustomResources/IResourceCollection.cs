using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Services.Models;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Services.Base.Repositories.CustomResources;

public interface IResourceCollection<TResourceType> where TResourceType : IKubernetesObject<V1ObjectMeta>
{
    /// <summary>
    /// Subscribe to a stream class updates
    /// </summary>
    /// <param name="name"></param>
    /// <param name="request">An object that contains required information for a Kubernetes API call</param>
    /// <returns>A task containing optional Resource</returns>
    Task<Option<TResourceType>> Get(string name, CustomResourceApiRequest request);
}
