using k8s;
using k8s.Models;

namespace Arcane.Operator.Extensions;

public static class KubernetesObjectExtensions
{
    /// <summary>
    /// Deconstructs the Kubernetes object into its namespace, kind, and name for patten matching
    /// </summary>
    /// <param name="obj">Object to deconstruct</param>
    /// <param name="nameSpace">Namespace</param>
    /// <param name="name">Object name</param>
    public static void Deconstruct(this IKubernetesObject<V1ObjectMeta> obj, out string nameSpace, out string name)
    {
        nameSpace = obj.Namespace();
        name = obj.Name();
    }
}
