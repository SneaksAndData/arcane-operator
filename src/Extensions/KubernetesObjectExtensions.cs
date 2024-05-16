using k8s;
using k8s.Models;

namespace Arcane.Operator.Extensions;

public static class KubernetesObjectExtensions
{
    public static void Deconstruct(this IKubernetesObject<V1ObjectMeta> obj, out string nameSpace, out string kind, out string name)
    {
        nameSpace = obj.Namespace();
        kind = obj.Kind;
        name = obj.Name();
    }
}
