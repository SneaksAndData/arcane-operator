using Arcane.Operator.Configurations;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Models.StreamClass.Base;

/// <summary>
/// Base interface for StreamClass objects
/// </summary>
public interface IStreamClass: IKubernetesObject<V1ObjectMeta> 
{
    /// <summary>
    /// Return Unique ID for the StreamClass object
    /// </summary>
    /// <returns></returns>
    string ToStreamClassId();

    StreamOperatorServiceConfiguration ToStreamOperatorServiceConfiguration();
}
