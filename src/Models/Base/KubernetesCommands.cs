using Arcane.Operator.Models.Api;
using k8s;
using k8s.Models;

namespace Arcane.Operator.Models.Base;

/// <summary>
/// Abstract base class for Kubernetes commands
/// </summary>
public abstract record KubernetesCommand;

/// <summary>
/// Abstract class for setting annotation on a stream definition Kubernetes object
/// </summary>
/// <param name="affectedResource">The resource to update</param>
/// <param name="annotationKey">Annotation key</param>
/// <param name="annotationValue">Annotation value</param>
/// <typeparam name="TObject">Affected object type</typeparam>
public abstract record SetAnnotationCommand<TObject>(TObject affectedResource,
    string annotationKey,
    string annotationValue) : KubernetesCommand where TObject : IKubernetesObject<V1ObjectMeta>;

/// <summary>
/// Abstract class for setting annotation on a stream definition Kubernetes object
/// </summary>
/// <param name="affectedResource">The resource to update</param>
/// <param name="annotationKey">Annotation key</param>
/// <typeparam name="TObject">Affected object type</typeparam>
public abstract record RemoveAnnotationCommand<TObject>(TObject affectedResource,
    string annotationKey) : KubernetesCommand where TObject : IKubernetesObject<V1ObjectMeta>;


/// <summary>
/// Update the stream definition status
/// </summary>
/// <param name="request"></param>
/// <param name="conditions"></param>
/// <param name="phase"></param>
public abstract record SetResourceStatusCommand<TCondition, TPhase>(CustomResourceApiRequest request,
    TCondition[] conditions,
    TPhase phase) : KubernetesCommand;

