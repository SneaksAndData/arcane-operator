using k8s;
using k8s.Models;

namespace Arcane.Operator.Services.Models;

/// <summary>
/// An event that represents an update of a Kubernetes object
/// </summary>
/// <param name="EventType">The type of the event</param>
/// <param name="kubernetesObject">Deserialized object</param>
/// <typeparam name="TUpdatedObject">Object type</typeparam>
public record ResourceEvent<TUpdatedObject>(WatchEventType EventType, TUpdatedObject kubernetesObject)
    where TUpdatedObject : IKubernetesObject<V1ObjectMeta>;
