namespace Arcane.Operator.Services.Models;

/// <summary>
/// A request object that contains required information for a Kubernetes API call
/// </summary>
/// <param name="Namespace">Namespace where the resource is created</param>
/// <param name="ApiGroup">Resource API group</param>
/// <param name="ApiVersion">Resource API version</param>
/// <param name="PluralName">Resource Plural name</param>
public record CustomResourceApiRequest(string Namespace, string ApiGroup, string ApiVersion, string PluralName);
