using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using k8s.Models;

namespace Arcane.Operator.Models.Resources.JobTemplates.V1Beta1;

[ExcludeFromCodeCoverage(Justification = "Model")]
public class V1Beta1StreamingJobTemplate : IStreamingJobTemplate
{
    /// <summary>
    /// Streaming job configuration
    /// </summary>
    [JsonPropertyName("spec")]
    public V1Beta1StreamingJobTemplateSpec Spec { get; set; }

    /// <summary>
    /// Api version
    /// </summary>
    [JsonPropertyName("apiVersion")]
    public string ApiVersion { get; set; }

    /// <summary>
    /// Object kind (should always be "StreamingJobTemplate")
    /// </summary>
    [JsonPropertyName("kind")]
    public string Kind { get; set; }

    /// <summary>
    /// Object metadata see <see cref="V1ObjectMeta"/>
    /// </summary>
    [JsonPropertyName("metadata")]
    public V1ObjectMeta Metadata { get; set; }

    /// <inheritdoc cref="IStreamingJobTemplate.GetJob"/>
    public V1Job GetJob()
    {
        return new V1Job
        {
            ApiVersion = "batch/v1",
            Kind = "Job",
            Metadata = this.Spec.Metadata ?? new V1ObjectMeta(),
            Spec = this.Spec.Template.Spec
        };
    }
}
