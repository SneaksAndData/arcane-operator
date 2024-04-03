using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.JobTemplates.V1Beta1;

namespace Arcane.Operator.Services.Base;

public interface IStreamingJobTemplateRepository
{
    /// <summary>
    /// Read the job template for the given kind, namespace and name
    /// </summary>
    /// <param name="kind">Kind of the job template</param>
    /// <param name="jobNamespace">Namespace to read from</param>
    /// <param name="templateName">Job template name</param>
    /// <returns></returns>
    Task<Option<V1Beta1StreamingJobTemplate>> GetStreamingJobTemplate(string kind, string jobNamespace,
        string templateName);
}
