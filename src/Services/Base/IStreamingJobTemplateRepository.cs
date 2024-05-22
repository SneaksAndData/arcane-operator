using System.Threading.Tasks;
using Akka.Util;
using Arcane.Operator.Models.Resources.JobTemplates.Base;

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
    Task<Option<IStreamingJobTemplate>> GetStreamingJobTemplate(string kind, string jobNamespace, string templateName);
}
