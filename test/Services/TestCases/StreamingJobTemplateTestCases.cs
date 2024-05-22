using System.Collections.Generic;
using Arcane.Operator.Models.Resources.JobTemplates.V1Beta1;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

public static class StreamingJobTemplateTestCases
{

    public static V1Beta1StreamingJobTemplate StreamingJobTemplate => new()
    {
        Spec = new V1Beta1StreamingJobTemplateSpec
        {
            Template = CreateJob(new V1PodSpec { Containers = new List<V1Container> { new() } })
        }
    };

    private static V1Job CreateJob(V1PodSpec v1PodSpec) =>
         new()
         {
             Metadata = new V1ObjectMeta
             {
                 Name = "template"
             },
             Spec = new V1JobSpec
             {
                 Template = new V1PodTemplateSpec
                 {
                     Metadata = new V1ObjectMeta(),
                     Spec = v1PodSpec
                 }
             },
         };
}
