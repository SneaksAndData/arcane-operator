using System;
using System.Collections.Generic;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Extensions;
using k8s.Models;
using Snd.Sdk.Kubernetes;

namespace Arcane.Operator.Tests.Services.TestCases;

public static class JobTestCases
{
    public static V1Job FailedJob => CreateJob(new List<V1JobCondition>
            { new() { Type = "Failed", Status = "True" } })
        .WithStreamingJobLabels("1", false, string.Empty);

    public static V1Job CompletedJob => CreateJob(new List<V1JobCondition>
            { new() { Type = "Complete", Status = "True" } })
        .WithStreamingJobLabels("1", false, string.Empty);

    public static V1Job ReloadRequestedJob => CompletedJob
        .Clone()
        .WithAnnotations(new Dictionary<string, string>
        {
            { Annotations.STATE_ANNOTATION_KEY, Annotations.RELOADING_STATE_ANNOTATION_VALUE }
        });

    public static V1Job TerminatingJob => CompletedJob
        .Clone()
        .WithAnnotations(new Dictionary<string, string>
        {
            { Annotations.STATE_ANNOTATION_KEY, Annotations.TERMINATING_STATE_ANNOTATION_VALUE }
        });

    public static V1Job ReloadingJob => CreateJob(new List<V1JobCondition>
            { new() { Type = "Complete", Status = "True" } })
        .Clone()
        .WithStreamingJobLabels(Guid.NewGuid().ToString(), true, string.Empty);

    public static V1Job RunningJob => CreateJob(null)
        .WithStreamingJobLabels("1", false, string.Empty);

    public static V1Job SchemaMismatchJob => RunningJob
        .Clone()
        .WithAnnotations(new Dictionary<string, string>
        {
            { Annotations.STATE_ANNOTATION_KEY, Annotations.SCHEMA_MISMATCH_STATE_ANNOTATION_VALUE }
        });

    public static V1Job JobWithChecksum(string checksum)
    {
        return RunningJob
            .Clone()
            .WithStreamingJobAnnotations(checksum);
    }

    private static V1Job CreateJob(List<V1JobCondition> conditions)
    {
        var job = new V1Job
        {
            Metadata = new V1ObjectMeta(),
            Spec = new V1JobSpec
            {
                Template = new V1PodTemplateSpec
                {
                    Metadata = new V1ObjectMeta()
                }
            },
            Status = new V1JobStatus { Conditions = conditions }
        };
        return job;
    }
}
