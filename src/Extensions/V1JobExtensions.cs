using System.Collections.Generic;
using Arcane.Models.StreamingJobLifecycle;
using k8s.Models;
using Snd.Sdk.Kubernetes;

namespace Arcane.Operator.Extensions;

public static class V1JobExtensions
{
    public const string STREAM_KIND_LABEL = "arcane/stream-kind";
    public const string STREAM_ID_LABEL = "arcane/stream-id";
    public const string BACK_FILL_LABEL = "arcane/backfilling";

    public static V1Job WithStreamingJobLabels(this V1Job job, string streamId, bool isBackfilling, string streamKind)
    {
        return job.WithLabels(new Dictionary<string, string>
        {
            { STREAM_ID_LABEL, streamId },
            { STREAM_KIND_LABEL, streamKind },
            { BACK_FILL_LABEL, isBackfilling.ToString().ToLowerInvariant() }
        });
    }

    public static V1Job WithStreamingJobAnnotations(this V1Job job, string configurationChecksum)
    {
        return job.WithAnnotations(new Dictionary<string, string>
        {
            { Annotations.CONFIGURATION_CHECKSUM_ANNOTATION_KEY, configurationChecksum }
        });
    }

    public static string GetStreamId(this V1Job job)
    {
        return job.Name();
    }

    public static string GetStreamKind(this V1Job job)
    {
        if (job.Labels() != null && job.Labels().TryGetValue(STREAM_KIND_LABEL, out var value))
        {
            return value;
        }

        return string.Empty;
    }

    public static string GetConfigurationChecksum(this V1Job job)
    {
        if (job.Annotations() != null && job.Annotations().TryGetValue(
                Annotations.CONFIGURATION_CHECKSUM_ANNOTATION_KEY,
                out var value))
        {
            return value;
        }

        return string.Empty;
    }

    public static bool IsStopRequested(this V1Job job)
    {
        return job.Annotations() != null
               && job.Annotations().TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
               && value == Annotations.TERMINATE_REQUESTED_STATE_ANNOTATION_VALUE;
    }

    public static bool IsRestartRequested(this V1Job job)
    {
        return job.Annotations() != null
               && job.Annotations().TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
               && value == Annotations.RESTARTING_STATE_ANNOTATION_VALUE;
    }

    public static bool IsReloadRequested(this V1Job job)
    {
        return job.Annotations() != null
               && job.Annotations().TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
               && value == Annotations.RELOADING_STATE_ANNOTATION_VALUE;
    }

    public static bool IsReloading(this V1Job job)
    {
        return job.Labels() != null
               && job.Labels().TryGetValue(BACK_FILL_LABEL, out var value)
               && value == "true";
    }

    public static bool IsSchemaMismatch(this V1Job job)
    {
        return job.Annotations() != null
               && job.Annotations().TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
               && value == Annotations.SCHEMA_MISMATCH_STATE_ANNOTATION_VALUE;
    }

    public static bool IsStopping(this V1Job job)
    {
        return job.Annotations() != null
               && job.Annotations().TryGetValue(Annotations.STATE_ANNOTATION_KEY, out var value)
               && value == Annotations.TERMINATING_STATE_ANNOTATION_VALUE;
    }
}
