using System.Linq;
using k8s.Models;

namespace Arcane.Operator.Tests.Extensions;

public static class V1JobExtensions
{
    public static bool IsBackfilling(this V1Job job) =>
        job.Spec.Template.Spec.Containers[0].Env.Any(i => i.Name == "STREAMCONTEXT__BACKFILL" && i.Value == "true");
}
