using System;
using System.Collections.Generic;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.StreamDefinitions;
using Arcane.Operator.StreamDefinitions.Base;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

public static class StreamDefinitionTestCases
{
    public static IStreamDefinition StreamDefinition => new StreamDefinition
    {
        Spec = new StreamDefinitionSpec(),
        Metadata = new V1ObjectMeta
        {
            Name = "stream"
        }
    };

    public static IStreamDefinition SuspendedStreamDefinition => new StreamDefinition
    {
        Spec = new StreamDefinitionSpec(),
        Metadata = new V1ObjectMeta
        {
            Name = "stream",
            Annotations = new Dictionary<string, string>
            {
                { Annotations.STATE_ANNOTATION_KEY, Annotations.SUSPENDED_STATE_ANNOTATION_VALUE }
            }
        }
    };

    public static IStreamDefinition ReloadRequestedStreamDefinition => new StreamDefinition
    {
        Spec = new StreamDefinitionSpec(),
        Metadata = new V1ObjectMeta
        {
            Name = "stream",
            Annotations = new Dictionary<string, string>
            {
                { Annotations.STATE_ANNOTATION_KEY, Annotations.RELOADING_STATE_ANNOTATION_VALUE }
            }
        }
    };

    public static FailedStreamDefinition FailedStreamDefinition(Exception exception)
    {
        return new FailedStreamDefinition(exception);
    }
}
