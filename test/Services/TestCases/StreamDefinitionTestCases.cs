using System;
using System.Collections.Generic;
using System.Text.Json;
using Arcane.Models.StreamingJobLifecycle;
using Arcane.Operator.Models.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

public static class StreamDefinitionTestCases
{
    public static IStreamDefinition StreamDefinition => new StreamDefinition
    {
        Spec = JsonDocument.Parse("{}").RootElement,
        Metadata = new V1ObjectMeta
        {
            Name = "stream"
        }
    };

    public static IStreamDefinition SuspendedStreamDefinition => new StreamDefinition
    {
        Spec = JsonDocument.Parse("{}").RootElement,
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
        Spec = JsonDocument.Parse("{}").RootElement,
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

    public static StreamDefinition NamedStreamDefinition(string name = null) => new()
    {
        Spec = JsonDocument.Parse("{}").RootElement,
        Metadata = new V1ObjectMeta
        {
            Name = name ?? Guid.NewGuid().ToString()
        }
    };
}
