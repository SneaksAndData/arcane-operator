using System;
using System.Collections.Generic;
using System.Text.Json;
using Arcane.Operator.Models.Resources.StreamDefinitions;
using Arcane.Operator.Models.StreamDefinitions.Base;
using Arcane.Operator.StreamingJobLifecycle;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

public static class StreamDefinitionTestCases
{
    private static readonly string StreamSpec = "{\"jobTemplateRef\": {\"name\": \"jobTemplate\"}, \"reloadingJobTemplateRef\": {\"name\": \"jobTemplate\"}}";
    public static readonly string Kind = "StreamDefinition";
    public static string ApiGroup = "streaming.sneaksanddata.com";
    public static string PluralName = "streams";
    public static string ApiVersion = "v1alpha1";


    public static IStreamDefinition StreamDefinition => new StreamDefinition
    {
        Spec = JsonDocument.Parse(StreamSpec).RootElement,
        Kind = Kind,
        Metadata = new V1ObjectMeta
        {
            Name = "stream"
        }
    };

    public static IStreamDefinition SuspendedStreamDefinition => new StreamDefinition
    {
        Spec = JsonDocument.Parse(StreamSpec).RootElement,
        Kind = Kind,
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
        Spec = JsonDocument.Parse(StreamSpec).RootElement,
        Kind = Kind,
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

    public static IStreamDefinition NamedStreamDefinition(string name = null) => new StreamDefinition
    {
        Spec = JsonDocument.Parse(StreamSpec).RootElement,
        Kind = Kind,
        Metadata = new V1ObjectMeta
        {
            Name = name ?? Guid.NewGuid().ToString(),
            NamespaceProperty = "namespace"
        }
    };
}
