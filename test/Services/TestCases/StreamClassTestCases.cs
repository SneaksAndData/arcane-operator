using System;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.Resources.StreamClass.V1Beta1;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

public static class StreamClassTestCases
{
    public static IStreamClass StreamClass => new V1Beta1StreamClass
    {
        Spec = new V1Beta1StreamClassSpec
        {
            MaxBufferCapacity = 100,
            KindRef = StreamDefinitionTestCases.Kind,
            ApiGroupRef = StreamDefinitionTestCases.ApiGroup,
            PluralName = StreamDefinitionTestCases.PluralName,
            ApiVersion = StreamDefinitionTestCases.ApiVersion
        },
        Metadata = new V1ObjectMeta
        {
            Name = "StreamClass",
            NamespaceProperty = "default"
        }
    };

    public static IStreamClass FailedStreamClass(Exception exception)
    {
        return new FailedStreamClass(exception);
    }
}
