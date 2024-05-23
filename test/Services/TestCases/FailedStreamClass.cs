using System;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using k8s.Models;
using Snd.Sdk.Kubernetes;

namespace Arcane.Operator.Tests.Services.TestCases;

/// <summary>
/// A stream Class that throws an exception (for tests)
/// </summary>
public class FailedStreamClass : IStreamClass
{
    private readonly Exception exception;

    public FailedStreamClass(Exception exception)
    {
        this.exception = exception;
    }

    public string ApiVersion
    {
        get => throw this.exception;
        set => throw this.exception;
    }

    public string Kind
    {
        get => throw this.exception;
        set => throw this.exception;
    }

    public V1ObjectMeta Metadata
    {
        get => throw this.exception;
        set => throw this.exception;
    }
    public string ToStreamClassId()
    {
        throw this.exception;
    }

    public string ApiGroupRef => throw this.exception;
    public string VersionRef => throw this.exception;
    public string PluralNameRef => throw this.exception;
    public string KindRef => throw this.exception;
    public int MaxBufferCapacity => throw this.exception;
    public NamespacedCrd ToNamespacedCrd()
    {
        throw this.exception;
    }

    public bool IsSecretRef(string propertyName)
    {
        throw this.exception;
    }
}
