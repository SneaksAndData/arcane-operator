using System;
using System.Collections.Generic;
using Arcane.Operator.Models.Resources.StreamClass.Base;
using Arcane.Operator.Models.StreamDefinitions.Base;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

/// <summary>
/// A stream definition that throws an exception (for tests)
/// </summary>
public class FailedStreamDefinition : IStreamDefinition
{
    private readonly Exception exception;

    public FailedStreamDefinition(Exception exception)
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

    public string StreamId => throw this.exception;
    public bool Suspended => throw this.exception;
    public bool ReloadRequested => throw this.exception;
    public V1TypedLocalObjectReference JobTemplateRef => throw this.exception;
    public V1TypedLocalObjectReference ReloadingJobTemplateRef => throw this.exception;
    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources(IStreamClass streamDefinition) => throw this.exception;

    public Dictionary<string, string> ToEnvironment(bool isBackfilling, IStreamClass streamDefinition) => throw this.exception;

    public IEnumerable<V1EnvFromSource> ToV1EnvFromSources()
    {
        throw this.exception;
    }

    public Dictionary<string, string> ToEnvironment(bool isBackfilling)
    {
        throw this.exception;
    }

    public string GetConfigurationChecksum()
    {
        throw this.exception;
    }

    public V1TypedLocalObjectReference GetJobTemplate(bool isBackfilling)
    {
        throw this.exception;
    }

    public void Deconstruct(out string nameSpace, out string kind, out string streamId)
    {
        throw this.exception;
    }
}
