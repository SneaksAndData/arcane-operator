using System;
using Arcane.Operator.Models.Resources.JobTemplates.Base;
using k8s.Models;

namespace Arcane.Operator.Tests.Services.TestCases;

/// <summary>
/// A streaming job templatethat throws an exception (for tests)
/// </summary>
public class FailedStreamingJobTemplate: IStreamingJobTemplate
{
    private readonly Exception exception;

    public FailedStreamingJobTemplate(Exception exception)
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

    public V1Job GetJob() => throw this.exception;
}
