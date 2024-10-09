using System;

namespace Arcane.Operator.Exceptions;

/// <summary>
/// Thrown when an error occurs in the job listener
/// </summary>
public class JobListenerException: Exception
{
    /// <summary>
    /// Thrown when an error occurs in the job listener
    /// </summary>
    /// <param name="message">Error message</param>
    public JobListenerException(string message) : base(message)
    {
    }
}
