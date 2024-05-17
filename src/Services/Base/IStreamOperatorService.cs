﻿using Akka.Streams;
using Arcane.Operator.Models.StreamClass.Base;

namespace Arcane.Operator.Services.Base;

public interface IStreamOperatorService
{
    /// <summary>
    /// Attach the StreamClass to the StreamOperatorService
    /// </summary>
    /// <param name="streamClass">Stream class event to start processing</param>
    void Attach(IStreamClass streamClass);

    /// <summary>
    /// Stop processing stream events
    /// </summary>
    /// <param name="streamClass">Stream class event to start processing</param>
    void Detach(IStreamClass streamClass);
}
