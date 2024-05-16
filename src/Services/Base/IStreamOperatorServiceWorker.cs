namespace Arcane.Operator.Services.Base;

public interface IStreamOperatorServiceWorker
{
    /// <summary>
    /// Start new Stream Operator service.
    /// </summary>
    /// <param name="toStreamClassId"></param>
    void Start(string toStreamClassId);

    /// <summary>
    /// Stop the Stream Operator service and wait for it to finish.
    /// </summary>
    void Stop();
}
