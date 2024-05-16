using Akka.Actor;
using Akka.Event;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Operator;

namespace Arcane.Operator.Services.Actors;

/// <summary>
/// Base class for stream class management messages.
/// </summary>
/// <param name="streamClass">The resource affected by the message</param>
public abstract record StreamClassManagementMessage(IStreamClass streamClass);

/// <summary>
/// Instructs the <see cref="StreamClassActor"/> to start listening for a stream class events.
/// </summary>
/// <param name="streamClass">The resource affected by the message</param>
public record StartListeningMessage(IStreamClass streamClass) : StreamClassManagementMessage(streamClass);

/// <summary>
/// Instructs the <see cref="StreamClassActor"/> to stop listening for a stream class events.
/// </summary>
/// <param name="streamClass">The resource affected by the message</param>
public record StopListeningMessage(IStreamClass streamClass) : StreamClassManagementMessage(streamClass);

/// <summary>
/// The actor responsible for managing stream class events listeners.
/// </summary>
public class StreamClassActor : ReceiveActor
{
    private readonly ILoggingAdapter Log = Context.GetLogger();

    private StreamOperatorServiceWorker worker;

    public StreamClassActor(IStreamOperatorServiceWorkerFactory streamOperatorServiceWorkerFactory)
    {
        this.Receive<StartListeningMessage>(message =>
        {
            this.worker = streamOperatorServiceWorkerFactory.Create(message.streamClass);
            this.worker.Start(message.streamClass.ToStreamClassId());
            Context.Sender.Tell(StreamClassOperatorResponse.Ready(message.streamClass));
        });

        this.Receive<StopListeningMessage>(message =>
        {
            if (this.worker == null)
            {
                this.Log.Error("Attempt to stop listening for a stream class that is not started: {@streamClass}", message.streamClass);
            }
            this.worker?.Stop();
            Context.Sender.Tell(StreamClassOperatorResponse.Stopped(message.streamClass));
        });
    }

}
