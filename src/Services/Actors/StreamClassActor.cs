using System.Threading;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;

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

    private CancellationTokenSource cts;

    public StreamClassActor(IStreamOperatorService streamOperatorService)
    {
        this.Receive<StartListeningMessage>(message =>
        {
            this.cts ??= new CancellationTokenSource();
            streamOperatorService
                .GetStreamDefinitionEventsGraph(this.cts.Token)
                .Run(Context.Materializer())
                .PipeTo(this.Sender, this.Self,
                    success: () => new StartListeningMessage(message.streamClass),
                    failure: exception =>
                    {
                        this.Log.Error(exception, "Failed to start listening for {streamClassId} events",
                            message.streamClass.ToStreamClassId());
                        return new StartListeningMessage(message.streamClass);
                    });
            Context.Sender.Tell(StreamClassOperatorResponse.Ready(message.streamClass));
        });

        this.Receive<StopListeningMessage>(message =>
        {
            this.cts?.Cancel();
            this.cts = null;
            Context.Sender.Tell(StreamClassOperatorResponse.Stopped(message.streamClass));
        });
    }
}
