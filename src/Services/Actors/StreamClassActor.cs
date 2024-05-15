using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Event;
using Arcane.Operator.Models;
using Arcane.Operator.Models.StreamClass.Base;
using Arcane.Operator.Services.Base;
using Arcane.Operator.Services.Operator;

namespace Arcane.Operator.Services.Actors;

public record StreamClassManagementMessage(IStreamClass streamClass);

public record StartListeningMessage(IStreamClass streamClass): StreamClassManagementMessage(streamClass);

public record StopListeningMessage(IStreamClass streamClass) : StreamClassManagementMessage(streamClass);

public class StreamClassActor: ReceiveActor
{
    private readonly ILoggingAdapter Log = Context.GetLogger();
    
    private StreamOperatorServiceWorker worker;

    public StreamClassActor(IStreamOperatorServiceWorkerFactory streamOperatorServiceWorkerFactory)
    {
        this.Receive<StartListeningMessage>(message =>
        {
            this.worker = streamOperatorServiceWorkerFactory.Create(message.streamClass);
            Context.Sender.Tell(StreamClassOperatorResponse.Ready(message.streamClass));
        });

        this.Receive<StopListeningMessage>(message =>
        {
            if (this.worker == null)
            {
                this.Log.Error("Attempt to stop listening for a stream class that is not started: {@streamClass}", message.streamClass);
                return;
            }
            this.worker.Stop();
            Context.Sender.Tell(StreamClassOperatorResponse.Stopped(message.streamClass));
        });
    }
    
}
