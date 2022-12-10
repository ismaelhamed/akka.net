using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Dispatch.MessageQueues;
using Akka.Event;
using Akka.Util.Collections;
using Akka.Util.Collections.Concurrent;
using Akka.Util.Internal;

namespace Akka.Dispatch
{
    /// <summary>
    /// A QueueBasedMessageQueue is a IMessageQueue backed by a Akka.Util.Collections.IQueue.
    /// </summary>
    public abstract class QueueBasedMessageQueue : IMessageQueue, IMultipleConsumerSemantics
    {
        protected abstract IQueue<Envelope> Queue { get; }

        public abstract void Enqueue(IActorRef receiver, Envelope envelope);

        public abstract bool TryDequeue(out Envelope envelope);

        public bool HasMessages => !Queue.IsEmpty();

        public int Count => Queue.Size();

        public virtual void CleanUp(IActorRef owner, IMessageQueue deadletters)
        {
            while (TryDequeue(out var msg))
                deadletters.Enqueue(owner, msg);
        }
    }

    public abstract class DequeBasedMessageQueue : QueueBasedMessageQueue, IDequeBasedMessageQueueSemantics
    {
        public abstract void EnqueueFirst(IActorRef receiver, Envelope envelope);

        [Obsolete("For compatibility purposes only.")]
        public void EnqueueFirst(Envelope envelope) => throw new NotImplementedException();

        //// Covariant types in netstandard2.0
        //protected abstract override IDeque<Envelope> Queue { get; }
    }

    /// <summary>
    /// BoundedMessageQueueSemantics adds bounded semantics to a DequeBasedMessageQueue, 
    /// i.e.blocking enqueue with timeout.
    /// </summary>
    public abstract class BoundedDequeBasedMessageQueue : DequeBasedMessageQueue, IBoundedDequeBasedMessageQueueSemantics
    {
        protected IBlockingDeque<Envelope> _queue;

        public TimeSpan PushTimeOut { get; protected set; }

        public BoundedDequeBasedMessageQueue() => Queue = _queue;

        protected sealed override IQueue<Envelope> Queue { get; }

        //// Covariant types in netstandard2.0
        //protected override IBlockingDeque<Envelope> Queue { get; }

        public override void Enqueue(IActorRef receiver, Envelope envelope)
        {
            if (PushTimeOut.Ticks > 0)
            {
                if (!_queue.Offer(envelope, PushTimeOut))
                {
                    receiver.AsInstanceOf<IInternalActorRef>()
                        .Provider
                        .DeadLetters
                        .Tell(new DeadLetter(envelope.Message, envelope.Sender, receiver), envelope.Sender);
                }
            }
            else _queue.Put(envelope);
        }

        public override void EnqueueFirst(IActorRef receiver, Envelope envelope)
        {
            if (PushTimeOut.Ticks > 0)
            {
                if (!_queue.OfferFirst(envelope, PushTimeOut))
                {
                    receiver.AsInstanceOf<IInternalActorRef>()
                        .Provider
                        .DeadLetters
                        .Tell(new DeadLetter(envelope.Message, envelope.Sender, receiver), envelope.Sender);
                }
            }
            else _queue.PutFirst(envelope);
        }

        public override bool TryDequeue(out Envelope envelope)
        {
            envelope = _queue.Poll();
            return true;
        }
    }

    public interface IProducesPushTimeoutSemanticsMailbox
    {
        TimeSpan PushTimeOut { get; }
    }

    public class BoundedDequeBasedMailbox2 : MailboxType, IProducesMessageQueue<BoundedDequeBasedMailbox2.MessageQueue>, IProducesPushTimeoutSemanticsMailbox
    {
        public int Capacity { get; }
        public TimeSpan PushTimeOut { get; }

        public BoundedDequeBasedMailbox2(int capacity, TimeSpan pushTimeOut)
            : base(null, null)
        {
            if (capacity < 0)
                throw new ArgumentException("The capacity for BoundedDequeBasedMailbox can not be negative", nameof(capacity));

            Capacity = capacity;
            PushTimeOut = pushTimeOut;
        }

        public BoundedDequeBasedMailbox2(Settings settings, Config config)
            : this(config.GetInt("mailbox-capacity"), config.GetTimeSpan("mailbox-push-timeout-time"))
        { }

        public override IMessageQueue Create(IActorRef owner, ActorSystem system) =>
            new MessageQueue(Capacity, PushTimeOut);

        private class MessageQueue : BoundedDequeBasedMessageQueue
        {
            public MessageQueue(int capacity, TimeSpan pushTimeOut)
            {
                _queue = new LinkedBlockingDeque<Envelope>(capacity);
                PushTimeOut = pushTimeOut;
            }
        }
    }
}
