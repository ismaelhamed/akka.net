//-----------------------------------------------------------------------
// <copyright file="AbstractActor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Dispatch;

namespace Akka.Actor
{
    public abstract class AbstractActor : ActorBase
    {
        // Cache the delegate per actor instance
        private Receive _receive;

        /// <summary>
        /// An actor has to define its initial receive behavior by implementing
        /// the `CreateReceive` method.
        /// </summary>
        protected abstract Receive CreateReceive { get; }

        protected override bool Receive(object message) => (_receive ??= CreateReceive)(message);

        /// <summary>
        /// Convenience factory of the `ReceiveBuilder`.
        /// Creates a new empty <see cref="ReceiveBuilder"/>.
        /// </summary>
        public static ReceiveBuilder2 ReceiveBuilder => ReceiveBuilder2.Create();
    }

    /// <summary>
    /// Support for scheduled `Self` messages via <see cref="Scheduler.TimerScheduler"/>.
    /// <para>
    /// Timers are bound to the lifecycle of the actor that owns it,
    /// and thus are cancelled automatically when it is restarted or stopped.
    /// </para>
    /// </summary>
    public abstract class AbstractActorWithTimers : AbstractActor, IWithTimers
    {
        public ITimerScheduler Timers { get; set; }
    }

    /// <summary>
    /// If the validation of the <see cref="ReceiveBuilder"/> match logic turns out to be a bottleneck for some of your
    /// actors you can consider to implement it at lower level by extending <see cref="UntypedAbstractActor"/> instead
    /// of <see cref="AbstractActor"/>. The partial functions created by the <see cref="ReceiveBuilder"/> consist of 
    /// multiple lambda expressions for every match statement, where each lambda is referencing the code to be run. 
    /// This is something that the CLR can have problems optimizing and the resulting code might not be as performant as the
    /// untyped version.
    /// <para>
    /// When extending <see cref="UntypedAbstractActor"/> each message is received as an untyped 
    /// <see cref="object"/> and you have to inspect and cast it to the actual message type in other ways.
    /// </para>
    /// </summary>
    public abstract class UntypedAbstractActor : AbstractActor
    {
        protected sealed override Receive CreateReceive =>
            throw new InvalidOperationException("CreateReceive should not be used by UntypedAbstractActor");

        protected override bool Receive(object message)
        {
            OnReceive(message);
            return true;
        }

        /// <summary>
        /// To be implemented by concrete <see cref="UntypedAbstractActor"/>, this defines the behavior of the actor.
        /// </summary>
        protected abstract void OnReceive(object message);
    }

    /// <summary>
    /// Actor base class that should be extended to create an actor with a stash.
    /// <para>
    /// The stash enables an actor to temporarily stash away messages that can not or
    /// should not be handled using the actor's current behavior.
    /// </para>
    /// <para>
    /// Note that the subclasses of `AbstractActorWithStash` by default request a deque-based mailbox since this class
    /// implements the <see cref="IRequiresMessageQueue{T}"/> marker interface. You can override the default mailbox 
    /// provided when <see cref="IDequeBasedMessageQueueSemantics"/> are requested via config:
    /// <code>
    /// akka.actor.mailbox.requirements {
    ///     "Akka.Dispatch.IBoundedDequeBasedMessageQueueSemantics" = your-custom-mailbox
    /// }
    /// </code>
    /// Alternatively, you can add your own requirement marker to the actor and configure a mailbox type to be used
    /// for your marker.
    /// </para>
    /// <para>
    /// For a `Stash` based actor that enforces unbounded deques see <see cref="AbstractActorWithUnboundedStash"/>.
    /// There is also an unrestricted version <see cref="AbstractActorWithUnrestrictedStash"/> that does not
    /// enforce the mailbox type.
    /// </para>
    /// </summary>
    public abstract class AbstractActorWithStash : AbstractActor, IWithStash
    {
        public IStash Stash { get; set; }
    }

    /// <summary>
    /// Actor base class with `Stash` that enforces an unbounded deque for the actor. 
    /// The proper mailbox has to be configured manually, and the mailbox should extend the 
    /// <see cref="IDequeBasedMessageQueueSemantics"/> marker interface. 
    /// <para>
    /// See <see cref="AbstractActorWithStash"/> for details on how `Stash` works.
    /// </para>
    /// </summary>
    public abstract class AbstractActorWithUnboundedStash : AbstractActor, IWithUnboundedStash
    {
        public IStash Stash { get; set; }
    }

    /// <summary>
    /// Actor base class with `Stash` that does not enforce any mailbox type. The mailbox of the actor has to be configured
    /// manually. 
    /// <para>
    /// See <see cref="AbstractActorWithStash"/> for details on how `Stash` works.
    /// </para>
    /// </summary>
    public abstract class AbstractActorWithUnrestrictedStash : AbstractActor, IWithUnrestrictedStash
    {
        public IStash Stash { get; set; }
    }
}
