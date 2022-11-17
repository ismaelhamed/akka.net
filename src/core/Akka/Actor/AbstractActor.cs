//-----------------------------------------------------------------------
// <copyright file="AbstractActor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Akka.Dispatch;
using Akka.Tools.MatchHandler;

namespace Akka.Actor
{
    public abstract class AbstractActor : ActorBase
    {
        /// <summary>
        /// An actor has to define its initial receive behavior by implementing
        /// the `CreateReceive` method.
        /// </summary>
        protected abstract Receive CreateReceive { get; }

        protected override bool Receive(object message) => CreateReceive(message);

        /// <summary>
        /// Convenience factory of the `ReceiveBuilder`.
        /// Creates a new empty <see cref="Tools.MatchHandler.ReceiveBuilder"/>.
        /// </summary>
        public static ReceiveBuilder ReceiveBuilder => ReceiveBuilder.Create();

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="action">TBD</param>
        protected void RunTask(Action action) => ActorTaskScheduler.RunTask(action);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="action">TBD</param>
        protected void RunTask(Func<Task> action) => ActorTaskScheduler.RunTask(action);
    }

    /// <summary>
    /// When extending `UntypedAbstractActor` each message is received as an untyped `object` 
    /// and you have to inspect and cast it to the actual message type in other ways (instanceof checks).
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
        /// To be implemented by concrete UntypedAbstractActor, this defines the behavior of the actor.
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
    /// Note that the subclasses of `AbstractActorWithStash` by default request a Deque based mailbox since this class
    /// implements the <c>IRequiresMessageQueue{IUnboundedDequeBasedMessageQueueSemantics}</c> marker interface.
    /// You can override the default mailbox provided when `DequeBasedMessageQueueSemantics` are requested via config:
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
    public abstract class AbstractActorWithStash : AbstractActor
    {
        // TODO
    }

    /// <summary>
    /// Actor base class with <see cref="IWithUnboundedStash"/> that enforces an unbounded deque for the actor. 
    /// The proper mailbox has to be configured manually, and the mailbox should extend the 
    /// <see cref="IUnboundedDequeBasedMessageQueueSemantics"/> marker trait. See <see cref="AbstractActorWithStash"/> 
    /// for details on how `Stash` works.
    /// </summary>
    public abstract class AbstractActorWithUnboundedStash : AbstractActor, IWithUnboundedStash
    {
        public IStash Stash { get; set; }
    }

    /// <summary>
    /// Actor base class with `Stash` that does not enforce any mailbox type. The mailbox of the actor has to be configured
    /// manually. See <see cref="AbstractActorWithStash"/> for details on how `Stash` works.
    /// </summary>
    public abstract class AbstractActorWithUnrestrictedStash : AbstractActor
    {
        // TODO
    }
}
