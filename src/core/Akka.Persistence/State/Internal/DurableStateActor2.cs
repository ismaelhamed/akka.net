//-----------------------------------------------------------------------
// <copyright file="DurableStateImpl.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Annotations;
using Akka.Event;
using Akka.Persistence.State.Dsl;
using Akka.Util.Internal;

namespace Akka.Persistence.State.Internal
{
    public interface IPendingHandlerInvocation
    {
        object State { get; }
        Action<object> Handler { get; }
    }

    /// <summary>
    /// Forces actor to stash incoming commands until all invocations are handled.
    /// </summary>
    public sealed class StashingHandlerInvocation : IPendingHandlerInvocation
    {
        public StashingHandlerInvocation(object state, Action<object> handler)
        {
            State = state;
            Handler = handler;
        }

        public object State { get; }

        public Action<object> Handler { get; }
    }

    internal sealed class DurableStateEnvelope
    {
        public object State { get; }
        public string PersistenceId { get; }
        public long SequenceNr { get; }
        public IActorRef Sender { get; }

        public DurableStateEnvelope(object state, string persistenceId, long sequenceNr, IActorRef sender)
        {
            State = state;
            PersistenceId = persistenceId;
            SequenceNr = sequenceNr;
            Sender = sender;
        }
    }

    public abstract partial class DurableStateActor2 : ActorBase, IWithUnboundedStash, IDurableStateActor
    {
        private static readonly AtomicCounter InstanceCounter = new AtomicCounter(1);

        private readonly Lazy<IDurableStateUpdateStore<object>> _durableStateUpdateStore;
        private readonly IStash _internalStash;
        internal DurableStateActorState _currentState;
        private bool _holdingRecoveryPermit;
        private bool _asyncTaskRunning;
        private IStash _stash;

        private bool _isWriteInProgress;
        private long _currentRevision;
        private LinkedList<DurableStateEnvelope> _eventBatch = new LinkedList<DurableStateEnvelope>();
        private ICollection<DurableStateEnvelope> _journalBatch = new List<DurableStateEnvelope>();

        /// Used instead of iterating `pendingInvocations` in order to check if safe to revert to processing commands
        private long _pendingStashingPersistInvocations = 0L;

        /// Holds user-supplied callbacks for persist/persistAsync calls
        private readonly LinkedList<IPendingHandlerInvocation> _pendingInvocations = new LinkedList<IPendingHandlerInvocation>();

        /// <summary>
        /// TODO
        /// </summary>
        protected PersistenceExtension Extension { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="Eventsourced"/> class.
        /// </summary>
        protected DurableStateActor2()
        {
            LastRevision = 0L;
            _isWriteInProgress = false;
            _currentRevision = 0L;

            Extension = Persistence.Instance.Apply(Context.System);
            _durableStateUpdateStore = new Lazy<IDurableStateUpdateStore<object>>(() =>
                DurableStateStoreRegistry.Get(Context.System)
                    .DurableStateStoreFor<IDurableStateUpdateStore<object>, object>(Settings.DurableStateStorePluginId));
            _currentState = null;
            _internalStash = CreateStash();
        }

        protected virtual ILoggingAdapter Log { get; } = Context.GetLogger();

        /// <summary>
        /// Id of the persistent entity for which messages should be replayed.
        /// </summary>
        public abstract string PersistenceId { get; }

        public string DurableStateStorePluginId { get; protected set; }

        public DurableStateSettings Settings { get; private set; }

        public virtual IStashOverflowStrategy InternalStashOverflowStrategy => Settings.StashOverflowStrategy;

        public IStash Stash
        {
            get { return _stash; }
            set { _stash = new InternalStashAwareStash(value, _internalStash); }
        }

        /// <summary>
        /// Returns true if this persistent entity is currently recovering.
        /// </summary>
        public bool IsRecovering => _currentState?.IsRecoveryRunning() ?? true;

        /// <summary>
        /// Returns true if this persistent entity has successfully finished recovery.
        /// </summary>
        public bool IsRecoveryFinished => !IsRecovering;

        /// <summary>
        /// Highest received revision so far or `0L` if this actor
        /// hasn't replayed or stored any persistent events yet.
        /// </summary>
        public long LastRevision { get; private set; }

        /// <summary>
        /// Recovery handler that receives persistent events during recovery. If a state snapshot has been captured and saved,
        /// this handler will receive a <see cref="SnapshotOffer"/> message followed by events that are younger than offer itself.
        ///
        /// This handler must not have side-effects other than changing persistent actor state i.e. it
        /// should not perform actions that may fail, such as interacting with external services,
        /// for example.
        ///
        /// If there is a problem with recovering the state of the actor from the journal, the error
        /// will be logged and the actor will be stopped.
        /// </summary>
        /// <param name="message">TBD</param>
        /// <returns>TBD</returns>
        protected abstract bool ReceiveRecover(object message);

        /// <summary>
        /// Command handler. Typically validates commands against current state - possibly by communicating with other actors.
        /// On successful validation, one or more events are derived from command and persisted.
        /// </summary>
        /// <param name="message">TBD</param>
        /// <returns>TBD</returns>
        protected abstract bool ReceiveCommand(object message);

        /// <summary>
        /// Asynchronously persists an <paramref name="event"/>. On successful persistence, the <paramref name="handler"/>
        /// is called with the persisted event. This method guarantees that no new commands will be received by a persistent actor
        /// between a call to <see cref="Persist{TEvent}(TEvent,System.Action{TEvent})"/> and execution of its handler. It also
        /// holds multiple persist calls per received command. Internally this is done by stashing. The stash used
        /// for that is an internal stash which doesn't interfere with the inherited user stash.
        ///
        ///
        /// An event <paramref name="handler"/> may close over eventsourced actor state and modify it. Sender of the persistent event
        /// is considered a sender of the corresponding command. That means one can respond to sender from within an event handler.
        ///
        ///
        /// Within an event handler, applications usually update persistent actor state using
        /// persisted event data, notify listeners and reply to command senders.
        ///
        ///
        /// If persistence of an event fails, <see cref="OnPersistFailure" /> will be invoked and the actor will
        /// unconditionally be stopped. The reason that it cannot resume when persist fails is that it
        /// is unknown if the event was actually persisted or not, and therefore it is in an inconsistent
        /// state. Restarting on persistent failures will most likely fail anyway, since the journal
        /// is probably unavailable. It is better to stop the actor and after a back-off timeout start
        /// it again.
        /// </summary>
        /// <typeparam name="TState">TBD</typeparam>
        /// <param name="state">TBD</param>
        /// <param name="handler">TBD</param>
        public void Persist<TState>(TState state, Action<TState> handler)
        {
            if (IsRecovering)
            {
                throw new InvalidOperationException("Cannot persist during replay. Events can be persisted when receiving RecoveryCompleted or later.");
            }

            _pendingStashingPersistInvocations++;
            _pendingInvocations.AddLast(new StashingHandlerInvocation(state, o => handler((TState)o)));
            _eventBatch.AddFirst(new DurableStateEnvelope(state, PersistenceId, NextRevision(), sender: Sender));
        }

        /// <summary>
        /// Called whenever a message replay succeeds.
        /// </summary>
        protected virtual void OnReplaySuccess() { }

        /// <summary>
        /// Called whenever the state recovery fails. By default it log the errors.
        /// </summary>
        /// <param name="reason">Reason of failure</param>
        protected virtual void OnRecoveryFailure(Exception reason) => 
            Log.Error(reason, "Exception when recovering state for persistenceId [{0}]", PersistenceId);

        /// <summary>
        /// Called when persist fails. By default it logs the error.
        /// Subclass may override to customize logging and for example send negative
        /// acknowledgment to sender.
        /// <para>
        /// The actor is always stopped after this method has been invoked.
        /// </para>
        /// <para>
        /// Note that the state may or may not have been saved, depending on the type of failure.
        /// </para>
        /// </summary>
        /// <param name="cause">TBD</param>
        /// <param name="state">TBD</param>
        protected virtual void OnPersistFailure(Exception cause, object state) =>
            Log.Error(cause, "Failed to persist state [{0}] for persistenceId [{1}].", state.GetType(), PersistenceId);

        ///// <summary>
        ///// Called when the store rejected <see cref="Persist{TState}(TState, Action{TState})"/> of state.
        ///// The state was not stored. By default this method logs the problem as an error, and the actor continues.
        ///// The callback handler that was passed to the <see cref="Persist{TState}(TState, Action{TState})"/>
        ///// method will not be invoked.
        ///// </summary>
        ///// <param name="cause">TBD</param>
        ///// <param name="state">TBD</param>
        //protected virtual void OnPersistRejected(Exception cause, object state)
        //{
        //    Log.Error(cause, "Rejected to persist state [{0}] for persistenceId [{1}] due to [{2}].",
        //        state.GetType(), PersistenceId, cause.Message);
        //}

        /// <summary>
        /// Runs an asynchronous task for incoming messages in context of <see cref="ReceiveCommand(object)"/> .
        /// <para>
        /// The actor will be suspended until the task returned by <paramref name="action"/> completes, 
        /// including the <see cref="Persist{TState}(TState, Action{TState})" /> calls.
        /// </para>
        /// </summary>
        /// <param name="action">Async task to run</param>
        protected void RunTask(Func<Task> action)
        {
            if (_asyncTaskRunning)
                throw new NotSupportedException("RunTask calls cannot be nested");

            Task Wrap()
            {
                var task = action();
                if (task.IsCompleted) return task;

                _asyncTaskRunning = true;
                var tcs = new TaskCompletionSource<object>();
                task.ContinueWith(r =>
                {
                    _asyncTaskRunning = false;
                    OnProcessingCommandsAroundReceiveComplete(r.IsFaulted || r.IsCanceled);

                    if (r.IsFaulted)
                        tcs.TrySetException(r.Exception);
                    else if (r.IsCanceled)
                        tcs.TrySetCanceled();
                    else
                        tcs.TrySetResult(null);
                }, TaskContinuationOptions.AttachedToParent & TaskContinuationOptions.ExecuteSynchronously);

                task = tcs.Task;
                return task;
            }

            Dispatch.ActorTaskScheduler.RunTask(Wrap);
        }

        private void ChangeState(DurableStateActorState state) => _currentState = state;

        private void UpdateLastSequenceNr(long sequenceNr)
        {
            if (sequenceNr > LastRevision) LastRevision = sequenceNr;
        }

        private long NextRevision() => ++_currentRevision;

        private void FlushJournalBatch()
        {
            if (!_isWriteInProgress && _journalBatch.Count > 0)
            {
                // TODO: 
                //currentSequenceNumber = state.SeqNr + 1;
                //var stateAfterApply = state.ApplyState(newState);
                //var stateToPersist = AdaptState(newState); // setup.snapshotAdapter.toJournal(newState)
                //var newState2 = InternalUpsert(message, stateAfterApply, stateToPersist);

                // TODO: should only be one?!
                _journalBatch.ForEach(envelop => InternalUpsert(envelop.State, envelop.SequenceNr));

                _journalBatch = new List<DurableStateEnvelope>(0);
                _isWriteInProgress = true;
            }
        }

        #region Stashing

        private IStash CreateStash()
        {
            return Context.CreateStash(GetType());
        }

        /// <summary>
        /// Stash a command to the internal stash buffer, which is used while waiting for persist to be completed.
        /// </summary>
        private void StashInternally(object msg)
        {
            try
            {
                _internalStash.Stash();
            }
            catch (StashOverflowException)
            {
                var strategy = Settings.StashOverflowStrategy;
                switch (strategy)
                {
                    case Drop _:
                        {
                            var sender = Sender;
                            var dropName = msg is IncomingCommand command ? command.Command.GetType().Name : msg.GetType().Name;
                            Context.System.Log.Warning("Stash buffer is full, dropping message [{0}]", dropName);
                            Context.System.DeadLetters.Tell(new DeadLetter(msg, sender, Self), Sender);
                            break;
                            // TODO: context.system.eventStream.publish(Dropped(msg, "Stash buffer is full", Context.Self))
                        }
                    case Fail _:
                        throw;
                    default: // should not happen
                        throw;
                }
            }
        }

        private void UnstashInternally(bool all)
        {
            if (all)
                _internalStash.UnstashAll();
            else
                _internalStash.Unstash();
        }

        private class InternalStashAwareStash : IStash
        {
            private readonly IStash _userStash;
            private readonly IStash _internalStash;

            public InternalStashAwareStash(IStash userStash, IStash internalStash)
            {
                _userStash = userStash;
                _internalStash = internalStash;
            }

            public void Stash()
            {
                _userStash.Stash();
            }

            public void Unstash()
            {
                _userStash.Unstash();
            }

            public void UnstashAll()
            {
                // Internally, all messages are processed by unstashing them from
                // the internal stash one-by-one. Hence, an unstashAll() from the
                // user stash must be prepended to the internal stash.
                _internalStash.Prepend(ClearStash());
            }

            public void UnstashAll(Func<Envelope, bool> predicate)
            {
                _userStash.UnstashAll(predicate);
            }

            public IEnumerable<Envelope> ClearStash()
            {
                return _userStash.ClearStash();
            }

            public void Prepend(IEnumerable<Envelope> envelopes)
            {
                _userStash.Prepend(envelopes);
            }
        }

        #endregion
    }

    //
    // Protocol used internally by the DurableStateActor
    //

    [InternalApi]
    public interface IInternalProtocol { }

    ///// <summary>
    ///// Used by DurableStateBehaviorTestKit to retrieve the state. Also, GetPersistenceId
    ///// </summary>
    //[InternalApi]
    //internal sealed class GetState : IInternalProtocol
    //{
    //    public static GetState Instance { get; } = new GetState();
    //    private GetState() { }
    //}

    [InternalApi]
    public sealed class RecoveryPermitGranted : IInternalProtocol
    {
        public static RecoveryPermitGranted Instance { get; } = new RecoveryPermitGranted();
        private RecoveryPermitGranted()
        { }
    }

    [InternalApi]
    public sealed class GetSuccess : IInternalProtocol
    {
        public GetObjectResult<object> Result { get; }
        public GetSuccess(GetObjectResult<object> result) => Result = result;
    }

    [InternalApi]
    public sealed class GetFailure : IInternalProtocol
    {
        public Exception Cause { get; }
        public GetFailure(Exception cause) => Cause = cause;
    }

    [InternalApi]
    public sealed class RecoveryTimeout : IInternalProtocol
    {
        public static RecoveryTimeout Instance { get; } = new RecoveryTimeout();
        private RecoveryTimeout() { }
    }

    [InternalApi]
    public sealed class UpsertSuccess : IInternalProtocol
    {
        //public static UpsertSuccess Instance { get; } = new UpsertSuccess();
        //private UpsertSuccess() { }

        public object State { get; }
        public long Revision { get; set; }

        public UpsertSuccess(object state, long revision)
        {
            State = state;
            Revision = revision;
        }
    }

    [InternalApi]
    public sealed class UpsertFailure : IInternalProtocol
    {
        public Exception Cause { get; }
        public object State { get; }
        public long Revision { get; set; }

        public UpsertFailure(Exception cause, object state, long revision)
        {
            Cause = cause;
            State = state;
            Revision = revision;
        }
    }

    [InternalApi]
    public sealed class IncomingCommand : IInternalProtocol
    {
        public object Command { get; }
        public IncomingCommand(object command) => Command = command;
    }
}
