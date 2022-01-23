//-----------------------------------------------------------------------
// <copyright file="DurableStateImpl.Recovery.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.Persistence.Internal;

namespace Akka.Persistence.State.Internal
{
    internal delegate void StateReceive(Receive receive, object message);

    internal class DurableStateActorState // FIXME: mouthful..  DurableStateBehavior, inherit from EvensourcedState
    {
        public DurableStateActorState(string name, Func<bool> isRecoveryRunning, /*Func<long> currentSequenceNumber, */StateReceive stateReceive)
        {
            Name = name;
            IsRecoveryRunning = isRecoveryRunning;
            //CurrentSequenceNumber = currentSequenceNumber;
            StateReceive = stateReceive;
        }

        public string Name { get; }
        public Func<bool> IsRecoveryRunning { get; }
        //public Func<long> CurrentSequenceNumber { get; }
        public StateReceive StateReceive { get; }

        public override string ToString() => Name;
    }

    /// <summary>
    /// TBD
    /// </summary>
    public abstract partial class DurableStateActor2 // FIXME: Rename to DurableStateImpl
    {
        /// <summary>
        /// First (of three) behavior of a DurableStateActor.
        /// <para>Requests a permit to start recovery of this actor; this is tone to avoid hammering the store with too many concurrently recovering actors.</para>
        /// <para>See next behavior <see cref="Recovering"/>.</para>
        /// </summary>
        private DurableStateActorState RequestingRecoveryPermit()
        {
            return new DurableStateActorState("RequestingRecoveryPermit", () => true, (_, message) =>
            {
                if (message is RecoveryPermitGranted)
                {
                    Log.Debug("Initializing recovery");
                    _holdingRecoveryPermit = true;
                    ChangeState(Recovering());
                }
                else StashInternally(message);
            });
        }

        /// <summary>
        /// Second (of three) behavior of a DurableStateActor.
        /// <para>
        /// In this state the recovery process is initiated. We try to obtain the state from the configured DurableStateActor, 
        /// and if it exists, we use it instead of the initial EmptyState.
        /// </para>
        /// <para>See next behavior <see cref="Running"/>. See previous behavior <see cref="RequestingRecoveryPermit"/>.</para>
        /// </summary>
        private DurableStateActorState Recovering()
        {
            var recoveryRunning = true;

            // Protect against store stalling forever because of store overloaded and such
            StartRecoveryTimer();
            InternalGet();

            void OnRecoveryCompleted(object state, long revision, long recoveryStartTime)
            {
                try
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("Recovery for persistenceId [{0}] took {1}", PersistenceId, TimeSpan.FromTicks(DateTime.UtcNow.Ticks - recoveryStartTime));

                    OnReplaySuccess();
                    recoveryRunning = false;

                    var highestRevision = Math.Max(revision, LastRevision);
                    _currentRevision = highestRevision;
                    LastRevision = highestRevision;

                    try
                    {
                        base.AroundReceive(ReceiveRecover, new RecoveryCompleted(state));
                    }
                    finally
                    {
                        if (_eventBatch.Count > 0) FlushBatch();
                        if (_pendingStashingPersistInvocations > 0)
                        {
                            ChangeState(PersistingEvents(DateTime.UtcNow.Ticks));
                        }
                        else
                        {
                            ChangeState(HandlingCommands());
                            _internalStash.UnstashAll();
                        }
                    }
                }
                finally
                {
                    CancelRecoveryTimer();
                }
                ReturnRecoveryPermit("recovery completed successfully");
            }

            void OnRecoveryFailed(Exception cause)
            {
                try
                {
                    if (Log.IsDebugEnabled) Log.Debug("Recovery failure for persistenceId [{0}]", PersistenceId);
                    OnRecoveryFailure(new DurableStateStoreException($"Exception during recovery. PersistenceId [{PersistenceId}]. {cause.Message}", cause));
                }
                finally
                {
                    CancelRecoveryTimer();
                    Context.Stop(Self);
                }
                ReturnRecoveryPermit($"on recovery failure: {cause.Message}");
            }

            void OnRecoveryTimeout() =>
                OnRecoveryFailed(new RecoveryTimedOutException($"Recovery timed out, didn't get state within {Settings.RecoveryTimeout}"));

            return new DurableStateActorState("Recovering", () => recoveryRunning, (_, message) =>
            {
                switch (message)
                {
                   case GetSuccess success:
                        // TODO: retrieve state
                        // TODO?: SnapshotAdapter to migrate classis persistent actors, or emptyState (passed by constructor)
                        object state = null;
                        Log.Debug("Recovered from revision [{0}]", success.Result.Revision);
                        CancelRecoveryTimer();
                        OnRecoveryCompleted(state, success.Result.Revision, DateTime.UtcNow.Ticks);
                        break;
                    case GetFailure failure:
                        OnRecoveryFailed(failure.Cause);
                        break;
                    case RecoveryTimeout _:
                        OnRecoveryTimeout();
                        break;
                    case IncomingCommand cmd:
                        // during recovery, stash all incoming commands
                        StashInternally(cmd);
                        break;
                    //case GetState get:
                    //    StashInternally(get);
                    //    break;
                    default:
                        base.Unhandled(message);
                        break;
                }
            });
        }

        private DurableStateActorState PersistingEvents(
            /*
            RunningState state,
            RunningState visibleState, // previous state until write success
            */
            long persistStartTime)
        {
            void OnWriteMessageComplete(bool all)
            {
                var invocation = _pendingInvocations.Pop();

                // enables an early return to `processingCommands`, because if this counter hits `0`,
                // we know the remaining pendingInvocations are all `persistAsync` created, which
                // means we can go back to processing commands also - and these callbacks will be called as soon as possible
                if (invocation is StashingHandlerInvocation)
                    _pendingStashingPersistInvocations--;

                if (_pendingStashingPersistInvocations == 0)
                {
                    ChangeState(HandlingCommands());
                    UnstashInternally(all);
                }
            }

            void OnUpsertSuccess(object stateToPersist, long revision)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Received UpsertSuccess response after: {0} ticks", DateTime.UtcNow.Ticks - persistStartTime);

                _isWriteInProgress = false;

                UpdateLastSequenceNr(revision);
                try
                {
                    PeekApplyHandler(stateToPersist);
                    OnWriteMessageComplete(false);
                }
                catch
                {
                    OnWriteMessageComplete(true);
                    throw;
                }
            }

            void OnUpsertFailed(Exception cause, object stateToPersist, long revision)
            {
                _isWriteInProgress = false;
                try
                {
                    OnWriteMessageComplete(false);
                    OnPersistFailure(new DurableStateStoreException(PersistenceId, revision, cause), stateToPersist);
                }
                finally
                {
                    Context.Stop(Self);
                }
            }

            return new DurableStateActorState("PersistingEvents", () => false, (_, message) =>
            {
                switch (message)
                {
                    case UpsertSuccess success:
                        OnUpsertSuccess(success.State, success.Revision);
                        break;
                    case UpsertFailure failure:
                        OnUpsertFailed(failure.Cause, failure.State, failure.Revision);
                        break;
                    default:
                        StashInternally(message);
                        break;
                }
            });
        }

        private DurableStateActorState HandlingCommands()
        {
            void OnWriteMessageComplete(bool all)
            {
                _pendingInvocations.Pop();
                UnstashInternally(all);
            }

            void OnCommand(Receive receive, object message)
            {
                try
                {
                    base.AroundReceive(receive, message);
                    OnProcessingCommandsAroundReceiveComplete(false);
                }
                catch (Exception)
                {
                    OnProcessingCommandsAroundReceiveComplete(true);
                    throw;
                }
            }

            return new DurableStateActorState("HandlingCommands", () => false, (receive, message) =>
            {
                switch (message)
                {
                    case UpsertSuccess _:
                        // TODO: Log Warning!
                        OnWriteMessageComplete(false);
                        break;
                    case UpsertFailure failure:
                        // TODO: Log Warning!
                        OnWriteMessageComplete(false);
                        break;
                    case IncomingCommand cmd:
                        OnCommand(receive, cmd.Command);
                        break;
                    //case GetState _:
                    //    // TODO: Used by DurableStateBehaviorTestKit to retrieve the state
                    //    Sender.Tell(state.State);
                    //    break;
                    default:
                        base.Unhandled(message);
                        break;
                }
            });
        }

        private void OnProcessingCommandsAroundReceiveComplete(bool err)
        {
            if (_eventBatch.Count > 0) FlushBatch();

            if (_asyncTaskRunning)
            {
                //do nothing, wait for the task to finish
            }
            else if (_pendingStashingPersistInvocations > 0)
                ChangeState(PersistingEvents(DateTime.UtcNow.Ticks));
            else
                UnstashInternally(err);
        }

        private void FlushBatch()
        {
            if (_eventBatch.Count > 0)
            {
                foreach (var envelop in _eventBatch.Reverse())
                {
                    _journalBatch.Add(envelop);
                }
                _eventBatch = new LinkedList<DurableStateEnvelope>();
            }

            FlushJournalBatch();
        }

        private void PeekApplyHandler(object payload)
        {
            try
            {
                _pendingInvocations.First.Value.Handler(payload);
            }
            finally
            {
                FlushBatch();
            }
        }
    }

    [Serializable]
    public sealed class RecoveryCompleted
    {
        public RecoveryCompleted(object state) => State = state;
        public object State { get; }
    }
}
