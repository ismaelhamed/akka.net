//-----------------------------------------------------------------------
// <copyright file="DurableStateActorImpl.Recovery.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Persistence.Internal;
using Akka.Persistence.State.Dsl;

namespace Akka.Persistence.State.Internal
{
    internal delegate void StateReceive(Receive receive, object message);

    internal class DurableStateActorState // FIXME: mouthful..  DurableStateBehavior
    {
        public DurableStateActorState(string name, Func<bool> isRecoveryRunning, Func<long> currentSequenceNumber, StateReceive stateReceive)
        {
            Name = name;
            IsRecoveryRunning = isRecoveryRunning;
            CurrentSequenceNumber = currentSequenceNumber;
            StateReceive = stateReceive;
        }

        public string Name { get; }
        public Func<bool> IsRecoveryRunning { get; }
        public Func<long> CurrentSequenceNumber { get; }
        public StateReceive StateReceive { get; }

        public override string ToString() => Name;
    }

    public abstract partial class DurableStateActorImpl
    {
        private ICancelable _recoveryTimer;

        private void StartRecoveryTimer()
        {
            CancelRecoveryTimer();
            _recoveryTimer = Context.System.Scheduler.ScheduleTellOnceCancelable(Settings.RecoveryTimeout, Context.Self, RecoveryTimeout.Instance, Context.Self);
        }

        private void CancelRecoveryTimer()
        {
            _recoveryTimer?.Cancel();
            _recoveryTimer = null;
        }

        /// <summary>
        /// Mutates start behaviour, by setting the holdingRecoveryPermit to false
        /// </summary>
        private void ReturnRecoveryPermit(string reason)
        {
            // No need to return the permit
            if (!_holdingRecoveryPermit)
                return;

            Log.Debug("Returning recovery permit, reason: {0}", reason);
            Extension.RecoveryPermitter().Tell(Akka.Persistence.ReturnRecoveryPermit.Instance, Self);
            _holdingRecoveryPermit = false;
        }

        private void TransitToProcessingState()
        {
            //if (_eventBatch.Count > 0) FlushBatch();

            if (_pendingStashingPersistInvocations > 0)
            {
                ChangeState(PersistingEvents());
            }
            else
            {
                ChangeState(ProcessingCommands());
                _internalStash.UnstashAll();
            }
        }

        /// <summary>
        /// First (of three) behavior of a DurableStateActor.
        /// <para>Requests a permit to start recovery of this actor; this is tone to avoid hammering the store with too many concurrently recovering actors.</para>
        /// <para>See next behavior <see cref="Recovering"/>.</para>
        /// </summary>
        private DurableStateActorState RequestingRecoveryPermit()
        {
            return new DurableStateActorState("waiting for recovery permit", () => true, null, (receive, message) =>
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
            var currentSequenceNumber = 0L;
            var recoveryRunning = true;

            // Protect against store stalling forever because of store overloaded and such
            StartRecoveryTimer();

            // DurableStateStoreInteractions.InternalGet
            _ = _durableStateUpdateStore.GetObject(PersistenceId)
                .PipeTo(Self,
                    success: state => new GetSuccess(state),
                    failure: exception => new GetFailure(exception));

            void OnRecoveryCompleted(RecoveryState state)
            {
                try
                {
                    // TODO: shouldn't we set the state?
                    currentSequenceNumber = state.SeqNr;
                    recoveryRunning = false;

                    OnReplaySuccess();                    

                    if (Log.IsDebugEnabled)
                        Log.Debug("Recovery for persistenceId [{0}] took {1}", PersistenceId, DateTime.UtcNow.Ticks - state.RecoveryStartTime);

                    // TODO: base.AroundReceive(recoveryBehavior, RecoveryCompleted.Instance);

                    ChangeState(HandlingCommands(new RunningState(state.SeqNr, state.State)));
                    // TODO: unstashOne -> _internalStash.Unstash() ??
                }
                finally
                {
                    CancelRecoveryTimer();
                }
                ReturnRecoveryPermit("recovery completed successfully");
            }

            void OnRecoveryFailed(Exception cause)
            {
                CancelRecoveryTimer();
                try
                {
                    if (Log.IsDebugEnabled) Log.Debug("Recovery failure for persistenceId [{0}]", PersistenceId);
                    OnRecoveryFailure(new DurableStateStoreException($"Exception during recovery. PersistenceId [{PersistenceId}]. {cause.Message}", cause));
                }
                finally
                {
                    Context.Stop(Self);
                }
                ReturnRecoveryPermit($"on recovery failure: {cause.Message}");
            }

            void OnRecoveryTimeout() =>
                OnRecoveryFailed(new RecoveryTimedOutException($"Recovery timed out, didn't get state within {Settings.RecoveryTimeout}"));

            return new DurableStateActorState("recovery started", () => recoveryRunning, () => currentSequenceNumber, (receive, message) =>
            {
                try
                {
                    switch (message)
                    {
                        case GetSuccess success:
                            object state = null; // TODO: SnapshotAdapter to migrate classis persistent actors, or emptyState (passed by constructor)
                            Log.Debug("Recovered from seqNr [{0}]", success.Result.SeqNr);
                            CancelRecoveryTimer();
                            OnRecoveryCompleted(new RecoveryState(success.Result.SeqNr, state, DateTime.UtcNow.Ticks));
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
                        case GetState get:
                            StashInternally(get);
                            break;
                        default:
                            base.Unhandled(message);
                            break;
                    }
                }
                catch (Exception)
                {
                    ReturnRecoveryPermit();
                    throw;
                }
            });
        }

        /// <summary>
        /// Conceptually third (of three) -- also known as 'final' or 'ultimate' -- form of <see cref="DurableStateActor"/>.
        /// <para>
        /// In this phase recovery has completed successfully and we continue handling incoming commands, as well
        /// as persisting new state as dictated by the user handlers.
        /// </para>
        /// <para>
        /// This behavior operates in two phases (also behaviors):
        /// <para>- HandlingCommands - where the command handler is invoked for incoming commands</para>
        /// <para>- PersistingState - where incoming commands are stashed until persistence completes</para>
        /// </para>
        /// <para>
        /// This is implemented as such to avoid creating many Running instances, which perform the Persistence
        /// extension lookup on creation and similar things (config lookup)
        /// </para>
        /// <para>See previous <see cref="Recovering"/>.</para>
        /// </summary>
        //private DurableStateActorState Running(RunningState state)
        //{
        //    // Needed for WithSeqNrAccessible, when unstashing
        //    var currentSequenceNumber = state.SeqNr;

        //    return new DurableStateActorState("handling commands", () => currentSequenceNumber, (receive, message) =>
        //    {
        //        switch (message)
        //        {
        //            case IncomingCommand cmd:
        //                // TODO
        //                break;
        //            case GetState _:
        //                Sender.Tell(state.State);
        //                break;
        //            default:
        //                base.Unhandled(message);
        //                break;
        //        }
        //    });
        //}

        private DurableStateActorState PersistingState(
            RunningState state,
            RunningState visibleState, // previous state until write success
            long persistStartTime)
        {
            var currentSequenceNumber = state.SeqNr;

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
                    ChangeState(HandlingCommands(state));
                    UnstashInternally(all);
                }
            }

            void OnUpsertSuccess()
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("Received UpsertSuccess response after: {0} ticks", DateTime.UtcNow.Ticks - persistStartTime);

                visibleState = state;
                //var newState = ApplySideEffects(sideEffects, state)
                //tryUnstashOne(newState)

                OnWriteMessageComplete(false);
            }

            void OnUpsertFailed(Exception cause)
            {
                try
                {
                    OnWriteMessageComplete(true);
                    OnPersistFailure(new DurableStateStoreException(PersistenceId, currentSequenceNumber, cause), state);
                }
                finally
                {
                    Context.Stop(Self);
                }
            }

            return new DurableStateActorState("persisting events", () => false, () => currentSequenceNumber, (receive, message) =>
            {
                switch (message)
                {
                    case UpsertSuccess _:
                        OnUpsertSuccess();
                        break;
                    case UpsertFailure failure:
                        OnUpsertFailed(failure.Cause);
                        break;
                    //case IncomingCommand command:
                    //    StashInternally(command);
                    //    break;
                    //case GetState get:
                    //    StashInternally(get);
                    //    break;
                    default:
                        StashInternally(message);
                        break;
                }
            });
        }

        private DurableStateActorState HandlingCommands(RunningState state)
        {
            var currentSequenceNumber = state.SeqNr;

            void OnCommand(Receive receive, object message)
            {
                try
                {
                    //// TODO: 
                    //currentSequenceNumber = state.SeqNr + 1;
                    //var stateAfterApply = state.ApplyState(newState);
                    //var stateToPersist = AdaptState(newState); // setup.snapshotAdapter.toJournal(newState)
                    //var newState2 = InternalUpsert(message, stateAfterApply, stateToPersist);

                    base.AroundReceive(receive, message);
                    OnProcessingCommandsAroundReceiveComplete(false);
                }
                catch (Exception)
                {
                    OnProcessingCommandsAroundReceiveComplete(true);
                    throw;
                }
            }

            return new DurableStateActorState("handling commands", () => false, () => state.SeqNr, (receive, message) =>
            {
                switch (message)
                {
                    case IncomingCommand cmd:
                        // TODO: OnCommand(, cmd)
                        break;
                    case GetState _:
                        // Used by DurableStateBehaviorTestKit to retrieve the state
                        Sender.Tell(state.State);
                        break;
                    default:
                        base.Unhandled(message);
                        break;
                }
            });
        }

        private void OnProcessingCommandsAroundReceiveComplete(bool err)
        {
            if (_asyncTaskRunning)
            {
                //do nothing, wait for the task to finish
            }
            else if (_pendingStashingPersistInvocations > 0)
                ChangeState(PersistingState());
            else
                UnstashInternally(err);
        }
    }

    internal sealed class RecoveryState
    {
        public RecoveryState(long seqNr, object state, long recoveryStartTime)
        {
            SeqNr = seqNr;
            State = state;
            RecoveryStartTime = recoveryStartTime;
        }

        public long SeqNr { get; }
        public object State { get; }
        public long RecoveryStartTime { get; }
    }

    internal sealed class RunningState
    {
        public RunningState(long seqNr, object state)
        {
            SeqNr = seqNr;
            State = state;
        }

        public long SeqNr { get; }
        public object State { get; }

        public RunningState NextSequenceNr() => Copy(SeqNr + 1);

        public RunningState ApplyState(object updated) => Copy(state: updated);

        private RunningState Copy(long? revision = null, object state = null) =>
            new RunningState(revision ?? SeqNr, state ?? State);
    }
}