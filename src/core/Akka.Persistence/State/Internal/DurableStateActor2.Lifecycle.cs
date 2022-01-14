//-----------------------------------------------------------------------
// <copyright file="DurableStateImpl.Lifecycle.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Akka.Persistence.State.Internal
{
    /// <summary>
    /// TBD
    /// </summary>
    public partial class DurableStateActor2
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

        private void RequestRecoveryPermit()
        {
            // request a permit, as only once we obtain one we can start recovery
            Extension.RecoveryPermitter().Tell(Akka.Persistence.RequestRecoveryPermit.Instance, Self);
            ChangeState(RequestingRecoveryPermit());
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

        protected internal override bool AroundReceive(Receive receive, object message)
        {
            var innerMsg = message switch
            {
                Akka.Persistence.RecoveryPermitGranted _ => RecoveryPermitGranted.Instance,
                IInternalProtocol _ => message, // such as RecoveryTimeout
                _ => new IncomingCommand(message),
            };

            var actorCell = (ActorCell)Context;
            // this is important for stash interaction, as stash will look directly at CurrentMessage
            actorCell.CurrentMessage = innerMsg;

            _currentState.StateReceive(receive, innerMsg);
            return true;
        }

        /// <inheritdoc/>
        public override void AroundPreStart()
        {
            if (PersistenceId == null)
                throw new ArgumentNullException($"PersistenceId is [null] for DurableStateActor [{Self.Path}]");

            Settings = DurableStateSettings.Create(Context.System, DurableStateStorePluginId ?? "");
            RequestRecoveryPermit();
            base.AroundPreStart();
        }

        /// <inheritdoc/>
        public override void AroundPreRestart(Exception cause, object message)
        {
            try
            {
                _internalStash.UnstashAll();
                Stash.UnstashAll(/* TODO: UnstashFilterPredicate*/);
            }
            finally
            {
                // TODO: Extract the originator message??
                var inner = message switch
                {
                    WriteMessageSuccess success => success.Persistent,
                    LoopMessageSuccess success => success.Message,
                    ReplayedMessage replayedMessage => replayedMessage.Persistent,
                    _ => message,
                };

                FlushJournalBatch();
                base.AroundPreRestart(cause, inner);
            }
        }

        /// <inheritdoc/>
        public override void AroundPostRestart(Exception reason, object message)
        {
            RequestRecoveryPermit();
            base.AroundPostRestart(reason, message);
        }

        /// <inheritdoc/>
        public override void AroundPostStop()
        {
            try
            {
                _internalStash.UnstashAll();
                Stash.UnstashAll(/* TODO: UnstashFilterPredicate */);
            }
            finally
            {
                base.AroundPostStop();
            }
        }
    }
}
