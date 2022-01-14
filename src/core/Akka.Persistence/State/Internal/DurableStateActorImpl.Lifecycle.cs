// //-----------------------------------------------------------------------
// // <copyright file="DurableStateActorImpl.Lifecycle.cs" company="Akka.NET Project">
// //     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
// //     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// // </copyright>
// //-----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Akka.Persistence.State.Internal
{
    public partial class DurableStateActorImpl
    {
        private void RequestRecoveryPermit()
        {
            Extension.RecoveryPermitter().Tell(Akka.Persistence.RequestRecoveryPermit.Instance, Self);
            ChangeState(RequestingRecoveryPermit());
        }

        protected internal override bool AroundReceive(Receive receive, object message)
        {
            _currentState.StateReceive(receive, message);
            return true;
        }
        
        public override void AroundPreStart()
        {
            if (PersistenceId == null)
                throw new ArgumentNullException($"PersistenceId is [null] for DurableStateActor [{Self.Path}]");
            
            // request a permit, as only once we obtain one we can start recovery
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
                // TODO
                var inner = message switch
                {
                    WriteMessageSuccess success => success.Persistent,
                    LoopMessageSuccess success => success.Message,
                    ReplayedMessage replayedMessage => replayedMessage.Persistent,
                    _ => message,
                };

                //FlushJournalBatch();
                base.AroundPreRestart(cause, inner);
            }
        }
        
        public override void AroundPostRestart(Exception reason, object message)
        {
            RequestRecoveryPermit();
            base.AroundPostRestart(reason, message);
        }

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