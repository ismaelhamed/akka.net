//-----------------------------------------------------------------------
// <copyright file="DurableStateStoreInteractions.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;

namespace Akka.Persistence.State.Internal
{
    public abstract partial class DurableStateActor2
    {
        // FIXME use CircuitBreaker here, that can also replace the RecoveryTimeout

        private void InternalGet()
        {
            _ = _durableStateUpdateStore.Value.GetObject(PersistenceId)
                .PipeTo(Self,
                    success: state => new GetSuccess(state),
                    failure: exception => new GetFailure(exception));
        }

        private void InternalUpsert(object value, long seqNr)
        {
            _ = _durableStateUpdateStore.Value.UpsertObject(PersistenceId, seqNr, value)
                .PipeTo(Self,
                    success: state => new UpsertSuccess(value, seqNr), // TODO: UpsertSuccess.Instance
                    failure: exception => new UpsertFailure(exception, value, seqNr));  // TODO: new UpsertFailure(exception);
        }
    }
}
