//-----------------------------------------------------------------------
// <copyright file="PersistenceTestKitDurableStateStoreProvider.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;
using Akka.Persistence.State;
using Akka.Persistence.State.Dsl;

namespace Akka.Persistence.TestKit.State
{
    public class PersistenceTestKitDurableStateStoreProvider : IDurableStateStoreProvider
    {
        private readonly IDurableStateStore _durableStateStore;

        public PersistenceTestKitDurableStateStoreProvider(ExtendedActorSystem system) => 
            _durableStateStore = new PersistenceTestKitDurableStateStore(system);

        public IDurableStateStore GetDurableStateStore() => _durableStateStore;
    }
}
