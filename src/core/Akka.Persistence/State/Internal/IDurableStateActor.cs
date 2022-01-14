// //-----------------------------------------------------------------------
// // <copyright file="IDurableStateActor.cs" company="Akka.NET Project">
// //     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
// //     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// // </copyright>
// //-----------------------------------------------------------------------

namespace Akka.Persistence.State.Internal
{
    public interface IDurableStateActor
    {
        /// <summary>
        /// Identifier of the persistent identity for which messages should be replayed.
        /// </summary>
        string PersistenceId { get; }

        /// <summary>
        /// DurableStateBehavior plugin id must not be null; use empty string for 'default' state store
        /// </summary>
        string DurableStateStorePluginId { get; }
    }
}