//-----------------------------------------------------------------------
// <copyright file="IDurableStateStoreProvider.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Persistence.State.Dsl;

namespace Akka.Persistence.State
{
    /// <summary>
    /// A durable state store plugin must implement a class that implements this interface.
    /// </summary>
    public interface IDurableStateStoreProvider
    {
        /// <summary>
        /// The `ReadJournal` implementation for the Scala API.
        /// This corresponds to the instance that is returned by <see cref="DurableStateStoreRegistry.DurableStateStoreFor{TStore, T}(string)"/>
        /// </summary>
        IDurableStateStore GetDurableStateStore();
    }
}
