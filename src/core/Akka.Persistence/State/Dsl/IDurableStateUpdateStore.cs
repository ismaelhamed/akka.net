//-----------------------------------------------------------------------
// <copyright file="IDurableStateUpdateStore.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;

namespace Akka.Persistence.State.Dsl
{
    /// <summary>
    /// API for updating durable state objects.
    /// </summary>
    public interface IDurableStateUpdateStore<T> : IDurableStateStore<T>
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="persistenceId">TBD</param>
        /// <param name="seqNr">Sequence number for optimistic locking. starts at 1.</param>
        /// <param name="value">TBD</param>
        /// <param name="tag">TBD</param>
        Task<Done> UpsertObject(string persistenceId, long seqNr, T value, string tag = null);

        // Task<Done> DeleteObject(string persistenceId);
    }
}
