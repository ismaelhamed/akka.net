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
    public interface IDurableStateUpdateStore : IDurableStateStore
    {
        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="persistenceId">TBD</param>
        /// <param name="revision">Revision number for optimistic locking. Starts at 1.</param>
        /// <param name="value">TBD</param>
        /// <param name="tag">TBD</param>
        Task<Done> UpsertObject(string persistenceId, long revision, object value, string tag = null);

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="persistenceId">TBD</param>
        Task<Done> DeleteObject(string persistenceId);
    }
}
