//-----------------------------------------------------------------------
// <copyright file="DurableStateStoreException.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Annotations;

namespace Akka.Persistence.State.Internal
{
    /// <summary>
    /// INTERNAL API
    /// <para>Used for store failures. Private to akka as only internal supervision strategies should use it.</para>
    /// </summary>
    [InternalApi]
    public class DurableStateStoreException : AkkaException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DurableStateStoreException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="cause">The exception that is the cause of the current exception.</param>
        public DurableStateStoreException(string message, Exception cause = null) 
            : base(message, cause)
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="DurableStateStoreException"/> class.
        /// </summary>
        /// <param name="persistenceId">TBD</param>
        /// <param name="revision">TBD</param>
        /// <param name="cause">The exception that is the cause of the current exception.</param>
        public DurableStateStoreException(string persistenceId, long revision, Exception cause = null) 
            : base($"Failed to persist state with revision [{revision}] for persistenceId [{persistenceId}]", cause)
        { }
    }
}
