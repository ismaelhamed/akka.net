//-----------------------------------------------------------------------
// <copyright file="DurableStateChange.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Util;

namespace Akka.Persistence.Query
{
    public sealed class DurableStateChange
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DurableStateChange"/> class.
        /// </summary>
        /// <param name="persistenceId">The persistence id of the origin entity.</param>
        /// <param name="revision">TBD</param>
        /// <param name="value">The object value.</param>
        /// <param name="offset">The offset that can be used in next `changes` or `currentChanges` query.</param>
        /// <param name="timestamp">The time the event was stored, in ticks. The value of this property represents the number of 100-nanosecond intervals that have elapsed since 12:00:00 midnight, January 1, 0001 in the Gregorian calendar (same as `DateTime.Now.Ticks`).</param>
        public DurableStateChange(string persistenceId, long revision, Option<object> value, Offset offset, long timestamp)
        {
            PersistenceId = persistenceId;
            Revision = revision;
            Value = value;
            Offset = offset;
            Timestamp = timestamp;
        }

        public string PersistenceId { get; }
        public long Revision { get; }
        public Option<object> Value { get; }
        public Offset Offset { get; }
        public long Timestamp { get; }
    }
}
