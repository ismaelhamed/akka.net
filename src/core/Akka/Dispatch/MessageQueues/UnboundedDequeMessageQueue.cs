//-----------------------------------------------------------------------
// <copyright file="UnboundedDequeMessageQueue.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Akka.Dispatch.MessageQueues
{
    /// <summary>
    /// An unbounded double-ended queue. Used in combination with <see cref="IStash"/>.
    /// </summary>
    public class UnboundedDequeMessageQueue : DequeWrapperMessageQueue, IUnboundedDequeBasedMessageQueueSemantics
    {
        /// <summary>
        /// TBD
        /// </summary>
        public UnboundedDequeMessageQueue() : base(new UnboundedMessageQueue())
        {
        }
    }
}

