//-----------------------------------------------------------------------
// <copyright file="MatchBuilder.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Akka.Actor
{
    public class ReceiveBuilder
    {
        private readonly ReceiveActorHandlers _messageHandlers = new();

        /// <summary>
        /// Return a new <see cref="ReceiveBuilder"/> with no case statements. They can be added later as the
        /// returned <see cref="ReceiveBuilder"/> is a mutable object.
        /// </summary>
        public static ReceiveBuilder Create() => new();

        public Receive Build() => _messageHandlers.TryHandle;

        public ReceiveBuilder Match<T>(Func<T, bool> handler)
        {
            _messageHandlers.AddGenericReceiveHandler<T>(null, handler);
            return this;
        }

        public ReceiveBuilder Match<T>(Action<T> handler, Predicate<T> shouldHandle = null)
        {
            _messageHandlers.AddGenericReceiveHandler<T>(shouldHandle, message =>
            {
                handler(message);
                return true;
            });
            return this;
        }

        /// <summary>
        /// Add a new case statement to this builder, that matches any argument.
        /// </summary>
        /// <param name="apply">An action to apply to the argument</param>
        /// <remarks>Note that since this matches all items, no more handlers may be added after this one.</remarks>
        /// <remarks>Note that if a previous added handler handled the item, this <paramref name="apply"/> will not be invoked.</remarks>    
        /// <returns>A builder with the case statement added</returns>
        public ReceiveBuilder MatchAny(Action<object> apply)
        {
            _messageHandlers.AddReceiveAnyHandler(apply);
            return this;
        }
    }
}

