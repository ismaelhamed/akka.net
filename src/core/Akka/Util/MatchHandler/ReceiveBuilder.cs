//-----------------------------------------------------------------------
// <copyright file="MatchBuilder.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;

namespace Akka.Tools.MatchHandler
{
    /// <summary>
    /// Used for building a partial function for <see cref="Actor.ActorBase.Receive(object)"/>.
    /// <para>There is both a match on type only, and a match on type and predicate.</para>
    /// <para>Inside an actor you can use it like this to define your receive method:
    /// <code>
    /// // TODO
    /// </code>
    /// </para>
    /// </summary>
    public class ReceiveBuilder
    {
        private readonly MatchBuilder _matchHandlerBuilder = new MatchBuilder(CachedMatchCompiler<object>.Instance);

        /// <summary>
        /// Return a new <see cref="ReceiveBuilder"/> with no case statements. They can be added later as the
        /// returned <see cref="ReceiveBuilder"/> is a mutable object.
        /// </summary>
        public static ReceiveBuilder Create() => new ReceiveBuilder();

        /// <summary>
        /// Build a <see cref="PartialAction{T}"/> from this builder. After this call the builder will be reset.
        /// </summary>
        public Receive Build() => _matchHandlerBuilder.Build().Invoke;

        public ReceiveBuilder Match<T>(Func<T, bool> handler)
        {
            _matchHandlerBuilder.Match(handler);
            return this;
        }

        public ReceiveBuilder Match<T>(Action<T> handler, Predicate<T> shouldHandle = null)
        {
            _matchHandlerBuilder.Match(handler, shouldHandle);
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
            _matchHandlerBuilder.MatchAny(apply);
            return this;
        }
    }

    public static class DelegateExtensions
    {
        /// <summary>
        /// https://github.com/dotnet/csharplang/issues/149#issuecomment-296172573
        /// </summary>
        public static T Cast<T>(this Delegate @delegate) where T : class
        {
            if (@delegate == null) return null;

            var multicastList = (@delegate as MulticastDelegate)?.GetInvocationList();
            if (multicastList != null)
            {
                switch (multicastList.Length)
                {
                    case 0:
                        return null;
                    case 1:
                        if (multicastList[0] != @delegate)
                            return multicastList[0].Cast<T>();
                        break;
                    default:
                        var convertedItems = new Delegate[multicastList.Length];
                        for (var i = 0; i < multicastList.Length; i++)
                            convertedItems[i] = (Delegate)(object)multicastList[i].Cast<T>();
                        return (T)(object)Delegate.Combine(convertedItems);
                }
            }

            return (T)(object)Delegate.CreateDelegate(typeof(T), @delegate.Target, @delegate.Method, true);
        }
    }
}

