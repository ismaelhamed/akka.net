//-----------------------------------------------------------------------
// <copyright file="MatchBuilder.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Akka.Actor
{
    public class ReceiveBuilder2
    {
        private readonly HandlerRegistry _registry = new();

        public static ReceiveBuilder2 Create() => new();

        public Receive Build()
        {
            var dispatcher = _registry.Freeze();
            return dispatcher.TryHandle;
        }

        public ReceiveBuilder2 Match<T>(Func<T, bool> handler)
        {
            _registry.AddGenericReceiveHandler<T>(null, handler);
            return this;
        }

        public ReceiveBuilder2 Match<T>(Action<T> handler, Predicate<T> shouldHandle = null)
        {
            _registry.AddGenericReceiveHandler<T>(shouldHandle, message =>
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
        public ReceiveBuilder2 MatchAny(Action<object> apply)
        {
            _registry.AddReceiveAnyHandler(apply);
            return this;
        }
    }

    internal sealed class HandlerRegistry
    {
        public static long Created;
        private readonly List<ITypeHandler> _handlers = new();

        private Action<object> _receiveAny;

        private bool _hadObjectHandlerWithNoPredicate;

        public HandlerRegistry()
        {
            Interlocked.Increment(ref Created);
        }

        private void CanAddMoreHandlers()
        {
            if (_hadObjectHandlerWithNoPredicate)
                throw new InvalidOperationException("A handler for object with no predicate has already been added.");

            if (_receiveAny != null)
                throw new InvalidOperationException("A MatchAny handler has already been added.");
        }

        private static ITypeHandler CreateTypeHandler<T>(Predicate<T> predicate, Func<T, bool> handler)
        {
            return predicate == null
                ? new TypeHandler<T>(handler)
                : new PredicateHandler<T>(predicate, handler);
        }

        private static ITypeHandler CreateTypeHandler(Type type, Predicate<object>? predicate, Func<object, bool> handler)
        {
            return predicate == null
                ? new WeaklyTypedHandler(type, handler)
                : new WeaklyTypedPredicateHandler(type, predicate, handler);
        }

        public void AddGenericReceiveHandler<T>(Predicate<T>? predicate, Func<T, bool> handler)
        {
            CanAddMoreHandlers();
            _handlers.Add(CreateTypeHandler(predicate, handler));
        }

        public void AddTypedReceiveHandler(Type type, Predicate<object> predicate, Func<object, bool> handler)
        {
            CanAddMoreHandlers();
            _handlers.Add(CreateTypeHandler(type, predicate, handler));

            if (type == typeof(object) && predicate == null)
                _hadObjectHandlerWithNoPredicate = true;
        }

        public void AddReceiveAnyHandler(Action<object> handler)
        {
            CanAddMoreHandlers();
            _receiveAny = handler;
        }

        public ReceiveDispatcher Freeze() => new(_handlers.ToArray(), _receiveAny);
    }

    //internal sealed class ReceiveDispatcher
    //{
    //    private readonly ITypeHandler[] _handlers;
    //    private readonly Action<object> _receiveAny;

    //    public ReceiveDispatcher(ITypeHandler[] handlers, Action<object> receiveAny)
    //    {
    //        _handlers = handlers;
    //        _receiveAny = receiveAny;
    //    }

    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public bool TryHandle(object message)
    //    {
    //        var messageType = message.GetType();

    //        for (var i = 0; i < _handlers.Length; i++)
    //        {
    //            var handler = _handlers[i];
    //            if (!handler.TargetType.IsAssignableFrom(messageType))
    //                continue;

    //            if (handler.TryHandle(message))
    //                return true;
    //        }

    //        if (_receiveAny == null)
    //            return false;

    //        _receiveAny(message);

    //        return true;
    //    }
    //}

    internal sealed class ReceiveDispatcher
    {
        private readonly ITypeHandler[] _handlers;
        private readonly Action<object> _receiveAny;
        public static long Created;

        private readonly ConcurrentDictionary<Type, Receive> _cache = new();

        public ReceiveDispatcher(ITypeHandler[] handlers, Action<object> receiveAny)
        {
            _handlers = handlers;
            _receiveAny = receiveAny;

            Interlocked.Increment(ref Created);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryHandle(object message)
        {
            var type = message.GetType();
            var receive = _cache.GetOrAdd(type, Resolve);
            return receive(message);
        }

        private Receive Resolve(Type messageType)
        {
            for (var i = 0; i < _handlers.Length; i++)
            {
                var handler = _handlers[i];
                if (!handler.TargetType.IsAssignableFrom(messageType))
                    continue;

                return handler.TryHandle;
            }

            if (_receiveAny != null)
                return message =>
                {
                    _receiveAny(message);
                    return true;
                };

            return _ => false;
        }

        //private sealed class CachedHandler
        //{
        //    private readonly ITypeHandler _typedHandler;
        //    private readonly Action<object> _receiveAny;

        //    public CachedHandler(ITypeHandler handler) => _typedHandler = handler;

        //    public CachedHandler(Action<object> receiveAny) => _receiveAny = receiveAny;

        //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
        //    public bool Invoke(object message)
        //    {
        //        if (_typedHandler != null)
        //            return _typedHandler.TryHandle(message);

        //        _receiveAny!(message);
        //        return true;
        //    }
        //}
    }
}

