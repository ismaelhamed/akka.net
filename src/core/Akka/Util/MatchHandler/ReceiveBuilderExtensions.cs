//-----------------------------------------------------------------------
// <copyright file="ReceiveBuilderExtensions.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Akka.Dispatch;

namespace Akka.Actor
{
    public static class ReceiveBuilderExtensions
    {
        public static ReceiveBuilder MatchAsync<T>(this ReceiveBuilder target, Func<T, Task> handler, Predicate<T> shouldHandle = null) =>
            target.Match(WrapAsyncHandler(handler), shouldHandle);

        static Action<T> WrapAsyncHandler<T>(Func<T, Task> asyncHandler) =>
            m =>
            {
                Task Wrap() => asyncHandler(m);
                ActorTaskScheduler.RunTask(Wrap);
            };
    }
}