//-----------------------------------------------------------------------
// <copyright file="InmemDurableStateStoreProvider.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Persistence.State.Dsl;

namespace Akka.Persistence.State.Inmem
{
    internal class InmemDurableStateStoreProvider : IDurableStateStoreProvider
    {
        public IDurableStateStore<T> GetDurableStateStore<T>() => new InmemDurableStateStore<T>();
    }
}
