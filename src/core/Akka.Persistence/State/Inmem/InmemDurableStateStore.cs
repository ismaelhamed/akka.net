//-----------------------------------------------------------------------
// <copyright file="InmemDurableStateStore.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Akka.Persistence.State.Dsl;
using Akka.Util;

namespace Akka.Persistence.State.Inmem
{
    internal class InmemDurableStateStore<T> : IDurableStateUpdateStore<T>
    {
        private readonly ConcurrentDictionary<string, T> _store = new ConcurrentDictionary<string, T>();

        public Task<Done> DeleteObject(string persistenceId)
        {
            _store.TryRemove(persistenceId, out var _);
            return Task.FromResult(Done.Instance);
        }

        public Task<GetObjectResult<T>> GetObject(string persistenceId)
        {
            _store.TryGetValue(persistenceId, out var result);
            return Task.FromResult(new GetObjectResult<T>(result ?? Option<T>.None, 0));
        }

        public Task<Done> UpsertObject(string persistenceId, long revision, T value, string tag)
        {
            _store.AddOrUpdate(persistenceId, value, (_, __) => value);
            return Task.FromResult(Done.Instance);
        }
    }
}
