//-----------------------------------------------------------------------
// <copyright file="IDurableStateStore.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using Akka.Util;

namespace Akka.Persistence.State.Dsl
{
    public interface IDurableStateStore
    { }

    /// <summary>
    /// API for reading durable state objects. See also <seealso cref="IDurableStateUpdateStore{T}"/>
    /// </summary>    
    public interface IDurableStateStore<T> : IDurableStateStore
    {
        Task<GetObjectResult<T>> GetObject(string persistenceId);
    }

    public sealed class GetObjectResult<T>
    {
        public GetObjectResult(Option<T> value, long seqNr)
        {
            Value = value;
            SeqNr = seqNr;
        }

        public Option<T> Value { get; }
        public long SeqNr { get; }
    }
}
