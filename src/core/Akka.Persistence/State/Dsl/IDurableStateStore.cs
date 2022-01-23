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
    /// <summary>
    /// API for reading durable state objects. See also <seealso cref="IDurableStateUpdateStore"/>
    /// </summary>    
    public interface IDurableStateStore
    {
        Task<GetObjectResult> GetObject(string persistenceId);
    }

    public sealed class GetObjectResult
    {
        public GetObjectResult(Option<object> value, long revision)
        {
            Value = value;
            Revision = revision;
        }

        public Option<object> Value { get; }
        public long Revision { get; }
    }
}
