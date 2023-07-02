//-----------------------------------------------------------------------
// <copyright file="AbstractActor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

namespace Akka.Actor
{
    /// <summary>
    /// Finite State Machine actor abstract base class.
    /// </summary>
    public abstract class AbstractFSM<TState, TData> : FSM<TState, TData>
    {

    }

    /// <summary>
    /// Finite State Machine actor abstract base class with Stash support.
    /// </summary>
    public abstract class AbstractFSMWithStash<TState, TData> : AbstractFSM<TState, TData>, IWithStash
    {
        public IStash Stash { get; set; }
    }
}
