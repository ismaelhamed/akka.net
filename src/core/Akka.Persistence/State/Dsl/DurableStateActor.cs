//-----------------------------------------------------------------------
// <copyright file="DurableStateActor.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Annotations;
using Akka.Persistence.State.Internal;

namespace Akka.Persistence.State.Dsl
{
    /// <summary>
    /// Further customization of the <see cref="DurableStateActor"/> can be done with the methods defined here.
    /// <para>API May Change</para>
    /// </summary>
    [ApiMayChange]
    public abstract class DurableStateActor : DurableStateActor2 //DurableStateActorImpl
    {
        protected override bool Receive(object message) => ReceiveCommand(message);
    }

    ///// <summary>
    ///// Further customization of the <see cref="UntypedPersistentActor"/> can be done with the methods defined here.
    ///// <para>API May Change</para>
    ///// </summary>
    //[ApiMayChange]
    //public abstract class UntypedPersistentActor : DurableStateActorImpl
    //{
    //}
}
