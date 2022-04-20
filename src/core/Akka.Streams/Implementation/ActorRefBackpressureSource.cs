//-----------------------------------------------------------------------
// <copyright file="ActorRefBackpressureSource.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Implementation.Stages;
using Akka.Streams.Stage;
using Akka.Util;

namespace Akka.Streams.Implementation
{
    /// <summary>
    /// INTERNAL API
    /// </summary>
    [InternalApi]
    internal class ActorRefBackpressureSource<TOut> : GraphStageWithMaterializedValue<SourceShape<TOut>, IActorRef>
    {
        private readonly Outlet<TOut> _outlet = new Outlet<TOut>("ActorRefSource.out");

        public ActorRefBackpressureSource(Option<IActorRef> ackTo, object ackMessage, Func<object, CompletionStrategy> onCompletion, Func<object, Exception> onFailureMessage)
        {
            Shape = new SourceShape<TOut>(_outlet);
        }

        public override SourceShape<TOut> Shape { get; }

        protected override Attributes InitialAttributes => DefaultAttributes.ActorRefWithAckSource;

        public override ILogicAndMaterializedValue<IActorRef> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            throw new NotImplementedException("Not supported");
        }       
    }
}
