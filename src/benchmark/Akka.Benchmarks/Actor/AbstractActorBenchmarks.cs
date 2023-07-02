//-----------------------------------------------------------------------
// <copyright file="AbstractActorBenchmarks.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Threading;
using Akka.Actor;
using BenchmarkDotNet.Attributes;

namespace Akka.Benchmarks.Actor
{
    [MemoryDiagnoser]
    [SimpleJob(targetCount: 5, launchCount: 1, warmupCount: 5)]
    public class AbstractActorBenchmarks
    {
        [Params(10_000)]
        public int ActorCount { get; set; }

        private ActorSystem system;

        [IterationSetup]
        public void Setup() => system = ActorSystem.Create("ActorBenchmark");

        [IterationCleanup]
        public void Cleanup() => system.Terminate().Wait();

        [Benchmark(Description = "UntypedActor")]
        public void SpawnUntypedActor()
        {
            var latch = new CountdownEvent(ActorCount);
            var actor = system.ActorOf(ParentUntypedActor.Props(latch));
            actor.Tell(new StartTest(ActorCount));
            latch.Wait();
        }

        [Benchmark(Description = "ReceiveActor")]
        public void SpawnReceiveActor()
        {
            var latch = new CountdownEvent(ActorCount);
            var actor = system.ActorOf(ParentReceiveActor.Props(latch));
            actor.Tell(new StartTest(ActorCount));
            latch.Wait();
        }
    }

    #region actors

    sealed class StartTest
    {
        public int ActorCount { get; }

        public StartTest(int actorCount) => ActorCount = actorCount;
    }

    sealed class ChildReady
    {
        public static readonly ChildReady Instance = new();
        private ChildReady() { }
    }

    sealed class ParentReceiveActor : ReceiveActor
    {
        public static Props Props(CountdownEvent latch) =>
            Akka.Actor.Props.Create<ParentReceiveActor>(latch);

        public ParentReceiveActor(CountdownEvent latch)
        {
            Receive<StartTest>(start =>
            {
                for (var i = 0; i < start.ActorCount; i++)
                    Context.ActorOf(Child.Props);
            });
            Receive<ChildReady>(_ =>
            {
                if (latch.Signal()) Context.Stop(Self);
            });
        }

        sealed class Child : ReceiveActor
        {
            public static readonly Props Props = Props.Create<Child>();

            public Child() => ReceiveAny(_ => { });

            protected override void PreStart()
            {
                base.PreStart();
                Context.Parent.Tell(ChildReady.Instance);
            }
        }
    }

    sealed class ParentUntypedActor : UntypedActor
    {
        private readonly CountdownEvent latch;

        public static Props Props(CountdownEvent latch) =>
            Akka.Actor.Props.Create<ParentUntypedActor>(latch);

        public ParentUntypedActor(CountdownEvent latch) => this.latch = latch;

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case StartTest start:
                    {
                        for (var i = 0; i < start.ActorCount; i++)
                            Context.ActorOf(Child.Props);
                    }
                    break;
                case ChildReady _:
                    if (latch.Signal()) Context.Stop(Self);
                    break;
            }
        }

        sealed class Child : UntypedActor
        {
            public static readonly Props Props = Props.Create<Child>();

            protected override void OnReceive(object message)
            {
                // ignore
            }

            protected override void PreStart()
            {
                base.PreStart();
                Context.Parent.Tell(ChildReady.Instance);
            }
        }
    }

    #endregion
}