//-----------------------------------------------------------------------
// <copyright file="AbstractActorBenchmarks.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.TestKit;
using BenchmarkDotNet.Attributes;

namespace Akka.Benchmarks.Actor
{
    [MemoryDiagnoser]
    [ShortRunJob]
    [RankColumn]
    public class AbstractActorBenchmarks
    {
        [Params(100_000)]
        public int ActorCount { get; set; }

        private ActorSystem system;

        [IterationSetup]
        public void Setup()
        {
            system = ActorSystem.Create("ActorBenchmark");
        }

        [IterationCleanup]
        public void Cleanup()
        {
            system.Terminate().Wait();
        }

        [Benchmark(Description = "UntypedActor")]
        public void SpawnUntypedActor()
        {
            var latch = new TestLatch(1, TimeSpan.FromMinutes(2));
            var actor = system.ActorOf(ParentUntypedActor.Props(latch));
            actor.Tell(new StartTest(ActorCount));
            latch.Ready();
        }

        [Benchmark(Description = "UntypedAbstractActor")]
        public void SpawnUntypedAbstractActor()
        {
            var latch = new TestLatch(1, TimeSpan.FromMinutes(2));
            var actor = system.ActorOf(ParentUntypedAbstractActor.Props(latch));
            actor.Tell(new StartTest(ActorCount));
            latch.Ready();
        }

        [Benchmark(Description = "AbstractActor")]
        public void SpawnAbstractActor()
        {
            var latch = new TestLatch(1, TimeSpan.FromMinutes(2));
            var actor = system.ActorOf(ParentAbstractActor.Props(latch));
            actor.Tell(new StartTest(ActorCount));
            latch.Ready();
        }

        [Benchmark(Description = "ReceiveActor")]
        public void SpawnReceiveActor()
        {
            var latch = new TestLatch(1, TimeSpan.FromMinutes(2));
            var actor = system.ActorOf(ParentReceiveActor.Props(latch));
            actor.Tell(new StartTest(ActorCount));
            latch.Ready();
        }
    }

    #region actors

    sealed class StartTest
    {
        public StartTest(int actorCount)
        {
            ActorCount = actorCount;
        }

        public int ActorCount { get; }
    }

    sealed class ChildReady
    {
        public static readonly ChildReady Instance = new ChildReady();
        private ChildReady() { }
    }

    sealed class TestDone
    {
        public static readonly TestDone Instance = new TestDone();
        private TestDone() { }
    }

    sealed class ParentReceiveActor : ReceiveActor
    {
        private int count;

        public static Props Props(TestLatch latch) =>
            Akka.Actor.Props.Create<ParentReceiveActor>(latch);

        public ParentReceiveActor(TestLatch latch)
        {
            Receive<StartTest>(_ =>
            {
                count = _.ActorCount - 1; // -1 because we also create the parent
                for (var i = 0; i < count; i++)
                    Context.ActorOf(Child.Props);
            });
            Receive<ChildReady>(_ =>
            {
                count--;
                if (count == 0)
                {
                    latch.CountDown();
                    Context.Stop(Self);
                }
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
        private readonly TestLatch latch;
        private int count;

        public static Props Props(TestLatch latch) =>
            Akka.Actor.Props.Create<ParentUntypedActor>(latch);

        public ParentUntypedActor(TestLatch latch) => this.latch = latch;

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case StartTest start:
                    {
                        count = start.ActorCount - 1; // -1 because we also create the parent
                        for (var i = 0; i < count; i++)
                            Context.ActorOf(Child.Props);
                    }
                    break;
                case ChildReady _:
                    {
                        count--;
                        if (count == 0)
                        {
                            latch.CountDown();
                            Context.Stop(Self);
                        }
                    }
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

    sealed class ParentUntypedAbstractActor : UntypedAbstractActor
    {
        private readonly TestLatch latch;
        private int count;

        public static Props Props(TestLatch latch) =>
            Akka.Actor.Props.Create<ParentUntypedAbstractActor>(latch);

        public ParentUntypedAbstractActor(TestLatch latch) => this.latch = latch;

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case StartTest start:
                    {
                        count = start.ActorCount - 1; // -1 because we also create the parent
                        for (var i = 0; i < count; i++)
                            Context.ActorOf(Child.Props);
                    }
                    break;
                case ChildReady _:
                    {
                        count--;
                        if (count == 0)
                        {
                            latch.CountDown();
                            Context.Stop(Self);
                        }
                    }
                    break;
            }
        }

        sealed class Child : UntypedAbstractActor
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

    sealed class ParentAbstractActor : AbstractActor
    {
        private readonly TestLatch latch;
        private int count;

        public static Props Props(TestLatch latch) =>
            Akka.Actor.Props.Create<ParentAbstractActor>(latch);

        public ParentAbstractActor(TestLatch latch) => this.latch = latch;

        protected override Receive CreateReceive => ReceiveBuilder
            .Match<StartTest>(msg =>
            {
                count = msg.ActorCount - 1; // -1 because we also create the parent
                for (var i = 0; i < count; i++)
                    Context.ActorOf(Child.Props);
            })
            .Match<ChildReady>(_ =>
            {
                count--;
                if (count == 0)
                {
                    latch.CountDown();
                    Context.Stop(Self);
                }
            })
            .Build();

        sealed class Child : AbstractActor
        {
            public static readonly Props Props = Props.Create<Child>();

            protected override Receive CreateReceive => EmptyReceive;

            protected override void PreStart()
            {
                base.PreStart();
                Context.Parent.Tell(ChildReady.Instance);
            }
        }
    }

    #endregion
}
