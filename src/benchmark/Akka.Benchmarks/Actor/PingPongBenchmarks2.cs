using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Benchmarks.Configurations;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace Akka.Benchmarks.Actor
{
    [MemoryDiagnoser]
    [Config(typeof(MonitoringConfig))]
    [SimpleJob(RunStrategy.Monitoring, launchCount: 5, warmupCount: 10)]
    public class PingPongBenchmark2
    {
        public const int Operations = 1_000_000;

        [Params(2, 5, 10, 20)]
        public int HandlerCount { get; set; }

        private TimeSpan _timeout;
        private ActorSystem _system;

        [IterationSetup]
        public void Setup()
        {
            _timeout = TimeSpan.FromMinutes(1);
            _system = ActorSystem.Create("system");
        }

        [IterationCleanup]
        public void Cleanup()
        {
            Console.WriteLine("---- Counters ----");
            Console.WriteLine($"HandlerRegistry: {HandlerRegistry.Created}");
            Console.WriteLine($"ReceiveDispatcher: {ReceiveDispatcher.Created}");
            //Console.WriteLine($"TypeHandler: {TypeHandlerBase.Created}");
            //Console.WriteLine($"PredicateHandler: {PredicateHandlerBase.Created}");
            //Console.WriteLine($"WeaklyTypedHandler: {WeaklyTypedHandler.Created}");
            //Console.WriteLine($"WeaklyTypedPredicateHandler: {WeaklyTypedPredicateHandler.Created}");

            _system.Dispose();

            ResetCounters();
        }

        private static void ResetCounters()
        {
            HandlerRegistry.Created = 0;
            ReceiveDispatcher.Created = 0;
            //TypeHandlerBase.Created = 0;
            //PredicateHandlerBase.Created = 0;
            //WeaklyTypedHandler.Created = 0;
            //WeaklyTypedPredicateHandler.Created = 0;
        }

        //[Benchmark]
        //public void DispatcherOnly()
        //{
        //    var receive =
        //        ReceiveBuilder
        //            .Create()
        //            .Match<Signal>(_ => { })
        //            .Build();

        //    var signal = new Signal(42);

        //    for (int i = 0; i < 10_000_000; i++)
        //        receive(signal);
        //}

        //[Benchmark(Baseline = true)]
        //public async Task UntypedActor()
        //{
        //    var pong = _system.ActorOf(Props.Create(() => new PongUntypedActor()));
        //    var ping = _system.ActorOf(Props.Create(() => new PingUntypedActor(pong)));

        //    await ping.Ask(StartTest.Instance, _timeout);
        //}

        //[Benchmark]
        //public async Task ReceiveActor()
        //{
        //    var pong = _system.ActorOf(Props.Create(() => new PongReceiveActor(HandlerCount)));
        //    var ping = _system.ActorOf(Props.Create(() => new PingReceiveActor(pong, HandlerCount)));

        //    await ping.Ask(StartTest.Instance, _timeout);
        //}

        [Benchmark]
        public async Task AbstractActor()
        {
            var pong = _system.ActorOf(Props.Create(() => new PongAbstractActor(HandlerCount)));
            var ping = _system.ActorOf(Props.Create(() => new PingAbstractActor(pong, HandlerCount)));

            long before = GC.GetAllocatedBytesForCurrentThread();

            await ping.Ask(StartTest.Instance, _timeout);

            long after = GC.GetAllocatedBytesForCurrentThread();

            Console.WriteLine($"Allocated: {after - before:N0} bytes");
        }

        #region Messages

        sealed class StartTest
        {
            public static readonly StartTest Instance = new();
        }

        sealed class Signal
        {
            public Signal(int remaining) => Remaining = remaining;
            public int Remaining { get; }
        }

        sealed class Dummy1 { }
        sealed class Dummy2 { }
        sealed class Dummy3 { }
        sealed class Dummy4 { }
        sealed class Dummy5 { }
        sealed class Dummy6 { }
        sealed class Dummy7 { }
        sealed class Dummy8 { }
        sealed class Dummy9 { }
        sealed class Dummy10 { }
        sealed class Dummy11 { }
        sealed class Dummy12 { }
        sealed class Dummy13 { }
        sealed class Dummy14 { }
        sealed class Dummy15 { }
        sealed class Dummy16 { }
        sealed class Dummy17 { }
        sealed class Dummy18 { }

        sealed class TestDone
        {
            public static readonly TestDone Instance = new();
        }

        #endregion

        #region ReceiveActor

        sealed class PingReceiveActor : ReceiveActor
        {
            private IActorRef _replyTo;

            public PingReceiveActor(IActorRef pong, int handlers)
            {
                if (handlers > 2) Receive<Dummy1>(_ => { });
                if (handlers > 3) Receive<Dummy2>(_ => { });
                if (handlers > 4) Receive<Dummy3>(_ => { });
                if (handlers > 5) Receive<Dummy4>(_ => { });
                if (handlers > 6) Receive<Dummy5>(_ => { });
                if (handlers > 7) Receive<Dummy6>(_ => { });
                if (handlers > 8) Receive<Dummy7>(_ => { });
                if (handlers > 9) Receive<Dummy8>(_ => { });
                if (handlers > 10) Receive<Dummy9>(_ => { });
                if (handlers > 11) Receive<Dummy10>(_ => { });
                if (handlers > 12) Receive<Dummy11>(_ => { });
                if (handlers > 13) Receive<Dummy12>(_ => { });
                if (handlers > 14) Receive<Dummy13>(_ => { });
                if (handlers > 15) Receive<Dummy14>(_ => { });
                if (handlers > 16) Receive<Dummy15>(_ => { });
                if (handlers > 17) Receive<Dummy16>(_ => { });
                if (handlers > 18) Receive<Dummy17>(_ => { });
                if (handlers > 19) Receive<Dummy18>(_ => { });

                Receive<StartTest>(_ =>
                {
                    _replyTo = Sender;
                    pong.Tell(new Signal(Operations));
                });

                Receive<Signal>(signal =>
                {
                    if (signal.Remaining <= 0)
                        _replyTo.Tell(TestDone.Instance);
                    else
                        Sender.Tell(new Signal(signal.Remaining - 1));
                });
            }
        }

        sealed class PongReceiveActor : ReceiveActor
        {
            public PongReceiveActor(int handlers)
            {
                if (handlers > 2) Receive<Dummy1>(_ => { });
                if (handlers > 3) Receive<Dummy2>(_ => { });
                if (handlers > 4) Receive<Dummy3>(_ => { });
                if (handlers > 5) Receive<Dummy4>(_ => { });
                if (handlers > 6) Receive<Dummy5>(_ => { });
                if (handlers > 7) Receive<Dummy6>(_ => { });
                if (handlers > 8) Receive<Dummy7>(_ => { });
                if (handlers > 9) Receive<Dummy8>(_ => { });
                if (handlers > 10) Receive<Dummy9>(_ => { });
                if (handlers > 11) Receive<Dummy10>(_ => { });
                if (handlers > 12) Receive<Dummy11>(_ => { });
                if (handlers > 13) Receive<Dummy12>(_ => { });
                if (handlers > 14) Receive<Dummy13>(_ => { });
                if (handlers > 15) Receive<Dummy14>(_ => { });
                if (handlers > 16) Receive<Dummy15>(_ => { });
                if (handlers > 17) Receive<Dummy16>(_ => { });
                if (handlers > 18) Receive<Dummy17>(_ => { });
                if (handlers > 19) Receive<Dummy18>(_ => { });

                Receive<Signal>(signal => Sender.Tell(new Signal(signal.Remaining - 1)));
            }
        }

        #endregion

        #region AbstractActor

        sealed class PingAbstractActor : AbstractActor
        {
            private readonly IActorRef _pong;
            private IActorRef _replyTo;
            private readonly int _handlers;

            public PingAbstractActor(IActorRef pong, int handlers)
            {
                _pong = pong;
                _handlers = handlers;
            }

            protected override Receive CreateReceive
            {
                get
                {
                    var builder = ReceiveBuilder2.Create();

                    if (_handlers > 2) builder.Match<Dummy1>(_ => { });
                    if (_handlers > 3) builder.Match<Dummy2>(_ => { });
                    if (_handlers > 4) builder.Match<Dummy3>(_ => { });
                    if (_handlers > 5) builder.Match<Dummy4>(_ => { });
                    if (_handlers > 6) builder.Match<Dummy5>(_ => { });
                    if (_handlers > 7) builder.Match<Dummy6>(_ => { });
                    if (_handlers > 8) builder.Match<Dummy7>(_ => { });
                    if (_handlers > 9) builder.Match<Dummy8>(_ => { });
                    if (_handlers > 10) builder.Match<Dummy9>(_ => { });
                    if (_handlers > 11) builder.Match<Dummy10>(_ => { });
                    if (_handlers > 12) builder.Match<Dummy11>(_ => { });
                    if (_handlers > 13) builder.Match<Dummy12>(_ => { });
                    if (_handlers > 14) builder.Match<Dummy13>(_ => { });
                    if (_handlers > 15) builder.Match<Dummy14>(_ => { });
                    if (_handlers > 16) builder.Match<Dummy15>(_ => { });
                    if (_handlers > 17) builder.Match<Dummy16>(_ => { });
                    if (_handlers > 18) builder.Match<Dummy17>(_ => { });
                    if (_handlers > 19) builder.Match<Dummy18>(_ => { });

                    builder.Match<StartTest>(_ =>
                    {
                        _replyTo = Sender;
                        _pong.Tell(new Signal(Operations));
                    });

                    builder.Match<Signal>(signal =>
                    {
                        if (signal.Remaining <= 0)
                            _replyTo.Tell(TestDone.Instance);
                        else
                            Sender.Tell(new Signal(signal.Remaining - 1));
                    });

                    return builder.Build();
                }
            }
        }

        sealed class PongAbstractActor : AbstractActor
        {
            private readonly int _handlers;

            public PongAbstractActor(int handlers) => _handlers = handlers;

            protected override Receive CreateReceive
            {
                get
                {
                    var builder = ReceiveBuilder2.Create();

                    if (_handlers > 2) builder.Match<Dummy1>(_ => { });
                    if (_handlers > 3) builder.Match<Dummy2>(_ => { });
                    if (_handlers > 4) builder.Match<Dummy3>(_ => { });
                    if (_handlers > 5) builder.Match<Dummy4>(_ => { });
                    if (_handlers > 6) builder.Match<Dummy5>(_ => { });
                    if (_handlers > 7) builder.Match<Dummy6>(_ => { });
                    if (_handlers > 8) builder.Match<Dummy7>(_ => { });
                    if (_handlers > 9) builder.Match<Dummy8>(_ => { });
                    if (_handlers > 10) builder.Match<Dummy9>(_ => { });
                    if (_handlers > 11) builder.Match<Dummy10>(_ => { });
                    if (_handlers > 12) builder.Match<Dummy11>(_ => { });
                    if (_handlers > 13) builder.Match<Dummy12>(_ => { });
                    if (_handlers > 14) builder.Match<Dummy13>(_ => { });
                    if (_handlers > 15) builder.Match<Dummy14>(_ => { });
                    if (_handlers > 16) builder.Match<Dummy15>(_ => { });
                    if (_handlers > 17) builder.Match<Dummy16>(_ => { });
                    if (_handlers > 18) builder.Match<Dummy17>(_ => { });
                    if (_handlers > 19) builder.Match<Dummy18>(_ => { });

                    builder.Match<Signal>(signal =>
                        Sender.Tell(new Signal(signal.Remaining - 1)));

                    return builder.Build();
                }
            }
        }

        #endregion

        #region Untyped

        sealed class PingUntypedActor : UntypedActor
        {
            private readonly IActorRef _pong;
            private IActorRef _replyTo;

            public PingUntypedActor(IActorRef pong)
            {
                _pong = pong;
            }

            protected override void OnReceive(object message)
            {
                switch (message)
                {
                    case StartTest:
                        _replyTo = Sender;
                        _pong.Tell(new Signal(Operations));
                        break;

                    case Signal s:
                        if (s.Remaining <= 0)
                            _replyTo.Tell(TestDone.Instance);
                        else
                            Sender.Tell(new Signal(s.Remaining - 1));
                        break;
                }
            }
        }

        sealed class PongUntypedActor : UntypedActor
        {
            protected override void OnReceive(object message)
            {
                if (message is Signal s)
                    Sender.Tell(new Signal(s.Remaining - 1));
            }
        }

        #endregion

    }
}