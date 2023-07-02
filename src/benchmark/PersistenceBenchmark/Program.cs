//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Pattern;
using Google.Protobuf.WellKnownTypes;
using ConfigurationFactory = Akka.Configuration.ConfigurationFactory;

namespace PersistenceBenchmark
{
    class Program
    {
        // if you want to benchmark your persistent storage provides, paste the configuration in string below
        // by default we're checking against in-memory journal
        private static Config config = ConfigurationFactory.ParseString(@"
            akka {
                suppress-json-serializer-warning = true
                persistence.journal {
                    plugin = ""akka.persistence.journal.sqlite""
                    sqlite {
                        class = ""Akka.Persistence.Sqlite.Journal.BatchingSqliteJournal, Akka.Persistence.Sqlite""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        table-name = event_journal
                        metadata-table-name = journal_metadata
                        auto-initialize = on
                        connection-string = ""Datasource=memdb-journal.db;Mode=Memory;Cache=Shared""
                    }
                }
            }");

        public const int ActorCount = 1000;
        public const int MessagesPerActor = 100;

        static void Main(string[] args)
        {
            using (var system = ActorSystem.Create("persistent-benchmark", config.WithFallback(ConfigurationFactory.Default())))
            {
                Console.WriteLine("Performance benchmark starting...");

                //var actors = new IActorRef[ActorCount];
                //for (int i = 0; i < ActorCount; i++)
                //{
                //    var pid = "a-" + i;
                //    actors[i] = system.ActorOf(Props.Create(() => new PerformanceTestActor(pid)));
                //}

                //Task.WaitAll(actors.Select(a => a.Ask<Done>(Init.Instance)).Cast<Task>().ToArray());

                //Console.WriteLine("All actors have been initialized...");

                //var stopwatch = new Stopwatch();
                //stopwatch.Start();

                //for (int i = 0; i < MessagesPerActor; i++)
                //    for (int j = 0; j < ActorCount; j++)
                //    {
                //        actors[j].Tell(new Store(1));
                //    }

                //var finished = new Task[ActorCount];
                //for (int i = 0; i < ActorCount; i++)
                //{
                //    finished[i] = actors[i].Ask<Finished>(Finish.Instance);
                //}

                //Task.WaitAll(finished);

                //var elapsed = stopwatch.ElapsedMilliseconds;

                //Console.WriteLine($"{ActorCount} actors stored {MessagesPerActor} events each in {elapsed/1000.0} sec. Average: {ActorCount*MessagesPerActor*1000.0/elapsed} events/sec");

                //foreach (Task<Finished> task in finished)
                //{
                //    if (!task.IsCompleted || task.Result.State != MessagesPerActor)
                //        throw new IllegalStateException("Actor's state was invalid");
                //}

                var actorRef = system.ActorOf(Props.Create<TestAbstractActor>());
                actorRef.Tell(StopMeasure.Instance);
                actorRef.Tell(new Measure(3));
                actorRef.Tell(new FailAt(5));
                actorRef.Tell(new Measure(7));
                actorRef.Tell(12);
            }

            Console.ReadLine();
        }
    }

    public class TestAbstractActor : AbstractActor
    {
        // OPTION 1
        //protected override Receive CreateReceive =>
        //    ReceiveBuilder
        //        .Match<Measure>(measure => Console.WriteLine("Receive Measure with message count: {0}", measure.MessagesCount))
        //        .Match<StopMeasure>(_ => Console.WriteLine("Receive StopMeasure"))
        //        .MatchAny(any => Console.WriteLine("Receive {0}", any))
        //        .Build();

        // OPTION 2
        //protected override Receive CreateReceive => ReceiveBuilder.Build();

        //protected override bool Receive(object message)
        //{
        //    switch (message)
        //    {
        //        case Measure measure:
        //            Console.WriteLine("Receive Measure with message count: {0}", measure.MessagesCount);
        //            return true;
        //        case StopMeasure _:
        //            Console.WriteLine("Receive StopMeasure");
        //            return true;
        //        default:
        //            Console.WriteLine("Receive {0}", message);
        //            return true;
        //    }
        //}

        // OPTION 3
        protected override Receive CreateReceive => Idle;

        public Receive Idle => message =>
        {
            switch (message)
            {
                case Measure measure:
                    Console.WriteLine("Idle Measure with message count: {0}", measure.MessagesCount);
                    Context.Become(Working);
                    return true;
                case StopMeasure _:
                    Console.WriteLine("Idle StopMeasure");                    
                    return true;
                default:
                    Console.WriteLine("Idle any: {0}", message);
                    return true;
            }
        };

        public Receive Working => message =>
        {
            switch (message)
            {
                case Measure measure:
                    Console.WriteLine("Working Measure with message count: {0}", measure.MessagesCount);
                    Context.Become(Idle);
                    return true;
                case FailAt fail:
                    Console.WriteLine("Working FailAt SequenceNr: {0}", fail.SequenceNr);
                    
                    return true;
            }

            return false;
        };
    }

    public class TestUntypedAbstractActor : UntypedAbstractActor
    {
        protected override void OnReceive(object message)
        {
            Context.Become(Working);
        }

        public Receive Working => message =>
        {
            switch (message)
            {
                case FailAt fail:
                    Console.WriteLine("Receive FailAt SequenceNr: {0}", fail.SequenceNr);
                    return true;
            }

            return false;
        };
    }
}
