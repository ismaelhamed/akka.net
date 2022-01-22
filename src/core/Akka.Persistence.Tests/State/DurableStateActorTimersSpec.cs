//-----------------------------------------------------------------------
// <copyright file="DurableStateActorTimersSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.State.Dsl;
using Akka.TestKit;
using Akka.Util.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Tests.State
{
    public class DurableStateActorTimersSpec : AkkaSpec
    {
        private static Config Configuration()
        {
            return ConfigurationFactory.ParseString(@"
                akka.loglevel = INFO
                akka.persistence.state.plugin = ""akka.persistence.state.inmem""
                akka.persistence.state.inmem {
                    class = ""Akka.Persistence.State.Inmem.InmemDurableStateStoreProvider""
                    recovery-timeout = 30s
                }");
        }

        public DurableStateActorTimersSpec(ITestOutputHelper output)
            : base(Configuration(), output)
        { }

        [Fact]
        public void A_DurableStateActor_WithTimers_must_be_able_to_schedule_message()
        {
            var probe = CreateTestProbe();
            var ref1 = Sys.ActorOf(Props.Create(() => new DurableStateTestActor("a", probe)));
            ref1.Tell("cmd-0");
            probe.ExpectMsg("scheduled");
        }

        [Fact]
        public void A_DurableStateActor_WithTimers_must_not_discard_timer_msg_due_to_stashing()
        {
            var probe = CreateTestProbe();
            var ref1 = Sys.ActorOf(Props.Create(() => new DurableStateTestActor("a", probe)));
            ref1.Tell("cmd-1");

            probe.ExpectMsg("cmd-1");
            probe.ExpectMsg("scheduled");
        }

        [Fact]
        public void A_DurableStateActor_WithTimers_must_be_able_to_schedule_message_from_PreStart()
        {
            var probe = CreateTestProbe();
            var ref1 = Sys.ActorOf(Props.Create(() => new DurableStatePreStartTestActor("a", probe)));

            probe.ExpectMsg("scheduled");

            Enumerable.Range(1, 20).ForEach(n => ref1.Tell($"cmd-{n}"));
            probe.ReceiveN(20);

            // start new instance that is likely to stash the timer message while replaying
            Sys.ActorOf(Props.Create(() => new DurableStatePreStartTestActor("a", probe)));
            probe.ExpectMsg("scheduled");
        }

        private class DurableStateTestActor : DurableStateActor, IWithTimers
        {
            private readonly string _persistenceId;
            private readonly IActorRef _probe;

            public ITimerScheduler Timers { get; set; }

            public DurableStateTestActor(string persistenceId, IActorRef probe)
            {
                _persistenceId = persistenceId;
                _probe = probe;
            }

            public override string PersistenceId => _persistenceId;

            protected override bool ReceiveRecover(object message) => true;

            protected override bool ReceiveCommand(object message)
            {
                switch (message)
                {
                    case "scheduled":
                        _probe.Tell("scheduled");
                        return true;
                    case "cmd-0":
                        Timers.StartSingleTimer("key", "scheduled", TimeSpan.Zero);
                        return true;
                    default:
                        Timers.StartSingleTimer("key", "scheduled", TimeSpan.Zero);
                        Persist(message, _ => _probe.Tell(message));
                        return true;
                }
            }
        }

        private class DurableStatePreStartTestActor : DurableStateActor, IWithTimers
        {
            private readonly string _persistenceId;
            private readonly IActorRef _probe;

            public ITimerScheduler Timers { get; set; }

            public DurableStatePreStartTestActor(string persistenceId, IActorRef probe)
            {
                _persistenceId = persistenceId;
                _probe = probe;
            }

            public override string PersistenceId => _persistenceId;

            protected override void PreStart()
            {
                base.PreStart();
                Timers.StartSingleTimer("key", "scheduled", TimeSpan.Zero);
            }

            protected override bool ReceiveRecover(object message) => true;

            protected override bool ReceiveCommand(object message)
            {
                switch (message)
                {
                    case "scheduled":
                        _probe.Tell("scheduled");
                        return true;
                    default:
                        Persist(message, _ => _probe.Tell(message));
                        return true;
                }
            }
        }
    }
}
