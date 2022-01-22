//-----------------------------------------------------------------------
// <copyright file="PersistentActorSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.State.Dsl;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Tests.State
{
    public class PrimitiveStateSpec : AkkaSpec
    {
        private static Config Configuration()
        {
            return ConfigurationFactory.ParseString(@"
                akka.loglevel = INFO
                akka.persistence.state.plugin = ""akka.persistence.state.inmem""
                akka.persistence.state.inmem {
                    class = ""Akka.Persistence.State.Inmem.InmemDurableStateStoreProvider""
                    recovery-timeout = 30s
                }
                akka.test.single-expect-default = 3s");
        }

        public PrimitiveStateSpec(ITestOutputHelper output)
            : base(Configuration(), output)
        { }

        [Fact]
        public void A_DurableStateActor_with_primitive_state_must_persist_primitive_events_and_update_state()
        {
            var probe = CreateTestProbe();
            var ref1 = Sys.ActorOf(Props.Create(() => new DurableStateActorTest("a", probe)));
            probe.Watch(ref1);

            ref1.Tell(1);
            probe.ExpectMsg("1");
            ref1.Tell(2);
            probe.ExpectMsg("2");

            ref1.Tell(-1);
            probe.ExpectTerminated(ref1);

            var ref2 = Sys.ActorOf(Props.Create(() => new DurableStateActorTest("a", probe)));
            // no events, no replay and hence no messages
            probe.ExpectNoMsg();
            ref2.Tell(3);
            probe.ExpectMsg("3");
        }

        private class DurableStateActorTest : DurableStateActor
        {
            private readonly string _persistenceId;
            private readonly IActorRef _probe;

            public DurableStateActorTest(string persistenceId, IActorRef probe)
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
                    case int command when command < 0:
                        Context.Stop(Self);
                        return true;
                    case int command:
                        Persist(command, _ => _probe.Tell(command.ToString()));
                        return true;
                }

                return false;
            }
        }
    }
}