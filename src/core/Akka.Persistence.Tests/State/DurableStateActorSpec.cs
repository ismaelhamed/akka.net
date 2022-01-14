//-----------------------------------------------------------------------
// <copyright file="PersistentActorSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.State;
using Akka.Persistence.State.Dsl;
using Akka.TestKit;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Tests.State
{
    public class DurableStateActorSpec : AkkaSpec
    {
        private static Config Configuration()
        {
            return ConfigurationFactory.ParseString(@"
                akka.loglevel = INFO
                akka.persistence.state.plugin = ""akka.persistence.state.inmem""
                akka.persistence.state.inmem {
                    class = ""Akka.Persistence.State.Inmem.InmemDurableStateStoreProvider""
                    # FIXME we should change this to use a fallback in reference.conf
                    recovery-timeout = 30s
                }
                akka.test.single-expect-default = 5s");
        }

        public DurableStateActorSpec(ITestOutputHelper output)
            : base(Configuration(), output)
        { }

        [Fact]
        public void DurableStateActorSpec_test1()
        {
            var pluginId = "akka.persistence.state.inmem";
            var durableStateStore = DurableStateStoreRegistry.Get(Sys).DurableStateStoreFor<IDurableStateStore<Record>, Record>(pluginId);
            var source = durableStateStore.GetObject("a-0").GetAwaiter().GetResult();
        }

        [Fact]
        public void A_durablestate_actor_with_primitive_state_must_persist_primitive_events_and_update_state()
        {
            var probe = CreateTestProbe();
            var ref1 = Sys.ActorOf(Props.Create(() => new DurableStateActorTest("a", probe)));
            ref1.Tell(1);
            probe.ExpectMsg("1");
            ref1.Tell(2);
            probe.ExpectMsg("3");
        }

        private class DurableStateActorTest : DurableStateActor
        {
            private readonly string _persistenceId;
            private readonly IActorRef _probe;
            private int _state;

            public DurableStateActorTest(string persistenceId, IActorRef probe)
            {
                _persistenceId = persistenceId;
                _probe = probe;

                DurableStateStorePluginId = "akka.persistence.state.inmem";
            }

            public override string PersistenceId => _persistenceId;

            protected override bool ReceiveCommand(object message)
            {
                switch (message)
                {
                    case int cmd when cmd < 0:
                        Context.Stop(Self);
                        return true;
                    case int cmd when cmd >= 0:
                        {
                            _state += cmd;
                            Persist(_state, state => _probe.Tell(state.ToString()));
                        }
                        return true;
                }

                return false;
            }

            protected override bool ReceiveRecover(object message)
            {
                return true;
            }
        }
    }

    public class Record
    {
        public Record(int version, string name, string address)
        {
            Version = version;
            Name = name;
            Address = address;
        }

        public int Version { get; }
        public string Name { get; }
        public string Address { get; }
    }
}