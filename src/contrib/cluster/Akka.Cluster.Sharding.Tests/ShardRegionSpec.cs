//-----------------------------------------------------------------------
// <copyright file="ShardRegionSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Cluster.Sharding.Tests
{
    public class ShardRegionSpec : AkkaSpec
    {
        public const int NumberOfShards = 3;
        public const string ShardTypeName = "Caat";

        public static readonly ExtractEntityId ExtractEntityId = message =>
            (((int)message).ToString(), message);

        public static readonly ExtractShardId ExtractShardId = message => message is ShardRegion.StartEntity msg
            ? (long.Parse(msg.EntityId) % NumberOfShards).ToString()
            : ((int)message % 10).ToString();

        internal class EntityActor : ActorBase
        {
            protected override bool Receive(object message)
            {
                Sender.Tell(message);
                return true;
            }
        }

        private static Config SpecConfig => ConfigurationFactory.ParseString(@"
            akka.loglevel = INFO
            akka.actor.provider = ""cluster""
            akka.remote.dot-netty.tcp.hostname = ""127.0.0.1""
            akka.remote.dot-netty.tcp.port = 0
            akka.remote.log-remote-lifecycle-events = off
            akka.test.single-expect-default = 5 s
            akka.cluster.sharding.distributed-data.durable.lmdb {
                dir = ""target/ShardRegionSpec/sharding-ddata""
                map-size = 10 MiB
            }
            #akka.cluster.downing-provider-class = ""Akka.Cluster.AutoDowning, Akka.Cluster""")
            .WithFallback(ClusterSharding.DefaultConfig());

        private readonly List<FileInfo> _storageLocations;
        private readonly ActorSystem _sysA;
        private readonly ActorSystem _sysB;
        private readonly TestProbe _p1;
        private readonly TestProbe _p2;
        private readonly IActorRef _region1;
        private readonly IActorRef _region2;

        public ShardRegionSpec(ITestOutputHelper output)
            : base(SpecConfig, output)
        {
            _sysA = Sys;
            _sysB = ActorSystem.Create(_sysA.Name, _sysA.Settings.Config);
            _p1 = CreateTestProbe(_sysA);
            _p2 = CreateTestProbe(_sysB);
            _region1 = StartShard(_sysA);
            _region2 = StartShard(_sysB);

            _storageLocations = new List<FileInfo>
            {
                new FileInfo(Sys.Settings.Config.GetString("akka.cluster.sharding.distributed-data.durable.lmdb.dir", null))
            };
            foreach (var fileInfo in _storageLocations.Where(fileInfo => fileInfo.Exists)) fileInfo.Delete();
        }

        protected override void BeforeTermination()
        {
            base.BeforeTermination();
            Shutdown(_sysB);
        }

        protected override void AfterTermination()
        {
            base.AfterTermination();
            foreach (var fileInfo in _storageLocations.Where(fileInfo => fileInfo.Exists)) fileInfo.Delete();
        }

        private static IActorRef StartShard(ActorSystem sys) =>
            ClusterSharding.Get(sys).Start(
                ShardTypeName,
                Props.Create<EntityActor>(),
                ClusterShardingSettings.Create(sys).WithRememberEntities(true),
                ExtractEntityId,
                ExtractShardId);

        private void InitCluster()
        {
            Cluster.Get(_sysA).Join(Cluster.Get(_sysA).SelfAddress); // coordinator on A
            AwaitAssert(() => Cluster.Get(_sysA).SelfMember.Status.ShouldBe(MemberStatus.Up), TimeSpan.FromSeconds(1));

            Cluster.Get(_sysB).Join(Cluster.Get(_sysA).SelfAddress);

            Within(TimeSpan.FromSeconds(30), () =>
            {
                AwaitAssert(() =>
                {
                    var systems = new List<ActorSystem> { _sysA, _sysB };
                    systems.ForEach(s =>
                    {
                        Cluster.Get(s).SendCurrentClusterState(TestActor);
                        ExpectMsg<ClusterEvent.CurrentClusterState>().Members.Count.ShouldBe(2);
                    });
                });
            });

            _region1.Tell(1, _p1.Ref);
            _p1.ExpectMsg(1);

            _region2.Tell(2, _p2.Ref);
            _p2.ExpectMsg(2);

            _region2.Tell(3, _p2.Ref);
            _p2.ExpectMsg(3);
        }

        [Fact]
        public void ClusterSharding_must_initialize_cluster_and_allocate_sharded_actors()
        {
            InitCluster();
        }

        [Fact]
        public void ClusterSharding_must_only_deliver_buffered_RestartShard_to_the_local_region()
        {
            IEnumerable<string> StatesFor(IActorRef region, TestProbe probe, int expect)
            {
                region.Tell(GetShardRegionState.Instance, probe.Ref);
                return probe.ReceiveWhile(x => x is CurrentShardRegionState e ? e.Shards.Select(s => s.ShardId) : null, msgs: expect)
                    .SelectMany(s => s);
            }

            bool AwaitRebalance(IActorRef region, int msg, TestProbe probe)
            {
                region.Tell(msg, probe.Ref);
                probe.ExpectMsg<int>(id => id == msg || AwaitRebalance(region, msg, probe), TimeSpan.FromSeconds(2));
                return true;
            }

            InitCluster();

            var region1Shards = StatesFor(_region1, _p1, 2);
            var region2Shards = StatesFor(_region2, _p2, 1);
            region1Shards.SequenceEqual(new string[] { "1", "3" });
            region2Shards.SequenceEqual(new string[] { "2" });
            var allShards = region1Shards.Concat(region2Shards);

            _region2.Tell(PoisonPill.Instance);
            AwaitAssert(() => ((IInternalActorRef)_region2).IsTerminated.ShouldBeTrue());

            // Difficult to raise the RestartShard in conjunction with the rebalance for mode=ddata
            AwaitAssert(() => AwaitRebalance(_region1, 2, _p1));

            var rebalancedOnRegion1 = StatesFor(_region1, _p1, NumberOfShards);
            AwaitAssert(() => rebalancedOnRegion1.Count().ShouldBe(NumberOfShards), TimeSpan.FromSeconds(5));
            rebalancedOnRegion1.Should().BeEquivalentTo(allShards);
        }
    }
}