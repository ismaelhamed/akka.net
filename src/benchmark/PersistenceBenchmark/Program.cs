//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2018 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Cluster.Tools.Singleton;
using Akka.Configuration;
using Akka.Persistence;
using Akka.Util;
using Serilog;

namespace PersistenceBenchmark
{
    internal class Program
    {
        private static ActorSystem system;
        public const int ActorCount = 20;
        public const int MessagesPerActor = 1;

        static void Main(string[] args)
        {
            // Set up logger
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console()
                .WriteTo.Seq("http://localhost:5341")
                .CreateLogger();

            var config = ConfigurationFactory.ParseString(File.ReadAllText("application.conf"))
                .WithFallback(Persistence.DefaultConfig())
                .WithFallback(ClusterSharding.DefaultConfig())
                .WithFallback(ClusterSingletonManager.DefaultConfig())
                .WithFallback(ClusterSingletonProxy.DefaultConfig());

            system = ActorSystem.Create("persistent-benchmark", config);

            //CreateSharding();
            CreateReadSideSharding();
            //PerformBenchmarkTest();

            Console.ReadLine();
            system.Terminate();
        }

        private static void CreateSharding()
        {
            Console.WriteLine("Initializing sharding...");

            ShardedSwitchEntity.Get(system).Start();
        }

        private static void CreateReadSideSharding()
        {
            Console.WriteLine("Initializing read-side sharding...");

            ClusterDistribution.Get(system).Start(
                typeName: "DeviceTagProcessor",
                entityProps: Props.Create<DeviceEventTagProcessor>(), 
                entityIds: DeviceEvent.Tag.AllTags().Select(t => t.Tag).ToArray(),
                settings: new ClusterDistributionSettings(system));
        }

        private static void PerformBenchmarkTest()
        {
            Console.WriteLine("Performance benchmark starting...");

            var actors = new IActorRef[ActorCount];
            for (var i = 0; i < ActorCount; i++)
            {
                var pid = "a-" + i;
                ShardedSwitchEntity.Get(system).Tell(pid, Init.Instance);
                //actors[i] = system.ActorOf(Props.Create(() => new PerformanceTestActor(pid)));
            }

            // ---------------------

            //var actors = new IActorRef[ActorCount];
            //for (var i = 0; i < ActorCount; i++)
            //{
            //    var pid = "a-" + i;
            //    actors[i] = system.ActorOf(Props.Create(() => new PerformanceTestActor(pid)));
            //}

            //Task.WaitAll(actors.Select(a => a.Ask<Done>(Init.Instance)).Cast<Task>().ToArray());

            //Console.WriteLine("All actors have been initialized...");

            //var stopwatch = new Stopwatch();
            //stopwatch.Start();

            //for (var i = 0; i < MessagesPerActor; i++)
            //    for (var j = 0; j < ActorCount; j++)
            //    {
            //        actors[j].Tell(new Store(1));
            //    }

            //var finished = new Task[ActorCount];
            //for (var i = 0; i < ActorCount; i++)
            //{
            //    finished[i] = actors[i].Ask<Finished>(Finish.Instance);
            //}

            //Task.WaitAll(finished);

            //var elapsed = stopwatch.ElapsedMilliseconds;

            //Console.WriteLine($"{ActorCount} actors stored {MessagesPerActor} events each in {elapsed / 1000.0} sec. Average: {ActorCount * MessagesPerActor * 1000.0 / elapsed} events/sec");

            //foreach (Task<Finished> task in finished)
            //{
            //    if (!task.IsCompleted || task.Result.State != MessagesPerActor)
            //        throw new IllegalStateException("Actor's state was invalid");
            //}
        }
    }

    public class DeviceEvent
    {
        /// <summary>
        /// As a rule of thumb, the number of shards should be a factor ten greater than the planned maximum number of cluster nodes.
        /// It can be changed after stopping all nodes in the cluster.
        /// </summary>
        private const int NumShards = 5;
        private const string TagName = "DeviceEvent";

        public static readonly EventTagShards Tag = new EventTagShards(TagName, NumShards);
    }

    internal class ShardedSwitchEntity : IExtension
    {
        private readonly ActorSystem system;

        private string typeName = "switch-processor"; // TODO: from settings
        private int shardCount = 20; // TODO: from settings

        public ShardedSwitchEntity(ExtendedActorSystem system)
        {
            this.system = system;
        }

        public static ShardedSwitchEntity Get(ActorSystem system)
        {
            return system.WithExtension<ShardedSwitchEntity, ShardedSwitchEntityProvider>();
        }

        public void Start()
        {
            ClusterSharding.Get(system).Start(
                typeName: typeName,
                entityProps: PerformanceTestActor.Props(),
                settings: ClusterShardingSettings.Create(system).WithRole("write-model"),
                extractEntityId: ExtractEntityId,
                extractShardId: ExtractShardId(shardCount));
        }

        public void Tell(string id, object message)
        {
            ClusterSharding.Get(system).ShardRegion(typeName).Tell(new EntityEnvelope(id, message), ActorRefs.NoSender);
        }

        private static readonly ExtractEntityId ExtractEntityId =
            message => message is EntityEnvelope env ? Tuple.Create(env.SwitchEntityId, env.Payload) : null;

        private static ExtractShardId ExtractShardId(int numberOfShards)
        {
            return message => message is EntityEnvelope env
                ? Math.Abs(MurmurHash.StringHash(env.SwitchEntityId) % numberOfShards).ToString()
                : null;
        }

        public class EntityEnvelope
        {
            public string SwitchEntityId { get; }
            public object Payload { get; }

            public EntityEnvelope(string switchEntityId, object payload)
            {
                SwitchEntityId = switchEntityId;
                Payload = payload;
            }
        }
    }

    internal class ShardedSwitchEntityProvider : ExtensionIdProvider<ShardedSwitchEntity>
    {
        public override ShardedSwitchEntity CreateExtension(ExtendedActorSystem system)
        {
            return new ShardedSwitchEntity(system);
        }
    }

    public class EventTag : IEquatable<EventTag>
    {
        public string Tag { get; }

        public EventTag(string tag)
        {
            Tag = tag;
        }

        /// <summary>
        /// Generate a shard tag according to the base tag name and the shard number.
        /// </summary>
        /// <param name="tag">The base tag name</param>
        /// <param name="shardNo">The shard number</param>
        public static string ShardTag(string tag, int shardNo) => $"{tag}-{shardNo}";

        /// <summary>
        /// Select a shard given the number of shards and the ID of the entity.
        /// </summary>
        /// <param name="entityId">The ID of the entity</param>
        /// <param name="numShards">The number of shards</param>
        public static int SelectShard(string entityId, int numShards) => Math.Abs(MurmurHash.StringHash(entityId) % numShards);

        public bool Equals(EventTag other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Tag, other.Tag, StringComparison.InvariantCulture);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == GetType() && Equals((EventTag)obj);
        }

        public override int GetHashCode()
        {
            return Tag != null ? Tag.GetHashCode() : 0;
        }

        public override string ToString() => $"EventTag({Tag})";
    }

    public class EventTagShards : IEquatable<EventTagShards>
    {
        public string Tag { get; }
        public int NumShards { get; }

        public EventTagShards(string tag, int numShards)
        {
            Tag = tag;
            NumShards = numShards;
        }

        /// <summary>
        /// Get the tag for the given entity ID
        /// </summary>
        /// <param name="entityId">The entity ID to get the tag for</param>
        public EventTag ForEntityId(string entityId)
        {
            return new EventTag(EventTag.ShardTag(Tag, EventTag.SelectShard(entityId, NumShards)));
        }

        /// <summary>
        /// Get all the tags for this shard
        /// </summary>
        public EventTag[] AllTags()
        {
            return Enumerable.Range(0, NumShards).Select(shardNo => new EventTag(EventTag.ShardTag(Tag, shardNo))).ToArray();
        }

        public bool Equals(EventTagShards other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(Tag, other.Tag, StringComparison.InvariantCulture) && NumShards == other.NumShards;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == GetType() && Equals((EventTagShards)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Tag != null ? Tag.GetHashCode() : 0) * 397) ^ NumShards;
            }
        }

        public override string ToString() => $"EventShards({Tag})";
    }
}
