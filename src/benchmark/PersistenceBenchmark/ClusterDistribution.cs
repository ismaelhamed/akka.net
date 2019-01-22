using System;
using System.Globalization;
using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Akka.Event;
using Akka.Util;
using Akka.Util.Internal;

namespace PersistenceBenchmark
{
    /// <summary>
    /// Settings for cluster distribution.
    /// </summary>
    public class ClusterDistributionSettings
    {
        /// <summary>
        /// The interval at which entities are ensured to be active.
        /// </summary>
        public TimeSpan EnsureActiveInterval { get; }
        
        public string Role { get; }

        public ClusterDistributionSettings(ActorSystem system)
            : this(system.Settings.Config.GetConfig("ecostruxure.persistence"))
        { }

        public ClusterDistributionSettings(Config config)
        {
            EnsureActiveInterval = config.GetTimeSpan("cluster.distribution.ensure-active-interval");
            Role = config.GetString("read-side.run-on-role");
        }

        public ClusterDistributionSettings(TimeSpan ensureActiveInterval, string role)
        {
            EnsureActiveInterval = ensureActiveInterval;
            Role = role;
        }

        public ClusterDistributionSettings Copy(TimeSpan? ensureActiveInterval = null, string role = null)
        {
            return new ClusterDistributionSettings(ensureActiveInterval ?? EnsureActiveInterval, role ?? Role);
        }
    }

    /// <summary>
    ///  Distributes a static list of entities evenly over a cluster, ensuring that they are all continuously active.
    ///
    ///  This uses cluster sharding underneath, using a scheduled tick sent to each entity as a mechanism to ensure they are active.
    ///
    ///  Entities are cluster sharding entities, so they can discover their ID by inspecting their name. Additionally,
    ///  entities should handle the <see cref="EnsureActive"/> message, typically they can do nothing in
    ///  response to it.
    /// </summary>
    public class ClusterDistribution : IExtension
    {
        /// <summary>
        /// The maximum number of shards we'll create to distribute the entities.
        /// </summary>
        private const int MaxShards = 1000;

        private readonly ActorSystem system;

        public ClusterDistribution(ExtendedActorSystem system)
        {
            this.system = system;
        }

        public static ClusterDistribution Get(ActorSystem system)
        {
            return system.WithExtension<ClusterDistribution, ClusterDistributionProvider>();
        }

        /// <summary>
        /// Start a cluster distribution
        /// </summary>
        public void Start(string typeName, Props entityProps, string[] entityIds, ClusterDistributionSettings settings)
        {
            Tuple<string, object> ExtractEntityId(object message)
            {
                if (message is EnsureActive msg) return new Tuple<string, object>(msg.EntityId, msg);
                return null;
            }

            string ExtractShardId(object message)
            {
                
                switch (message)
                {
                    case EnsureActive msg when entityIds.Length > MaxShards:
                        return Math.Abs(MurmurHash.StringHash(msg.EntityId) % 1000).ToString(CultureInfo.InvariantCulture);
                    case EnsureActive msg:
                        return msg.EntityId;
                    case ShardRegion.StartEntity msg:
                        return msg.EntityId;
                }
                return null;
            }

            var shardRegion = ClusterSharding.Get(system).Start(
                typeName: typeName,
                entityProps: entityProps,
                settings: ClusterShardingSettings.Create(system).WithRole(settings.Role),
                extractEntityId: ExtractEntityId,
                extractShardId: ExtractShardId);

            system.ActorOf(EnsureActiveActor.Props(entityIds, shardRegion, settings.EnsureActiveInterval), $"cluster-distribution-{typeName}");
        }

        public class EnsureActive
        {
            public string EntityId { get; }

            public EnsureActive(string entityId)
            {
                EntityId = entityId;
            }
        }
    }

    internal class ClusterDistributionProvider : ExtensionIdProvider<ClusterDistribution>
    {
        public override ClusterDistribution CreateExtension(ExtendedActorSystem system)
        {
            return new ClusterDistribution(system);
        }
    }

    internal class EnsureActiveActor : UntypedActor
    {
        private readonly string[] entityIds;
        private readonly IActorRef shardRegion;
        private readonly TimeSpan ensureActiveInterval;
        private ICancelable tick;

        public static Props Props(string[] entityIds, IActorRef shardRegion, TimeSpan ensureActiveInterval) =>
            Akka.Actor.Props.Create(() => new EnsureActiveActor(entityIds, shardRegion, ensureActiveInterval));

        public EnsureActiveActor(string[] entityIds, IActorRef shardRegion, TimeSpan ensureActiveInterval)
        {
            this.entityIds = entityIds;
            this.shardRegion = shardRegion;
            this.ensureActiveInterval = ensureActiveInterval;
        }

        protected override void PreStart()
        {
            base.PreStart();

            tick = Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(TimeSpan.Zero, ensureActiveInterval, Self, Tick.Instance, Self);
            Context.Watch(shardRegion);
        }

        protected override void PostStop()
        {
            base.PostStop();
            tick.CancelIfNotNull();
        }

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case Tick _:
                    entityIds.ForEach(id => shardRegion.Tell(new ClusterDistribution.EnsureActive(id)));
                    break;
                case Terminated terminated:
                    if (ReferenceEquals(terminated.ActorRef, shardRegion)) Context.Stop(Self);
                    break;
            }
        }

        private class Tick : IDeadLetterSuppression
        {
            public static Tick Instance { get; } = new Tick();
            private Tick() { }
        }
    }
}
