using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Logger.Serilog;
using Akka.Persistence.Query;
using Akka.Persistence.Query.Sql;
using Akka.Streams;
using Akka.Streams.Supervision;
using Serilog;

namespace PersistenceBenchmark
{
    internal class DeviceEventTagProcessor : UntypedActor
    {
        private readonly string eventProcessorId = $"user/tags/{Context.Self.Path.Name}/processor";
        //private readonly ISessionFactory sessionFactory;
        //private UniqueKillSwitch killSwitch;

        //protected ILoggingAdapter Log { get; } = Context.GetLogger<SerilogLoggingAdapter>();

        //public static Props Props(ISessionFactory sessionFactory) =>
        //    Akka.Actor.Props.Create(() => new DeviceEventTagProcessor(sessionFactory));

        //public DeviceEventTagProcessor(ISessionFactory sessionFactory)
        //{
        //    this.sessionFactory = sessionFactory ?? throw new ArgumentNullException(nameof(sessionFactory));
        //}

        //protected override void PostStop()
        //{
        //    base.PostStop();
        //    killSwitch?.Shutdown();
        //}
        
        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case ClusterDistribution.EnsureActive active:
                    Self.Tell(Start.Instance);
                    Context.Become(Prepare(active.EntityId));
                    break;
            }
        }

        Receive Prepare(string tagName)
        {
            return message =>
            {
                switch (message)
                {
                    case Start _:
                        {
                            //var store = new DeviceManagementStore(sessionFactory);
                            //var settings = ActorMaterializerSettings.Create(Context.System).WithSupervisionStrategy(Deciders.ResumingDecider);

                            //var backoffSource = RestartSource.WithBackoff(() =>
                            //    {
                            //        Log.Info("Starting stream [{EventProcessorId}] for tag [{TagName}]...", eventProcessorId, tagName);

                            //        var offsetStore = new OffsetStore(eventProcessorId, tagName, sessionFactory);
                            //        var lastOffset = offsetStore.ReadOffset();

                            //        return PersistenceQuery.Get(Context.System)
                            //            .ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier)
                            //            .EventsByTag(tagName, lastOffset)
                            //            .SelectAsync(1, envelope => store.Save(envelope))
                            //            .SelectAsync(1, envelope => offsetStore.UpdateOffset(envelope.Offset));
                            //    },
                            //    minBackoff: TimeSpan.FromSeconds(3),
                            //    maxBackoff: TimeSpan.FromSeconds(30),
                            //    randomFactor: 0.2);

                            //var (uniqueKillSwitch, task) = backoffSource
                            //    .ViaMaterialized(KillSwitches.Single<Offset>(), Keep.Right)
                            //    .ToMaterialized(Sink.Ignore<Offset>(), Keep.Both)
                            //    .Run(Context.System.Materializer(settings));

                            //killSwitch = uniqueKillSwitch;
                            //task.PipeTo(Self, success: () => Done.Instance);

                            var settings = ActorMaterializerSettings.Create(Context.System).WithSupervisionStrategy(Deciders.ResumingDecider);
                            PersistenceQuery.Get(Context.System)
                                .ReadJournalFor<SqlReadJournal>(tagName.ToLower() /*SqlReadJournal.Identifier*/)
                                .EventsByTag(tagName, Offset.NoOffset())
                                .RunForeach(envelope =>
                                {
                                    Log.Information("Read event {EventType}", envelope.Event.GetType().FullName);
                                }, Context.System.Materializer(settings));
                        }
                        return true;
                    case Done _:
                        throw new InvalidOperationException("Stream terminated when it shouldn't");
                    case ClusterDistribution.EnsureActive _:
                        return true;
                }
                return false;
            };
        }

        private class Start
        {
            public static Start Instance { get; } = new Start();
            private Start() { }
        }
    }
}
