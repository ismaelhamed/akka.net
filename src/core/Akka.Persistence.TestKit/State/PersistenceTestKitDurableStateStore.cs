//-----------------------------------------------------------------------
// <copyright file="PersistenceTestKitDurableStateStore.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Query;
using Akka.Persistence.State.Dsl;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Internal;

namespace Akka.Persistence.TestKit.State
{
    public class PersistenceTestKitDurableStateStore : IDurableStateUpdateStore, IDurableStateStoreQuery
    {
        private readonly ActorSystem _system;
        private readonly Dictionary<string, Record> _store = new Dictionary<string, Record>();
        private readonly ValueTuple<IActorRef, Source<Record, NotUsed>> _valueTuple;

        private const long EarliestOffset = 0L;
        private readonly AtomicCounterLong _lastGlobalOffset = new AtomicCounterLong(EarliestOffset);

        public PersistenceTestKitDurableStateStore(ActorSystem system)
        {
            _system = system;
            _valueTuple = Source.ActorRef<Record>(256, OverflowStrategy.DropHead)
                .ToMaterialized(BroadcastHub.Sink<Record>(), Keep.Both)
                .Run(system.Materializer());
        }

        public Task<GetObjectResult> GetObject(string persistenceId)
        {
            _store.TryGetValue(persistenceId, out var result);
            return Task.FromResult(new GetObjectResult(result.Value, 0L));
        }

        public Task<Done> UpsertObject(string persistenceId, long revision, object value, string tag = null)
        {
            var globalOffset = _lastGlobalOffset.IncrementAndGet();
            var record = new Record(globalOffset, persistenceId, revision, value, tag);
            _store.AddOrSet(persistenceId, record);
            _valueTuple.Item1.Tell(record);
            return Task.FromResult(Done.Instance);
        }

        public Task<Done> DeleteObject(string persistenceId)
        {
            _store.Remove(persistenceId);
            return Task.FromResult(Done.Instance);
        }

        public Source<DurableStateChange, NotUsed> Changes(string tag, Offset offset)
        {
            throw new NotImplementedException();

            //var fromOffset = offset switch
            //{
            //    NoOffset _ => EarliestOffset,
            //    Sequence sequence => sequence.Value,
            //    _ => throw new InvalidOperationException($"{offset} not supported in PersistenceTestKitDurableStateStore.")
            //};

            //bool ByTagFromOffset(Record rec) => rec.Tag == tag && rec.GlobalOffset > fromOffset;
            //bool ByTagFromOffsetNotDeleted(Record rec) => ByTagFromOffset(rec) && _store.ContainsKey(rec.PersistenceId);

            //return Source.From(_store.Values.Where(ByTagFromOffset).OrderBy(rec => rec.GlobalOffset))
            //    .Concat(_valueTuple.Item2)
            //    .Where(ByTagFromOffsetNotDeleted)
            //    // TODO: StatefulSelectManyConcat
            //    .StatefulSelectMany<Record, Record, NotUsed>(() =>
            //    {
            //        var globalOffsetSeen = EarliestOffset;
            //        return record =>
            //        {
            //            if (record.GlobalOffset > globalOffsetSeen)
            //            {
            //                globalOffsetSeen = record.GlobalOffset;
            //                return record;
            //            }
            //            else return null;
            //        };
            //    })
            //    .Select(rec => rec.ToDurableStateChange());
        }

        public Source<DurableStateChange, NotUsed> CurrentChanges(string tag, Offset offset)
        {
            var currentGlobalOffset = _lastGlobalOffset.Current;
            return Changes(tag, offset).TakeWhile(change =>
            {
                return change.Offset switch
                {
                    Sequence sequence => sequence.Value <= currentGlobalOffset,
                    _ => throw new InvalidOperationException($"{change.Offset} not supported in PersistenceTestKitDurableStateStore.")
                };
            });
        }
    }

    public class Record
    {
        public Record(long globalOffset, string persistenceId, long revision, Option<object> value, string tag, long? timestamp = null)
        {
            GlobalOffset = globalOffset;
            PersistenceId = persistenceId;
            Revision = revision;
            Value = value;
            Tag = tag;
            Timestamp = timestamp ?? DateTime.UtcNow.Ticks;
        }

        public long GlobalOffset { get; }
        public string PersistenceId { get; }
        public long Revision { get; }
        public Option<object> Value { get; }
        public string Tag { get; }
        public long Timestamp { get; }

        public DurableStateChange ToDurableStateChange() =>
            new DurableStateChange(PersistenceId, Revision, Value, new Sequence(GlobalOffset), Timestamp);
    }
}