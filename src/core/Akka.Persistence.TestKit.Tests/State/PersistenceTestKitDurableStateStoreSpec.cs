//-----------------------------------------------------------------------
// <copyright file="PersistenceTestKitDurableStateStoreSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Akka.Configuration;
using Akka.Persistence.Query;
using Akka.Persistence.TestKit.State;
using Akka.Streams;
using Akka.Streams.TestKit;
using Akka.Util;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.TestKit.Tests.State
{
    public class PersistenceTestKitDurableStateStoreSpec : PersistenceTestKit
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

        private readonly IMaterializer _materializer;

        public PersistenceTestKitDurableStateStoreSpec(ITestOutputHelper output)
            : base(Configuration(), "PersistenceTestKitDurableStateStoreSpec", output)
        {
            _materializer = ActorMaterializer.Create(Sys);
        }

        [Fact]
        public void PersistenceTestKitDurableStateStore_must_find_individual_objects()
        {
            var stateStore = new PersistenceTestKitDurableStateStore(Sys);
            var record = new Record(1, "name-1");
            var tag = "tag-1";
            var persistenceId = "record-1";
            stateStore.UpsertObject(persistenceId, 1L, record, tag);
            var updated = stateStore.GetObject(persistenceId).Result;
            updated.Value.Should().Be(new Option<Record>());
            updated.Revision.Should().Be(1L);
        }

        //[Fact]
        public void PersistenceTestKitDurableStateStore_changes_query_must_find_tagged_state_changes_ordered_by_upsert()
        {
            var stateStore = new PersistenceTestKitDurableStateStore(Sys);
            var record = new Record(1, "name-1");
            var recordChange = new Record(1, "name-1");
            var tag = "tag-1";
            stateStore.UpsertObject("record-1", 1L, record, tag);
            var testSink = stateStore.Changes(tag, NoOffset.Instance)
                .RunWith(this.SinkProbe<DurableStateChange>(), _materializer);

            var firstStateChange = testSink.Request(1).ExpectNext();
            firstStateChange.Value.Should().Be(record);
            firstStateChange.Revision.Should().Be(1L);

            stateStore.UpsertObject("record-1", 2L, recordChange, tag);
            var secondStateChange = testSink.Request(1).ExpectNext();
            secondStateChange.Value.Should().Be(recordChange);
            secondStateChange.Revision.Should().Be(2L);
            secondStateChange.Offset.Should().BeAssignableTo<Sequence>()
                .Which.Value.Should().BeGreaterOrEqualTo(((Sequence)firstStateChange.Offset).Value);
        }

        //[Fact]
        public void PersistenceTestKitDurableStateStore_changes_query_must_find_tagged_current_state_changes_ordered_by_upsert()
        {

        }

        private class Record
        {
            public Record(int id, string name)
            {
                Id = id;
                Name = name;
            }

            public int Id { get; }
            public string Name { get; }
        }
    }
}
