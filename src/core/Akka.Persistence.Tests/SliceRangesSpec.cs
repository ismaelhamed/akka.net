//-----------------------------------------------------------------------
// <copyright file="OptimizedRecoverySpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace Akka.Persistence.Tests
{
    public class SliceRangesSpec : PersistenceSpec
    {
        private readonly PersistenceExtension _persistence;

        public SliceRangesSpec()
            : base(Configuration("SliceRangesSpec"))
        {
            _persistence = Persistence.Instance.Apply(Sys);
        }

        [Fact]
        public void Persistence_slices_must_have_fixed_numberOfSlices()
        {
            _persistence.NumberOfSlices.Should().Be(128);
        }

        [Fact]
        public void Persistence_slices_must_be_deterministic_from_persistence_id()
        {
            _persistence.SliceForPersistenceId("pid-1").Should().Be(78);
            _persistence.SliceForPersistenceId("pid-2").Should().Be(103);
            _persistence.SliceForPersistenceId("pid-6712").Should().Be(54);
        }

        [Fact]
        public void Persistence_slices_must_be_within_the_numberOfSlices()
        {
            var pid = $"pid-{Guid.NewGuid()}";
            var slice = _persistence.SliceForPersistenceId(pid);
            slice.Should().BeGreaterOrEqualTo(0);
            slice.Should().BeLessThan(_persistence.NumberOfSlices);
        }

        [Fact]
        public void Persistence_slices_must_create_ranges()
        {
            _persistence.GetSliceRanges(4).Should().BeEquivalentTo(
                new List<(int, int)>() { (0, 31), (32, 63), (64, 95), (96, 127) } );
            _persistence.GetSliceRanges(1).Should().BeEquivalentTo(
                new List<(int, int)>() { (0, 127) } );
        }
    }
}
