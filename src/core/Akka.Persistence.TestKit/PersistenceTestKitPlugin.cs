//-----------------------------------------------------------------------
// <copyright file="PersistenceTestKitPlugin.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using Akka.Configuration;

namespace Akka.Persistence.TestKit
{
    public static class PersistenceTestKitPlugin
    {
        public const string PluginId = "akka.persistence.testkit.journal";
        public static Config Config => null; // TODO: ConfigFactory.parseMap(Map("akka.persistence.state.plugin" -> PluginId))

        // TODO: https://github.com/akka/akka/commit/c4903ebe1e5fe0ce50522f5624170ad9cbbe79e8#diff-a9c43087ab0cea74b9c41dd8aca6f663d842da0fa84da34e42d284103b31116d
    }
}
