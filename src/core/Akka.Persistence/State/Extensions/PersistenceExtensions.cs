//-----------------------------------------------------------------------
// <copyright file="PersistenceExtensions.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Annotations;
using Akka.Configuration;

namespace Akka.Persistence
{
    public static class PersistenceExtensions
    {
        /// <summary>
        /// Throws <see cref="ArgumentNullException"/> if config path for the <paramref name="pluginId"/> doesn't exist
        /// </summary>
        /// <param name="config">TBD</param>
        /// <param name="pluginId">TBD</param>
        /// <param name="pluginType">TBD</param>
        /// <exception cref="ArgumentNullException"></exception>
        [InternalApi]
        public static void VerifyPluginConfigExists(Config config, string pluginId, string pluginType)
        {
            if (!string.IsNullOrEmpty(pluginId) && !config.HasPath(pluginId))
                throw new ArgumentNullException($"{pluginType} plugin [{pluginId}] configuration doesn't exist.");
        }

        /// <summary>
        /// Throws <see cref="ArgumentNullException"/> if <paramref name="pluginId"/> is empty (undefined)
        /// </summary>
        /// <param name="pluginId">TBD</param>
        /// <param name="pluginType">TBD</param>
        /// <exception cref="ArgumentNullException"></exception>
        [InternalApi]
        public static void VerifyPluginConfigIsDefined(string pluginId, string pluginType)
        {
            if (string.IsNullOrEmpty(pluginId))
                throw new ArgumentNullException($"{pluginType} plugin is not configured, see 'reference.conf'");
        }
    }
}
