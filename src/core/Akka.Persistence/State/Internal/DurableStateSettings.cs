//-----------------------------------------------------------------------
// <copyright file="DurableStateSettings.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Annotations;
using Akka.Configuration;

namespace Akka.Persistence.State.Internal
{
    [InternalApi]
    public sealed class DurableStateSettings
    {
        public static DurableStateSettings Create(ActorSystem system, string durableStateStorePluginId) =>
            Create(system.Settings.Config, durableStateStorePluginId);

        public static DurableStateSettings Create(Config config, string durableStateStorePluginId)
        {
            var typedConfig = config.GetConfig("akka.persistence"); // "akka.persistence.state"
            var stashOverflowStrategy = typedConfig.GetString("stash-overflow-strategy").ToLowerInvariant() switch
            {
                "drop" => Drop.Instance,
                "fail" => Fail.Instance,
                var unknown => throw new ArgumentException($"Unknown value for stash-overflow-strategy: [{unknown}]")
            };

            var stashCapacity = typedConfig.GetInt("stash-capacity");
            // TODO: require(stashCapacity > 0, "stash-capacity MUST be > 0, unbounded buffering is not supported.")

            var logOnStashing = typedConfig.GetBoolean("log-stashing");
            var durableStateStoreConfig = DurableStateStoreConfigFor(config, durableStateStorePluginId);
            var recoveryTimeout = durableStateStoreConfig.GetTimeSpan("recovery-timeout");

            var useContextLoggerForInternalLogging = typedConfig.GetBoolean("use-context-logger-for-internal-logging");

            return new DurableStateSettings(
                stashCapacity,
                stashOverflowStrategy,
                logOnStashing,
                recoveryTimeout,
                durableStateStorePluginId,
                useContextLoggerForInternalLogging);
        }

        private DurableStateSettings(int stashCapacity, IStashOverflowStrategy stashOverflowStrategy, bool logOnStashing, TimeSpan recoveryTimeout, string durableStateStorePluginId, bool useContextLoggerForInternalLogging)
        {
            if (durableStateStorePluginId == null)
                throw new ArgumentNullException("DurableStateActor plugin id must not be null; use empty string for 'default' state store");

            StashCapacity = stashCapacity;
            StashOverflowStrategy = stashOverflowStrategy;
            LogOnStashing = logOnStashing;
            RecoveryTimeout = recoveryTimeout;
            DurableStateStorePluginId = durableStateStorePluginId;
            UseContextLoggerForInternalLogging = useContextLoggerForInternalLogging;
        }

        private static Config DurableStateStoreConfigFor(Config config, string pluginId)
        {
            var defaultPluginId = config.GetString("akka.persistence.state.plugin");
            PersistenceExtensions.VerifyPluginConfigIsDefined(defaultPluginId, "Default DurableStateStore");

            var configPath = pluginId == "" ? defaultPluginId : pluginId;
            PersistenceExtensions.VerifyPluginConfigExists(config, configPath, "DurableStateStore");
            return config.GetConfig(configPath).WithFallback(config.GetConfig("akka.persistence.state-plugin-fallback"));
        }

        public int StashCapacity { get; }
        public IStashOverflowStrategy StashOverflowStrategy { get; }
        public bool LogOnStashing { get; }
        public TimeSpan RecoveryTimeout { get; }
        public string DurableStateStorePluginId { get; }
        public bool UseContextLoggerForInternalLogging { get; }
    }

    [InternalApi]
    public interface IStashOverflowStrategy
    { }

    [InternalApi]
    public sealed class Drop : IStashOverflowStrategy
    {
        public static IStashOverflowStrategy Instance { get; } = new Drop();
        private Drop() { }
    }

    [InternalApi]
    public sealed class Fail : IStashOverflowStrategy
    {
        public static IStashOverflowStrategy Instance { get; } = new Fail();
        private Fail() { }
    }
}
