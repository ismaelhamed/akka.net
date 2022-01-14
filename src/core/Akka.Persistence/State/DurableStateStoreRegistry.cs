//-----------------------------------------------------------------------
// <copyright file="DurableStateStoreRegistry.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2021 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2021 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.State.Dsl;

namespace Akka.Persistence.State
{
    /// <summary>
    /// Persistence extension for durable state store.
    /// </summary>
    public class DurableStateStoreRegistry : IExtension
    {
        private readonly ExtendedActorSystem _system;
        private readonly ConcurrentDictionary<string, IDurableStateStore> _plugins = new ConcurrentDictionary<string, IDurableStateStore>();
        private ILoggingAdapter _log;

        public ILoggingAdapter Log => _log ??= _system.Log;

        public DurableStateStoreRegistry(ExtendedActorSystem system)
        {
            _system = system;
            _system.Settings.InjectTopLevelFallback(Persistence.DefaultConfig());
        }

        public static DurableStateStoreRegistry Get(ActorSystem system) => 
            system.WithExtension<DurableStateStoreRegistry, DurableStateStoreRegistryProvider>();

        /// <summary>
        /// The provided durableStateStorePluginConfig will be used to configure the journal plugin instead of the actor system config.
        /// </summary>
        /// <typeparam name="TStore">TBD</typeparam>
        /// <typeparam name="T">TBD</typeparam>
        /// <param name="durableStateStorePluginId">TBD</param>
        /// <returns>Returns the <see cref="IDurableStateStore{T}"/> specified by the given read journal configuration entry.</returns>
        public TStore DurableStateStoreFor<TStore, T>(string durableStateStorePluginId) where TStore : IDurableStateStore<T> =>
            DurableStateStoreFor<TStore, T>(durableStateStorePluginId, ConfigurationFactory.Empty);

        /// <summary>
        /// The provided durableStateStorePluginConfig will be used to configure the journal plugin instead of the actor system config.
        /// </summary>
        /// <typeparam name="TStore">TBD</typeparam>
        /// <typeparam name="T">TBD</typeparam>
        /// <param name="durableStateStorePluginId">TBD</param>
        /// <param name="durableStateStorePluginConfig">TBD</param>
        /// <returns>Returns the <see cref="IDurableStateStore{T}"/> specified by the given read journal configuration entry.</returns>
        public TStore DurableStateStoreFor<TStore, T>(string durableStateStorePluginId, Config durableStateStorePluginConfig) where TStore : IDurableStateStore<T> =>
            DurableStateStorePluginFor<TStore, T>(durableStateStorePluginId, durableStateStorePluginConfig);

        private TStore DurableStateStorePluginFor<TStore, T>(string pluginId, Config pluginConfig) where TStore : IDurableStateStore<T>
        {
            var plugin = _plugins.GetOrAdd(pluginId, path => CreatePlugin(path, pluginConfig).GetDurableStateStore<T>());
            return (TStore)plugin;
        }

        private IDurableStateStoreProvider CreatePlugin(string configPath, Config durableStateStorePluginConfig)
        {
            static IDurableStateStoreProvider CreateType(Type pluginType, object[] parameters)
            {
                var ctor = pluginType.GetConstructor(new[] { typeof(ExtendedActorSystem), typeof(Config) });
                if (ctor != null) return (IDurableStateStoreProvider)ctor.Invoke(parameters);

                ctor = pluginType.GetConstructor(new[] { typeof(ExtendedActorSystem) });
                if (ctor != null) return (IDurableStateStoreProvider)ctor.Invoke(new[] { parameters[0] });

                ctor = pluginType.GetConstructor(Type.EmptyTypes);
                if (ctor != null) return (IDurableStateStoreProvider)ctor.Invoke(Array.Empty<object>());

                throw new ArgumentException($"Unable to create durable state store plugin instance type {pluginType}!");
            }

            var mergedConfig = durableStateStorePluginConfig.WithFallback(_system.Settings.Config);

            if (string.IsNullOrEmpty(configPath) || !mergedConfig.HasPath(configPath))
                throw new ArgumentException($"'reference.conf' is missing durable state store plugin config path: '{configPath}'");

            var pluginConfig = mergedConfig.GetConfig(configPath);
            var pluginTypeName = pluginConfig.GetString("class");
            Log.Debug("Create plugin: {0}, {1}", configPath, pluginTypeName);
            var pluginType = Type.GetType(pluginTypeName, true);

            return CreateType(pluginType, new object[] { _system, pluginConfig });
        }
    }

    public class DurableStateStoreRegistryProvider : ExtensionIdProvider<DurableStateStoreRegistry>
    {
        public override DurableStateStoreRegistry CreateExtension(ExtendedActorSystem system) => new DurableStateStoreRegistry(system);
    }
}
