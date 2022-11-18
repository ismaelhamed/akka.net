//-----------------------------------------------------------------------
// <copyright file="IWithTimers.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

namespace Akka.Actor
{
    /// <summary>
    /// Marker interface for adding Timers support
    /// </summary>
    public interface IWithTimers
    {
        /// <summary>
        /// Start and cancel timers via the enclosed  <see cref="TimerScheduler"/>. 
        /// <para>
        /// This will be automatically populated by the framework in base constructor.
        /// Implement this as an auto property.
        /// </para>
        /// </summary>
        ITimerScheduler Timers { get; set; }
    }
}
