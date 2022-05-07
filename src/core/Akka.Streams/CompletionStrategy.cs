using System;
using Akka.Annotations;

namespace Akka.Streams
{
    public static class CompletionStrategy
    {
        [DoNotInherit]
        public interface ICompletionStrategy
        { }

        /// <summary>
        /// The completion will be signaled immediately even if elements are still buffered.
        /// </summary>
        [InternalApi, Serializable]
        public sealed class Immediately : ICompletionStrategy
        {
            public static readonly Immediately Instance = new Immediately();
            private Immediately() { }
        }

        /// <summary>
        /// Already buffered elements will be signaled before siganling completion.
        /// </summary>
        [InternalApi, Serializable]
        public sealed class Draining : ICompletionStrategy
        {
            public static readonly Draining Instance = new Draining();
            private Draining() { }
        }
    }
}
