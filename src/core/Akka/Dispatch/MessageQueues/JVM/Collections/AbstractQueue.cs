using System;
using System.Collections.Generic;

namespace Akka.Util.Collections
{
    public abstract class AbstractQueue<E> : AbstractCollection<E>, IQueue<E> where E : class
    {
        public abstract bool Offer(E element);

        public abstract E Poll();

        public abstract E Peek();

        public override bool Add(E o)
        {
            if (null == o)
            {
                throw new NullReferenceException();
            }

            if (Offer(o))
            {
                return true;
            }

            throw new InvalidOperationException();
        }

        public override bool AddAll(ICollection<E> c)
        {
            if (null == c)
            {
                throw new NullReferenceException();
            }

            if (ReferenceEquals(this, c))
            {
                throw new ArgumentException();
            }

            return base.AddAll(c);
        }

        /// <summary>
        /// Remove and return the element at the head of the queue.
        /// </summary>
        public virtual E Remove()
        {
            var o = Poll();
            if (null == o)
            {
                throw new NoSuchElementException();
            }

            return o;
        }

        /// <summary>
        /// Returns but does not remove the element at the head of the queue.
        /// </summary>
        /// <exception cref='NoSuchElementException'>
        /// Is thrown if the queue is empty.
        /// </exception>
        public virtual E Element()
        {
            var o = Peek();
            if (null == o)
            {
                throw new NoSuchElementException();
            }
            return o;
        }

        /// <summary>
        /// Removes all elements of the queue, leaving it empty.
        /// </summary>
        public override void Clear()
        {
            E o;
            do
            {
                o = Poll();
            }
            while (null != o);
        }
    }
}
