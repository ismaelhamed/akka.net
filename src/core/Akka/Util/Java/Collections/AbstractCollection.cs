using System;
using System.Text;

namespace Akka.Util.Collections
{
    public abstract class AbstractCollection<E> : ICollection<E>
    {
        public abstract int Size();

        public abstract IIterator<E> Iterator();

        public virtual bool Add(E item) => throw new NotSupportedException();

        public virtual bool AddAll(ICollection<E> collection)
        {
            bool result = false;
            IIterator<E> iterator = collection.Iterator();
            while (iterator.HasNext)
            {
                if (Add(iterator.Next()))
                {
                    result = true;
                }
            }
            return result;
        }

        public virtual void Clear()
        {
            IIterator<E> iterator = Iterator();
            while (iterator.HasNext)
            {
                iterator.Next();
                iterator.Remove();
            }
        }

        public virtual bool Contains(E element)
        {
            IIterator<E> it = Iterator();
            if (element != null)
            {
                while (it.HasNext)
                {
                    if (element.Equals(it.Next()))
                    {
                        return true;
                    }
                }
            }
            else
            {
                while (it.HasNext)
                {
                    if (it.Next() == null)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public virtual bool ContainsAll(ICollection<E> collection)
        {
            IIterator<E> it = collection.Iterator();
            while (it.HasNext)
            {
                if (!Contains(it.Next()))
                {
                    return false;
                }
            }
            return true;
        }

        public virtual bool IsEmpty()
        {
            return Size() == 0;
        }

        public virtual bool Remove(E element)
        {
            IIterator<E> it = Iterator();
            if (element != null)
            {
                while (it.HasNext)
                {
                    if (element.Equals(it.Next()))
                    {
                        it.Remove();
                        return true;
                    }
                }
            }
            else
            {
                while (it.HasNext)
                {
                    if (it.Next() == null)
                    {
                        it.Remove();
                        return true;
                    }
                }
            }
            return false;
        }

        public virtual bool RemoveAll(ICollection<E> collection)
        {
            bool result = false;
            IIterator<E> it = Iterator();
            while (it.HasNext)
            {
                if (collection.Contains(it.Next()))
                {
                    it.Remove();
                    result = true;
                }
            }
            return result;
        }

        public virtual bool RetainAll(ICollection<E> collection)
        {
            bool result = false;
            IIterator<E> it = Iterator();
            while (it.HasNext)
            {
                if (!collection.Contains(it.Next()))
                {
                    it.Remove();
                    result = true;
                }
            }
            return result;
        }

        public virtual E[] ToArray()
        {
            int size = Size();
            int index = 0;
            IIterator<E> it = Iterator();
            E[] array = new E[size];
            while (index < size)
            {
                array[index++] = it.Next();
            }
            return array;
        }

        /// <summary>
        /// Returns the string representation of this Collection. The presentation
        /// has a specific format. It is enclosed by square brackets ("[]"). Elements
        /// are separated by ', ' (comma and space).
        /// </summary>
        public override string ToString()
        {
            if (IsEmpty())
            {
                return "[]";
            }

            StringBuilder buffer = new StringBuilder(Size() * 16);
            buffer.Append('[');
            IIterator<E> it = Iterator();
            while (it.HasNext)
            {
                E next = it.Next();
                if (!ReferenceEquals(next, this))
                {
                    buffer.Append(next);
                }
                else
                {
                    buffer.Append("(this Collection)");
                }

                if (it.HasNext)
                {
                    buffer.Append(", ");
                }
            }
            buffer.Append(']');
            return buffer.ToString();
        }
    }
}
