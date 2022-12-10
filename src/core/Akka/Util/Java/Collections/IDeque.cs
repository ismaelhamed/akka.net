/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Akka.Util.Collections
{
    public interface IDeque<T> : IQueue<T>
    {
        void AddFirst(T entry);
        void AddLast(T entry);
        bool OfferFirst(T entry);
        bool OfferLast(T entry);
        T PollFirst();
        T PollLast();
        T TakeFirst();
        T TakeLast();
        T RemoveFirst();
        T RemoveLast();
        T GetFirst();
        T GetLast();
        T PeekFirst();
        T PeekLast();
        bool RemoveFirstOccurrence(T entry);
        bool RemoveLastOccurrence(T entry);
        void Push(T entry);
        T Pop();
    }
}
