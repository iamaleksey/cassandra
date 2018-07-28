/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net.async;

/*
 * A growing array-based queue that allows efficient bulk in-place removal.
 */
final class PrunableArrayQueue<E>
{
    public interface Pruner<E>
    {
        boolean shouldPrune(E e);
        void onPruned(E e);
        void onKept(E e);
    }

    private E[] buffer;
    private int capacity;

    private int head = 0;
    private int tail = 0;

    @SuppressWarnings("unchecked")
    PrunableArrayQueue(int requestedCapacity)
    {
        capacity = Math.max(8, findNextPositivePowerOfTwo(requestedCapacity));
        buffer = (E[]) new Object[capacity];
    }

    boolean offer(E e)
    {
        buffer[tail] = e;
        if ((tail = (tail + 1) & (capacity - 1)) == head)
            doubleCapacity();
        return true;
    }

    E peek()
    {
        return buffer[head];
    }

    E poll()
    {
        E result = buffer[head];
        if (null == result)
            return null;

        buffer[head] = null;
        head = (head + 1) & (capacity - 1);

        return result;
    }

    int size()
    {
        return (tail - head) & (capacity - 1);
    }

    boolean isEmpty()
    {
        return head == tail;
    }

    void prune(Pruner<E> pruner)
    {
        E e;
        int removed = 0;

        try
        {
            int size = size();
            for (int i = 0; i < size; i++)
            {
                // we start at the tail and work backwards to minimise the number of copies
                // as we expect to primarily prune from the front
                int k = (tail - 1 - i) & (capacity - 1);
                e = buffer[k];

                if (pruner.shouldPrune(e))
                {
                    buffer[k] = null;
                    removed++;
                    pruner.onPruned(e);
                }
                else
                {
                    if (removed > 0)
                    {
                        buffer[(k + removed) & (capacity - 1)] = e;
                        buffer[k] = null;
                    }
                    pruner.onKept(e);
                }
            }
        }
        finally
        {
            head = (head + removed) & (capacity - 1);
        }
    }

    @SuppressWarnings("unchecked")
    private void doubleCapacity()
    {
        assert head == tail;

        int newCapacity = capacity << 1;
        E[] newBuffer = (E[]) new Object[newCapacity];

        int headPortionLen = capacity - head;
        System.arraycopy(buffer, head, newBuffer, 0, headPortionLen);
        System.arraycopy(buffer, 0, newBuffer, headPortionLen, tail);

        head = 0;
        tail = capacity;

        capacity = newCapacity;
        buffer = newBuffer;
    }

    private static int findNextPositivePowerOfTwo(int value)
    {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }
}
