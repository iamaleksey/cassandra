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

import java.io.EOFException;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.io.util.RebufferingInputStream;

class ChunkedInputPlus extends RebufferingInputStream
{
    private final PeekingIterator<SharedBytes> iter;

    private ChunkedInputPlus(PeekingIterator<SharedBytes> iter)
    {
        super(iter.peek().get());
        this.iter = iter;
    }

    static ChunkedInputPlus of(Iterable<SharedBytes> buffers)
    {
        PeekingIterator<SharedBytes> iter = Iterators.peekingIterator(buffers.iterator());
        if (!iter.hasNext())
            throw new IllegalArgumentException();
        return new ChunkedInputPlus(iter);
    }

    @Override
    protected void reBuffer() throws EOFException
    {
        buffer = null;
        iter.peek().release();
        iter.next();

        if (!iter.hasNext())
            throw new EOFException();

        buffer = iter.peek().get();
    }

    @Override
    public void close()
    {
        buffer = null;
        iter.forEachRemaining(SharedBytes::release);
    }

    /**
     * Returns the number of unconsumed bytes, destructively. Should only be used for sanity checking, once the input is no longer needed.
     */
    int remainder()
    {
        int bytes = 0;
        while (iter.hasNext())
        {
            bytes += iter.peek().readableBytes();
            iter.peek().release();
            iter.next();
        }
        return bytes;
    }
}
